import threading
import time
from logging import getLogger, NullHandler

import requests
from hyper import HTTPConnection, HTTP20Connection
from yarl import URL

from .algorithm import DelayRequestAlgorithm
from .exception import FileSizeError
from .utils import match_all, CustomDeque, SphttpFlowControlManager

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())

DEFAULT_SPLIT_SIZE = 10 ** 6
DEFAULT_SLEEP_SEC = 0.01


class MultiHTTPDownloader(object):
    def __init__(self,
                 urls,
                 *,
                 split_size=DEFAULT_SPLIT_SIZE,
                 sleep_sec=DEFAULT_SLEEP_SEC,
                 enable_trace_log=False,
                 verify=True,
                 delay_request_algorithm=DelayRequestAlgorithm.DIFFERENCES,
                 multi_stream_setting=None,
                 logger=local_logger, ):

        if multi_stream_setting is None:
            multi_stream_setting = {}
        self._urls = [URL(url) for url in urls]
        self._num_of_host = len(self._urls)
        self._split_size = split_size
        self._sleep_sec = sleep_sec
        self._verify = verify
        self._delay_request_algorithm = delay_request_algorithm
        self._logger = logger

        length = [int(requests.head(url, verify=self._verify).headers['content-length']) for url in urls]

        if match_all(length) is False:
            message = 'File size differs for each host.'
            raise FileSizeError(message)

        self.length = length[0]

        self._sessions = []
        self._multi_stream_setting = []
        for i, url in enumerate(urls):
            try:
                num_of_stream = multi_stream_setting[url]
            except KeyError:
                num_of_stream = 0
                conn = HTTPConnection
            else:
                conn = HTTP20Connection
            finally:
                url = self._urls[i]
                self._multi_stream_setting.append(num_of_stream)
                self._sessions.append(conn(host='{}:{}'.format(url.host, url.port),
                                           window_manager=SphttpFlowControlManager,
                                           verify=self._verify))

        self._num_of_request = self.length // self._split_size
        reminder = self.length % self._split_size

        self._params = CustomDeque()
        begin = 0

        for i in range(self._num_of_request):
            end = begin + self._split_size - 1
            self._params.append({'block_num': i,
                                 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})
            begin += self._split_size

        if reminder:
            end = begin + reminder - 1
            self._params.append({'block_num': self._num_of_request,
                                 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})
            self._num_of_request += 1

        self._threads = [threading.Thread(target=self._download, args=(i,)) for i in range(self._num_of_host)]
        self._buffer = [None] * self._num_of_request
        self._is_started = False
        self._read_index = 0

        self._receive_count = 0
        self._host_usage_count = [0] * self._num_of_host
        self._previous_read_count = [0] * self._num_of_host

        self._enable_trace_log = enable_trace_log
        self._begin_time = None
        if self._enable_trace_log:
            self._receive_log = []
            self._send_log = []

        self._logger.debug('Init')

    def start(self):
        if self._is_started is False:
            self._is_started = True

            self._begin_time = time.monotonic()

            for thread in self._threads:
                thread.start()

    def close(self):
        for sess in self._sessions:
            sess.close()

    def generator(self):
        if self._is_started is False:
            self.start()

        while self._is_completed() is False:
            yield self._concatenate_buffer()

    def get_trace_log(self):
        if self._enable_trace_log:
            return self._send_log, self._receive_log

    def _get_block_number(self, range_header):
        tmp = range_header.split(' ')
        tmp = tmp[1].split('/')
        tmp = tmp[0].split('-')
        block_number = int(tmp[0]) // self._split_size

        return block_number

    def _get_request_pos(self, url_id):
        if self._delay_request_algorithm is DelayRequestAlgorithm.NORMAL:
            return 0
        elif self._delay_request_algorithm is DelayRequestAlgorithm.DIFFERENCES:
            return self._measure_diff(url_id)
        elif self._delay_request_algorithm is DelayRequestAlgorithm.INVERSE:
            return self._calc_inverse(url_id)

    def _measure_diff(self, url_id):
        previous = self._previous_read_count[url_id]
        current = self._receive_count
        self._previous_read_count[url_id] = current

        diff = current - previous - (self._num_of_host - 1)
        self._logger.debug('Diff: thread_name={}, diff={}'.format(threading.currentThread().getName(), diff))

        if diff < 0:
            pos = 0
        else:
            pos = diff

        return pos

    def _calc_inverse(self, url_id):
        try:
            ratio = self._host_usage_count[url_id] / max(self._host_usage_count)
        except ZeroDivisionError:
            ratio = 1

        try:
            pos = int((1 / ratio) - 1)
        except ZeroDivisionError:
            pos = self._num_of_request - self._read_index

        if pos > self._num_of_request - self._read_index:
            pos = self._num_of_request - self._read_index

        return pos

    def _get_param(self, url_id):
        pos = min(self._get_request_pos(url_id), len(self._params) - 1)
        param = self._params.custom_pop(pos)

        return param

    def _send_request(self, url_id):
        sess = self._sessions[url_id]
        url = self._urls[url_id]
        param = self._get_param(url_id)
        thread_name = threading.currentThread().getName()

        stream_id = sess.request('GET', url.path, headers=param['headers'])
        self._logger.debug('Send request: thread_name={}, block_num={}, time={}'
                           .format(thread_name, param['block_num'], self._current_time()))

        if self._enable_trace_log:
            self._send_log.append((self._current_time(), param['block_num']))

        return stream_id

    def _receive_response(self, url_id, stream_id):
        sess = self._sessions[url_id]
        thread_name = threading.currentThread().getName()

        if stream_id is None:
            resp = sess.get_response()
        else:
            resp = sess.get_response(stream_id)

        self._host_usage_count[url_id] += 1
        self._receive_count += 1

        block_num = self._get_block_number(resp.headers[b'content-range'][0].decode())
        self._logger.debug('Receive response: thread_name={}, block_num={}, time={}'
                           .format(thread_name, block_num, self._current_time()))

        self._buffer[block_num] = resp.read()
        self._logger.debug('Get chunk: thread_name={}, block_num={}, time={}, stream_id={}'
                           .format(thread_name, block_num, self._current_time(), stream_id))

        if self._enable_trace_log:
            self._receive_log.append((self._current_time(), block_num))

    def _download(self, url_id):

        n = self._multi_stream_setting[url_id]

        while len(self._params):
            stream_ids = []
            if n != 0:
                for i in range(n):
                    if len(self._params) == 0:
                        break

                    stream_ids.append(self._send_request(url_id))
            else:
                stream_ids.append(self._send_request(url_id))

            for stream_id in stream_ids:
                self._receive_response(url_id, stream_id)

    def _concatenate_buffer(self):
        concatenated = bytearray()
        i = self._read_index
        length = len(self._buffer)

        while i < length:
            if self._buffer[i] is None:
                break
            else:
                concatenated += self._buffer[i]
                self._buffer[i] = None
            i += 1

        self._read_index = i

        if len(concatenated) == 0:
            time.sleep(self._sleep_sec)
            return self._concatenate_buffer()

        return concatenated

    def _current_time(self):
        return time.monotonic() - self._begin_time

    def _is_completed(self):
        if self._read_index == self._num_of_request:
            return True
        else:
            return False

    def __iter__(self):
        return self

    def __next__(self):
        if self._is_completed():
            raise StopIteration

        if self._is_started is False:
            self.start()

        return self._concatenate_buffer()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

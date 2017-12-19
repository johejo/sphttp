import threading
import time
from logging import getLogger, NullHandler

import requests
from hyper import HTTPConnection, HTTP20Connection
from yarl import URL

from .algorithm import DelayRequestAlgorithm
from .exception import FileSizeError, ParameterPositionError
from .utils import match_all, CustomDeque, SphttpFlowControlManager

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())

DEFAULT_SPLIT_SIZE = 10 ** 6
DEFAULT_SLEEP_SEC = 0.05


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
                 multi_connection_setting=None,
                 logger=local_logger, ):

        if multi_stream_setting is None:
            multi_stream_setting = {}
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

        self._urls = {}
        self._conns = {}
        self._multi_stream_setting = {}
        con_id = 0
        self._parallel_num = 0

        for i, url in enumerate(urls):
            try:
                num_of_connection = multi_connection_setting[url]
            except (KeyError, TypeError):
                num_of_connection = 1

            for _ in range(num_of_connection):
                try:
                    num_of_stream = multi_stream_setting[url]
                except (KeyError, TypeError):
                    num_of_stream = 1
                    conn = HTTPConnection
                else:
                    conn = HTTP20Connection
                finally:
                    self._multi_stream_setting[con_id] = num_of_stream
                    self._urls[con_id] = URL(url)
                    self._conns[con_id] = conn(host='{}:{}'.format(self._urls[con_id].host, self._urls[con_id].port),
                                               window_manager=SphttpFlowControlManager,
                                               verify=self._verify)
                    con_id += 1
                    self._parallel_num += num_of_stream

        self._num_of_conn = len(self._conns)

        self._num_of_request = self.length // self._split_size
        reminder = self.length % self._split_size

        self._params = CustomDeque()
        begin = 0

        for block_num in range(self._num_of_request):
            end = begin + self._split_size - 1
            self._params.append({'block_num': block_num,
                                 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})
            begin += self._split_size

        if reminder:
            end = begin + reminder - 1
            self._params.append({'block_num': self._num_of_request,
                                 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})
            self._num_of_request += 1

        self._threads = [threading.Thread(target=self._download,
                                          args=(con_id,),
                                          name=str(con_id)) for con_id in range(self._num_of_conn)]
        self._buffer = [None] * self._num_of_request
        self._is_started = False
        self._read_index = 0

        self._receive_count = 0
        self._host_usage_count = [0] * self._num_of_conn
        self._previous_receive_count = [0] * self._num_of_conn

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
        for conn in self._conns:
            conn.close()

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

    def _get_request_pos(self, conn_id):
        if self._delay_request_algorithm is DelayRequestAlgorithm.NORMAL:
            return 0
        elif self._delay_request_algorithm is DelayRequestAlgorithm.DIFFERENCES:
            return self._measure_diff(conn_id)
        elif self._delay_request_algorithm is DelayRequestAlgorithm.INVERSE:
            return self._calc_inverse(conn_id)

    def _measure_diff(self, conn_id):
        previous = self._previous_receive_count[conn_id]
        current = self._receive_count
        diff = current - previous - self._parallel_num - self._multi_stream_setting[conn_id] - 1
        self._logger.debug('Diff: thread_name={}, diff={}'.format(threading.currentThread().getName(), diff))

        pos = max(0, diff)

        return pos

    def _calc_inverse(self, conn_id):
        try:
            ratio = self._host_usage_count[conn_id] / max(self._host_usage_count)
        except ZeroDivisionError:
            ratio = 1

        try:
            pos = int((1 / ratio) - 1)
        except ZeroDivisionError:
            pos = self._num_of_request - self._read_index

        pos = min(self._num_of_request - self._read_index, pos)

        return pos

    def _get_param(self, conn_id):
        pos = self._get_request_pos(conn_id)
        remain = len(self._params) - 1
        if pos * 0.9 > remain:
            raise ParameterPositionError
        else:
            pos = min(pos, remain)

        param = self._params.custom_pop(pos)

        return param

    def _send_request(self, conn_id):
        conn = self._conns[conn_id]
        try:
            param = self._get_param(conn_id)
        except ParameterPositionError:
            raise

        thread_name = threading.currentThread().getName()
        url = self._urls[conn_id]

        stream_id = conn.request('GET', url.path, headers=param['headers'])
        self._logger.debug('Send request: thread_name={}, block_num={}, time={}'
                           .format(thread_name, param['block_num'], self._current_time()))

        if self._enable_trace_log:
            self._send_log.append((self._current_time(), param['block_num']))

        return stream_id

    def _receive_response(self, conn_id, stream_id):
        conn = self._conns[conn_id]
        thread_name = threading.currentThread().getName()

        if stream_id is None:
            resp = conn.get_response()
        else:
            resp = conn.get_response(stream_id)

        block_num = self._get_block_number(resp.headers[b'content-range'][0].decode())
        # self._logger.debug('Receive response: thread_name={}, block_num={}, time={}'
        #                    .format(thread_name, block_num, self._current_time()))

        self._buffer[block_num] = resp.read()
        self._logger.debug('Get chunk: thread_name={}, block_num={}, time={}, stream_id={}'
                           .format(thread_name, block_num, self._current_time(), stream_id))

        self._host_usage_count[conn_id] += 1
        self._receive_count += 1

        if self._enable_trace_log:
            self._receive_log.append((self._current_time(), block_num))

    def _download(self, conn_id):

        n = self._multi_stream_setting[conn_id]

        while len(self._params):
            stream_ids = []
            for i in range(n):
                if len(self._params) == 0:
                    break
                try:
                    stream_ids.append(self._send_request(conn_id))
                except ParameterPositionError:
                    return
            self._previous_receive_count[conn_id] = self._receive_count

            for stream_id in stream_ids:
                self._receive_response(conn_id, stream_id)

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

import threading
import time
from logging import getLogger, NullHandler

import requests

from .exception import FileSizeError
from .utils import match_all, CustomDeque
from sphttp.algorithm import DelayRequestAlgorithm

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())

DEFAULT_SPLIT_SIZE = 10**6
DEFAULT_SLEEP_SEC = 0.1


class MultiHTTPDownloader(object):
    def __init__(self,
                 urls,
                 *,
                 split_size=DEFAULT_SPLIT_SIZE,
                 sleep_sec=DEFAULT_SLEEP_SEC,
                 enable_trace_log=False,
                 delay_request_algorithm=DelayRequestAlgorithm.NORMAL,
                 logger=local_logger,
                 ):

        self._urls = urls
        self._num_of_host = len(self._urls)
        self._split_size = split_size
        self._sleep_sec = sleep_sec
        self._delay_request_algorithm = delay_request_algorithm
        self._logger = logger

        self._sessions = [requests.Session() for _ in self._urls]
        length = [int(sess.head(url).headers['content-length']) for url, sess in zip(self._urls, self._sessions)]

        if match_all(length) is False:
            message = 'File size differs for each host.'
            raise FileSizeError(message)

        self.length = length[0]
        self._num_of_request = self.length // self._split_size
        reminder = self.length % self._split_size

        if reminder != 0:
            self._num_of_request += 1

        self._params = CustomDeque()
        begin = 0

        for i in range(self._num_of_request - 1):
            end = begin + self._split_size - 1
            self._params.append({'block_num': i,
                                 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})
            begin += self._split_size

        if reminder != 0:
            end = begin + reminder - 1
            self._params.append({'block_num': self._num_of_request,
                                 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})
        else:
            end = begin + self._split_size - 1
            self._params.append({'block_num': self._num_of_request,
                                 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})

        self._threads = [threading.Thread(target=self._download, args=(i,)) for i in range(self._num_of_host)]
        self._buffer = [None for _ in range(self._num_of_request)]
        self._is_started = False
        self._read_index = 0

        self._receive_count = 0
        self._host_usage_count = [0 for _ in range(self._num_of_host)]
        self._previous_read_count = [0 for _ in range(self._num_of_host)]

        self._enable_trace_log = enable_trace_log
        if self._enable_trace_log:
            self._begin_time = None
            self._trace_log = []

        self._logger.debug('Init')

    def start(self):
        if self._is_started is False:
            self._is_started = True

            if self._enable_trace_log:
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
            return self._trace_log

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

        diff = current - previous - self._num_of_host
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

    def _download(self, url_id):
        sess = self._sessions[url_id]
        url = self._urls[url_id]
        thread_name = threading.currentThread().getName()

        while len(self._params):
            pos = self._get_request_pos(url_id)
            if pos > len(self._params) - 1:
                pos = len(self._params) - 1

            param = self._params.custom_pop(pos)
            self._logger.debug('Send request: thread_name={}, block_num={}'.format(thread_name, param['block_num']))
            resp = sess.get(url, headers=param['headers'])

            self._host_usage_count[url_id] += 1
            self._receive_count += 1

            block_num = self._get_block_number(resp.headers['content-range'])
            self._logger.debug('Receive response: thread_name={}, block_num={}'.format(thread_name, block_num))
            self._buffer[block_num] = resp.content

            if self._enable_trace_log:
                self._trace_log.append((time.monotonic() - self._begin_time, block_num))

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

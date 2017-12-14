import threading
import time
from logging import getLogger, NullHandler

import requests

from .exception import FileSizeError
from .utils import match_all, CustomDeque, get_index
from .sphttp import DEFAULT_SPLIT_SIZE

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())


class MultiHTTPDownloader(object):
    def __init__(self,
                 urls,
                 *,
                 split_size=DEFAULT_SPLIT_SIZE,
                 logger=local_logger,
                 ):

        self._urls = urls
        self._split_size = split_size
        self._logger = logger

        self._sessions = [requests.Session() for _ in self._urls]
        length = [int(sess.head(url).headers['content-length']) for url, sess in zip(self._urls, self._sessions)]

        if match_all(length) is False:
            message = 'File size differs for each host.'
            raise FileSizeError(message)
        self.length = length[0]

        self._request_num = self.length // self._split_size

        reminder = self.length % self._split_size
        if reminder != 0:
            self._request_num += 1

        self._params = CustomDeque()
        begin = 0
        for i in range(self._request_num):
            if reminder != 0 and i == self._request_num - 1:
                end = begin + reminder - 1
            else:
                end = begin + self._split_size - 1
            self._params.append({'index': i, 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})
            begin += self._split_size

        self._threads = [threading.Thread(target=self._get_data, args=(i,), name=str(i)) for i in range(len(self._urls))]
        self._buffer = [None for _ in range(self._request_num)]
        self._is_started = False
        self._is_completed = False
        self._read_index = 0
        # self._host_usage_count = [i for i in range(len(self.))]

        self._logger.debug('Init')

    def start(self):
        if self._is_started is False:
            self._is_started = True
            for thread in self._threads:
                thread.start()

    def close(self):
        for sess in self._sessions:
            sess.close()

    def generator(self):
        if self._is_started is False:
            self.start()
        while self._is_completed is False:
            yield self._concatenate_buffer()

    def _get_data(self, i):
        sess = self._sessions[i]
        url = self._urls[i]
        thread_name = threading.currentThread().getName()
        while len(self._params):
            pos = 0
            if pos > len(self._params):
                pos = len(self._params)
            param = self._params.custom_pop(pos)

            self._logger.debug('Send request: thread_name={}, index={}'.format(thread_name, param['index']))
            resp = sess.get(url, headers=param['headers'])
            index = get_index(resp.headers['content-range'], self._split_size)
            self._logger.debug('Receive response: thread_name={}, index={}'.format(thread_name, index))
            self._buffer[index] = resp.content

        self._is_completed = True

    def _concatenate_buffer(self):
        concatenated = bytearray()
        i = self._read_index
        while i < len(self._buffer):
            if self._buffer[i] is None:
                break
            else:
                concatenated += self._buffer[i]
                self._buffer[i] = bytearray()
            i += 1

        self._read_index = i
        if len(concatenated) == 0:
            time.sleep(0.1)
            return self._concatenate_buffer()

        return concatenated

    def __iter__(self):
        return self
    
    def __next__(self):
        if self._is_completed:
            raise StopIteration

        if self._is_started is False:
            self.start()

        return self._concatenate_buffer()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

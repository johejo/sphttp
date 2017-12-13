import time
import gc
import math
import random
from queue import Queue
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from urllib.parse import urlparse
from logging import getLogger, NullHandler

import requests

from .utils import get_length, map_all, get_order, get_port
from .algorithm import DelayRequestAlgorithm
from .exception import (
    FileSizeError, InvalidStatusCode, IncompleteError, DelayRequestAlgorithmError
)

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())

DEFAULT_SPLIT_SIZE = 10 ** 6


class SplitHTTP11Downloader(object):
    def __init__(self, urls, *,
                 verify=True,
                 split_size=DEFAULT_SPLIT_SIZE,
                 delay_request_algorithm=DelayRequestAlgorithm.NORMAL,
                 logger=local_logger,
                 ):

        self._logger = logger

        length_list = []
        for i, u in enumerate(urls):
            url, length = get_length(target_url=u, verify=verify)
            if u != url:
                urls[i] = url
            length_list.append(length)

        if map_all(length_list) is False:
            message = 'File size differs for each host.'
            raise FileSizeError(message)

        self.length = length_list[0]

        self._host_ids = [i for i in range(len(urls))]
        self._hosts = {host_id: (urlparse(url).hostname, get_port(url)) for host_id, url in zip(self._host_ids, urls)}
        self._host_http2_flags = {host_id: False for host_id in self._host_ids}

        self._split_size = split_size
        self._request_num = self.length // self._split_size

        self._params = []
        self._set_params()

        self._urls = {host_id: url for host_id, url in zip(self._host_ids, urls)}

        self._sessions = {}
        for host_id in self._hosts.keys():
            sess = requests.Session()
            self._sessions[host_id] = sess

        self._host_id_queue = Queue()
        for host_id in self._host_ids:
            self._host_id_queue.put(host_id)

        self._max_workers = len(self._hosts)

        random.shuffle(self._host_id_queue.queue)

        self._delay_request_algorithm = delay_request_algorithm
        if self._delay_request_algorithm is DelayRequestAlgorithm.ESTIMATE_DIFFERENCES:
            self._previous_counts = Counter()

        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self._future_body = None

        self._data = [None for _ in range(self._request_num)]
        self._received_index = 0
        self._receive_count = 0

        self._host_counts = {host_id: 0 for host_id in self._host_ids}
        self._log = []

        self._is_completed = False
        self._is_started = False

        self._ratio = []
        self._degree = []

        self._index = []
        self._send_time = []

        self._begin_time = None

        self._logger.debug('Init')

    def get_trace_data(self):
        if self._is_completed:
            return self._hosts, self._host_counts, self._log
        else:
            message = 'Cannot return trace data before the download is completed'
            raise IncompleteError(message)

    def get_degree_data(self):
        return self._ratio, self._degree

    def get_send_time(self):
        return self._index, self._send_time

    def start(self):
        self._begin_time = time.monotonic()
        self._future_body = [self._executor.submit(self._get_body) for _ in range(self._request_num)]
        self._is_started = True

    def _close(self):
        self._executor.shutdown()

    def _set_params(self):

        reminder = self.length % self._split_size
        if reminder != 0:
            self._request_num += 1

        begin = 0
        for i in range(self._request_num):
            if reminder != 0 and i == self._request_num - 1:
                end = begin + reminder - 1
            else:
                end = begin + self._split_size - 1
            self._params.append({'index': i, 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})
            begin += self._split_size

    def _get_delay_request_degree(self, host_id):
        if self._delay_request_algorithm is DelayRequestAlgorithm.NORMAL:
            return 0
        elif self._delay_request_algorithm is DelayRequestAlgorithm.ESTIMATE_DIFFERENCES:
            return self._estimate_differences(host_id)
        elif self._delay_request_algorithm is DelayRequestAlgorithm.ORIGINAL:
            return self._original_func(host_id)
        elif self._delay_request_algorithm is DelayRequestAlgorithm.INVERSE_PROPORTION:
            return self._inverse_proportion_to_host_usage_count(host_id)
        elif self._delay_request_algorithm is DelayRequestAlgorithm.CONVEX_DOWNWARD:
            return self._convex_downward_to_host_usage_count(host_id)
        elif self._delay_request_algorithm is DelayRequestAlgorithm.LOGARITHM:
            return self._logarithm_func(host_id)
        else:
            message = '{} is an unsupported algorithm.'.format(self._delay_request_algorithm.name)
            raise DelayRequestAlgorithmError(message)

    def _original_func(self, host_id):
        ratio = self._get_host_count_ratio(host_id)

        shift = 0

        degree = abs(1 / (ratio - shift) - 1)

        degree = int(degree)
        self._logger.debug('Original: host={}, ratio={}, degree={}'.format(self._hosts[host_id], ratio, degree))

        if time.monotonic() - self._begin_time > 10:
            self._ratio.append(ratio)
            self._degree.append(degree)
        return degree

    def _convex_downward_to_host_usage_count(self, host_id):
        ratio = self._get_host_count_ratio(host_id)

        degree = int(100 * (1 - ratio) ** 15)

        self._logger.debug('Inverse: host={}, ratio={}, degree={}'.format(self._hosts[host_id], ratio, degree))

        self._ratio.append(ratio)
        self._degree.append(degree)

        if degree > 0:
            return degree
        else:
            return 0

    def _logarithm_func(self, host_id):
        ratio = self._get_host_count_ratio(host_id)

        degree = int(-40 * math.log(2.5 * ratio, 10))

        self._logger.debug('logarithm: host={}, ratio={}, degree={}'.format(self._hosts[host_id], ratio, degree))

        self._ratio.append(ratio)
        self._degree.append(degree)

        if degree > 0:
            return degree
        else:
            return 0

    def _inverse_proportion_to_host_usage_count(self, host_id):
        ratio = self._get_host_count_ratio(host_id)
        try:
            degree = int((1 / ratio) - 1)
        except ZeroDivisionError:
            degree = self._request_num

        self._logger.debug('Inverse: host={}, ratio={}, degree={}'.format(self._hosts[host_id], ratio, degree))

        self._ratio.append(ratio)
        self._degree.append(degree)

        if degree > 0:
            return degree
        else:
            return 0

    def _get_host_count_ratio(self, host_id):
        host_usage_count = self._host_counts[host_id]
        max_host_count = max(self._host_counts.values())

        try:
            ratio = host_usage_count / max_host_count
        except ZeroDivisionError:
            ratio = 1

        self._logger.debug('Ratio: host={}, host_usage_count={}, max_host_count={}, ratio={}'
                           .format(self._hosts[host_id], host_usage_count, max_host_count, ratio))
        return ratio

    def _estimate_differences(self, host_id):
        current_count = self._receive_count
        previous_count = self._previous_counts[host_id]
        diff = current_count - previous_count - (len(self._urls))
        self._previous_counts[host_id] = current_count

        self._logger.debug('Diff: host={}, current_count={}, previous_count={}, diff={}'
                           .format(self._hosts[host_id], current_count, previous_count, diff))

        if diff < 0:
            diff = 0
        ratio = self._get_host_count_ratio(host_id)
        if time.monotonic() - self._begin_time > 10:
            self._ratio.append(ratio)
            self._degree.append(diff)

        return diff

    def _get_body(self):
        host_id = self._host_id_queue.get()
        conn = self._sessions[host_id]
        url = self._urls[host_id]

        x = self._get_delay_request_degree(host_id)
        skip = x
        while True:
            if skip < x * 0.8:
                return self._get_body()
            try:
                param = self._params.pop(skip)
                break
            except IndexError:
                skip -= 1

        self._index.append(param['index'])
        self._send_time.append(time.monotonic() - self._begin_time)

        try:
            resp = conn.request('GET', url, headers=param['headers'])

            self._logger.debug('Send request: host_id={}, host={}, index={}, skip={}'
                               .format(host_id, self._hosts[host_id], param['index'], skip))

            if resp.status_code != 206:
                message = 'status_code: {}, host: {}'.format(resp.status, self._hosts[host_id])
                raise InvalidStatusCode(message)

            range_header = resp.headers['Content-Range']
            order = get_order(range_header, self._split_size)
            body = resp.content

        except requests.exceptions.ConnectionError:
            self._params.insert(0, param)
            self._reset_connection(host_id)
            self._host_id_queue.put(host_id)
            return self._get_body()

        self._logger.debug('Receive response: host_id={}, host={}, order={}'
                           .format(host_id, self._hosts[host_id], order))

        self._host_counts[host_id] += 1
        self._log.append({'time': time.monotonic() - self._begin_time, 'order': order})

        self._host_id_queue.put(host_id)
        self._receive_count += 1

        return order, body

    def _reset_connection(self, host_id):
        host, port = self._hosts[host_id]
        self._logger.debug('ConnectionRestError: host_id={}, host={}:{}'.format(host_id, host, port))
        conn = requests.Session()
        self._sessions[host_id] = conn

    def _get_result(self):
        i = 0
        while i < len(self._future_body):
            if self._future_body[i].running():
                i += 1
            else:
                try:
                    order, body = self._future_body[i].result(timeout=0)
                    self._data[order] = body
                    del self._future_body[i]
                    gc.collect()
                except TimeoutError or OSError:
                    i += 1

    def __next__(self):
        if self._received_index >= self._request_num:
            self._is_completed = True
            self._close()
            self._logger.debug('Finish')
            raise StopIteration

        if self._is_started is False:
            self.start()

        self._get_result()
        returnable = False
        b = bytearray()

        i = self._received_index
        while i < len(self._data):
            if self._data[i] is None:
                break
            else:
                b += self._data[i]
                self._data[i] = b''
                gc.collect()
                returnable = True
            i += 1

        if returnable:
            return_count = len(b) // self._split_size
            if return_count < 1:
                return_count = 1
            self._received_index += return_count
            self._logger.debug('Return blocks: length={}, return_count={}'.format(len(b), return_count))
            return b
        else:
            time.sleep(0.1)
            return self.__next__()

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

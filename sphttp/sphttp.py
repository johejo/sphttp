import time
import ssl
import gc
import random
from queue import Queue
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from urllib.parse import urlparse
from logging import getLogger, NullHandler

from hyper import HTTPConnection, HTTP20Connection, HTTP20Response
from hyper.tls import init_context

from .utils import get_length, map_all, get_order, get_port, SphttpFlowControlManager
from .algorithm import DelayRequestAlgorithm
from .exception import (
    FileSizeError, InvalidStatusCode, IncompleteError, DelayRequestAlgorithmError
)

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())

DEFAULT_SPLIT_SIZE = 10 ** 6


class SplitDownloader(object):
    def __init__(self, urls, *,
                 verify=True,
                 split_size=DEFAULT_SPLIT_SIZE,
                 http2_multiple_stream_setting=None,
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

        self._context = init_context()
        if verify is False:
            self._context.check_hostname = False
            self._context.verify_mode = ssl.CERT_NONE

        self._urls = {host_id: url for host_id, url in zip(self._host_ids, urls)}

        self._parallel_setting = http2_multiple_stream_setting  # type: dict
        if self._parallel_setting is None:
            self._parallel_setting = {}

        self._conns = {}
        self._set_connections()

        for url in urls:
            if url not in self._parallel_setting.keys():
                self._parallel_setting[url] = 1

        self._host_id_queue = Queue()
        for host_id in self._host_ids:
            self._host_id_queue.put(host_id)

        if http2_multiple_stream_setting:
            self._max_workers = None
            self._set_multi_stream()

        else:
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

        self._logger.debug('Init')

    def get_trace_data(self):
        if self._is_completed:
            return self._hosts, self._host_counts, self._log
        else:
            message = 'Cannot return trace data before the download is completed'
            raise IncompleteError(message)

    def _start(self):
        self._begin_time = time.monotonic()
        self._future_body = [self._executor.submit(self._get_body) for _ in range(self._request_num)]

    def _close(self):
        self._executor.shutdown()

    def _set_connections(self):
        for host_id, (host, port) in self._hosts.items():
            match = False
            for url, parallel_num in self._parallel_setting.items():
                parsed_url = urlparse(url)
                target_host = parsed_url.hostname
                target_port = get_port(url)
                if (host, port) == (target_host, target_port):
                    self._conns[host_id] = HTTP20Connection(host + ':' + str(port),
                                                            ssl_context=self._context,
                                                            window_manager=SphttpFlowControlManager
                                                            )
                    match = True
                    break
            if match is False:
                self._conns[host_id] = HTTPConnection(host + ':' + str(port),
                                                      ssl_context=self._context,
                                                      window_manager=SphttpFlowControlManager
                                                      )

    def _set_multi_stream(self):
        s = 0
        for url, parallel_num in self._parallel_setting.items():
            s += parallel_num
            parsed_url = urlparse(url)
            target_port = get_port(url)
            target_host = parsed_url.hostname
            target_host_id = None
            for host_id, (host, port) in self._hosts.items():
                if (host, port) == (target_host, target_port):
                    target_host_id = host_id

            for _ in range(parallel_num - 1):
                self._host_id_queue.put(target_host_id)
        self._max_workers = s

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
        elif self._delay_request_algorithm is DelayRequestAlgorithm.INVERSE_PROPORTION:
            return self._inverse_proportion_to_host_usage_count(host_id)
        else:
            message = '{} is an unsupported algorithm.'.format(self._delay_request_algorithm.name)
            raise DelayRequestAlgorithmError(message)

    def _inverse_proportion_to_host_usage_count(self, host_id):
        ratio = self._get_host_count_ratio(host_id)
        try:
            degree = int((1 / ratio) - 1)
        except ZeroDivisionError:
            degree = self._request_num

        self._logger.debug('Inverse: host={}, ratio={}, degree={}'.format(self._hosts[host_id], ratio, degree))
        return degree

    def _get_host_count_ratio(self, host_id):
        host_usage_count = self._host_counts[host_id]
        max_host_count = max(self._host_counts.values())
        sum_host_count = sum(self._host_counts.values())

        if host_usage_count == max_host_count:
            ratio = 1
        else:
            try:
                ratio = host_usage_count / sum_host_count
            except ZeroDivisionError:
                ratio = 1

        self._logger.debug('Ratio: host={}, host_usage_count={}, max_host_count={}, sum_host_count={}, ratio={}'
                           .format(self._hosts[host_id], host_usage_count, max_host_count, sum_host_count, ratio))
        return ratio

    def _estimate_differences(self, host_id):
        current_count = self._receive_count
        previous_count = self._previous_counts[host_id]
        diff = current_count - previous_count - (len(self._urls) - 1)
        self._previous_counts[host_id] = current_count

        self._logger.debug('Diff: host={}, current_count={}, previous_count={}, diff={}'
                           .format(self._hosts[host_id], current_count, previous_count, diff))

        if diff > 0:
            return diff
        else:
            return 0

    def _get_body(self):
        host_id = self._host_id_queue.get()
        conn = self._conns[host_id]  # type: HTTPConnection
        parsed_url = urlparse(self._urls[host_id])

        skip = self._get_delay_request_degree(host_id)
        while True:
            try:
                param = self._params.pop(skip)
                break
            except IndexError:
                skip -= 1

        try:
            if self._host_http2_flags[host_id]:
                stream_id = conn.request('GET', parsed_url.path, headers=param['headers'])
                resp = conn.get_response(stream_id)
            else:
                stream_id = None
                conn.request('GET', parsed_url.path, headers=param['headers'])
                resp = conn.get_response()

            self._logger.debug('Send request: host_id={}, host={}, index={}, stream_id={}, skip={}'
                               .format(host_id, self._hosts[host_id], param['index'], stream_id, skip))

            if resp.status != 206:
                message = 'status_code: {}, host: {}'.format(resp.status, self._hosts[host_id])
                raise InvalidStatusCode(message)

            if self._host_http2_flags[host_id] is False and type(resp) is HTTP20Response:
                self._host_http2_flags[host_id] = True

            range_header = resp.headers['Content-Range'][0].decode()
            order = get_order(range_header, self._split_size)
            body = resp.read()

        except ConnectionResetError:
            self._params.insert(0, param)
            self._reset_connection(host_id)
            return self._get_body()

        self._logger.debug('Receive response: host_id={}, host={}, order={}, stream_id={}'
                           .format(host_id, self._hosts[host_id], order, stream_id))

        self._host_counts[host_id] += 1
        self._log.append({'time': time.monotonic() - self._begin_time, 'order': order})

        self._host_id_queue.put(host_id)
        self._receive_count += 1

        return order, body

    def _reset_connection(self, host_id):
        host, port = self._hosts[host_id]
        self._logger.debug('ConnectionRestError: host_id={}, host={}:{}'.format(host_id, host, port))
        conn = HTTPConnection(host + ':' + str(port), ssl_context=self._context,
                              window_manager=SphttpFlowControlManager
                              )
        self._conns[host_id] = conn
        self._host_counts[host_id] = 0
        self._host_id_queue.put(host_id)

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
                except TimeoutError:
                    i += 1

    def __next__(self):
        if self._received_index >= self._request_num:
            self._is_completed = True
            self._close()
            raise StopIteration

        if self._is_started is False:
            self._start()
            self._is_started = True

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

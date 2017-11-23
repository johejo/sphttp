import time
import ssl
import gc
import random
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from urllib.parse import urlparse
from logging import getLogger, NullHandler

from hyper import HTTPConnection, HTTP20Connection, HTTP20Response
from hyper.tls import init_context

from .utils import get_length, map_all, get_order, get_port
from .exception import (
    FileSizeError, InvalidStatusCode, IncompleteError, DuplicatedStartError
)

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())

DEFAULT_SPLIT_SIZE = 10 ** 6


class SplitDownloader(object):
    def __init__(self, urls, *,
                 split_size=DEFAULT_SPLIT_SIZE,
                 http2_multiple_stream_setting=None,
                 logger=local_logger,
                 ):

        self._logger = logger

        length_list = []
        for i, u in enumerate(urls):
            url, length = get_length(target_url=u)
            if u != url:
                urls[i] = url
            length_list.append(length)

        if map_all(length_list) is False:
            message = 'File size differs for each host.'
            raise FileSizeError(message)

        self._length = length_list[0]

        self._host_ids = [i for i in range(len(urls))]
        self._hosts = {host_id: (urlparse(url).hostname, get_port(url)) for host_id, url in zip(self._host_ids, urls)}
        self._host_http2_flags = {host_id: False for host_id in self._host_ids}

        self._split_size = split_size
        self._request_num = self._length // self._split_size

        self._params = []
        self._set_params()

        self._context = init_context()
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

        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self._future_body = None

        self._data = [None for _ in range(self._request_num)]
        self._received_index = 0

        self._host_counts = {host_id: {'count': 0, 'host': host} for host_id, (host, _) in self._hosts.items()}
        self._return_counts = []
        self._stack_counts = []
        self._total_thp = 0
        self._begin_time = None
        self._end_time = None
        self._log = []

        self._is_completed = False
        self._is_started = False

        self._logger.debug('Init')

    def start(self):
        if self._is_started is False:
            self._begin_time = time.monotonic()
            self._future_body = [self._executor.submit(self._get_body) for _ in range(self._request_num)]
            self._is_started = True
        else:
            message = 'Duplicated start is forbidden.'
            raise DuplicatedStartError(message)

    def get_trace_data(self):
        if self._is_completed:
            return self._total_thp, self._return_counts, self._stack_counts, self._host_counts, self._log
        else:
            message = 'Cannot return trace data before the download is completed'
            raise IncompleteError(message)

    def close(self):
        self._executor.shutdown()

    def _set_connections(self):
        for host_id, (host, port) in self._hosts.items():
            match = False
            for url, parallel_num in self._parallel_setting.items():
                parsed_url = urlparse(url)
                target_host = parsed_url.hostname
                target_port = get_port(url)
                if (host, port) == (target_host, target_port):
                    self._conns[host_id] = HTTP20Connection(host + ':' + str(port), ssl_context=self._context)
                    match = True
                    break
            if match is False:
                self._conns[host_id] = HTTPConnection(host + ':' + str(port), ssl_context=self._context)

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

        reminder = self._length % self._split_size
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

    def _get_body(self):
        host_id = self._host_id_queue.get()
        conn = self._conns[host_id]  # type: HTTPConnection
        parsed_url = urlparse(self._urls[host_id])

        x = 0
        while True:
            try:
                param = self._params.pop(x)
                break
            except IndexError:
                x -= 1

        try:
            if self._host_http2_flags[host_id]:
                stream_id = conn.request('GET', parsed_url.path, headers=param['headers'])
                resp = conn.get_response(stream_id)
            else:
                stream_id = None
                conn.request('GET', parsed_url.path, headers=param['headers'])
                resp = conn.get_response()

            self._logger.debug('Send request: host_id={}, host={}, index={}, stream_id={}'
                               .format(host_id, self._hosts[host_id], param['index'], stream_id))

            if resp.status != 206:
                message = 'status_code: {}, host: {}'.format(resp.status, self._hosts[host_id])
                raise InvalidStatusCode(message)

            if self._host_http2_flags[host_id] is False and type(resp) is HTTP20Response:
                self._host_http2_flags[host_id] = True

            range_header = resp.headers['Content-Range'][0].decode()
            order = get_order(range_header, self._split_size)
            body = resp.read()

        except ConnectionResetError:
            host, port = self._hosts[host_id]
            self._logger.debug('ConnectionRestError: host_id={}, host={}:{}'.format(host_id, host, port))
            conn = HTTPConnection(host + ':' + str(port), ssl_context=self._context)
            self._conns[host_id] = conn
            self._host_counts[host_id]['count'] = 0
            self._host_id_queue.put(host_id)
            self._params.insert(0, param)
            return self._get_body()

        self._logger.debug('Receive response: host_id={}, host={}, order={}, stream_id={}'
                           .format(host_id, self._hosts[host_id], order, stream_id))

        self._log.append({'time': time.monotonic() - self._begin_time, 'order': order})
        self._host_counts[host_id]['count'] += 1
        self._host_id_queue.put(host_id)

        return order, body

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
        if self._received_index == self._request_num:
            self._end_time = time.monotonic()
            self._total_thp = (self._length * 8) / (self._end_time - self._begin_time) / 10 ** 6
            self._is_completed = True
            raise StopIteration

        self._get_result()
        linkable = True
        returnable = False
        b = bytearray()
        i = self._received_index
        return_count = 0
        stack_count = 0
        while i < len(self._data):
            if self._data[i] is None:
                linkable = False
            else:
                if linkable:
                    return_count += 1
                    b += self._data[i]
                    self._data[i] = None
                    gc.collect()
                    returnable = True
                else:
                    stack_count += 1
            i += 1

        if returnable:
            self._received_index += return_count
            self._return_counts.append(return_count)
            self._stack_counts.append(stack_count)
            self._logger.debug('Return blocks: length={}, return_count={}'.format(len(b), return_count))
            return b
        else:
            time.sleep(0.1)
            return self.__next__()

    def __iter__(self):
        return self

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return

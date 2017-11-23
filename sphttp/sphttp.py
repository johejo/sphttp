import time
import ssl
import gc
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from urllib.parse import urlparse
from logging import getLogger, NullHandler

from hyper import HTTPConnection
from hyper.tls import init_context

from .utils import get_length, map_all, get_order
from .exception import FileSizeError, InvalidStatusCode, IncompleteError

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())

DEFAULT_SPLIT_SIZE = 10 ** 6


class SplitDownloader(object):
    def __init__(self, urls, *,
                 split_size=DEFAULT_SPLIT_SIZE,
                 logger=local_logger,
                 ):

        self._logger = logger

        length_list = []
        for i, u in enumerate(urls):
            url, length = get_length(url=u)
            if u != url:
                urls[i] = url
            length_list.append(length)

        if map_all(length_list) is False:
            raise FileSizeError('File size differs for each host.')

        self._length = length_list[0]

        self._hosts = [urlparse(url).hostname for url in urls]
        self._host_ids = [i for i in range(len(self._hosts))]

        self._split_size = split_size
        self._request_num = self._length // self._split_size

        self._params = []
        self._set_params()

        max_workers = len(self._hosts)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._future_body = None

        context = init_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        self._urls = {host_id: url for host_id, url in zip(self._host_ids, urls)}

        self._conns = {host_id: HTTPConnection(host, ssl_context=context)
                       for host_id, host in zip(self._host_ids, self._hosts)}

        self._host_id_queue = Queue()
        for host_id in self._host_ids:
            self._host_id_queue.put(host_id)

        self._data = [None for _ in range(self._request_num)]
        self._received_index = 0

        self._host_counts = {host_id: {'count': 0, 'host': host} for host_id, host in zip(self._host_ids, self._hosts)}
        self._return_counts = []
        self._stack_counts = []
        self._total_thp = 0
        self._begin_time = None
        self._end_time = None
        self._log = []

        self._is_completed = False

        self._logger.debug('Init')

    def start(self):
        self._begin_time = time.monotonic()
        self._future_body = [self._executor.submit(self._get_body) for _ in range(self._request_num)]

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
        conn = self._conns[host_id]
        parsed_url = urlparse(self._urls[host_id])

        x = 0
        while True:
            try:
                param = self._params.pop(x)
                break
            except IndexError:
                x -= 1

        conn.request('GET', parsed_url.path, headers=param['headers'])
        self._logger.debug('Send request: host_id={}, index={}'.format(host_id, param['index']))

        resp = conn.get_response()

        if resp.status != 206:
            self._logger.debug('Invalid status_code: host={}, status={}'.format(self._hosts[host_id], resp.status))
            raise InvalidStatusCode

        range_header = resp.headers['Content-Range'][0].decode()
        order = get_order(range_header, self._split_size)
        body = resp.read()

        self._logger.debug('Receive response: host_id={}, order={}'.format(host_id, order))
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

    def get_trace_data(self):
        if self._is_completed:
            return self._total_thp, self._return_counts, self._stack_counts, self._host_counts, self._log
        else:
            raise IncompleteError

    def close(self):
        self._executor.shutdown()

    def __iter__(self):
        return self

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return

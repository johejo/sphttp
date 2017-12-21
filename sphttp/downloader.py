import threading
import time
from logging import getLogger, NullHandler

from hyper import HTTPConnection
from yarl import URL

from .algorithm import DelayRequestAlgorithm
from .exception import FileSizeError, ParameterPositionError, DelayRequestAlgorithmError, SphttpConnectionError
from .utils import match_all, SphttpFlowControlManager, async_get_length
from .structures import AnyPoppableDeque

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())

DEFAULT_SPLIT_SIZE = 10 ** 6
DEFAULT_SLEEP_SEC = 0.1
DEFAULT_INVALID_BLOCK_COUNT_THRESHOLD = 20
DEFAULT_INIT_DELAY_COEF = 5


class Downloader(object):
    def __init__(self,
                 urls,
                 *,
                 split_size=DEFAULT_SPLIT_SIZE,
                 sleep_sec=DEFAULT_SLEEP_SEC,
                 enable_trace_log=False,
                 verify=True,
                 delay_req_algo=DelayRequestAlgorithm.DIFFERENCES,
                 enable_dup_req=True,
                 static_delay_req_val=None,
                 invalid_block_count_threshold=DEFAULT_INVALID_BLOCK_COUNT_THRESHOLD,
                 init_delay_coef=DEFAULT_INIT_DELAY_COEF,
                 logger=local_logger, ):

        self._split_size = abs(split_size)
        self._sleep_sec = sleep_sec
        self._verify = verify
        self._invalid_block_count_threshold = abs(invalid_block_count_threshold)
        self._delay_req_algo = delay_req_algo
        self._enable_dup_req = enable_dup_req
        self._logger = logger

        length, raw_delays = async_get_length(urls)

        if match_all(length) is False:
            message = 'File size differs for each host.'
            raise FileSizeError(message)

        self.length = length[0]

        self._urls = [URL(url) for url in urls]
        self._conns = [HTTPConnection(host='{}:{}'.format(url.host, url.port),
                                      window_manager=SphttpFlowControlManager,
                                      verify=self._verify) for url in self._urls]
        min_delay = min(raw_delays.values())
        self._init_delay = {URL(url): (int((delay / min_delay) - 1) * init_delay_coef)
                            for url, delay in raw_delays.items()}

        self._num_of_req = self.length // self._split_size
        reminder = self.length % self._split_size

        self._params = AnyPoppableDeque()
        begin = 0

        for block_num in range(self._num_of_req):
            end = begin + self._split_size - 1
            self._params.append({'block_num': block_num,
                                 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})
            begin += self._split_size

        if reminder:
            end = begin + reminder - 1
            self._params.append({'block_num': self._num_of_req,
                                 'headers': {'Range': 'bytes={0}-{1}'.format(begin, end)}})
            self._num_of_req += 1

        num_of_conns = len(self._conns)

        self._threads = [threading.Thread(target=self._download,
                                          args=(conn_id,),
                                          name=str(conn_id)) for conn_id in range(num_of_conns)]
        self._buf = [None] * self._num_of_req
        self._is_started = False
        self._read_index = 0
        self._initial = [True] * num_of_conns
        self._invalid_block_count = 0
        self._unreceived_block_param = AnyPoppableDeque()

        self._receive_count = 0
        self._host_usage_count = [0] * num_of_conns
        self._previous_receive_count = [0] * num_of_conns
        self._previous_param = [AnyPoppableDeque() for _ in self._conns]

        if self._delay_req_algo is DelayRequestAlgorithm.STATIC and static_delay_req_val:
            self._static_delay_req_val = static_delay_req_val

        self._enable_trace_log = enable_trace_log
        self._begin_time = None
        if self._enable_trace_log:
            self._recv_log = []
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
            yield self._concat_buf()

    def get_trace_log(self):
        if self._enable_trace_log:
            return self._send_log, self._recv_log

    def _get_block_number(self, range_header):
        tmp = range_header.split(' ')
        tmp = tmp[1].split('/')
        tmp = tmp[0].split('-')
        block_number = int(tmp[0]) // self._split_size

        return block_number

    def _get_request_pos(self, conn_id):
        if self._delay_req_algo is DelayRequestAlgorithm.NORMAL:
            return 0
        elif self._delay_req_algo is DelayRequestAlgorithm.DIFFERENCES:
            return self._measure_diff(conn_id)
        elif self._delay_req_algo is DelayRequestAlgorithm.INVERSE:
            return self._calc_inverse(conn_id)
        elif self._delay_req_algo is DelayRequestAlgorithm.STATIC:
            return self._static_diff(conn_id)
        else:
            raise DelayRequestAlgorithmError

    def _static_diff(self, conn_id):
        try:
            diff = self._static_delay_req_val[conn_id]
        except KeyError:
            diff = 0

        return diff

    def _measure_diff(self, conn_id):
        previous = self._previous_receive_count[conn_id]
        current = self._receive_count
        diff = current - previous
        self._previous_receive_count[conn_id] = self._receive_count

        if self._initial[conn_id]:
            url = self._urls[conn_id]
            diff = self._init_delay[url]
            self._initial[conn_id] = False
        else:
            if self._host_usage_count[conn_id] == max(self._host_usage_count):
                diff = 0

        self._logger.debug('Diff: conn_id={}, diff={}'.format(conn_id, diff))
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
            pos = self._num_of_req - self._read_index

        pos = min(self._num_of_req - self._read_index, pos)

        return pos

    def _get_param(self, conn_id):
        pos = self._get_request_pos(conn_id)
        remain = len(self._params)
        if remain == 1:
            pos = 0
        elif pos * 0.9 > remain:
            raise ParameterPositionError
        else:
            pos = min(pos, remain - 1)

        param = self._params.pop_at_any_pos(pos)

        return param

    def _send_req(self, conn_id):
        conn = self._conns[conn_id]
        url = self._urls[conn_id]

        if self._enable_dup_req and self._invalid_block_count < self._invalid_block_count_threshold or \
                conn_id != self._host_usage_count.index(max(self._host_usage_count)):
            try:
                param = self._get_param(conn_id)
            except ParameterPositionError:
                raise

        else:
            param = self._unreceived_block_param[0]
            self._logger.debug('Duplicate request: conn_id={}, block_num={}'.format(conn_id, param['block_num']))

        conn.request('GET', url.path, headers=param['headers'])
        self._logger.debug('Send request: conn_id={}, block_num={}, time={}, remain={}'
                           .format(conn_id, param['block_num'], self._current_time(), len(self._params)))

        if self._enable_trace_log:
            self._send_log.append((self._current_time(), param['block_num']))

        return param

    def _recv_resp(self, conn_id):
        conn = self._conns[conn_id]
        try:
            resp = conn.get_response()
        except ConnectionResetError:
            message = 'ConnectionResetError has occurred.: {}'.format(self._urls[conn_id])
            raise SphttpConnectionError(message)

        block_num = self._get_block_number(resp.headers[b'Content-Range'][0].decode())

        body = resp.read()
        if self._buf[block_num] is None:
            self._buf[block_num] = body
            self._logger.debug('Get chunk: conn_id={}, block_num={}, time={}, protocol={}'
                               .format(conn_id, block_num, self._current_time(), resp.version.value))

            if self._enable_trace_log:
                self._recv_log.append((self._current_time(), block_num))

            self._host_usage_count[conn_id] += 1
            self._receive_count += 1
            self._invalid_block_count += 1

    def _download(self, conn_id):

        while len(self._params):
            try:
                param = self._send_req(conn_id)
            except ParameterPositionError:
                return

            self._previous_param[conn_id].append(param)
            self._unreceived_block_param.append(param)

            try:
                self._recv_resp(conn_id)
            except SphttpConnectionError:
                self._logger.debug('Connection abort: {}'.format(self._urls[conn_id]))
                failed_param = self._previous_param[conn_id].pop()
                failed_block_num = failed_param['block_num']
                if self._buf[failed_block_num] is None:
                    self._params.appendleft(self._previous_param[conn_id].pop())

                return

            pos = self._unreceived_block_param.index(param)
            self._unreceived_block_param.pop_at_any_pos(pos)

    def _concat_buf(self):
        b = bytearray()
        i = self._read_index
        length = len(self._buf)

        while i < length:
            if self._buf[i] is None:
                break
            else:
                b += self._buf[i]
                self._buf[i] = True
            i += 1

        self._read_index = i
        length = len(b)

        if length == 0:
            time.sleep(self._sleep_sec)
            return self._concat_buf()

        self._invalid_block_count = 0
        self._logger.debug('Return: bytes={}, num={}, read_index={}'
                           .format(length, length // self._split_size, self._read_index))
        return b

    def _current_time(self):
        return time.monotonic() - self._begin_time

    def _is_completed(self):
        if self._read_index == self._num_of_req:
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

        return self._concat_buf()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

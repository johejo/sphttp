from threading import Thread, Event
import time
from logging import getLogger, NullHandler

from hyper import HTTPConnection
from yarl import URL

from .algorithm import DelayRequestAlgorithm, DuplicateRequestAlgorithm
from .exception import (
    FileSizeError, ParameterPositionError, DelayRequestAlgorithmError, SphttpConnectionError, StatusCodeError,
)
from .utils import match_all, SphttpFlowControlManager, async_get_length
from .structures import AnyPoppableDeque, RangeRequestParam

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())


class Downloader(object):
    def __init__(self,
                 urls,
                 *,
                 split_size=10**6,
                 enable_trace_log=False,
                 verify=True,
                 delay_req_algo=DelayRequestAlgorithm.DIFF,
                 enable_dup_req=True,
                 dup_req_algo=DuplicateRequestAlgorithm.NIBIB,
                 close_bad_conn=False,
                 static_delay_req_vals=None,
                 enable_init_delay=True,
                 invalid_block_count_threshold=20,
                 init_delay_coef=10,
                 logger=local_logger):

        self._split_size = abs(split_size)
        self._verify = verify
        self._invalid_block_count_threshold = max(20, invalid_block_count_threshold)
        self._delay_req_algo = delay_req_algo
        self._enable_dup_req = enable_dup_req
        self._dup_req_algo = dup_req_algo
        self._close_bad_conn = close_bad_conn
        self._enable_init_delay = enable_init_delay

        self._logger = logger

        init_delay_coef = max(1, init_delay_coef)

        length, self._raw_delays = async_get_length(urls)

        if match_all(length) is False:
            message = 'File size differs for each host.'
            raise FileSizeError(message)

        self.length = length[0]

        self._urls = [URL(url) for url in self._raw_delays.keys()]

        # create hyper connection
        self._conns = [HTTPConnection(host='{}:{}'.format(url.host, url.port),
                                      window_manager=SphttpFlowControlManager,
                                      verify=self._verify) for url in self._urls]

        self._num_req = self.length // self._split_size
        reminder = self.length % self._split_size

        if self._enable_init_delay:
            min_delay = min(self._raw_delays.values())
            self._init_delay = {URL(url): (int((delay / min_delay) - 1) * init_delay_coef)
                                for url, delay in self._raw_delays.items()}
        else:
            self._init_delay = {URL(url): 0
                                for url in self._raw_delays.keys()}

        self._params = AnyPoppableDeque()
        begin = 0

        for block_id in range(self._num_req):
            self._params.append(RangeRequestParam(block_id, {'Range': 'bytes={0}-{1}'
                                                  .format(begin, begin + self._split_size - 1)}))
            begin += self._split_size

        if reminder:
            self._params.append(RangeRequestParam(self._num_req, {'Range': 'bytes={0}-{1}'
                                                  .format(begin, begin + reminder - 1)}))
            self._num_req += 1

        num_conns = len(self._conns)

        self._threads = [Thread(target=self._download, args=(conn_id,), name=str(conn_id))
                         for conn_id in range(num_conns)]
        self._buf = [None] * self._num_req
        self._is_started = False
        self._read_index = 0
        self._initial = [True] * num_conns
        self._invalid_block_count = 0
        self._sent_block_param = [None] * self._num_req
        self._bad_conn_ids = set()

        self._receive_count = 0
        self._host_usage_count = [0] * num_conns
        self._previous_receive_count = [0] * num_conns
        self._previous_param = [RangeRequestParam() for _ in self._conns]

        if self._delay_req_algo is DelayRequestAlgorithm.STATIC and static_delay_req_vals:
            self._static_delay_req_val = static_delay_req_vals

        self._enable_trace_log = enable_trace_log
        self._begin_time = None

        if self._enable_trace_log:
            self._recv_log = []
            self._send_log = []

        self._event = Event()

        self._logger.debug('Init')

    def start(self):
        if self._is_started is False:
            self._is_started = True

            self._begin_time = time.monotonic()

            for thread in self._threads:
                thread.start()

    def close(self):
        return

    def generator(self):
        if self._is_started is False:
            self.start()

        while self._is_completed() is False:
            yield self._concat_buf()

        self.close()

    def get_trace_log(self):
        if self._enable_trace_log:
            return self._send_log, self._recv_log, self._raw_delays

    def _send_req(self, conn_id):

        def _get_param():

            def _get_request_pos():
                if self._delay_req_algo is DelayRequestAlgorithm.NORMAL:
                    return 0
                elif self._delay_req_algo is DelayRequestAlgorithm.DIFF:
                    return _measure_diff()
                elif self._delay_req_algo is DelayRequestAlgorithm.INV:
                    return _calc_inverse()
                elif self._delay_req_algo is DelayRequestAlgorithm.STATIC:
                    return _static_diff()
                else:
                    raise DelayRequestAlgorithmError

            def _static_diff():
                try:
                    diff = self._static_delay_req_val[url.human_repr()]
                except KeyError:
                    diff = 0

                return diff

            def _measure_diff():

                diff = self._receive_count - self._previous_receive_count[conn_id]\
                       - len(self._urls) + 1 + len(self._bad_conn_ids)

                self._previous_receive_count[conn_id] = self._receive_count

                if self._initial[conn_id]:
                    diff = self._init_delay[url]
                    self._initial[conn_id] = False

                self._logger.debug('Diff: conn_id={}, diff={}'.format(conn_id, diff))
                return max(0, diff)

            def _calc_inverse():
                try:
                    ratio = self._host_usage_count[conn_id] / max(self._host_usage_count)
                except ZeroDivisionError:
                    ratio = 1

                try:
                    p = int((1 / ratio) - 1)
                except ZeroDivisionError:
                    p = self._num_req - self._read_index

                return min(self._num_req - self._read_index, p)

            pos = _get_request_pos()
            remain = len(self._params)

            if remain == 1:
                pos = 0
            elif pos * 0.9 > remain:
                raise ParameterPositionError
            else:
                pos = min(pos, remain - 1)

            while True:
                try:
                    parameter = self._params.pop_at_any_pos(pos)
                except IndexError:
                    pos -= 1
                else:
                    break

            return parameter

        def _should_send_dup_req():
            if self._invalid_block_count > self._invalid_block_count_threshold:
                return True
            else:
                return False

        def _is_conn_perf_highest():
            if conn_id == self._host_usage_count.index(max(self._host_usage_count)):
                return True
            else:
                return False

        conn = self._conns[conn_id]
        url = self._urls[conn_id]

        if self._enable_dup_req and _should_send_dup_req() and _is_conn_perf_highest() \
                and self._buf[self._read_index] is None and self._sent_block_param[self._read_index] is not None:

            bad_conn_id, param = self._sent_block_param[self._read_index]

            if self._close_bad_conn:
                self._bad_conn_ids.add(bad_conn_id)

            self._logger.debug('Duplicate request: conn_id={}, bad_conn_id={}, block_id={}, invalid_block_count={}'
                               .format(conn_id, bad_conn_id, param.block_id, self._invalid_block_count))

        else:
            try:
                param = _get_param()
            except ParameterPositionError:
                self._logger.debug('ParameterError: conn_id={}'.format(conn_id))
                raise

            self._previous_param[conn_id] = param
            self._sent_block_param[param.block_id] = (conn_id, param)

        conn.request('GET', url.path, headers=param.headers)
        self._logger.debug('Send request: conn_id={}, block_id={}, time={}, remain={}'
                           .format(conn_id, param.block_id, self._current_time(), len(self._params)))

        if self._enable_trace_log:
            self._send_log.append((self._current_time(), param.block_id, self._urls[conn_id].host))

    def _recv_resp(self, conn_id):

        def get_block_id(range_header):
            tmp = range_header.split(' ')
            tmp = tmp[1].split('/')
            tmp = tmp[0].split('-')
            return int(tmp[0]) // self._split_size

        conn = self._conns[conn_id]
        try:
            resp = conn.get_response()
        except (ConnectionResetError, ConnectionAbortedError):
            self._bad_conn_ids.add(conn_id)
            raise SphttpConnectionError

        if resp.status != 206:
            message = 'status_code: {}, url={}'.format(resp.status, self._urls[conn_id].host)
            raise StatusCodeError(message)

        block_id = get_block_id(resp.headers[b'Content-Range'][0].decode())
        self._logger.debug('Receive response: conn_id={}, block_id={}, time={}, protocol={}'
                           .format(conn_id, block_id, self._current_time(), resp.version.value))

        body = resp.read()
        if self._buf[block_id] is None:
            self._buf[block_id] = body
            self._event.set()
            self._logger.debug('Get chunk: conn_id={}, block_id={}, time={}, protocol={}'
                               .format(conn_id, block_id, self._current_time(), resp.version.value))

            if self._enable_trace_log:
                self._recv_log.append((self._current_time(), block_id, self._urls[conn_id].host))

            self._host_usage_count[conn_id] += 1
            self._receive_count += 1

    def _download(self, conn_id):

        while not self._is_completed():
            if conn_id in self._bad_conn_ids or not len(self._params):
                break

            try:
                self._send_req(conn_id)
            except ParameterPositionError:
                break

            try:
                self._recv_resp(conn_id)
            except SphttpConnectionError:
                self._logger.debug('Connection abort: conn_id={}, url={}'.format(conn_id, self._urls[conn_id]))
                self._host_usage_count[conn_id] = 0
                failed_block_id = self._previous_param[conn_id].block_id
                if self._buf[failed_block_id] is None:
                    self._params.appendleft(self._previous_param[conn_id])

                break

        self._conns[conn_id].close()
        self._logger.debug('Finish: conn_id={}'.format(conn_id))

    def _concat_buf(self):
        b = bytearray()
        i = self._read_index
        buf_len = len(self._buf)
        n = 0

        while i < buf_len:
            if self._buf[i] is None:
                break
            else:
                b += self._buf[i]
                self._buf[i] = True
                n += 1
            i += 1

        self._read_index = i

        if self._dup_req_algo is DuplicateRequestAlgorithm.NIBIB:
            c = 0
            for buf in self._buf:
                if type(buf) is bytes:
                    c += 1
            self._invalid_block_count = c

        b_len = len(b)

        if b_len == 0:
            if self._dup_req_algo is DuplicateRequestAlgorithm.IBRC or \
                    self._dup_req_algo is DuplicateRequestAlgorithm.IBRC_X:
                self._invalid_block_count += 1

            self._event.clear()
            self._event.wait()
            return self._concat_buf()

        else:
            if self._dup_req_algo is DuplicateRequestAlgorithm.IBRC:
                self._invalid_block_count -= n - 1
                self._invalid_block_count = max(self._invalid_block_count, 0)

            elif self._dup_req_algo is DuplicateRequestAlgorithm.IBRC_X:
                self._invalid_block_count = 0

            self._logger.debug('Return: bytes={}, num={}, read_index={}'
                               .format(b_len, n, self._read_index))
            return bytes(b)

    def _current_time(self):
        return time.monotonic() - self._begin_time

    def _is_completed(self):
        if self._read_index == self._num_req:
            return True
        else:
            return False

    def __len__(self):
        return self.length

    def __iter__(self):
        return self

    def __next__(self):
        if self._is_completed():
            self.close()
            raise StopIteration

        if self._is_started is False:
            self.start()

        return self._concat_buf()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

from threading import Thread, Event, Lock
import time
from logging import getLogger, NullHandler

from yarl import URL
from requests.exceptions import RequestException

from .algorithm import DelayRequestAlgorithm, DuplicateRequestAlgorithm
from .exception import (
    FileSizeError, ParameterPositionError,
    DelayRequestAlgorithmError, SphttpConnectionError,
)
from .utils import match_all, async_get_length
from .structures import AnyPoppableDeque, RangeParam

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())


# This is core downloader class.
# Inherit this class and implement Downloader using various HTTP libraries.
class CoreDownloader(object):
    def __init__(self,
                 urls,
                 *,
                 split_size=10 ** 6,
                 enable_trace_log=False,
                 verify=True,
                 delay_req_algo=DelayRequestAlgorithm.DIFF,
                 enable_dup_req=True,
                 dup_req_algo=DuplicateRequestAlgorithm.IBRC,
                 close_bad_conn=False,
                 static_delay_req_vals=None,
                 enable_init_delay=True,
                 invalid_block_threshold=20,
                 init_delay_coef=10,
                 logger=local_logger):

        self._split_size = abs(split_size)
        self._verify = verify
        self._invalid_block_threshold = max(20, invalid_block_threshold)
        self._delay_req_algo = delay_req_algo
        self._enable_dup_req = enable_dup_req
        self._dup_req_algo = dup_req_algo
        self._close_bad_conn = close_bad_conn
        self._enable_init_delay = enable_init_delay
        self._coef = init_delay_coef

        self._logger = logger

        init_delay_coef = max(1, init_delay_coef)

        length, self._raw_delays = async_get_length(urls)

        if match_all(length) is False:
            message = 'File size differs for each host.'
            raise FileSizeError(message)

        self.length = length[0]

        self._urls = [URL(url) for url in self._raw_delays.keys()]

        self._num_req = int(self.length // self._split_size)
        reminder = self.length % self._split_size

        if self._enable_init_delay:
            min_d = min(self._raw_delays.values())
            self._init_delay = {URL(u): int(((d / min_d - 1) *
                                             init_delay_coef))
                                for u, d in self._raw_delays.items()}
        else:
            self._init_delay = {URL(u): 0
                                for u in self._raw_delays.keys()}

        self._params = AnyPoppableDeque()
        begin = 0

        for block_id in range(self._num_req):
            end = begin + self._split_size - 1
            param = RangeParam(block_id,
                               {'Range': 'bytes={0}-{1}'.format(begin, end)})
            self._params.append(param)
            begin += self._split_size

        if reminder:
            end = begin + reminder - 1
            param = RangeParam(self._num_req,
                               {'Range': 'bytes={0}-{1}'.format(begin, end)})
            self._params.append(param)
            self._num_req += 1

        num_hosts = len(self._urls)

        self._threads = [
            Thread(target=self._download, args=(sess_id,), name=str(sess_id))
            for sess_id in range(num_hosts)]
        self._buf = [None for _ in range(self._num_req)]
        self._is_started = False
        self._read_index = 0
        self._initial = [True] * num_hosts
        self._invalid_block_count = 0
        self._sent_block_param = [(None, None) for _ in range(self._num_req)]
        self._bad_sess_ids = set()

        self._receive_count = 0
        self._host_usage_count = [0] * num_hosts
        self._previous_receive_count = [0] * num_hosts
        self._previous_param = [RangeParam() for _ in self._urls]

        if self._delay_req_algo is DelayRequestAlgorithm.STATIC \
                and static_delay_req_vals:
            self._static_delay_req_val = static_delay_req_vals

        self._enable_trace_log = enable_trace_log
        self._begin_time = None

        if self._enable_trace_log:
            self._recv_log = []
            self._send_log = []

        self._event = Event()
        self._lock = Lock()

        self._sessions = []
        self.set_sessions()

        self._logger.debug('Init')

    def set_sessions(self):

        # This method establish HTTP sessions

        raise NotImplementedError

    def request(self, sess_id, param):

        # This methos must return tuple of range-header and response-body.

        raise NotImplementedError

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

    def _download(self, sess_id):

        while not self._is_completed():
            if sess_id in self._bad_sess_ids or not len(self._params):
                break

            if self._check_bad_sess(sess_id):
                bad_sess_id, param = self._sent_block_param[self._read_index]

                if self._close_bad_conn:
                    self._bad_sess_ids.add(bad_sess_id)

                self._logger.debug('Duplicate request: sess_id={}, '
                                   'bad_sess_id={}, block_id={}, '
                                   'invalid_block_count={}'
                                   .format(sess_id, bad_sess_id,
                                           param.block_id,
                                           self._invalid_block_count))

            else:
                try:
                    param = self._get_param(sess_id)
                except ParameterPositionError:
                    self._logger.debug(
                        'ParameterError: sess_id={}'.format(sess_id))
                    break

                self._previous_param[sess_id] = param

                with self._lock:
                    self._sent_block_param[param.block_id] = (sess_id, param)

            try:
                range_header, body = self.request(sess_id, param)

            except (RequestException, SphttpConnectionError):
                self._logger.debug('Connection abort: sess_id={}, url={}'
                                   .format(sess_id, self._urls[sess_id]))
                self._host_usage_count[sess_id] = 0

                failed_block_id = self._previous_param[sess_id].block_id
                if self._buf[failed_block_id] is None:
                    self._params.appendleft(self._previous_param[sess_id])
                break

            self._store_body(sess_id, range_header, memoryview(body))

        self._sessions[sess_id].close()
        self._logger.debug('Finish: sess_id={}'.format(sess_id))

    def _store_body(self, sess_id, range_header, body):

        if type(range_header) is bytes:
            range_header = range_header.decode()

        tmp = range_header.split(' ')
        tmp = tmp[1].split('/')
        tmp = tmp[0].split('-')

        block_id = int(tmp[0]) // self._split_size

        if self._buf[block_id] is None:
            self._buf[block_id] = body
            self._event.set()
            self._logger.debug('Get chunk: sess_id={}, block_id={}, time={}'
                               .format(sess_id, block_id,
                                       self._current_time()))

            if self._enable_trace_log:
                self._recv_log.append((self._current_time(), block_id,
                                       self._urls[sess_id].host))

            self._host_usage_count[sess_id] += 1

            with self._lock:
                self._receive_count += 1

    def _check_bad_sess(self, sess_id):

        if self._enable_dup_req and self._should_send_dup_req() \
                and self._is_conn_perf_highest(sess_id) \
                and self._buf[self._read_index] is None \
                and self._sent_block_param[self._read_index] is not None:
            return True
        else:
            return False

    def _get_request_pos(self, sess_id):
        if self._delay_req_algo is DelayRequestAlgorithm.NORMAL:
            return 0
        elif self._delay_req_algo is DelayRequestAlgorithm.DIFF:
            return self._measure_diff(sess_id)
        elif self._delay_req_algo is DelayRequestAlgorithm.INV:
            return self._calc_inverse(sess_id)
        elif self._delay_req_algo is DelayRequestAlgorithm.STATIC:
            return self._static_diff(sess_id)
        else:
            raise DelayRequestAlgorithmError

    def _get_param(self, sess_id):

        pos = self._get_request_pos(sess_id)
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

    def _static_diff(self, sess_id):
        try:
            diff = self._static_delay_req_val[self._urls[sess_id].human_repr()]
        except KeyError:
            diff = 0

        return diff

    def _measure_diff(self, sess_id):

        d = self._receive_count - self._previous_receive_count[sess_id]
        diff = d - len(self._urls) + len(self._bad_sess_ids)

        self._previous_receive_count[sess_id] = self._receive_count

        if self._initial[sess_id]:
            diff = self._init_delay[self._urls[sess_id]]
            self._initial[sess_id] = False

        self._logger.debug('Diff: sess_id={}, diff={}'.format(sess_id, diff))
        return max(0, diff)

    def _calc_inverse(self, sess_id):
        try:
            ratio = self._host_usage_count[sess_id] / max(
                self._host_usage_count)
        except ZeroDivisionError:
            ratio = 1

        try:
            p = int((1 / ratio) - 1)
        except ZeroDivisionError:
            p = self._num_req - self._read_index

        return min(self._num_req - self._read_index, p)

    def _should_send_dup_req(self):
        if self._invalid_block_count > self._invalid_block_threshold:
            return True
        else:
            return False

    def _is_conn_perf_highest(self, sess_id):
        if sess_id == self._host_usage_count.index(
                max(self._host_usage_count)):
            return True
        else:
            return False

    def _concat_buf(self):
        b = bytearray()
        i = self._read_index
        buf_len = len(self._buf)
        n = 0

        while i < buf_len:
            if self._buf[i] is None:
                break
            else:
                b += self._buf[i].tobytes()
                self._buf[i].release()
                self._buf[i] = True
            i += 1

        with self._lock:
            self._read_index = i

        if self._dup_req_algo is DuplicateRequestAlgorithm.NIBIB:
            c = 0
            for buf in self._buf:
                if type(buf) is memoryview:
                    c += 1

            with self._lock:
                self._invalid_block_count = c

        b_len = len(b)

        if b_len == 0:
            if self._dup_req_algo is DuplicateRequestAlgorithm.IBRC:
                with self._lock:
                    self._invalid_block_count += 1

            self._event.clear()
            self._event.wait()
            return self._concat_buf()

        else:
            if self._dup_req_algo is DuplicateRequestAlgorithm.IBRC:
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

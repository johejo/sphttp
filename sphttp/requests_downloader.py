from logging import getLogger, NullHandler

import requests

from .core import CoreDownloader
from .algorithm import DuplicateRequestAlgorithm, DelayRequestAlgorithm
from .exception import StatusCodeError

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())


class RequestsDownloader(CoreDownloader):

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

        super().__init__(urls,
                         split_size=split_size,
                         enable_trace_log=enable_trace_log, verify=verify,
                         delay_req_algo=delay_req_algo,
                         enable_dup_req=enable_dup_req,
                         dup_req_algo=dup_req_algo,
                         close_bad_conn=close_bad_conn,
                         static_delay_req_vals=static_delay_req_vals,
                         enable_init_delay=enable_init_delay,
                         init_delay_coef=init_delay_coef,
                         invalid_block_threshold=invalid_block_threshold,
                         logger=logger)

    def set_sessions(self):
        for _ in self._urls:
            sess = requests.Session()
            self._sessions.append(sess)

    def request(self, sess_id, param):

        sess = self._sessions[sess_id]
        url = self._urls[sess_id]

        resp = sess.get(url.human_repr(), headers=param.headers, stream=True)

        self._logger.debug('Send request: sess_id={}, '
                           'block_id={}, time={}, remain={}'
                           .format(sess_id, param.block_id,
                                   self._current_time(), len(self._params)))

        if self._enable_trace_log:
            self._send_log.append((self._current_time(), param.block_id,
                                   self._urls[sess_id].host))

        if resp.status_code != 206:
            message = 'status: {}, url={}'.format(resp.status_code,
                                                  self._urls[sess_id].host)
            raise StatusCodeError(message)

        return resp.headers['Content-Range'], memoryview(resp.content)

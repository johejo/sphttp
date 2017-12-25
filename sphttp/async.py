import asyncio
import time
from threading import Event, Thread
from logging import getLogger, NullHandler
from queue import Queue

import aiohttp
import async_timeout

from .exception import StatusCodeError, FileSizeError, NoAcceptRanges, NoContentLength, DuplicateStartError
from .utils import match_all
from .structures import AnyPoppableDeque, RangeRequestParam

local_logger = getLogger(__name__)
local_logger.addHandler(NullHandler())


class Downloader(object):
    def __init__(self, urls, split_size=10**6, timeout=10, logger=local_logger):

        self._urls = urls
        self._split_size = split_size
        self._timeout = timeout
        self._logger = logger

        # self._urls = {}
        # self._sessions = {}
        #
        # async def create_sess(sess_id, url):
        #     sess = aiohttp.ClientSession()
        #     self._sessions[sess_id] = sess
        #     self._urls[sess_id] = url
        #
        # loop = asyncio.get_event_loop()
        # loop.run_until_complete(asyncio.wait([create_sess(sess_id, url) for sess_id, url in enumerate(urls)]))

        length_tmp = []
        self._initial_delays = {}

        async def init_head_req(url):
            with async_timeout.timeout(self._timeout):
                head_req_begin = time.monotonic()

                with aiohttp.ClientSession() as sess:
                    async with sess.head(url) as resp:
                        if resp.status != 200:
                            err_msg = 'status={}, url={}'.format(resp.status, url)
                            raise StatusCodeError(err_msg)

                        try:
                            resp.headers['Accept-Ranges']
                        except KeyError:
                            raise NoAcceptRanges

                        try:
                            length_tmp.append(int(resp.headers['Content-Length']))
                        except KeyError:
                            raise NoContentLength

                        self._initial_delays[url] = time.monotonic() - head_req_begin

        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait([init_head_req(url) for url in urls]))

        if match_all(length_tmp) is False:
            message = 'File size differs for each host.'
            raise FileSizeError(message)

        self._length = length_tmp[0]
        self._num_req = self._length // split_size
        reminder = self._length % split_size

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

        self._dl_thread = Thread(target=self._background_dl, name='DL-Thread')
        self._event = Event()

        self._buf = [None] * self._num_req
        self._read_index = 0

        self._is_started = False

        self._logger.debug('Init')

    def _background_dl(self):

        async def download():
            sem = asyncio.Semaphore(len(self._urls))

            with await sem:
                session_id = sess_id_queue.get()
                param = get_param()

                with async_timeout.timeout(self._timeout):

                    async with aiohttp.ClientSession() as sess:
                        async with sess.get(self._urls[session_id], headers=param.headers) as resp:
                            if resp.status != 200:
                                err_msg = 'status={}, url={}'.format(resp.status, self._urls[session_id])
                                raise StatusCodeError(err_msg)

                            block_id = get_block_id(resp.headers['Content-Range'])
                            body = await resp.read()
                            self._logger.debug('Get chunk: sess_id={}, block_id={}, time={}'
                                               .format(session_id, block_id, get_current_time()))
                            self._buf[block_id] = body
                            self._event.set()

        def get_param():
            pos = 0
            param = self._params.pop_at_any_pos(pos)
            return param

        def get_block_id(range_header):
            tmp = range_header.split(' ')
            tmp = tmp[1].split('/')
            tmp = tmp[0].split('-')
            block_id = int(tmp[0]) // self._split_size
            return block_id

        def get_current_time():
            return begin - time.monotonic()

        # sessions = {}
        # urls = {}

        # async def create_sess(sess_id, url):
        #     sess = aiohttp.ClientSession()
        #     sessions[sess_id] = sess
        #     urls[sess_id] = url

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        # loop = asyncio.get_event_loop()
        begin = time.monotonic()

        # loop.run_until_complete(asyncio.wait([create_sess(sess_id, url) for sess_id, url in enumerate(self._urls)]))

        sess_id_queue = Queue()
        for sess_id in range(len(self._urls)):
            sess_id_queue.put(sess_id)

        loop.run_until_complete(asyncio.wait([download() for _ in range(self._num_req)]))

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
            self._event.clear()
            self._event.wait()
            return self._concat_buf()

        self._logger.debug('Return: bytes={}, num={}, read_index={}'
                           .format(length, length // self._split_size, self._read_index))

        return b

    def _is_completed(self):
        if self._read_index == self._num_req:
            return True
        else:
            return False

    def start(self):
        if self._is_started is False:
            self._dl_thread.start()
            self._is_started = True
        else:
            err_msg = 'Duplicate start is not allowed.'
            raise DuplicateStartError(err_msg)

        while self._is_completed() is False:
            yield self._concat_buf()


def multi_source_http_download(urls, *, split_size=10**6, timeout=10):

    sessions = []

    async def create_sessions():
        s = aiohttp.ClientSession()
        sessions.append(s)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait([create_sessions() for _ in urls]))

    length_tmp = []
    delays = {}

    async def init_head_request(url, sess):
        with async_timeout.timeout(timeout):
            head_req_begin = time.monotonic()

            async with sess.head(url) as resp:
                if resp.status != 200:
                    err_msg = 'status={}'.format(resp.status)
                    raise StatusCodeError(err_msg)

                if not resp.headers['Accept-Ranges']:
                    raise NoAcceptRanges

                length_tmp.append(int(resp.headers['Content-Length']))
                delays[url] = time.monotonic() - head_req_begin

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait([init_head_request(url, sess) for url, sess in zip(urls, sessions)]))

    if match_all(length_tmp) is False:
        message = 'File size differs for each host.'
        raise FileSizeError(message)

    length = length_tmp[0]

    num_req = length // split_size
    reminder = length % split_size

    params = AnyPoppableDeque()

    begin = 0

    for block_num in range(num_req):
        end = begin + split_size - 1
        params.append(RangeRequestParam(block_num, {'Range': 'bytes={}-{}'.format(begin, end)}))
        begin += split_size

    if reminder:
        params.append(RangeRequestParam(num_req, {'Range': 'bytes={}-{}'.format(begin, begin + reminder - 1)}))
        num_req += 1

    buf = [None] * num_req

    def block_index(range_header):
        tmp = range_header.split(' ')
        tmp = tmp[1].split('/')
        tmp = tmp[0].split('-')
        index = int(tmp[0]) // split_size
        return index

    async def req():
        sem = asyncio.Semaphore(len(sessions))

        with await sem:
            sess_id = sess_id_queue.get()
            sess = sessions[sess_id]
            url = urls[sess_id]
            pos = 0
            param = params.pop_at_any_pos(pos)

            with sess.get(url, headers=param.headers) as resp:
                index = block_index(resp.headers['Content-Range'])
                body = await resp.read()
                buf[index] = body
                sess_id_queue.put(sess_id)
                event.set()

    sess_id_queue = asyncio.Queue()
    for i in range(len(sessions)):
        sess_id_queue.put(i)

    event = Event()

    read_index = 0

    loop = asyncio.get_event_loop()
    for _ in range(num_req):
        loop.call_soon(req)
    loop.run_forever()
    print('HE')
    loop.close()

    def concat_buf(ri):
        b = bytearray()

        while ri < len(buf):
            if buf[ri] is None:
                break
            else:
                b += buf[ri]
                buf[ri] = True
            ri += 1

        if len(b) == 0:
            event.clear()
            event.wait()
            return concat_buf(ri)

    while read_index <= num_req:
        if len(params) == 0:
            loop.stop()
        yield concat_buf(read_index)

    for s in sessions:
        s.close()

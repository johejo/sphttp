import asyncio
import time
from threading import Event

import aiohttp
import async_timeout

from .exception import StatusCodeError, FileSizeError, NoAcceptRanges
from .utils import match_all
from .structures import AnyPoppableDeque


class RangeRequestParam(object):
    def __init__(self, block_num, headers):
        self.block_num = block_num
        self.headers = headers


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

from urllib.parse import urlparse
import asyncio
import time
from yarl import URL

import requests
from hyper import HTTP20Connection
from hyper.http20.window import BaseFlowControlManager
from hyper.tls import init_context
import aiohttp

from .exception import StatusCodeError, NoContentLength, NoAcceptRanges, SchemeError

REDIRECT_STATUSES = (301, 302, 303, 307, 308)


# hyper's FlowControlManager does not perform very well in a high-throughput environment
class SphttpFlowControlManager(BaseFlowControlManager):

    def increase_window_size(self, frame_size):
        increase = self.window_size * 10
        future_window_size = self.window_size - frame_size + increase
        if future_window_size >= 2147483647:  # 2^31 - 1
            return 0

        return increase

    def blocked(self):
        return self.initial_window_size - self.window_size


# first head request to get content length
# and measure delay of head request
def async_get_length(urls):
    length = []
    delays = {}

    async def init_head_request(url):
        async with aiohttp.ClientSession() as sess:
            begin = time.monotonic()
            async with sess.head(url) as resp:

                # Redirects are not supported
                if resp.status != 200:
                    message = 'status={}'.format(resp.status)
                    raise StatusCodeError(message)
                try:
                    length.append(int(resp.headers['Content-Length']))
                except KeyError:
                    message = 'Host does not support Content-Length header.'
                    raise NoContentLength(message)

                delays[URL(url).host] = time.monotonic() - begin

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.wait([init_head_request(url) for url in urls]))

    return length, delays


# Check if the contents of sequence are all the same
def match_all(es):
    """
    :param es: list
    :return: bool
    """
    return all([e == es[0] for e in es[1:]]) if es else False


# The parts below this are not maintained.

def get_length(target_url, *, verify=True):
    resp = requests.head(url=target_url, verify=verify)
    status = resp.status_code
    if status == 200:
        try:
            length = int(resp.headers['Content-Length'])
        except KeyError:
            message = 'Host does not support Content-Length header.'
            raise NoContentLength(message)

        try:
            resp.headers['Accept-Ranges']
        except KeyError:
            message = 'Host does not support Accept-Ranges header.'
            raise NoAcceptRanges(message)

    elif status in REDIRECT_STATUSES:
        location = resp.headers['Location']
        return get_length(target_url=location)

    else:
        message = 'Status code: {}. Failed URL: {}'.format(status, target_url)
        raise StatusCodeError(message)
    return target_url, length


def get_port(url):
    parsed_url = urlparse(url)
    if parsed_url.scheme == 'http':
        port = 80
    elif parsed_url.scheme == 'https':
        port = 443
    else:
        message = 'Scheme is not set.'
        raise SchemeError(message)
    return port


def get_index(range_header, split_size):
    """
    :param range_header: str
    :param split_size: int
    :return:
    """
    tmp = range_header.split(' ')
    tmp = tmp[1].split('/')
    tmp = tmp[0].split('-')
    index = int(tmp[0]) // split_size
    return index


def get_length_hyper(target_url, *, ssl_context=None):
    parsed_url = urlparse(target_url)
    if ssl_context is None:
        ssl_context = init_context()

    length = None

    with HTTP20Connection(parsed_url.hostname, ssl_context=ssl_context) as conn:
        conn.request('HEAD', url=parsed_url.path)
        resp = conn.get_response()
        status = resp.status
        if status == 200:
            try:
                length = int(resp.headers['Content-Length'][0])
            except KeyError:
                message = 'Host does not support Content-Length header.'
                raise NoContentLength(message)

            try:
                resp.headers['Accept-Ranges']
            except KeyError:
                message = 'Host does not support Accept-Ranges header.'
                raise NoAcceptRanges(message)

        elif status in REDIRECT_STATUSES:
            location = resp.headers['Location'][0].decode()
            return get_length_hyper(target_url=location)

        else:
            message = 'Status code: {}. Failed URL: {}'.format(status, target_url)
            raise StatusCodeError(message)

    return target_url, length


def init_http2_multi_stream_setting(http2_server_urls):
    setting = {url: 1 for url in http2_server_urls}
    return setting

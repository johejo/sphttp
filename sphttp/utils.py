import asyncio
import time

from hyper.http20.window import BaseFlowControlManager
import aiohttp

from .exception import StatusCodeError, NoContentLength

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
                    message = 'status={}, url={}'.format(resp.status, url)
                    raise StatusCodeError(message)
                try:
                    length.append(int(resp.headers['Content-Length']))
                except KeyError:
                    message = 'Host does not support Content-Length header. url={}'.format(url)
                    raise NoContentLength(message)

                delays[url] = time.monotonic() - begin

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

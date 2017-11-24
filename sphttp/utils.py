from urllib.parse import urlparse
import ssl

import requests
from hyper import HTTP20Connection
from hyper.tls import init_context

from .exception import StatusCodeError, NoContentLength, NoAcceptRanges, SchemeError

REDIRECT_STATUSES = [301, 302, 303, 307, 308]


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


def map_all(es):
    """
    :param es: list
    :return: bool
    """
    return all([e == es[0] for e in es[1:]]) if es else False


def get_order(range_header, split_size):
    """
    :param range_header: str
    :param split_size: int
    :return:
    """
    tmp = range_header.split(' ')
    tmp = tmp[1].split('/')
    tmp = tmp[0].split('-')
    order = int(tmp[0]) // split_size
    return order


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

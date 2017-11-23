import requests

from .exception import StatusCodeError, NoContentLength, NoAcceptRanges

REDIRECT_STATUS = [301, 302, 303, 307, 308]


def get_length(url):
    resp = requests.head(url, verify=False)
    status = resp.status_code
    if status == 200:
        try:
            length = int(resp.headers['Content-Length'])
        except KeyError:
            raise NoContentLength('Host does not support Content-Length header.')

        try:
            resp.headers['Accept-Ranges']
        except KeyError:
            raise NoAcceptRanges('Host does not support Accept-Ranges header.')

    elif status in REDIRECT_STATUS:
        location = resp.headers['Location']
        return get_length(url=location)

    else:
        raise StatusCodeError('Status code: {}. Failed URL: {}'.format(status, url))
    return url, length


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

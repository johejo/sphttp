import requests

from .exception import FileNotFound


def get_length(url):
    resp = requests.head(url, verify=False)
    if resp.status_code == 200:
        length = int(resp.headers['content-length'])
    else:
        raise FileNotFound
    return length


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

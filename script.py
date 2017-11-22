from logging import getLogger, StreamHandler, DEBUG
import sys
import os
sys.path.extend(['.'])
from sphttp import SplitDownloader

handler = StreamHandler()
handler.setLevel(DEBUG)
logger = getLogger(__name__)
logger.setLevel(DEBUG)
logger.addHandler(handler)


def main():
    urls = [
            'http://165.242.111.92/ubuntu-17.10-server-i386.template',
            'https://165.242.111.92/ubuntu-17.10-server-i386.template',
            'http://165.242.111.93/ubuntu-17.10-server-i386.template',
            'https://165.242.111.93/ubuntu-17.10-server-i386.template',

            'http://ftp.ne.jp/Linux/packages/ubuntu/releases-cd/17.10/ubuntu-17.10-server-i386.template',  # KDDI
            'http://ubuntutym2.u-toyama.ac.jp/ubuntu/17.10/ubuntu-17.10-server-i386.template',  # toyama
            # 'http://ftp.riken.go.jp/Linux/ubuntu-releases/17.10/ubuntu-17.10-server-i386.template',  # riken
            'http://ftp.jaist.ac.jp/pub/Linux/ubuntu-releases/17.10/ubuntu-17.10-server-i386.template',  # jaist
            'http://ftp.yz.yamagata-u.ac.jp/pub/linux/ubuntu/releases/17.10/ubuntu-17.10-server-i386.template'  # yamagata

            # 'http://165.242.111.94/ubuntu-17.10-server-i386.template',
            # 'https://165.242.111.94/ubuntu-17.10-server-i386.template',
            ]

    with open('test', 'wb') as _:
        pass

    with open('test', 'ab') as f:
        with SplitDownloader(urls=urls, logger=logger) as sd:
            for b in sd:
                f.write(b)
            thp, return_counts, stack_counts, hosts_counts, log = sd.get_logs()
        print(thp)
        print(return_counts)
        print(stack_counts)
        print(hosts_counts)

    os.remove('test')


if __name__ == '__main__':
    main()

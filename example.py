import os
from sphttp import SplitDownloader

if __name__ == '__main__':

    urls = [
        'http://ftp.ne.jp/Linux/packages/ubuntu/releases-cd/17.10/ubuntu-17.10-server-amd64.iso',  # KDDI
        'http://ubuntutym2.u-toyama.ac.jp/ubuntu/17.10/ubuntu-17.10-server-amd64.iso',  # toyama
        'http://ftp.jaist.ac.jp/pub/Linux/ubuntu-releases/17.10/ubuntu-17.10-server-amd64.iso',  # jaist
    ]

    filename = os.path.basename(urls[0])

    with open(filename, 'wb') as _:
        pass

    with open(filename, 'ab') as f:
        with SplitDownloader(urls) as sd:
            for part in sd:
                f.write(part)

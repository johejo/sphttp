sphttp: HTTP split downloader supporting HTTP/2
===============================================

# Description

While using split download using HTTP range-header, you can immediately use the ordered part.

# Install

```bash
$ pip install git+https://github.com/johejo/sphttp.git
```

# Usage

```python
from sphttp import SplitDownloader

urls = [
    'https://example0.com/1GB.txt', 
    'https://example1.com/1GB.txt', 
    'https://example2.com/1GB.txt', 
]

sd = SplitDownloader(urls)
sd.start()
for part in sd:
    # Do something to 'part'.
    print(len(part))   
sd.close()
```

# Example
A HTTP multiple downloader
```python
import os
from sphttp import SplitDownloader

if __name__ == '__main__':
    
    urls = [
        'http://ftp.ne.jp/Linux/packages/ubuntu/releases-cd/17.10/ubuntu-17.10-server-amd64.iso',  # KDDI
        'http://ubuntutym2.u-toyama.ac.jp/ubuntu/17.10/ubuntu-17.10-server-amd64.iso',  # toyama
        'http://ftp.riken.go.jp/Linux/ubuntu-releases/17.10/ubuntu-17.10-server-amd64.iso',  # riken
        'http://ftp.jaist.ac.jp/pub/Linux/ubuntu-releases/17.10/ubuntu-17.10-server-amd64.iso',  # jaist
        'http://ftp.yz.yamagata-u.ac.jp/pub/linux/ubuntu/releases/17.10/ubuntu-17.10-server-amd64.iso',  # yamagata
    ]
    
    filename = os.path.basename(urls[0])
    
    with open(filename, 'wb') as _:
        pass
    
    with open(filename, 'ab') as f:
        with SplitDownloader(urls) as sd:
            for part in sd:
                f.write(part)

```

# Advanced 
It supports HTTP/2 multiple streams as well.

## Caution
It does not check whether the target server supports HTTP/2.  
It is undefined what to do when multiple stream setting is done for access to a server that supports HTTP/1.1 only.
## How to set multiple streams for one connection
multiple streaming
```python

from sphttp import SplitDownloader

setting = {
    'https://example0.com/1GB.txt': 5, 
    'https://example1.com/1GB.txt': 4, 
    'https://example2.com/1GB.txt': 3, 
}

urls = [
    'https://example0.com/1GB.txt', 
    'https://example1.com/1GB.txt', 
    'https://example2.com/1GB.txt', 
]

with SplitDownloader(urls, http2_multiple_stream_setting=setting) as sd:
    for part in sd:
        # Do something to part.
        print(len(part))

```
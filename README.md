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
for part in sd:
    # Do something with 'part'.
    print(len(part))
```

# Example
A HTTP multiple downloader
```python
import os
from sphttp import SplitDownloader

if __name__ == '__main__':
    
    urls = [
        'https://example0.com/1GB.txt', 
        'https://example1.com/1GB.txt', 
        'https://example2.com/1GB.txt', 
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

Multi stream setting is done using a simple dictionary whose key is url.

```python

from sphttp import SplitDownloader, init_http2_multi_stream_setting


urls = [
    'https://example0.com/1GB.txt', 
    'https://example1.com/1GB.txt', 
    'https://example2.com/1GB.txt', 
]

# Initialize the number of streams per host to 1.
setting = init_http2_multi_stream_setting(urls)

# Here, the number of streams of all the hosts is set to 3.
for key in setting.keys():
    setting[key] = 3

with SplitDownloader(urls, http2_multiple_stream_setting=setting) as sd:
    for part in sd:
        # Do something with 'part'.
        print(len(part))

```
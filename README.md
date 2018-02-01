sphttp: HTTP split downloader supporting multi source
===============================================

# Description
While using split download using HTTP range-header, you can immediately use the ordered part.

# Install
```bash
$ pip install -U git+https://github.com/johejo/sphttp.git
$ pip install -U git+https://github.com/Lukasa/hyper.git
```

# Usage

```python
from sphttp import Downloader

urls = [
    'https://example0.com/1GB.txt', 
    'https://example1.com/1GB.txt', 
    'https://example2.com/1GB.txt', 
]

sd = Downloader(urls)
for part in sd:
    # Do something with 'part'.
    print(len(part))
```

# Generator Object
You can also get generator object from Downloader. 

For example, you can use this to create a simple proxy that makes an HTTP-Range request.

This is a flask sample. 

```python
from flask import Flask, Response, request

from sphttp import Downloader

app = Flask(__name__)


@app.route('/proxy')
def proxy():
    string = request.args['host']
    hosts = string.split(',')
    d = Downloader(urls=hosts)
    return Response(d.generator(), mimetype='application/octet-stream')


def main():
    app.run(host='', port=8080)


if __name__ == '__main__':
    main()
```
http://localhost:8080/proxy?host=http://example0.com/1GB.txt,http://example1.com/1GB.txt,http://example2.com/1GB.txt

GET this URL and the file will be downloaded via the proxy.

This proxy gets the file separately from the URL included in the query and returns the ordered part to the original request sender.  
For original request senders this looks like a single HTTP request.  
Don`t you think that playing a movie file as a network stream on VLC is fun!

# Proxy

You can quickly launch a sample of proxy server with nginx, uwsgi, and sphttp set using docker!  
[sphttp-proxy](https://github.com/johejo/sphttp-proxy/)

```bash
$ docker pull johejo/sphttp-proxy
$ docker run --name sphttp -d -p 8080:80 johejo/sphttp-proxy
```

[docker hub](https://hub.docker.com/r/johejo/sphttp-proxy/)

# Advanced

## Delay Request Algorithm
You can select an algorithm on how to send a range request to multiple hosts. 

About algorithms it would be better to read my graduation thesis (Please wait for a while as it is being written as of January 2018)

### How to select
Pass such parameters when generating Downloader object.

```python
from sphttp import Downloader, DelayRequestAlgorithm

d = Downloader(urls, delay_req_algo=DelayRequestAlgorithm.NORMAL)
```

NORMAL: No consideration is given to performance differences (such as bandwidth) between the hosts.

DIFF: Measure the performance difference between the hosts and make a delay request according to the situation.(default)

INV: The delay request width is determined by using an inverse proportional function from the ratio of the number of times of use for each host, and a delay request is made. (It will not be much better than DIFF)

STATIC: By inputting the performance difference of the host in advance, an accurate delay request is made. (Although it is not realistic)

STATIC sample

```python
from sphttp import Downloader, DelayRequestAlgorithm

performance = {
    'https://example0.com/1GB.txt': 0,  # Set 0 to the most performance host
    'https://example0.com/1GB.txt': N,  # Set the reciprocal of the performance ratio with N with the best performance server. 
                                        # Set 10 if the performance is 1/10 of the server like the most performance.
}

d = Downloader(urls, delay_req_algo=DelayRequestAlgorithm.STATIC, static_delay_req_vals=performance)
```

## Duplicate Request Algorithm
About duplicate request when delays of divided blocks occur

### How to select

```python
from sphttp import Downloader, DuplicateRequestAlgorithm

d = Downloader(urls, enable_dup_req=True, dup_req_algo=DuplicateRequestAlgorithm.NIBIB, invalid_block_count_threshold=30)
```

NIBIB: Based on the number of invalid blocks in the buffer (default)

The threshold value for duplicate request is 20 by default

## HTTP Session

By standard, **sphttp.downloader.Downloader** is using [hyper](https://github.com/Lukasa/hyper) as HTTP Session.  (HTTP/2 is supported)  
The downloader using [requests](https://github.com/requests/requests) as HTTP Session is **sphttp.http11_downloader.HTTP11Downloader** (this may be stupid)

# License
MIT

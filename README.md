arq
===

[![Build Status](https://travis-ci.org/samuelcolvin/arq.svg?branch=master)](https://travis-ci.org/samuelcolvin/arq)
[![Coverage](https://codecov.io/gh/samuelcolvin/arq/branch/master/graph/badge.svg)](https://codecov.io/gh/samuelcolvin/arq)
[![pypi](https://img.shields.io/pypi/v/arq.svg)](https://pypi.python.org/pypi/arq)
[![license](https://img.shields.io/pypi/l/arq.svg)](https://github.com/samuelcolvin/arq)

[rq](https://github.com/nvie/rq) meets asyncio.

Job queues in python with asyncio, redis and msgpack.

## Install

**Python >=3.5 is required.**

    pip install arq
    
## Usage

Usage is best described with an example, `demo.py`:

```python
import asyncio
from aiohttp import ClientSession
from arq import Actor, AbstractWorker, concurrent


class Downloader(Actor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session = ClientSession(loop=self.loop)

    @concurrent
    async def download_content(self, url):
        async with self.session.get(url) as response:
            assert response.status == 200
            content = await response.read()
            print('{}: {:.80}...'.format(url, content.decode()))
        return len(content)

    async def close(self):
        await super().close()
        self.session.close()


class Worker(AbstractWorker):
    shadows = [Downloader]


async def download_lots(loop):
    d = Downloader(loop=loop)
    for url in ('https://facebook.com', 'https://microsoft.com', 'https://github.com'):
        await d.download_content(url)
    await d.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_lots(loop))
```

You can then enqueue the jobs with just `python demo.py`, and run
the worker to do the jobs with `arq demo.py`.

`arq --help` for more help on how to run the worker.

## TODO

* job timeouts
* jobs results
* job uniqueness

arq
===

[![Build Status](https://travis-ci.org/samuelcolvin/arq.svg?branch=master)](https://travis-ci.org/samuelcolvin/arq)
[![Coverage](https://codecov.io/gh/samuelcolvin/arq/branch/master/graph/badge.svg)](https://codecov.io/gh/samuelcolvin/arq)
[![pypi](https://img.shields.io/pypi/v/arq.svg)](https://pypi.python.org/pypi/arq)
[![license](https://img.shields.io/pypi/l/arq.svg)](https://github.com/samuelcolvin/arq)

Job queues in python with asyncio, redis and msgpack.

rq meets asyncio.

arq is not production ready yet, **use with caution.**

arq is a tool for distributing tasks by first encoding a description of the job and adding it
to a redis list, then pop the job description from the list and executing it somewhere else. 
The "somewhere else" can be another process or another computer. arq is inspired by 
[rq](https://github.com/nvie/rq) but takes a significantly different approach.

It is:
 * **non-blocking** built using python's [asyncio](https://docs.python.org/3/library/asyncio.html) allowing non-blocking
 job enqueuing and job execution.
 * **pre-forked** In other works the worker starts two processes and uses the 
 subprocess to execute all jobs, there's no overhead in forking a process for each job.
 * **fast** Asyncio, pre-forking and use of msgpack for job encoding make arq around 7x faster 
 (see [benchmarks](/performance_benchmarks)) than rq for small jobs with no io, 
 with io that might increase to around 40x faster. TODO
 * **elegant** arq uses a novel approach to variable scope with the `@concurrent` decorator being applied to bound 
 methods of "Actor" classes which hold the connection pool. This works well with 
 [aiohttp](http://aiohttp.readthedocs.io/en/stable/), avoids extended head scratching over how variables 
 like connections are defined (is this attached to the request? or thread local? or truly global? 
 where am I, what does global mean?) and allows for easier testing. See below.
 * **small** and easy to reason with - currently arq is only about 500 lines, that won't change significantly.

## Install

**Python >=3.5** and **redis** are required. After than:

    pip install arq
    
Should install everything you need.
    
## Usage

Usage is best described with an example, `demo.py`:

```python
import asyncio
from aiohttp import ClientSession
from arq import Actor, BaseWorker, concurrent


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


class Worker(BaseWorker):
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

Still to be documented but working fine:
* multiple queues
* multiple actors
* worker `max_concurrency`
* worker job timeout
* advanced worker logging
* `.testing` py.test plugins.

## Actors, Shadows and global variables

TODO

## TODO

* jobs results
* job uniqueness

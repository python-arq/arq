import asyncio
import functools
import os

import msgpack
import pytest
from aioredis import create_redis_pool

from arq.connections import ArqRedis, create_pool
from arq.worker import Worker


@pytest.fixture(name='loop')
def _fix_loop(event_loop):
    asyncio.set_event_loop(event_loop)
    return event_loop


@pytest.fixture
async def arq_redis(loop):
    redis_ = await create_redis_pool(
        ('localhost', 6379), encoding='utf8', loop=loop, commands_factory=ArqRedis, minsize=5
    )
    await redis_.flushall()
    yield redis_
    redis_.close()
    await redis_.wait_closed()


@pytest.fixture
async def arq_redis_msgpack(loop):
    redis_ = await create_redis_pool(
        ('localhost', 6379),
        encoding='utf8',
        loop=loop,
        commands_factory=functools.partial(
            ArqRedis, job_serializer=msgpack.packb, job_deserializer=functools.partial(msgpack.unpackb, raw=False)
        ),
    )
    await redis_.flushall()
    yield redis_
    redis_.close()
    await redis_.wait_closed()


@pytest.fixture
async def worker(arq_redis):
    worker_: Worker = None

    def create(functions=[], burst=True, poll_delay=0, max_jobs=10, arq_redis=arq_redis, **kwargs):
        nonlocal worker_
        worker_ = Worker(
            functions=functions, redis_pool=arq_redis, burst=burst, poll_delay=poll_delay, max_jobs=max_jobs, **kwargs
        )
        return worker_

    yield create

    if worker_:
        await worker_.close()


@pytest.fixture(name='create_pool')
async def fix_create_pool(loop):
    pools = []

    async def create_pool_(settings, *args, **kwargs):
        pool = await create_pool(settings, *args, **kwargs)
        pools.append(pool)
        return pool

    yield create_pool_

    for p in pools:
        p.close()

    await asyncio.gather(*[p.wait_closed() for p in pools])


@pytest.fixture(name='cancel_remaining_task')
def fix_cancel_remaining_task(loop):
    async def cancel_remaining_task():
        tasks = asyncio.all_tasks(loop)
        cancelled = []
        for task in tasks:
            # in repr works in 3.7 where get_coro() is not available
            if 'cancel_remaining_task()' not in repr(task):
                cancelled.append(task)
                task.cancel()
        await asyncio.gather(*cancelled, return_exceptions=True)

    yield

    loop.run_until_complete(cancel_remaining_task())


class SetEnv:
    def __init__(self):
        self.envars = set()

    def set(self, name, value):
        self.envars.add(name)
        os.environ[name] = value

    def clear(self):
        for n in self.envars:
            os.environ.pop(n)


@pytest.fixture
def env():
    setenv = SetEnv()

    yield setenv

    setenv.clear()

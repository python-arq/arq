import pytest
from aioredis import create_redis_pool

from arq.connections import ArqRedis
from arq.worker import Worker


@pytest.yield_fixture
def redis(loop):
    """
    yield fixture which creates a redis connection, and flushes redis before the test.

    Note: redis is not flushed after the test both for performance and to allow later debugging.
    """

    async def _create_redis():
        r = await create_redis_pool(('localhost', 6379), encoding='utf8', loop=loop)
        await r.flushall()
        return r

    async def _close(r):
        r.close()
        await r.wait_closed()

    redis_ = loop.run_until_complete(_create_redis())
    yield redis_
    loop.run_until_complete(_close(redis_))


@pytest.yield_fixture
async def arq_redis(loop):
    redis_ = await create_redis_pool(('localhost', 6379), encoding='utf8', loop=loop, commands_factory=ArqRedis)
    await redis_.flushall()
    yield redis_
    redis_.close()
    await redis_.wait_closed()


@pytest.yield_fixture
async def worker(arq_redis):
    worker_: Worker = None

    def create(functions, burst=True, poll_delay=0, **kwargs):
        nonlocal worker_
        worker_ = Worker(functions=functions, redis_pool=arq_redis, burst=burst, poll_delay=poll_delay, **kwargs)
        return worker_

    yield create

    if worker_:
        await worker_.close()

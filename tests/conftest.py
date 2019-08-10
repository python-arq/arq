import pytest
from aioredis import create_redis_pool

from arq.connections import ArqRedis
from arq.worker import Worker


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

    def create(functions=[], burst=True, poll_delay=0, queue_read_limit=40, **kwargs):
        nonlocal worker_
        worker_ = Worker(
            functions=functions,
            redis_pool=arq_redis,
            burst=burst,
            poll_delay=poll_delay,
            queue_read_limit=queue_read_limit,
            **kwargs,
        )
        return worker_

    yield create

    if worker_:
        await worker_.close()

import pytest
from aioredis import create_redis_pool


@pytest.yield_fixture
def redis(loop):
    """
    yield fixture which creates a redis connection, and flushes redis before the test.

    Note: redis is not flushed after the test both for performance and to allow later debugging.
    """

    async def _create_redis():
        r = await create_redis_pool(('localhost', 6379), loop=loop)
        await r.flushall()
        return r

    async def _close(r):
        r.close()
        await r.wait_closed()

    redis_ = loop.run_until_complete(_create_redis())
    yield redis_
    loop.run_until_complete(_close(redis_))

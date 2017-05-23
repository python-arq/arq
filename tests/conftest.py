import pytest

from aioredis import create_pool, create_redis

from .fixtures import DemoActor, MockRedisDemoActor, MockRedisWorker


@pytest.yield_fixture
def redis_conn(loop):
    """
    yield fixture which creates a redis connection, and flushes redis before the test.

    Note: redis is not flushed after the test both for performance and to allow later debugging.
    """
    async def _get_conn():
        conn = await create_redis(('localhost', 6379), loop=loop)
        await conn.flushall()
        return conn
    conn = loop.run_until_complete(_get_conn())
    conn.loop = loop
    yield conn

    conn.close()
    loop.run_until_complete(conn.wait_closed())


@pytest.yield_fixture
def redis_pool(loop):
    """
    yield fixture which creates a redis connection, and flushes redis before the test.

    Note: redis is not flushed after the test both for performance and to allow later debugging.
    """
    async def _create_pool():
        p = await create_pool(('localhost', 6379), loop=loop)
        async with p.get() as redis:
            await redis.flushall()
        return p

    async def _close_pool(p):
        p.close()
        await p.wait_closed()
        await p.clear()

    pool = loop.run_until_complete(_create_pool())
    yield pool
    loop.run_until_complete(_close_pool(pool))


@pytest.yield_fixture
def actor(loop):
    _actor = DemoActor(loop=loop)
    yield _actor
    loop.run_until_complete(_actor.close())


@pytest.yield_fixture
def mock_actor(loop):
    _actor = MockRedisDemoActor(loop=loop)
    yield _actor
    loop.run_until_complete(_actor.close())


@pytest.yield_fixture
def mock_actor_worker(mock_actor):
    _worker = MockRedisWorker(loop=mock_actor.loop, burst=True)
    _worker.mock_data = mock_actor.mock_data
    yield mock_actor, _worker
    mock_actor.loop.run_until_complete(_worker.close())


@pytest.fixture
def caplog(caplog):
    caplog.set_loggers(log_names=('arq.main', 'arq.work', 'arq.jobs'), fmt='%(name)s: %(message)s')
    return caplog

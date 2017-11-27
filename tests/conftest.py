import pytest
from aioredis import create_redis, create_redis_pool

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
    try:
        loop.run_until_complete(conn.wait_closed())
    except RuntimeError:
        pass


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
    caplog.set_loggers(log_names=('arq.control', 'arq.main', 'arq.work', 'arq.jobs'), fmt='%(name)s: %(message)s')
    return caplog

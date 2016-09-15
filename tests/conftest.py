import pytest

from .fixtures import DemoActor, MockRedisDemoActor, MockRedisWorker

pytest_plugins = 'arq.testing'


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

import logging
import os
from datetime import datetime

from arq import RedisSettings
from arq.logs import ColourHandler
from arq.testing import MockRedis
from arq.utils import timestamp


def test_settings_changed():
    settings = RedisSettings(port=123)
    assert settings.port == 123


def test_timestamp():
    assert os.getenv('TZ') == 'Asia/Singapore', 'tests should always be run with TZ=Asia/Singapore'

    assert 7.99 < (datetime.now() - datetime.utcnow()).total_seconds() / 3600 < 8.01, ('timezone not set to '
                                                                                       'Asia/Singapore')
    unix_stamp = int(datetime.now().strftime('%s'))
    assert abs(timestamp() - unix_stamp) < 2


def test_arbitrary_logger(capsys):
    logger = logging.getLogger('foobar')
    logger.addHandler(ColourHandler())
    logger.warning('this is a test')
    out, err = capsys.readouterr()
    # click cleverly removes ANSI colours as the output is not a terminal
    assert [out, err] == ['this is a test\n', '']


async def test_mock_redis_expiry_ok(loop):
    r = MockRedis(loop=loop)
    await r.setex('foo', 10, 'bar')
    assert 'bar' == await r.get('foo')


async def test_mock_redis_expiry_expired(loop):
    r = MockRedis(loop=loop)
    await r.setex('foo', -10, 'bar')
    assert None is await r.get('foo')


async def test_mock_redis_flushdb(loop):
    r = MockRedis(loop=loop)
    await r.set('foo', 'bar')
    assert 'bar' == await r.get('foo')
    await r.flushdb()
    assert None is await r.get('foo')

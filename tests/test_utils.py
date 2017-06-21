import logging
import os
from datetime import datetime

import pytest

import arq.utils
from arq import RedisMixin, RedisSettings
from arq.logs import ColourHandler
from arq.testing import MockRedis
from arq.utils import next_datetime, timestamp


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


async def test_redis_timeout(loop, mocker):
    mocker.spy(arq.utils.asyncio, 'sleep')
    r = RedisMixin(redis_settings=RedisSettings(port=0, conn_retry_delay=0), loop=loop)
    with pytest.raises(OSError):
        await r.get_redis_pool()
    assert arq.utils.asyncio.sleep.call_count == 5


@pytest.mark.parametrize('previous,expected,kwargs', [
    (datetime(2016, 6, 1, 12, 10, 10), datetime(2016, 6, 1, 12, 10, 20), dict(second=20)),
    (datetime(2016, 6, 1, 12, 10, 10), datetime(2016, 6, 1, 12, 11, 0), dict(minute=11)),
    (
        datetime(2016, 6, 1, 12, 10, 10),
        datetime(2017, 6, 1, 12, 10, 10),
        dict(month=6, day=1, hour=12, minute=10, second=10),
    ),
    (
        datetime(2016, 6, 1, 12, 10, 10),
        datetime(2016, 7, 1, 12, 10, 10),
        dict(day=1, hour=12, minute=10, second=10),
    ),
    (
        datetime(2032, 1, 31, 0, 0, 0),
        datetime(2032, 2, 28, 0, 0, 0),
        dict(day=28),
    ),
    (
        datetime(2032, 1, 1, 0, 5),
        datetime(2032, 1, 1, 4, 0),
        dict(hour=4),
    ),
    (
        datetime(2032, 1, 1, 0, 0),
        datetime(2032, 1, 1, 4, 2),
        dict(hour=4, minute={2, 4, 6}),
    ),
    # (
    #     datetime(2032, 1, 1, 0, 5),
    #     datetime(2032, 1, 1, 4, 2),
    #     dict(hour=4, minute={2, 4, 6}),
    # ),
])
def test_next_datetime(previous, expected, kwargs):
    assert next_datetime(previous, **kwargs) == expected

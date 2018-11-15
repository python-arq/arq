import logging
import os
import re
from datetime import datetime, timedelta
from random import random

import pytest

import arq.utils
from arq import RedisMixin, RedisSettings, create_pool_lenient
from arq.logs import ColourHandler
from arq.testing import MockRedis
from arq.utils import next_cron, timestamp


def test_settings_changed():
    settings = RedisSettings(port=123)
    assert settings.port == 123
    assert ('<RedisSettings host=localhost port=123 database=0 password=None conn_retries=5 '
            'conn_timeout=1 conn_retry_delay=1>') == str(settings)


@pytest.mark.skipif(os.getenv('TZ') is None, reason='TZ=Asia/Singapore must be set')
def test_timestamp():
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


async def test_mock_redis_getset(loop):
    r = MockRedis(loop=loop)
    await r.set('foo', 'bar')
    assert 'bar' == await r.get('foo')
    assert 'bar' == await r.getset('foo', 'baz')
    assert 'baz' == await r.get('foo')


async def test_mock_redis_expiry_ok(loop):
    r = MockRedis(loop=loop)
    await r.setex('foo', 10, 'bar')
    assert 'bar' == await r.get('foo')


async def test_mock_redis_expiry_expired(loop):
    r = MockRedis(loop=loop)
    await r.setex('foo', -10, 'bar')
    assert None is await r.get('foo')


async def test_mock_redis_expire_expired(loop):
    r = MockRedis(loop=loop)
    await r.set('foo', 'bar')
    await r.expire('foo', -10)
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
        await r.get_redis()
    assert arq.utils.asyncio.sleep.call_count == 5


async def test_redis_success_log(loop, caplog):
    settings = RedisSettings()
    pool = await create_pool_lenient(settings, loop)
    assert 'redis connection successful' not in caplog
    pool.close()
    await pool.wait_closed()

    pool = await create_pool_lenient(settings, loop, _retry=1)
    assert 'redis connection successful' not in caplog
    pool.close()
    await pool.wait_closed()


async def test_redis_log(loop):
    r = RedisMixin(loop=loop)
    redis = await r.get_redis()
    await redis.flushall()
    await redis.set(b'a', b'1')
    await redis.set(b'b', b'2')

    log_msgs = []

    def _log(s):
        log_msgs.append(s)

    await r.log_redis_info(_log)
    await r.close()
    print(log_msgs)
    assert len(log_msgs) == 1
    assert re.search(r'redis_version=\d\.', log_msgs[0]), log_msgs
    assert log_msgs[0].endswith(' clients_connected=1 db_keys=2')


@pytest.mark.parametrize('previous,expected,kwargs', [
    (
        datetime(2016, 6, 1, 12, 10, 10),
        datetime(2016, 6, 1, 12, 10, 20, microsecond=123456),
        dict(second=20)
    ),
    (
        datetime(2016, 6, 1, 12, 10, 10),
        datetime(2016, 6, 1, 12, 11, 0, microsecond=123456),
        dict(minute=11)
    ),
    (
        datetime(2016, 6, 1, 12, 10, 10),
        datetime(2016, 6, 1, 12, 10, 20),
        dict(second=20, microsecond=0)
    ),
    (
        datetime(2016, 6, 1, 12, 10, 10),
        datetime(2016, 6, 1, 12, 11, 0),
        dict(minute=11, microsecond=0)
    ),
    (
        datetime(2016, 6, 1, 12, 10, 11),
        datetime(2017, 6, 1, 12, 10, 10, microsecond=123456),
        dict(month=6, day=1, hour=12, minute=10, second=10),
    ),
    (
        datetime(2016, 6, 1, 12, 10, 10, microsecond=1),
        datetime(2016, 7, 1, 12, 10, 10),
        dict(day=1, hour=12, minute=10, second=10, microsecond=0),
    ),
    (
        datetime(2032, 1, 31, 0, 0, 0),
        datetime(2032, 2, 28, 0, 0, 0, microsecond=123456),
        dict(day=28),
    ),
    (
        datetime(2032, 1, 1, 0, 5),
        datetime(2032, 1, 1, 4, 0, microsecond=123456),
        dict(hour=4),
    ),
    (
        datetime(2032, 1, 1, 0, 0),
        datetime(2032, 1, 1, 4, 2, microsecond=123456),
        dict(hour=4, minute={2, 4, 6}),
    ),
    (
        datetime(2032, 1, 1, 0, 5),
        datetime(2032, 1, 1, 4, 2, microsecond=123456),
        dict(hour=4, minute={2, 4, 6}),
    ),
    (
        datetime(2032, 2, 5, 0, 0, 0),
        datetime(2032, 3, 31, 0, 0, 0, microsecond=123456),
        dict(day=31),
    ),
    (
        datetime(2001, 1, 1, 0, 0, 0),  # Monday
        datetime(2001, 1, 7, 0, 0, 0, microsecond=123456),
        dict(weekday='Sun'),  # Sunday
    ),
    (
        datetime(2001, 1, 1, 0, 0, 0),
        datetime(2001, 1, 7, 0, 0, 0, microsecond=123456),
        dict(weekday=6),  # Sunday
    ),
    (
        datetime(2001, 1, 1, 0, 0, 0),
        datetime(2001, 11, 7, 0, 0, 0, microsecond=123456),
        dict(month=11, weekday=2),
    ),
    (
        datetime(2001, 1, 1, 0, 0, 0),
        datetime(2001, 1, 3, 0, 0, 0, microsecond=123456),
        dict(weekday='wed'),
    ),
])
def test_next_cron(previous, expected, kwargs):
    start = datetime.now()
    assert next_cron(previous, **kwargs) == expected
    diff = datetime.now() - start
    print(f'{diff.total_seconds() * 1000:0.3f}ms')


def test_next_cron_invalid():
    with pytest.raises(ValueError):
        next_cron(datetime(2001, 1, 1, 0, 0, 0), weekday='monday')


@pytest.mark.parametrize('max_previous,kwargs,expected', [
    (1, dict(microsecond=59867), datetime(2001, 1, 1, 0, 0, microsecond=59867)),
    (59, dict(second=28, microsecond=0), datetime(2023, 1, 1, 1, 59, 28)),
    (3600, dict(minute=10, second=20), datetime(2016, 6, 1, 12, 10, 20, microsecond=123456)),
    (68400, dict(hour=3), datetime(2032, 1, 1, 3, 0, 0, microsecond=123456)),
    (68400 * 60, dict(day=31, minute=59), datetime(2032, 3, 31, 0, 59, 0, microsecond=123456)),
    (
        68400 * 7,
        dict(weekday='tues', minute=59),
        datetime(2032, 3, 30, 0, 59, 0, microsecond=123456),
    ),
    (
        68400 * 175,  # previous friday the 13th is February
        dict(day=13, weekday='fri', microsecond=1),
        datetime(2032, 8, 13, 0, 0, 0, microsecond=1),
    ),
    (68400 * 365, dict(month=10, day=4, hour=23), datetime(2032, 10, 4, 23, 0, 0, microsecond=123456)),
    (
        1,
        dict(month=1, day=1, hour=0, minute=0, second=0, microsecond=69875),
        datetime(2001, 1, 1, 0, 0, microsecond=69875)
    ),
])
def test_next_cron_random(max_previous, kwargs, expected):
    for i in range(100):
        previous = expected - timedelta(seconds=0.9 + random() * max_previous)
        start = datetime.now()
        v = next_cron(previous, **kwargs)
        diff = datetime.now() - start
        print(f'previous: {previous}, expected: {expected}, time: {diff.total_seconds() * 1000:0.3f}ms')
        assert v == expected

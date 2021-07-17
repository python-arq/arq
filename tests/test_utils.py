import logging
import re
from datetime import timedelta

import pytest
from pydantic import BaseModel, validator

import arq.typing
import arq.utils
from arq.connections import RedisSettings, log_redis_info


def test_settings_changed():
    settings = RedisSettings(port=123)
    assert settings.port == 123
    assert (
        "RedisSettings(host='localhost', port=123, database=0, password=None, ssl=None, conn_timeout=1, "
        "conn_retries=5, conn_retry_delay=1, sentinel=False, sentinel_master='mymaster')"
    ) == str(settings)


async def test_redis_timeout(mocker, create_pool):
    mocker.spy(arq.utils.asyncio, 'sleep')
    with pytest.raises(OSError):
        await create_pool(RedisSettings(port=0, conn_retry_delay=0))
    assert arq.utils.asyncio.sleep.call_count == 5


async def test_redis_sentinel_failure(create_pool):
    """
    FIXME: this is currently causing 3 "Task was destroyed but it is pending!" warnings
    """
    settings = RedisSettings()
    settings.host = [('localhost', 6379), ('localhost', 6379)]
    settings.sentinel = True
    try:
        pool = await create_pool(settings)
        await pool.ping('ping')
    except Exception as e:
        assert 'unknown command `SENTINEL`' in str(e)


async def test_redis_success_log(caplog, create_pool):
    caplog.set_level(logging.INFO)
    settings = RedisSettings()
    pool = await create_pool(settings)
    assert 'redis connection successful' not in [r.message for r in caplog.records]
    await pool.close()

    pool = await create_pool(settings, retry=1)
    assert 'redis connection successful' in [r.message for r in caplog.records]
    await pool.close()


async def test_redis_log(create_pool):
    redis = await create_pool(RedisSettings())
    await redis.flushall()
    await redis.set(b'a', b'1')
    await redis.set(b'b', b'2')

    log_msgs = []

    def _log(s):
        log_msgs.append(s)

    await log_redis_info(redis, _log)
    assert len(log_msgs) == 1
    assert re.search(r'redis_version=\d\.', log_msgs[0]), log_msgs
    assert log_msgs[0].endswith(' db_keys=2')


def test_truncate():
    assert arq.utils.truncate('123456', 4) == '123…'


def test_args_to_string():
    assert arq.utils.args_to_string((), {'d': 4}) == 'd=4'
    assert arq.utils.args_to_string((1, 2, 3), {}) == '1, 2, 3'
    assert arq.utils.args_to_string((1, 2, 3), {'d': 4}) == '1, 2, 3, d=4'


@pytest.mark.parametrize(
    'input,output', [(timedelta(days=1), 86_400_000), (42, 42000), (42.123, 42123), (42.123_987, 42124), (None, None)]
)
def test_to_ms(input, output):
    assert arq.utils.to_ms(input) == output


@pytest.mark.parametrize('input,output', [(timedelta(days=1), 86400), (42, 42), (42.123, 42.123), (None, None)])
def test_to_seconds(input, output):
    assert arq.utils.to_seconds(input) == output


def test_typing():
    assert 'OptionType' in arq.typing.__all__


def test_redis_settings_validation():
    class Settings(BaseModel):
        redis_settings: RedisSettings

        @validator('redis_settings', always=True, pre=True)
        def parse_redis_settings(cls, v):
            if isinstance(v, str):
                return RedisSettings.from_dsn(v)
            else:
                return v

    s1 = Settings(redis_settings='redis://foobar:123/4')
    assert s1.redis_settings.host == 'foobar'
    assert s1.redis_settings.host == 'foobar'
    assert s1.redis_settings.port == 123
    assert s1.redis_settings.database == 4
    assert s1.redis_settings.ssl is False

    s2 = Settings(redis_settings={'host': 'testing.com'})
    assert s2.redis_settings.host == 'testing.com'
    assert s2.redis_settings.port == 6379

    with pytest.raises(ValueError, match='instance of SSLContext expected'):
        Settings(redis_settings={'ssl': 123})

    s3 = Settings(redis_settings={'ssl': True})
    assert s3.redis_settings.host == 'localhost'
    assert s3.redis_settings.ssl is True

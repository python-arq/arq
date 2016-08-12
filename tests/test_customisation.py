import json
from datetime import datetime, timedelta, timezone

import pytest
import pytz

from arq.jobs import DatetimeJob, JobSerialisationError, Job
from arq.worker import BaseWorker

from .fixtures import Worker, CustomSettings, TestActor


class DatetimeActor(TestActor):
    job_class = DatetimeJob


class DatetimeWorker(BaseWorker):
    job_class = DatetimeJob
    shadows = [DatetimeActor]


async def test_custom_settings(actor, redis_conn):
    await actor.store_info()

    settings = CustomSettings()
    worker = Worker(loop=actor.loop, burst=True, settings=settings)
    await worker.run()
    info = await redis_conn.get(b'actor_info')
    info = info.decode()
    info = json.loads(info)
    assert info['is_shadow'] is True
    assert info['settings']['data']['X_THING'] == 2

    await worker.close()


async def test_bad_encoder(loop):
    actor = TestActor(loop=loop)
    with pytest.raises(JobSerialisationError):
        await actor.save_values(datetime.now())
    await actor.close()


async def test_bad_encoder_dt(loop):
    actor = DatetimeActor(loop=loop)
    with pytest.raises(JobSerialisationError):
        await actor.subtract(datetime)
    await actor.close()


async def test_encode_datetimes(tmpworkdir, loop, redis_conn):
    actor = DatetimeActor(loop=loop)
    d1 = datetime(2032, 2, 2, 9, 0)
    d2 = datetime(2032, 1, 3, 9, 0)
    await actor.subtract(d1, d2)
    await actor.close()

    worker = DatetimeWorker(loop=actor.loop, burst=True)
    await worker.run()
    assert worker.jobs_failed == 0
    assert tmpworkdir.join('subtract').exists()
    assert tmpworkdir.join('subtract').read() == '30 days, 0:00:00'

    await worker.close()


async def test_encode_datetimes_tz(tmpworkdir, loop, redis_conn):
    d1 = datetime(2032, 2, 2, 9, 0, tzinfo=timezone(timedelta(seconds=-3600)))
    d2 = datetime(2032, 1, 3, 9, 0, tzinfo=timezone.utc)
    actor = DatetimeActor(loop=loop)
    await actor.subtract(d1, d2)
    await actor.close()

    worker = DatetimeWorker(loop=actor.loop, burst=True)
    await worker.run()
    assert worker.jobs_failed == 0
    assert tmpworkdir.join('subtract').read() == '30 days, 1:00:00'

    await worker.close()


async def test_encode_non_datetimes(tmpworkdir, loop, redis_conn):
    actor = DatetimeActor(loop=loop)
    await actor.save_values({'a': 1}, {'a': 2})
    await actor.close()

    worker = DatetimeWorker(loop=actor.loop, burst=True)
    await worker.run()
    assert worker.jobs_failed == 0
    assert tmpworkdir.join('values').exists()
    assert tmpworkdir.join('values').read() == "<{'a': 1}>, <{'a': 2}>"
    await worker.close()


async def test_wrong_job_class(loop):
    worker = DatetimeWorker(loop=loop, burst=True, shadows=[TestActor, TestActor, DatetimeActor])
    with pytest.raises(TypeError) as excinfo:
        await worker.run()
    assert excinfo.value.args[0].endswith("has a different job class to the first shadow: "
                                          "<class 'arq.jobs.DatetimeJob'> != <class 'arq.jobs.Job'>")
    await worker.close()


async def test_switch_job_class(loop):
    worker = DatetimeWorker(loop=loop, burst=True, shadows=[TestActor])
    assert worker.job_class is None
    await worker.run()
    assert worker.job_class == Job
    await worker.close()


def test_naïve_dt_encoding():
    t = datetime(2000, 1, 1)
    assert str(t) == '2000-01-01 00:00:00'
    p = DatetimeJob._encode(t)
    t2 = DatetimeJob._decode(p)
    assert t == t2
    assert str(t2) == '2000-01-01 00:00:00'


def test_utc_dt_encoding():
    t = datetime(2000, 1, 1, tzinfo=timezone.utc)
    assert str(t) == '2000-01-01 00:00:00+00:00'
    p = DatetimeJob._encode(t)
    t2 = DatetimeJob._decode(p)
    assert t == t2
    assert str(t2) == '2000-01-01 00:00:00+00:00'


def test_new_york_dt_encoding():
    t = datetime(2000, 1, 1, tzinfo=timezone(timedelta(hours=-5)))
    assert str(t) == '2000-01-01 00:00:00-05:00'
    p = DatetimeJob._encode(t)
    t2 = DatetimeJob._decode(p)
    assert t == t2
    assert str(t2) == '2000-01-01 00:00:00-05:00'


def test_pytz_new_york_dt_encoding():
    ny = pytz.timezone('America/New_York')
    t = ny.localize(datetime(2000, 1, 1))
    assert str(t) == '2000-01-01 00:00:00-05:00'
    p = DatetimeJob._encode(t)
    t2 = DatetimeJob._decode(p)
    assert t == t2
    assert datetime(2000, 1, 1, tzinfo=timezone(timedelta(hours=-5))) == t2
    assert str(t2) == '2000-01-01 00:00:00-05:00'


def test_dt_encoding_with_ms():
    t = datetime(2000, 1, 1, 0, 0, 0, 123000)
    assert str(t) == '2000-01-01 00:00:00.123000'
    p = DatetimeJob._encode(t)
    t2 = DatetimeJob._decode(p)
    assert t == t2
    assert str(t2) == '2000-01-01 00:00:00.123000'


def test_dt_encoding_with_μs():
    t = datetime(2000, 1, 1, 0, 0, 0, 123456)
    assert str(t) == '2000-01-01 00:00:00.123456'
    p = DatetimeJob._encode(t)
    t2 = DatetimeJob._decode(p)
    assert t != t2
    assert (t - t2) == timedelta(microseconds=456)
    assert str(t2) == '2000-01-01 00:00:00.123000'

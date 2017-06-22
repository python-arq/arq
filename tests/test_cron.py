import pickle
from datetime import datetime

from arq.utils import to_unix_ms

from .fixtures import CronWorker

datetimes = [
    datetime(2032, 1, 1),  # init one cron func
    datetime(2032, 1, 1),  # init the other cron func
    datetime(2032, 1, 1),  # init the third cron func
    datetime(2032, 1, 1, 3, 0, 1),
    datetime(2032, 1, 1, 3, 0, 1),
    datetime(2032, 1, 1, 3, 0, 1),
    datetime(2032, 1, 1, 3, 0, 1),
]


async def test_run_cron_start(tmpworkdir, redis_conn, actor, debug):
    worker = CronWorker(burst=True, loop=actor.loop)

    assert not tmpworkdir.join('foobar').exists()
    assert not tmpworkdir.join('spam').exists()
    await worker.run()
    assert tmpworkdir.join('foobar').exists()
    assert tmpworkdir.join('foobar').read() == 'foobar the value'
    assert worker.jobs_failed == 0
    assert not tmpworkdir.join('spam').exists()
    shadow = worker._shadow_lookup['CronActor']
    assert repr(shadow.save_foobar).startswith('<cron function CronActor.save_foobar of <CronActor(CronActor) at ')


async def test_cron_time_match(tmpworkdir, redis_conn, actor):
    with open('datatime.pkl', 'wb') as f:
        pickle.dump(datetimes, f)

    worker = CronWorker(burst=True, loop=actor.loop)

    assert not tmpworkdir.join('spam').exists()
    await worker.run()
    assert tmpworkdir.join('spam').exists()


async def test_cron_time_match_sentinel_set(tmpworkdir, redis_conn, actor):
    with open('datatime.pkl', 'wb') as f:
        pickle.dump(datetimes, f)

    v = str(to_unix_ms(datetime(2032, 1, 1, 3, 0, 0, 123456))[0]).encode()
    await redis_conn.set(b'arq:cron:CronActor.save_spam', v)

    worker = CronWorker(burst=True, loop=actor.loop)

    assert not tmpworkdir.join('spam').exists()
    await worker.run()
    assert not tmpworkdir.join('spam').exists()


async def test_cron_time_match_not_unique(tmpworkdir, redis_conn, actor):
    with open('datatime.pkl', 'wb') as f:
        pickle.dump(datetimes, f)

    v = str(to_unix_ms(datetime(2032, 1, 1, 3, 0, 0, 123456))[0]).encode()
    await redis_conn.set(b'arq:cron:CronActor.save_not_unique', v)

    worker = CronWorker(burst=True, loop=actor.loop)

    assert not tmpworkdir.join('not_unique').exists()
    await worker.run()
    assert tmpworkdir.join('not_unique').exists()

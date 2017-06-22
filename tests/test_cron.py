from .fixtures import CronWorker


async def test_run_cron_start(tmpworkdir, redis_conn, actor, debug):
    worker = CronWorker(burst=True, loop=actor.loop)

    assert not tmpworkdir.join('foobar').exists()
    assert not tmpworkdir.join('spam').exists()
    await worker.run()
    assert tmpworkdir.join('foobar').exists()
    assert tmpworkdir.join('foobar').read() == 'foobar the value'
    assert worker.jobs_failed == 0


async def test_run_cron_start_sentinel_set(tmpworkdir, redis_conn, actor):
    worker = CronWorker(burst=True, loop=actor.loop)

    await redis_conn.set(b'arq:cron:CronActor.save_foobar', b'1')
    assert not tmpworkdir.join('foobar').exists()
    await worker.run()
    assert not tmpworkdir.join('foobar').exists()
    assert worker.jobs_failed == 0

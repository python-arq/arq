import re
import logging

import pytest

from arq.worker import import_string, start_worker

from .fixtures import Worker, MockRedisDemo, EXAMPLE_FILE, WorkerQuit
from .example import ActorTest


async def test_run_job(tmpworkdir, redis_conn, demo):
    worker = Worker(batch_mode=True, loop=demo.loop)

    await demo.add_numbers(1, 2)
    assert not tmpworkdir.join('add_numbers').exists()
    await worker.run()
    assert tmpworkdir.join('add_numbers').read() == '3'


async def test_long_args(mock_demo_worker, logcap):
    demo, worker = mock_demo_worker
    v = ','.join(map(str, range(20)))
    await demo.concat(a=v, b=v)
    await worker.run()
    log = re.sub('0.0\d\ds', '0.0XXs', logcap.log)
    assert ('dft  queued  0.0XXs → MockRedisDemo.concat'
            '(a=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19, b=0,1,2,3,4,5,6,7,8,9,10...)\n') in log, log
    assert ('dft  ran in  0.0XXs ← MockRedisDemo.concat ● '
            '0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 + 0,1,2,3,4,5,6,7,8,9,10,11...\n') in log, log


async def test_wrong_worker(mock_demo_worker, logcap):
    demo, worker = mock_demo_worker
    demo2 = MockRedisDemo(name='missing')
    demo2.mock_data = worker.mock_data
    assert None is await demo2.concat('a', 'b')
    await worker.run()
    assert worker.jobs_failed == 1
    print(logcap)
    assert 'Job Error: unable to find shadow for <Job missing.concat(a, b) on dft>' in logcap.log


def test_import_string_good(tmpworkdir):
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    attr = import_string('test.py', 'Worker')
    assert attr.__name__ == 'Worker'
    assert attr.signature == 'foobar'


def test_import_string_missing_attr(tmpworkdir):
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    with pytest.raises(ImportError):
        import_string('test.py', 'wrong')


def test_import_string_missing_file(tmpworkdir):
    with pytest.raises(ImportError):
        import_string('test.py', 'wrong')


async def test_import_start_worker(tmpworkdir, redis_conn, loop):
    actor = ActorTest(loop=loop)
    await actor.foo(1, 2)
    await actor.close()

    assert await redis_conn.exists(b'arq:q:dft')
    dft_queue = await redis_conn.lrange(b'arq:q:dft', 0, -1)
    assert len(dft_queue) == 1
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    start_worker('test.py', 'Worker', True)
    assert tmpworkdir.join('add_numbers').read() == '3'


async def test_run_4_quit(tmpworkdir, redis_conn, demo, logcap):
    logcap.set_level(logging.DEBUG)
    worker = WorkerQuit(loop=demo.loop)

    await demo.save_slow(1, 0.1)
    await demo.save_slow(2, 0.1)
    await demo.save_slow(3, 0.1)
    await demo.save_slow(4, 0.1)
    assert not tmpworkdir.join('save_slow').exists()
    await worker.run()
    print(logcap)
    assert tmpworkdir.join('save_slow').read() == '3'
    assert '1 pending tasks, waiting for one to finish before creating task for Demo.save_slow(2, 0.1)' in logcap.log

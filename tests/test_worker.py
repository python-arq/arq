import os
import re

from .fixtures import Worker


async def test_run_job(tmpworkdir, redis_conn, demo):
    worker = Worker(batch_mode=True, loop=demo.loop)

    assert None is await demo.add_numbers(1, 2)
    assert not os.path.exists('add_numbers')
    await worker.run()
    assert os.path.exists('add_numbers')

    with open('add_numbers') as f:
        assert f.read() == '3'
    await demo.close()


async def test_long_args(mock_demo_worker, logcap):
    demo, worker = mock_demo_worker
    v = ','.join(map(str, range(20)))
    assert None is await demo.concat(a=v, b=v)
    await worker.run()
    log = re.sub('0.0\d\ds', '0.0XXs', logcap.log)
    assert ('dft  queued  0.0XXs → MockRedisDemo.concat'
            '(a=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19, b=0,1,2,3,4,5,6,7,8,9,10...)\n') in log, log
    assert ('dft  ran in  0.0XXs ← MockRedisDemo.concat ● '
            '0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 + 0,1,2,3,4,5,6,7,8,9,10,11...\n') in log, log

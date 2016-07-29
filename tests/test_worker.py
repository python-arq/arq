import os
from .fixtures import Worker


async def test_run_job(tmpworkdir, redis_conn, create_demo):
    demo = await create_demo()
    worker = Worker(batch_mode=True, loop=demo.loop)

    assert None is await demo.add_numbers(1, 2)
    assert not os.path.exists('add_numbers')
    await worker.run()
    assert os.path.exists('add_numbers')

    with open('add_numbers') as f:
        assert f.read() == '3'
    await demo.close()

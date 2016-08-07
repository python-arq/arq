import asyncio
from arq import Actor, BaseWorker, concurrent

from jobs import fast_job, generate_big_dict, big_argument_job


class TestActor(Actor):
    @concurrent
    async def fast(self):
        fast_job()

    @concurrent
    async def big_argument(self, v):
        return big_argument_job(v)


class Worker(BaseWorker):
    shadows = [TestActor]


async def start_jobs():
    actor = TestActor()
    for i in range(1000):
        await actor.fast()
        v = generate_big_dict()
        await actor.big_argument(v)
    await actor.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_jobs())

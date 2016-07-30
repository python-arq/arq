import asyncio
from pathlib import Path

from arq import concurrent, Actor, AbstractWorker
from arq.mock_redis import MockRedisMixin


class Demo(Actor):
    @concurrent
    async def add_numbers(self, a, b):
        with open('add_numbers', 'w') as f:
            r = a + b
            f.write('{}'.format(r))

    @concurrent(Actor.HIGH_QUEUE)
    async def high_add_numbers(self, a, b, c=4):
        with open('high_add_numbers', 'w') as f:
            r = a + b + c
            f.write('{}'.format(r))
        return r

    @concurrent
    async def concat(self, a, b):
        return a + ' + ' + b

    @concurrent
    async def boom(self):
        raise RuntimeError('boom')

    @concurrent
    async def save_slow(self, v, sleep_for=1):
        await asyncio.sleep(sleep_for, loop=self.loop)
        with open('save_slow', 'w') as f:
            f.write(str(v))


class MockRedisDemo(MockRedisMixin, Demo):
    pass


class Worker(AbstractWorker):
    async def shadow_factory(self):
        return {Demo(loop=self.loop)}


class WorkerQuit(AbstractWorker):
    """
    worker which stops taking new jobs after 2 jobs
    """
    max_concurrent_tasks = 1
    async def shadow_factory(self):
        return {Demo(loop=self.loop)}

    def job_callback(self, task):
        super().job_callback(task)
        if self.jobs_complete >= 2:
            self.running = False


class MockRedisWorker(MockRedisMixin, AbstractWorker):
    async def shadow_factory(self):
        return {MockRedisDemo()}


with Path(__file__).resolve().parent.joinpath('example.py').open() as f:
    EXAMPLE_FILE = f.read()

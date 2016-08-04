import asyncio
from pathlib import Path

from arq import concurrent, Actor, BaseWorker
from arq.testing import MockRedisMixin


class TestActor(Actor):
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
    async def sleeper(self, t):
        await asyncio.sleep(t, loop=self.loop)
        return t

    @concurrent
    async def save_slow(self, v, sleep_for=0):
        await asyncio.sleep(sleep_for, loop=self.loop)
        with open('save_slow', 'w') as f:
            f.write(str(v))

    async def direct_method(self, a, b):
        return a + b


class MockRedisTestActor(MockRedisMixin, TestActor):
    pass


class Worker(BaseWorker):
    shadows = [TestActor]


class WorkerQuit(Worker):
    """
    worker which stops taking new jobs after 2 jobs
    """
    max_concurrent_tasks = 1

    def job_callback(self, task):
        super().job_callback(task)
        if self.jobs_complete >= 2:
            self.running = False


class WorkerFail(Worker):
    async def run_job(self, j):
        raise RuntimeError('foobar')


class MockRedisWorker(MockRedisMixin, BaseWorker):
    shadows = [MockRedisTestActor]


class MockRedisWorkerQuit(MockRedisWorker):
    def job_callback(self, task):
        super().job_callback(task)
        self.running = False


class FoobarActor(MockRedisTestActor):
    name = 'foobar'


with Path(__file__).resolve().parent.joinpath('example.py').open() as f:
    EXAMPLE_FILE = f.read()

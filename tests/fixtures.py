import asyncio
import json
import os
import signal
import time
from pathlib import Path

from arq import Actor, BaseWorker, StopJob, concurrent
from arq.drain import Drain
from arq.testing import MockRedisMixin


class DemoActor(Actor):
    @concurrent
    async def add_numbers(self, a, b):
        """add_number docs"""
        with open('add_numbers', 'w') as f:
            r = a + b
            f.write('{}'.format(r))

    @concurrent
    async def subtract(self, a, b):
        with open('subtract', 'w') as f:
            try:
                r = a - b
            except TypeError as e:
                r = str(e)
            f.write('{}'.format(r))

    @concurrent
    async def save_values(self, *args):
        with open('values', 'w') as f:
            r = ', '.join('<{}>'.format(arg) for arg in args)
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

    @concurrent
    async def store_info(self, key_suffix=''):
        data = {
            'self': str(self),
            'class': self.__class__.__name__,
            'is_shadow': self.is_shadow,
            'loop': str(self.loop),
            'settings': {
                'data': dict(self.settings),
                'class': self.settings.__class__.__name__
            },
        }
        async with await self.get_redis_conn() as redis:
            await redis.set('actor_info' + key_suffix, json.dumps(data, indent=2))
        return data

    @concurrent
    async def stop_job_normal(self):
        raise StopJob('stopping job normally')

    @concurrent
    async def stop_job_warning(self):
        raise StopJob('stopping job with warning', warning=True)


class MockRedisDemoActor(MockRedisMixin, DemoActor):
    pass


class Worker(BaseWorker):
    shadows = [DemoActor]


class StartupActor(Actor):
    async def startup(self):
        with open('events', 'a') as f:
            f.write('startup[{}],'.format(self.is_shadow))

    @concurrent
    async def concurrent_func(self, v):
        with open('events', 'a') as f:
            f.write('concurrent_func[{}],'.format(v))

    async def shutdown(self):
        with open('events', 'a') as f:
            f.write('shutdown[{}],'.format(self.is_shadow))


class StartupWorker(BaseWorker):
    shadows = [DemoActor, StartupActor]


class FastShutdownWorker(BaseWorker):
    shadows = [DemoActor]
    shutdown_delay = 0.1


class DrainQuit2(Drain):
    def _job_callback(self, task):
        super()._job_callback(task)
        if self.jobs_complete >= 2:
            self.running = False


class WorkerQuit(Worker):
    """
    worker which stops taking new jobs after 2 jobs
    """
    max_concurrent_tasks = 1
    drain_class = DrainQuit2


class WorkerFail(Worker):
    async def run_job(self, j):
        raise RuntimeError('foobar')


class MockRedisWorker(MockRedisMixin, BaseWorker):
    shadows = [MockRedisDemoActor]


class DrainQuitImmediate(Drain):
    def _job_callback(self, task):
        super()._job_callback(task)
        self.running = False


class MockRedisWorkerQuit(MockRedisWorker):
    drain_class = DrainQuitImmediate


class FoobarActor(MockRedisDemoActor):
    name = 'foobar'


def kill_parent():
    time.sleep(0.5)
    os.kill(os.getppid(), signal.SIGTERM)


with Path(__file__).resolve().parent.joinpath('example.py').open() as f:
    EXAMPLE_FILE = f.read()


class ParentActor(MockRedisMixin, Actor):
    v = 'Parent'

    @concurrent
    async def save_value(self, file_name):
        with open(file_name, 'w') as f:
            f.write(self.v)


class ChildActor(ParentActor):
    v = 'Child'


class ParentChildActorWorker(MockRedisMixin, BaseWorker):
    shadows = [ParentActor, ChildActor]

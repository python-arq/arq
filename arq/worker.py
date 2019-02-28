import asyncio
import logging
import pickle
import signal
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from signal import Signals
from typing import Callable, Optional, Union, Sequence, Dict

import async_timeout
from aioredis import MultiExecError
from pydantic.utils import import_string

from .connections import RedisSettings, ArqRedis, create_pool, log_redis_info
from .utils import timestamp, poll, timedelta_to_ms
from .constants import (
    queue_name, in_progress_key_prefix, result_key_prefix, job_key_prefix, default_timeout,
    default_max_jobs, default_keep_result
)

logger = logging.getLogger('arq.worker')
no_result = object()
IntTimedelta = Union[int, timedelta]


@dataclass
class Function:
    name: str
    coroutine: Callable
    keep_result: Optional[int]
    timeout: Optional[int]


def func(coroutine: Callable, keep_result: Optional[IntTimedelta] = None, timeout: Optional[IntTimedelta] = None) -> Function:
    if isinstance(coroutine, Function):
        return coroutine

    if isinstance(coroutine, str):
        coroutine = import_string(coroutine)

    timeout = timedelta_to_ms(timeout)
    keep_result = timedelta_to_ms(keep_result)

    name = coroutine.__qualname__
    return Function(name, coroutine, keep_result, timeout)


class RetryJob(RuntimeError):
    __slots__ = ('defer_score',)

    def __init__(self, defer: Union[None, int, timedelta] = None):
        if isinstance(defer, timedelta):
            self.defer_score = timedelta_to_ms(defer)
        else:
            self.defer_score = defer

    def __repr__(self):
        return f'<RetryJob defer {self.defer_score or 0}ms>'


class Worker:
    def __init__(self, functions: Dict[str, Function], *, settings: RedisSettings,
                 max_jobs: int = default_max_jobs, job_timeout: IntTimedelta = default_timeout,
                 keep_result: IntTimedelta = default_keep_result):
        self.functions = functions
        self.settings = settings
        self.sem = asyncio.BoundedSemaphore(max_jobs)
        self.job_timeout = timedelta_to_ms(job_timeout)
        self.keep_result = timedelta_to_ms(keep_result)
        self.pool = None
        self.tasks = []
        self.main_task = None
        self.loop = asyncio.get_event_loop()
        self.ctx = {}

    def run(self):
        self._add_signal_handler(signal.SIGINT, self.handle_sig)
        self._add_signal_handler(signal.SIGTERM, self.handle_sig)
        self.main_task = self.loop.create_task(self._run())
        try:
            self.loop.run_until_complete(self.main_task)
        except asyncio.CancelledError:
            self.loop.run_until_complete(asyncio.gather(*self.tasks))

    async def _run(self):
        self.pool: ArqRedis = await create_pool(self.settings)
        logger.info('Starting worker')
        await log_redis_info(self.pool, logger.info)

        async for _ in poll():
            async with self.sem:  # don't both with zrangebyscore until we have "space" to run the jobs
                now = timestamp()
                job_ids = await self.pool.zrangebyscore(queue_name, max=now, withscores=True)
            await self._run_jobs(job_ids)

            # required to make sure errors in run_job get propagated
            for t in self.tasks:
                if t.done():
                    self.tasks.remove(t)
                    t.result()

    async def _run_jobs(self, job_ids):
        for job_id, score in job_ids:
            await self.sem.acquire()
            with await self.pool as conn:
                ongoing_key = in_progress_key_prefix + job_id
                r = await asyncio.gather(
                    conn.unwatch(),
                    conn.watch(ongoing_key),
                    conn.exists(ongoing_key),
                )
                if r[2]:
                    # job already started elsewhere
                    self.sem.release()
                    continue

                tr = conn.multi_exec()
                tr.setex(ongoing_key, 3600, b'1')
                try:
                    await tr.execute()
                except MultiExecError:
                    # job already started elsewhere since we got 'existing'
                    self.sem.release()
                else:
                    self.tasks.append(asyncio.create_task(self._run_job(job_id)))

    async def _run_job(self, job_id):
        v = await self.pool.get(job_key_prefix + job_id, encoding=None)
        if not v:
            print('job expired', job_id)
            return await self._abort_job(job_id)

        enqueue_time_ms, defer_ms, function_name, args, kwargs = pickle.loads(v)

        try:
            function: Function = self.functions[function_name]
        except KeyError:
            print(f'job not found "{function_name}"', job_id)
            return await self._abort_job(job_id)

        print(f'running job {job_id}...', function_name, args, kwargs)

        result = no_result
        finish = False
        timeout = self.job_timeout if function.timeout is None else function.timeout
        incr_score = None
        start_time_ms = timestamp()
        try:
            async with async_timeout.timeout(timeout / 1000):
                result = await function.coroutine(self.ctx, *args, **kwargs)
        except asyncio.CancelledError:
            # job got cancelled while running, needs to be run again
            print(f'job {job_id} cancelled, will be run again')
        except RetryJob as e:
            print('retry job')
            incr_score = e.defer_score
        except Exception as e:
            result = e
            print(f'job {job_id} failed')
            finish = True
        else:
            print(f'job {job_id} complete')
            finish = True

        keep_result = self.keep_result if function.keep_result is None else function.keep_result
        result_data = None
        if result is not no_result and keep_result > 0:
            finish_time_ms = timestamp()
            result_data = pickle.dumps(
                (enqueue_time_ms, defer_ms, function, args, kwargs, result, start_time_ms, finish_time_ms)
            )

        with await self.pool as conn:
            await conn.unwatch()
            tr = conn.multi_exec()
            tr.delete(in_progress_key_prefix + job_id)
            if finish:
                if result_data:
                    tr.psetex(result_key_prefix + job_id, keep_result, result_data)
                tr.delete(job_key_prefix + job_id)
                tr.zrem(queue_name, job_id)
            elif incr_score:
                tr.zincrby(queue_name, incr_score, job_id)
            await tr.execute()
        self.sem.release()

    async def _abort_job(self, job_id):
        with await self.pool as conn:
            await conn.unwatch()
            tr = conn.multi_exec()
            tr.delete(in_progress_key_prefix + job_id)
            tr.zrem(queue_name, job_id)
            await tr.execute()

    def _add_signal_handler(self, signal, handler):
        self.loop.add_signal_handler(signal, partial(handler, signal))

    def handle_sig(self, signum):
        logger.info('pid=%d, got signal: %s, stopping...', Signals(signum).name)
        if self.tasks:
            print(f'\ncancelling {len(self.tasks)} ongoing jobs')
            for t in self.tasks:
                if not t.done():
                    t.cancel()
        self.main_task and self.main_task.cancel()


def run_worker(functions: Sequence[Function], *, settings: RedisSettings = None, **kwargs):
    functions = {f.name: f for f in map(func, functions)}
    Worker(functions, settings=settings or RedisSettings(), **kwargs).run()

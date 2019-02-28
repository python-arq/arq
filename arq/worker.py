import asyncio
import logging
import pickle
import signal
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from signal import Signals
from typing import Callable, Optional, Union, Sequence, List, Type, Dict, Awaitable

import async_timeout
from aioredis import MultiExecError
from pydantic import BaseSettings
from pydantic.utils import import_string

from .connections import RedisSettings, ArqRedis, create_pool, log_redis_info
from .utils import timestamp, poll, timedelta_to_ms, args_to_string, truncate
from .constants import (
    queue_name, in_progress_key_prefix, result_key_prefix, job_key_prefix, default_timeout,
    default_max_jobs, default_keep_result, default_max_tries,
    retry_key_prefix)

logger = logging.getLogger('arq.worker')
no_result = object()
IntTimedelta = Union[int, timedelta]


@dataclass
class Function:
    name: str
    coroutine: Callable
    keep_result: Optional[int]
    timeout: Optional[int]
    max_tries: Optional[int]


def func(coroutine: Callable, keep_result: Optional[IntTimedelta] = None, timeout: Optional[IntTimedelta] = None,
         max_tries: Optional[int] = None) -> Function:
    if isinstance(coroutine, Function):
        return coroutine

    if isinstance(coroutine, str):
        coroutine = import_string(coroutine)

    timeout = timedelta_to_ms(timeout)
    keep_result = timedelta_to_ms(keep_result)

    name = coroutine.__qualname__
    return Function(name, coroutine, keep_result, timeout, max_tries)


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
    def __init__(self, functions: Sequence[Function], *, redis_settings: RedisSettings = None,
                 on_startup: List[Callable[[Dict], Awaitable]] = None,
                 on_shutdown: List[Callable[[Dict], Awaitable]] = None,
                 max_jobs: int = default_max_jobs, job_timeout: IntTimedelta = default_timeout,
                 keep_result: IntTimedelta = default_keep_result, max_tries: int = default_max_tries):
        self.functions = {f.name: f for f in map(func, functions)}
        self.redis_settings = redis_settings or RedisSettings
        self.on_startup = on_startup or []
        self.on_shutdown = on_shutdown or []
        self.sem = asyncio.BoundedSemaphore(max_jobs)
        self.job_timeout = timedelta_to_ms(job_timeout)
        self.keep_result = timedelta_to_ms(keep_result)
        self.max_tries = max_tries
        self.pool = None
        self.tasks = []
        self.main_task = None
        self.loop = asyncio.get_event_loop()
        self.ctx = None
        self.in_progress_timeout = max(f.timeout or self.job_timeout for f in self.functions.values()) + 10_000
        self.jobs_complete = 0
        self.jobs_retried = 0
        self.jobs_failed = 0

    def run(self):
        self._add_signal_handler(signal.SIGINT, self.handle_sig)
        self._add_signal_handler(signal.SIGTERM, self.handle_sig)
        self.main_task = self.loop.create_task(self._run())
        try:
            self.loop.run_until_complete(self.main_task)
        except asyncio.CancelledError:
            self.loop.run_until_complete(asyncio.gather(*self.tasks))
            for f in self.on_shutdown:
                self.loop.run_until_complete(f(self.ctx))

    async def _run(self):
        self.pool: ArqRedis = await create_pool(self.redis_settings)
        logger.info('Starting worker for %d functions: %s', len(self.functions), ', '.join(self.functions))
        await log_redis_info(self.pool, logger.info)
        self.ctx = {'redis': self.pool}
        for f in self.on_startup:
            await f(self.ctx)

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
                in_progress_key = in_progress_key_prefix + job_id
                _, _, ongoing_exists, in_queue = await asyncio.gather(
                    conn.unwatch(),
                    conn.watch(in_progress_key),
                    conn.exists(in_progress_key),
                    conn.zscore(queue_name, job_id)
                )
                if ongoing_exists or not in_queue:
                    # job already started elsewhere, or already finished and removed from queue
                    self.sem.release()
                    continue

                tr = conn.multi_exec()
                tr.psetex(in_progress_key, self.in_progress_timeout, b'1')
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
            logger.warning('job %s expired', job_id)
            return await asyncio.shield(self._abort_job(job_id))

        enqueue_time_ms, defer_ms, function_name, args, kwargs = pickle.loads(v)

        try:
            function: Function = self.functions[function_name]
        except KeyError:
            logger.warning('job %s, function %r not found', job_id, function_name)
            return await asyncio.shield(self._abort_job(job_id))

        max_tries = self.max_tries if function.max_tries is None else function.max_tries
        job_try = await self.pool.incr(retry_key_prefix + job_id)
        if job_try > max_tries:
            t = (timestamp() - enqueue_time_ms) / 1000
            logger.warning('%6.2fs ! %s:%s max retries %d exceeded', t, job_id, function_name, max_tries)
            return await asyncio.shield(self._abort_job(job_id))

        result = no_result
        finish = False
        timeout = self.job_timeout if function.timeout is None else function.timeout
        incr_score = None
        start_ms = timestamp()
        job_ctx = {'job_try': job_try, 'job_id': job_id}
        ctx = {**self.ctx, **job_ctx}
        try:
            s = args_to_string(args, kwargs)
            tries_text = f' try={job_try}' if job_try > 1 else ''
            logger.info('%6.2fs → %s.%s(%s)%s', (start_ms - enqueue_time_ms) / 1000, job_id, function_name, s,
                        tries_text)
            async with async_timeout.timeout(timeout / 1000):
                result = await function.coroutine(ctx, *args, **kwargs)
            # could raise an exception:
            result_str = '' if result is None else truncate(repr(result))
        except asyncio.CancelledError:
            # job got cancelled while running, needs to be run again
            finished_ms = timestamp()
            t = (finished_ms - start_ms) / 1000
            logger.info('%6.2fs ↻ %s:%s cancelled, will be run again', t, job_id, function_name)
            self.jobs_retried += 1
        except RetryJob as e:
            incr_score = e.defer_score
            finished_ms = timestamp()
            t = (finished_ms - start_ms) / 1000
            logger.info('%6.2fs ↻ %s:%s retrying job in %0.2fs', t, job_id, function_name, (incr_score or 0) / 1000)
            self.jobs_retried += 1
        except Exception as e:
            result = e
            finished_ms = timestamp()
            t = (finished_ms - start_ms) / 1000
            # TODO add extra from exception
            logger.exception('%6.2fs ! %s:%s failed, %s: %s', t, job_id, function_name, e.__class__.__name__, e)
            finish = True
            self.jobs_failed += 1
        else:
            finished_ms = timestamp()
            logger.info('%6.2fs ← %s:%s ● %s', (finished_ms - start_ms) / 1000, job_id, function_name, result_str)
            finish = True
            self.jobs_complete += 1

        result_timeout = self.keep_result if function.keep_result is None else function.keep_result
        result_data = None
        if result is not no_result and result_timeout > 0:
            d = enqueue_time_ms, defer_ms, function, args, kwargs, result, start_ms, finished_ms
            result_data = pickle.dumps(d)

        await asyncio.shield(self.finish_job(job_id, finish, result_data, result_timeout, incr_score))

    async def finish_job(self, job_id, finish, result_data, result_timeout, incr_score):
        with await self.pool as conn:
            await conn.unwatch()
            tr = conn.multi_exec()
            delete_keys = [in_progress_key_prefix + job_id]
            if finish:
                if result_data:
                    tr.psetex(result_key_prefix + job_id, result_timeout, result_data)
                delete_keys += [retry_key_prefix + job_id, job_key_prefix + job_id]
                tr.zrem(queue_name, job_id)
            elif incr_score:
                tr.zincrby(queue_name, incr_score, job_id)
            tr.delete(*delete_keys)
            await tr.execute()
        self.sem.release()

    async def _abort_job(self, job_id):
        with await self.pool as conn:
            await conn.unwatch()
            tr = conn.multi_exec()
            tr.delete(retry_key_prefix + job_id, in_progress_key_prefix + job_id, job_key_prefix + job_id)
            tr.zrem(queue_name, job_id)
            await tr.execute()

    def _add_signal_handler(self, signal, handler):
        self.loop.add_signal_handler(signal, partial(handler, signal))

    def handle_sig(self, signum):
        logger.info('shutdown on %s ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ %d ongoing to cancel',
                    Signals(signum).name, self.jobs_complete, self.jobs_failed, self.jobs_retried, len(self.tasks))
        for t in self.tasks:
            if not t.done():
                t.cancel()
        self.main_task and self.main_task.cancel()


class BaseWorkerSettings(BaseSettings):
    functions: List[Function]
    redis_settings: RedisSettings = None
    on_startup: List[Callable] = None
    on_shutdown: List[Callable] = None
    max_jobs: int = default_max_jobs
    job_timeout: int = default_timeout
    keep_result: int = default_keep_result
    max_tries: int = default_max_tries


def run_worker(settings_cls: Type[BaseWorkerSettings], **kwargs):
    settings = settings_cls(**kwargs)
    worker = Worker(
        functions=settings.functions,
        redis_settings=settings.redis_settings,
        on_startup=settings.on_startup,
        on_shutdown=settings.on_shutdown,
        max_jobs=settings.max_jobs,
        job_timeout=settings.job_timeout,
        keep_result=settings.keep_result,
        max_tries=settings.max_tries,
    )
    worker.run()

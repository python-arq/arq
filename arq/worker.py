import asyncio
import inspect
import logging
import signal
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from signal import Signals
from time import time
from typing import Awaitable, Callable, Dict, List, Optional, Sequence, Union

import async_timeout
from aioredis import MultiExecError
from pydantic.utils import import_string

from arq.cron import CronJob
from arq.jobs import pickle_result, unpickle_job_raw

from .connections import ArqRedis, RedisSettings, create_pool, log_redis_info
from .constants import (
    default_queue_name,
    health_check_key_suffix,
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    retry_key_prefix,
)
from .utils import (
    SecondsTimedelta,
    args_to_string,
    ms_to_datetime,
    poll,
    timestamp_ms,
    to_ms,
    to_seconds,
    to_unix_ms,
    truncate,
)

logger = logging.getLogger('arq.worker')
no_result = object()


@dataclass
class Function:
    name: str
    coroutine: Callable
    timeout_s: Optional[float]
    keep_result_s: Optional[float]
    max_tries: Optional[int]


def func(
    coroutine: Union[str, Function, Callable],
    *,
    name: Optional[str] = None,
    keep_result: Optional[SecondsTimedelta] = None,
    timeout: Optional[SecondsTimedelta] = None,
    max_tries: Optional[int] = None,
) -> Function:
    """
    Wrapper for a job function which lets you configure more settings.

    :param coroutine: coroutine function to call, can be a string to import
    :param name: name for function, if None, ``coroutine.__qualname__`` is used
    :param keep_result: duration to keep the result for, if 0 the result is not kept
    :param timeout: maximum time the job should take
    :param max_tries: maximum number of tries allowed for the function, use 1 to prevent retrying
    """
    if isinstance(coroutine, Function):
        return coroutine

    if isinstance(coroutine, str):
        name = name or coroutine
        coroutine = import_string(coroutine)

    assert asyncio.iscoroutinefunction(coroutine), f'{coroutine} is not a coroutine function'
    timeout = to_seconds(timeout)
    keep_result = to_seconds(keep_result)

    return Function(name or coroutine.__qualname__, coroutine, timeout, keep_result, max_tries)


class Retry(RuntimeError):
    """
    Special exception to retry the job (if ``max_retries`` hasn't been reached).

    :param defer: duration to wait before rerunning the job
    """

    __slots__ = ('defer_score',)

    def __init__(self, defer: Optional[SecondsTimedelta] = None):
        self.defer_score = to_ms(defer)

    def __repr__(self):
        return f'<Retry defer {(self.defer_score or 0) / 1000:0.2f}s>'

    def __str__(self):
        return repr(self)


class FailedJobs(RuntimeError):
    def __init__(self, count, job_results):
        self.count = count
        self.job_results = job_results

    def __str__(self):
        if self.count == 1 and self.job_results:
            exc = self.job_results[0].result
            return f'1 job failed "{exc.__class__.__name__}: {exc}"'
        else:
            return f'{self.count} jobs failed'

    def __repr__(self):
        return f'<{str(self)}>'


class Worker:
    """
    Main class for running jobs.

    :param functions: list of functions to register, can either be raw coroutine functions or the
      result of :func:`arq.worker.func`.
    :param queue_name: queue name to get jobs from
    :param cron_jobs:  list of cron jobs to run, use :func:`arq.cron.cron` to create them
    :param redis_settings: settings for creating a redis connection
    :param redis_pool: existing redis pool, generally None
    :param burst: whether to stop the worker once all jobs have been run
    :param on_startup: coroutine function to run at startup
    :param on_shutdown: coroutine function to run at shutdown
    :param max_jobs: maximum number of jobs to run at a time
    :param job_timeout: default job timeout (max run time)
    :param keep_result: default duration to keep job results for
    :param poll_delay: duration between polling the queue for new jobs
    :param max_tries: default maximum number of times to retry a job
    :param health_check_interval: how often to set the health check key
    :param health_check_key: redis key under which health check is set
    """

    def __init__(
        self,
        functions: Sequence[Function] = (),
        *,
        queue_name: str = default_queue_name,
        cron_jobs: Optional[Sequence[CronJob]] = None,
        redis_settings: RedisSettings = None,
        redis_pool: ArqRedis = None,
        burst: bool = False,
        on_startup: Callable[[Dict], Awaitable] = None,
        on_shutdown: Callable[[Dict], Awaitable] = None,
        max_jobs: int = 10,
        job_timeout: SecondsTimedelta = 300,
        keep_result: SecondsTimedelta = 3600,
        poll_delay: SecondsTimedelta = 0.5,
        max_tries: int = 5,
        health_check_interval: SecondsTimedelta = 3600,
        health_check_key: Optional[str] = None,
        ctx: Optional[Dict] = None,
    ):
        self.functions: Dict[str, Union[Function, CronJob]] = {f.name: f for f in map(func, functions)}
        self.queue_name = queue_name
        self.cron_jobs: List[CronJob] = []
        if cron_jobs:
            assert all(isinstance(cj, CronJob) for cj in cron_jobs), 'cron_jobs, must be instances of CronJob'
            self.cron_jobs = cron_jobs
            self.functions.update({cj.name: cj for cj in self.cron_jobs})
        assert len(self.functions) > 0, 'at least one function or cron_job must be registered'
        self.burst = burst
        self.on_startup = on_startup
        self.on_shutdown = on_shutdown
        self.sem = asyncio.BoundedSemaphore(max_jobs)
        self.job_timeout_s = to_seconds(job_timeout)
        self.keep_result_s = to_seconds(keep_result)
        self.poll_delay_s = to_seconds(poll_delay)
        self.max_tries = max_tries
        self.health_check_interval = to_seconds(health_check_interval)
        if health_check_key is None:
            self.health_check_key = self.queue_name + health_check_key_suffix
        else:
            self.health_check_key = health_check_key
        self.pool = redis_pool
        if self.pool is None:
            self.redis_settings = redis_settings or RedisSettings()
        else:
            self.redis_settings = None
        self.tasks = []
        self.main_task = None
        self.loop = asyncio.get_event_loop()
        self.ctx = ctx or {}
        max_timeout = max(f.timeout_s or self.job_timeout_s for f in self.functions.values())
        self.in_progress_timeout_s = max_timeout + 10
        self.jobs_complete = 0
        self.jobs_retried = 0
        self.jobs_failed = 0
        self._last_health_check = 0
        self._last_health_check_log = None
        self._add_signal_handler(signal.SIGINT, self.handle_sig)
        self._add_signal_handler(signal.SIGTERM, self.handle_sig)
        self.on_stop = None

    def run(self) -> None:
        """
        Sync function to run the worker, finally closes worker connections.
        """
        self.main_task = self.loop.create_task(self.main())
        try:
            self.loop.run_until_complete(self.main_task)
        except asyncio.CancelledError:
            # happens on shutdown, fine
            pass
        finally:
            self.loop.run_until_complete(self.close())

    async def async_run(self) -> None:
        """
        Asynchronously run the worker, does not close connections. Useful when testing.
        """
        self.main_task = self.loop.create_task(self.main())
        await self.main_task

    async def run_check(self) -> int:
        """
        Run :func:`arq.worker.Worker.async_run`, check for failed jobs and raise :class:`arq.worker.FailedJobs`
        if any jobs have failed.

        :return: number of completed jobs
        """
        await self.async_run()
        if self.jobs_failed:
            failed_job_results = [r for r in await self.pool.all_job_results() if not r.success]
            raise FailedJobs(self.jobs_failed, failed_job_results)
        else:
            return self.jobs_complete

    async def main(self):
        if self.pool is None:
            self.pool = await create_pool(self.redis_settings)

        logger.info('Starting worker for %d functions: %s', len(self.functions), ', '.join(self.functions))
        await log_redis_info(self.pool, logger.info)
        self.ctx['redis'] = self.pool
        if self.on_startup:
            await self.on_startup(self.ctx)

        async for _ in poll(self.poll_delay_s):  # noqa F841
            async with self.sem:  # don't both with zrangebyscore until we have "space" to run the jobs
                now = timestamp_ms()
                job_ids = await self.pool.zrangebyscore(self.queue_name, max=now)
            await self.run_jobs(job_ids)

            # required to make sure errors in run_job get propagated
            for t in self.tasks:
                if t.done():
                    self.tasks.remove(t)
                    t.result()

            await self.heart_beat()

            if self.burst:
                queued_jobs = await self.pool.zcard(self.queue_name)
                if queued_jobs == 0:
                    return

    async def run_jobs(self, job_ids):
        for job_id in job_ids:
            await self.sem.acquire()
            in_progress_key = in_progress_key_prefix + job_id
            with await self.pool as conn:
                _, _, ongoing_exists, score = await asyncio.gather(
                    conn.unwatch(),
                    conn.watch(in_progress_key),
                    conn.exists(in_progress_key),
                    conn.zscore(self.queue_name, job_id),
                )
                if ongoing_exists or not score:
                    # job already started elsewhere, or already finished and removed from queue
                    self.sem.release()
                    continue

                tr = conn.multi_exec()
                tr.setex(in_progress_key, self.in_progress_timeout_s, b'1')
                try:
                    await tr.execute()
                except MultiExecError:
                    # job already started elsewhere since we got 'existing'
                    self.sem.release()
                else:
                    self.tasks.append(self.loop.create_task(self.run_job(job_id, score)))

    async def run_job(self, job_id, score):  # noqa: C901
        v, job_try, _ = await asyncio.gather(
            self.pool.get(job_key_prefix + job_id, encoding=None),
            self.pool.incr(retry_key_prefix + job_id),
            self.pool.expire(retry_key_prefix + job_id, 88400),
        )
        if not v:
            logger.warning('job %s expired', job_id)
            self.jobs_failed += 1
            return await asyncio.shield(self.abort_job(job_id))

        function_name, args, kwargs, enqueue_job_try, enqueue_time_ms = unpickle_job_raw(v)

        try:
            function: Union[Function, CronJob] = self.functions[function_name]
        except KeyError:
            logger.warning('job %s, function %r not found', job_id, function_name)
            self.jobs_failed += 1
            return await asyncio.shield(self.abort_job(job_id))

        if hasattr(function, 'next_run'):
            # cron_job
            ref = function_name
        else:
            ref = f'{job_id}:{function_name}'

        if enqueue_job_try and enqueue_job_try > job_try:
            job_try = enqueue_job_try
            await self.pool.setex(retry_key_prefix + job_id, 88400, str(job_try))

        max_tries = self.max_tries if function.max_tries is None else function.max_tries
        if job_try > max_tries:
            t = (timestamp_ms() - enqueue_time_ms) / 1000
            logger.warning('%6.2fs ! %s max retries %d exceeded', t, ref, max_tries)
            self.jobs_failed += 1
            return await asyncio.shield(self.abort_job(job_id))

        result = no_result
        exc_extra = None
        finish = False
        timeout_s = self.job_timeout_s if function.timeout_s is None else function.timeout_s
        incr_score = None
        job_ctx = {
            'job_id': job_id,
            'job_try': job_try,
            'enqueue_time': ms_to_datetime(enqueue_time_ms),
            'score': score,
        }
        ctx = {**self.ctx, **job_ctx}
        start_ms = timestamp_ms()
        success = False
        try:
            s = args_to_string(args, kwargs)
            extra = f' try={job_try}' if job_try > 1 else ''
            if (start_ms - score) > 1200:
                extra += f' delayed={(start_ms - score) / 1000:0.2f}s'
            logger.info('%6.2fs → %s(%s)%s', (start_ms - enqueue_time_ms) / 1000, ref, s, extra)
            # run repr(result) and extra inside try/except as they can raise exceptions
            try:
                async with async_timeout.timeout(timeout_s):
                    result = await function.coroutine(ctx, *args, **kwargs)
            except Exception as e:
                exc_extra = getattr(e, 'extra', None)
                if callable(exc_extra):
                    exc_extra = exc_extra()
                raise
            else:
                result_str = '' if result is None else truncate(repr(result))
        except Exception as e:
            finished_ms = timestamp_ms()
            t = (finished_ms - start_ms) / 1000
            if isinstance(e, Retry):
                incr_score = e.defer_score
                logger.info('%6.2fs ↻ %s retrying job in %0.2fs', t, ref, (e.defer_score or 0) / 1000)
                if e.defer_score:
                    incr_score = e.defer_score + (timestamp_ms() - score)
                self.jobs_retried += 1
            elif isinstance(e, asyncio.CancelledError):
                logger.info('%6.2fs ↻ %s cancelled, will be run again', t, ref)
                self.jobs_retried += 1
            else:
                logger.exception(
                    '%6.2fs ! %s failed, %s: %s', t, ref, e.__class__.__name__, e, extra={'extra': exc_extra}
                )
                result = e
                finish = True
                self.jobs_failed += 1
        else:
            success = True
            finished_ms = timestamp_ms()
            logger.info('%6.2fs ← %s ● %s', (finished_ms - start_ms) / 1000, ref, result_str)
            finish = True
            self.jobs_complete += 1

        result_timeout_s = self.keep_result_s if function.keep_result_s is None else function.keep_result_s
        result_data = None
        if result is not no_result and result_timeout_s > 0:
            result_data = pickle_result(
                function_name, args, kwargs, job_try, enqueue_time_ms, success, result, start_ms, finished_ms, ref
            )

        await asyncio.shield(self.finish_job(job_id, finish, result_data, result_timeout_s, incr_score))

    async def finish_job(self, job_id, finish, result_data, result_timeout_s, incr_score):
        with await self.pool as conn:
            await conn.unwatch()
            tr = conn.multi_exec()
            delete_keys = [in_progress_key_prefix + job_id]
            if finish:
                if result_data:
                    tr.setex(result_key_prefix + job_id, result_timeout_s, result_data)
                delete_keys += [retry_key_prefix + job_id, job_key_prefix + job_id]
                tr.zrem(self.queue_name, job_id)
            elif incr_score:
                tr.zincrby(self.queue_name, incr_score, job_id)
            tr.delete(*delete_keys)
            await tr.execute()
        self.sem.release()

    async def abort_job(self, job_id):
        with await self.pool as conn:
            await conn.unwatch()
            tr = conn.multi_exec()
            tr.delete(retry_key_prefix + job_id, in_progress_key_prefix + job_id, job_key_prefix + job_id)
            tr.zrem(self.queue_name, job_id)
            await tr.execute()

    async def heart_beat(self):
        await self.record_health()
        await self.run_cron()

    async def run_cron(self):
        n = datetime.now()
        job_futures = set()

        for cron_job in self.cron_jobs:
            if cron_job.next_run is None:
                if cron_job.run_at_startup:
                    cron_job.next_run = n
                else:
                    cron_job.set_next(n)

            if n >= cron_job.next_run:
                job_id = f'{cron_job.name}:{to_unix_ms(cron_job.next_run)}' if cron_job.unique else None
                job_futures.add(self.pool.enqueue_job(cron_job.name, _job_id=job_id))
                cron_job.set_next(n)

        job_futures and await asyncio.gather(*job_futures)

    async def record_health(self):
        now_ts = time()
        if (now_ts - self._last_health_check) < self.health_check_interval:
            return
        self._last_health_check = now_ts
        pending_tasks = sum(not t.done() for t in self.tasks)
        queued = await self.pool.zcard(self.queue_name)
        info = (
            f'{datetime.now():%b-%d %H:%M:%S} j_complete={self.jobs_complete} j_failed={self.jobs_failed} '
            f'j_retried={self.jobs_retried} j_ongoing={pending_tasks} queued={queued}'
        )
        await self.pool.setex(self.health_check_key, self.health_check_interval + 1, info.encode())
        log_suffix = info[info.index('j_complete=') :]
        if self._last_health_check_log and log_suffix != self._last_health_check_log:
            logger.info('recording health: %s', info)
            self._last_health_check_log = log_suffix
        elif not self._last_health_check_log:
            self._last_health_check_log = log_suffix

    def _add_signal_handler(self, signal, handler):
        self.loop.add_signal_handler(signal, partial(handler, signal))

    def handle_sig(self, signum):
        sig = Signals(signum)
        logger.info(
            'shutdown on %s ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ %d ongoing to cancel',
            sig.name,
            self.jobs_complete,
            self.jobs_failed,
            self.jobs_retried,
            len(self.tasks),
        )
        for t in self.tasks:
            if not t.done():
                t.cancel()
        self.main_task and self.main_task.cancel()
        self.on_stop and self.on_stop(sig)

    async def close(self):
        if not self.pool:
            return
        await asyncio.gather(*self.tasks)
        await self.pool.delete(self.health_check_key)
        if self.on_shutdown:
            await self.on_shutdown(self.ctx)
        self.pool.close()
        await self.pool.wait_closed()
        self.pool = None

    def __repr__(self):
        return (
            f'<Worker j_complete={self.jobs_complete} j_failed={self.jobs_failed} j_retried={self.jobs_retried} '
            f'j_ongoing={sum(not t.done() for t in self.tasks)}>'
        )


def get_kwargs(settings_cls):
    worker_args = set(inspect.signature(Worker).parameters.keys())
    d = settings_cls if isinstance(settings_cls, dict) else settings_cls.__dict__
    return {k: v for k, v in d.items() if k in worker_args}


def create_worker(settings_cls, **kwargs) -> Worker:
    return Worker(**{**get_kwargs(settings_cls), **kwargs})


def run_worker(settings_cls, **kwargs) -> Worker:
    worker = create_worker(settings_cls, **kwargs)
    worker.run()
    return worker


async def async_check_health(
    redis_settings: Optional[RedisSettings], health_check_key: Optional[str] = None, queue_name: Optional[str] = None
):
    redis_settings = redis_settings or RedisSettings()
    redis: ArqRedis = await create_pool(redis_settings)
    queue_name = queue_name or default_queue_name
    health_check_key = health_check_key or (queue_name + health_check_key_suffix)

    data = await redis.get(health_check_key)
    if not data:
        logger.warning('Health check failed: no health check sentinel value found')
        r = 1
    else:
        logger.info('Health check successful: %s', data)
        r = 0
    redis.close()
    await redis.wait_closed()
    return r


def check_health(settings_cls) -> int:
    """
    Run a health check on the worker and return the appropriate exit code.
    :return: 0 if successful, 1 if not
    """
    cls_kwargs = get_kwargs(settings_cls)
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        async_check_health(
            cls_kwargs.get('redis_settings'), cls_kwargs.get('health_check_key'), cls_kwargs.get('queue_name')
        )
    )

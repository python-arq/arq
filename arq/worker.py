import asyncio
import logging
import os
import pickle
import signal
import sys
import time
from functools import partial
from signal import Signals

from aioredis import MultiExecError
from async_timeout import timeout

from .connections import ArqRedis, create_pool_lenient, log_redis_info
from .utils import timestamp, poll
from .keys import queue_name, in_progress_key_prefix, result_key_prefix, job_key_prefix

logger = logging.getLogger('arq.worker')


class Worker:
    def __init__(self):
        self.pool = None
        self.sem = asyncio.BoundedSemaphore(5)
        self.tasks = []
        self.main_task = None
        self.loop = asyncio.get_event_loop()
        self.job_timeout = 300

    def run(self):
        self._add_signal_handler(signal.SIGINT, self.handle_sig)
        self._add_signal_handler(signal.SIGTERM, self.handle_sig)
        self.main_task = self.loop.create_task(self._run())
        try:
            self.loop.run_until_complete(self.main_task)
        except asyncio.CancelledError:
            self.loop.run_until_complete(asyncio.gather(*self.tasks))

    async def _run(self):
        self.pool: ArqRedis = await create_pool_lenient()
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
                result_key = result_key_prefix + job_id
                r = await asyncio.gather(
                    conn.unwatch(),
                    conn.watch(ongoing_key, result_key),
                    conn.exists(ongoing_key, result_key),
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
            with await self.pool as conn:
                await conn.unwatch()
                tr = conn.multi_exec()
                tr.delete(in_progress_key_prefix + job_id)
                tr.zrem(queue_name, job_id)
                await tr.execute()
            return

        enqueue_time_ms, defer_ms, function, args, kwargs = pickle.loads(v)
        print(f'running job {job_id}...', function, args, kwargs)

        result = None
        incr_score = None
        start_time_ms = timestamp()
        try:
            async with timeout(self.job_timeout):
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            # job got cancelled while running, needs to be run again
            print(f'job {job_id} cancelled, will be run again')
        except Exception as e:
            result = e
            print(f'job {job_id} failed')
        else:
            result = 123
            print(f'job {job_id} complete')

        if result is not None:  # TODO use sentinel value
            finish_time_ms = timestamp()
            result = pickle.dumps(
                (enqueue_time_ms, defer_ms, function, args, kwargs, result, start_time_ms, finish_time_ms)
            )

        with await self.pool as conn:
            await conn.unwatch()
            tr = conn.multi_exec()
            tr.delete(in_progress_key_prefix + job_id)
            if result:
                tr.setex(result_key_prefix + job_id, 3600, result)
                tr.delete(job_key_prefix + job_id)
                tr.zrem(queue_name, job_id)
            elif incr_score:
                tr.zincrby(queue_name, incr_score, job_id)
            await tr.execute()
        self.sem.release()

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

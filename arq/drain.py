"""
:mod:`drain`
=============

Responsible for popping jobs from redis lists and managing a set of tasks with a limitted size.
"""
import asyncio
import logging
from typing import Set  # noqa

from aioredis import RedisPool

__all__ = ['Drain']

work_logger = logging.getLogger('arq.work')
jobs_logger = logging.getLogger('arq.jobs')


class Drain:
    def __init__(self, *,
                 redis_pool: RedisPool,
                 re_queue: bool=False,
                 max_concurrent_tasks: int=50,
                 shutdown_delay: float=6,
                 timeout_seconds: int=60) -> None:
        """
        :param max_concurrent_tasks; maximum number of jobs which can be execute at the same time by the event loop
        :param shutdown_delay: number of seconds to wait for tasks to finish
        :param timeout_seconds: maximum duration of a job, after that the job will be cancelled by the event loop
        """
        self.redis_pool = redis_pool
        self.loop = redis_pool._loop
        self.re_queue = re_queue
        self.max_concurrent_tasks = max_concurrent_tasks
        self.shutdown_delay = max(shutdown_delay, 0.1)
        self.timeout_seconds = timeout_seconds
        self.pending_tasks: Set[asyncio.futures.Future] = set()
        self.task_exception: Exception = None

        self.jobs_complete, self.jobs_failed, self.jobs_timed_out = 0, 0, 0
        self.running = False
        self._finish_lock = asyncio.Lock(loop=self.loop)

    async def __aenter__(self):
        self.running = True
        self.redis = await self.redis_pool.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.finish()
        self.running = False
        self.redis_pool.release(self.redis)
        self.redis = None
        # if self.task_exception:
        #     raise RuntimeError('A processed task failed') from self.task_exception

    async def iter(self, *raw_queues: bytes, pop_timeout=1):
        """
        blpop jobs from redis queues and yield them. Waits the number of tasks to drop below max_concurrent_tasks
        before popping.

        :param raw_queues: tuple of bytes defining queue(s) to pop from.
        :param pop_timeout: how long to wait on each blpop before yielding None.
        :yield: tuple: (queue_name, data) or (None, None) if all jobs are empty
        """
        work_logger.debug('starting main blpop loop')
        assert self.running, 'cannot get jobs from the queues when processor is not running'
        while self.running:
            msg = await self.redis.blpop(*raw_queues, timeout=pop_timeout)
            if msg is None:
                yield None, None
            else:
                yield msg
                await self.wait()

    def add(self, coro, job):
        work_logger.debug('scheduling job from queue %s', job.queue)
        task = self.loop.create_task(coro(job))
        task.job = job

        task.add_done_callback(self._job_callback)
        self.loop.call_later(self.timeout_seconds, self._cancel_job, task, job)
        self.pending_tasks.add(task)

    async def wait(self):
        pt_cnt = len(self.pending_tasks)
        while True:
            if pt_cnt < self.max_concurrent_tasks:
                return
            work_logger.info('%d pending tasks, waiting for one to finish', pt_cnt)
            _, self.pending_tasks = await asyncio.wait(self.pending_tasks, loop=self.loop,
                                                       return_when=asyncio.FIRST_COMPLETED)
            pt_cnt = len(self.pending_tasks)

    async def finish(self, timeout=None):
        timeout = timeout or self.shutdown_delay
        if self.pending_tasks:
            with await self._finish_lock:
                work_logger.info('processor waiting %0.1fs for %d tasks to finish', timeout, len(self.pending_tasks))
                _, pending = await asyncio.wait(self.pending_tasks, timeout=timeout, loop=self.loop)
                if pending:
                    if self.re_queue:
                        pipe = self.redis.pipeline()
                        for task in pending:
                            pipe.rpush(task.job.raw_queue, task.job.raw_data)
                        await pipe.execute()
                    for task in pending:
                        task.cancel()

    def _job_callback(self, task):
        self.jobs_complete += 1
        task_exception = task.exception()
        if task_exception:
            self.running = False
            self.task_exception = task_exception
        elif task.result():
            self.jobs_failed += 1
        jobs_logger.debug('task complete, %d jobs done, %d failed', self.jobs_complete, self.jobs_failed)

    def _cancel_job(self, task, *args):
        if not task.cancel():
            return
        self.jobs_timed_out += 1
        jobs_logger.error('task timed out %r', args[0])

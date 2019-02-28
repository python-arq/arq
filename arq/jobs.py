"""
:mod:`jobs`
===========

Defines the ``Job`` class and descendants which deal with encoding and decoding job data.
"""
import asyncio
import pickle
from datetime import timedelta
from enum import Enum
from typing import Optional

from .keys import result_key_prefix, job_key_prefix, queue_name, in_progress_key_prefix
from .utils import timestamp, to_datetime, poll
from .connections import ArqRedis


class JobStatues(str, Enum):
    deferred = 'deferred'
    queued = 'queued'
    in_progress = 'in_progress'
    finished = 'finished'
    unknown = 'unknown'


class Job:
    __slots__ = 'job_id', '_redis'

    def __init__(self, job_id: str, redis: ArqRedis):
        self.job_id = job_id
        self._redis = redis

    async def result_info(self):
        v = await self._redis.get(result_key_prefix + self.job_id, encoding=None)
        if v:
            enqueue_time_ms, defer_ms, function, args, kwargs, result, start_time_ms, finish_time_ms = pickle.loads(v)
            return dict(
                enqueue_time=to_datetime(enqueue_time_ms),
                defer_time=timedelta(seconds=defer_ms / 1000),
                function=function,
                args=args,
                kwargs=kwargs,
                result=result,
                start_time=to_datetime(start_time_ms),
                finish_time=to_datetime(finish_time_ms),
            )

    async def result(self, timeout: Optional[float] = None, *, pole_delay: float = 0.5):
        async for delay in poll(pole_delay):
            info = await self.result_info()
            if info:
                result = info['result']
                if isinstance(result, Exception):
                    raise result
                else:
                    return result
            if timeout is not None and delay > timeout:
                raise asyncio.TimeoutError()

    async def info(self):
        info = await self.result_info()
        if not info:
            v = await self._redis.get(job_key_prefix + self.job_id, encoding=None)
            if v:
                enqueue_time_ms, defer_ms, function, args, kwargs = pickle.loads(v)
                info = dict(
                    enqueue_time=to_datetime(enqueue_time_ms),
                    defer_time=timedelta(seconds=defer_ms / 1000),
                    function=function,
                    args=args,
                    kwargs=kwargs,
                )
        if info:
            info['score'] = await self._redis.zscore(queue_name, self.job_id)
        return info

    async def status(self) -> JobStatues:
        if await self._redis.exists(result_key_prefix + self.job_id):
            return JobStatues.finished
        elif await self._redis.exists(in_progress_key_prefix + self.job_id):
            return JobStatues.in_progress
        else:
            score = await self._redis.zscore(queue_name, self.job_id)
            if not score:
                return JobStatues.unknown
            return JobStatues.deferred if score > timestamp() else JobStatues.queued

    def __repr__(self):
        return f'<arq job {self.job_id}>'

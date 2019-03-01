import asyncio
import pickle
from enum import Enum
from typing import TYPE_CHECKING, Optional

from .constants import in_progress_key_prefix, job_key_prefix, queue_name, result_key_prefix
from .utils import ms_to_datetime, poll, timestamp_ms

if TYPE_CHECKING:  # pragma: no cover
    from .connections import ArqRedis


class JobStatus(str, Enum):
    deferred = 'deferred'
    queued = 'queued'
    in_progress = 'in_progress'
    complete = 'complete'
    not_found = 'not_found'


class Job:
    __slots__ = 'job_id', '_redis'

    def __init__(self, job_id: str, redis: 'ArqRedis'):
        self.job_id = job_id
        self._redis = redis

    async def result_info(self):
        v = await self._redis.get(result_key_prefix + self.job_id, encoding=None)
        if v:
            enqueue_time_ms, function, args, kwargs, result, tries, start_time_ms, finish_time_ms = pickle.loads(v)
            return dict(
                enqueue_time=ms_to_datetime(enqueue_time_ms),
                function=function,
                args=args,
                kwargs=kwargs,
                result=result,
                try_count=tries,
                start_time=ms_to_datetime(start_time_ms),
                finish_time=ms_to_datetime(finish_time_ms),
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
                enqueue_time_ms, function, args, kwargs = pickle.loads(v)
                info = dict(enqueue_time=ms_to_datetime(enqueue_time_ms), function=function, args=args, kwargs=kwargs)
        if info:
            info['score'] = await self._redis.zscore(queue_name, self.job_id)
        return info

    async def status(self) -> JobStatus:
        if await self._redis.exists(result_key_prefix + self.job_id):
            return JobStatus.complete
        elif await self._redis.exists(in_progress_key_prefix + self.job_id):
            return JobStatus.in_progress
        else:
            score = await self._redis.zscore(queue_name, self.job_id)
            if not score:
                return JobStatus.not_found
            return JobStatus.deferred if score > timestamp_ms() else JobStatus.queued

    def __repr__(self):
        return f'<arq job {self.job_id}>'

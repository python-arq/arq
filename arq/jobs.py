import asyncio
import pickle
from enum import Enum
from typing import Any, Dict, Optional

from .constants import in_progress_key_prefix, job_key_prefix, queue_name, result_key_prefix
from .utils import ms_to_datetime, poll, timestamp_ms


class JobStatus(str, Enum):
    """
    Enum of job statuses.
    """

    #: job is in the queue, time it should be run not yet reached
    deferred = 'deferred'
    #: job is in the queue, time it should run has been reached
    queued = 'queued'
    #: job is in progress
    in_progress = 'in_progress'
    #: job is complete, result is available
    complete = 'complete'
    #: job not found in any way
    not_found = 'not_found'


class Job:
    """
    Holds data a reference to a job.
    """

    __slots__ = 'job_id', '_redis'

    def __init__(self, job_id: str, redis):
        self.job_id = job_id
        self._redis = redis

    async def result(self, timeout: Optional[float] = None, *, pole_delay: float = 0.5) -> Any:
        """
        Get the result of the job, including waiting if it's not yet available. If the job raised an exception,
        it will be raised here.

        :param timeout: maximum time to wait for the job result before raising ``TimeoutError``, will wait forever
        :param pole_delay: how often to poll redis for the job result
        """
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

    async def info(self) -> Optional[Dict[str, Any]]:
        """
        All information on a job, including its result if it's available, does not wait for the result.
        """
        info = await self.result_info()
        if not info:
            v = await self._redis.get(job_key_prefix + self.job_id, encoding=None)
            if v:
                enqueue_time_ms, job_try, function, args, kwargs = pickle.loads(v)
                info = dict(
                    enqueue_time=ms_to_datetime(enqueue_time_ms),
                    job_try=job_try,
                    function=function,
                    args=args,
                    kwargs=kwargs,
                )
        if info:
            info['score'] = await self._redis.zscore(queue_name, self.job_id)
        return info

    async def result_info(self) -> Optional[Dict[str, Any]]:
        """
        Information about the job result if available, does not wait for the result. Does not raise an exception
        even if the job raised one.
        """
        v = await self._redis.get(result_key_prefix + self.job_id, encoding=None)
        if v:
            enqueue_time_ms, job_try, function, args, kwargs, result, start_time_ms, finish_time_ms = pickle.loads(v)
            return dict(
                enqueue_time=ms_to_datetime(enqueue_time_ms),
                job_try=job_try,
                function=function,
                args=args,
                kwargs=kwargs,
                result=result,
                start_time=ms_to_datetime(start_time_ms),
                finish_time=ms_to_datetime(finish_time_ms),
            )

    async def status(self) -> JobStatus:
        """
        Status of the job.
        """
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

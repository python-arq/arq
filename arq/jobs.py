import asyncio
import logging
import pickle
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from .constants import default_queue_name, in_progress_key_prefix, job_key_prefix, result_key_prefix
from .utils import ms_to_datetime, poll, timestamp_ms

logger = logging.getLogger('arq.jobs')


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


@dataclass
class JobDef:
    function: str
    args: tuple
    kwargs: dict
    job_try: int
    enqueue_time: datetime
    score: Optional[int]


@dataclass
class JobResult(JobDef):
    success: bool
    result: Any
    start_time: datetime
    finish_time: datetime
    job_id: Optional[str] = None


class Job:
    """
    Holds data a reference to a job.
    """

    __slots__ = 'job_id', '_redis', '_queue_name'

    def __init__(self, job_id: str, redis, _queue_name: str = default_queue_name):
        self.job_id = job_id
        self._redis = redis
        self._queue_name = _queue_name

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
                result = info.result
                if info.success:
                    return result
                else:
                    raise result
            if timeout is not None and delay > timeout:
                raise asyncio.TimeoutError()

    async def info(self) -> Optional[JobDef]:
        """
        All information on a job, including its result if it's available, does not wait for the result.
        """
        info = await self.result_info()
        if not info:
            v = await self._redis.get(job_key_prefix + self.job_id, encoding=None)
            if v:
                info = unpickle_job(v)
        if info:
            info.score = await self._redis.zscore(self._queue_name, self.job_id)
        return info

    async def result_info(self) -> Optional[JobResult]:
        """
        Information about the job result if available, does not wait for the result. Does not raise an exception
        even if the job raised one.
        """
        v = await self._redis.get(result_key_prefix + self.job_id, encoding=None)
        if v:
            return unpickle_result(v)

    async def status(self) -> JobStatus:
        """
        Status of the job.
        """
        if await self._redis.exists(result_key_prefix + self.job_id):
            return JobStatus.complete
        elif await self._redis.exists(in_progress_key_prefix + self.job_id):
            return JobStatus.in_progress
        else:
            score = await self._redis.zscore(self._queue_name, self.job_id)
            if not score:
                return JobStatus.not_found
            return JobStatus.deferred if score > timestamp_ms() else JobStatus.queued

    def __repr__(self):
        return f'<arq job {self.job_id}>'


class PickleError(RuntimeError):
    pass


def pickle_job(function_name: str, args: tuple, kwargs: dict, job_try: int, enqueue_time_ms: int):
    data = {'t': job_try, 'f': function_name, 'a': args, 'k': kwargs, 'et': enqueue_time_ms}
    try:
        return pickle.dumps(data)
    except Exception as e:
        raise PickleError(f'unable to pickle job "{function_name}"') from e


def pickle_result(
    function: str,
    args: tuple,
    kwargs: dict,
    job_try: int,
    enqueue_time_ms: int,
    success: bool,
    result: Any,
    start_ms: int,
    finished_ms: int,
    ref: str,
) -> Optional[bytes]:
    data = {
        't': job_try,
        'f': function,
        'a': args,
        'k': kwargs,
        'et': enqueue_time_ms,
        's': success,
        'r': result,
        'st': start_ms,
        'ft': finished_ms,
    }
    try:
        return pickle.dumps(data)
    except Exception:
        logger.warning('error pickling result of %s', ref, exc_info=True)

    data.update(r=PickleError('unable to pickle result'), s=False)
    try:
        return pickle.dumps(data)
    except Exception:
        logger.critical('error pickling result of %s even after replacing result', ref, exc_info=True)


def unpickle_job(r: bytes) -> JobDef:
    d = pickle.loads(r)
    return JobDef(
        function=d['f'], args=d['a'], kwargs=d['k'], job_try=d['t'], enqueue_time=ms_to_datetime(d['et']), score=None
    )


def unpickle_job_raw(r: bytes) -> tuple:
    d = pickle.loads(r)
    return d['f'], d['a'], d['k'], d['t'], d['et']


def unpickle_result(r: bytes) -> JobResult:
    d = pickle.loads(r)
    return JobResult(
        job_try=d['t'],
        function=d['f'],
        args=d['a'],
        kwargs=d['k'],
        enqueue_time=ms_to_datetime(d['et']),
        score=None,
        success=d['s'],
        result=d['r'],
        start_time=ms_to_datetime(d['st']),
        finish_time=ms_to_datetime(d['ft']),
    )

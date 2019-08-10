import asyncio
import logging
import pickle
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional

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

    __slots__ = 'job_id', '_redis', '_queue_name', '_deserialize'

    def __init__(
        self,
        job_id: str,
        redis,
        _queue_name: str = default_queue_name,
        _deserialize: Optional[Callable[[bytes], Any]] = None,
    ):
        self.job_id = job_id
        self._redis = redis
        self._queue_name = _queue_name
        self._deserialize = _deserialize

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
                info = deserialize_job(v, _deserialize=self._deserialize)
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
            return deserialize_result(v, _deserialize=self._deserialize)

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


class SerializationError(RuntimeError):
    pass


def serialize_job(
    function_name: str,
    args: tuple,
    kwargs: dict,
    job_try: int,
    enqueue_time_ms: int,
    *,
    _serialize: Optional[Callable[[Any], bytes]] = None,
) -> Optional[bytes]:
    data = {'t': job_try, 'f': function_name, 'a': args, 'k': kwargs, 'et': enqueue_time_ms}
    if _serialize is None:
        _serialize = pickle.dumps
    try:
        return _serialize(data)
    except Exception as e:
        raise SerializationError(f'unable to pickle job "{function_name}"') from e


def serialize_result(
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
    *,
    _serialize: Optional[Callable[[Any], bytes]] = None,
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
    if _serialize is None:
        _serialize = pickle.dumps
    try:
        return _serialize(data)
    except Exception:
        logger.warning('error pickling result of %s', ref, exc_info=True)

    data.update(r=SerializationError('unable to serialize result'), s=False)
    try:
        return _serialize(data)
    except Exception:
        logger.critical('error serializing result of %s even after replacing result', ref, exc_info=True)


def deserialize_job(r: bytes, _deserialize: Optional[Callable[[bytes], Any]] = None) -> JobDef:
    if _deserialize is None:
        _deserialize = pickle.loads
    try:
        d = _deserialize(r)
        return JobDef(
            function=d['f'],
            args=d['a'],
            kwargs=d['k'],
            job_try=d['t'],
            enqueue_time=ms_to_datetime(d['et']),
            score=None,
        )
    except Exception as e:
        raise SerializationError(f'unable to deserialize job: {r!r}') from e


def deserialize_job_raw(r: bytes, _deserialize: Optional[Callable[[bytes], Any]] = None) -> tuple:
    if _deserialize is None:
        _deserialize = pickle.loads
    try:
        d = _deserialize(r)
        return d['f'], d['a'], d['k'], d['t'], d['et']
    except Exception as e:
        raise SerializationError(f'unable to deserialize job: {r!r}') from e


def deserialize_result(r: bytes, _deserialize: Optional[Callable[[bytes], Any]] = None) -> JobResult:
    if _deserialize is None:
        _deserialize = pickle.loads
    try:
        d = _deserialize(r)
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
    except Exception as e:
        raise SerializationError(f'unable to deserialize job result: {r!r}') from e

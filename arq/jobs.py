import asyncio
import logging
import pickle
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional

from redis.asyncio import Redis

from .constants import (
    DEFAULT_PRIORITY,
    MIN_PRIORITY,
    abort_jobs_ss,
    compute_ready_score,
    default_queue_name,
    deferred_queue_key,
    in_progress_key_prefix,
    job_key_prefix,
    ready_queue_key,
    result_key_prefix,
)
from .utils import ms_to_datetime, poll, timestamp_ms

logger = logging.getLogger('arq.jobs')

Serializer = Callable[[dict[str, Any]], bytes]
Deserializer = Callable[[bytes], dict[str, Any]]


class ResultNotFound(RuntimeError):
    pass


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


_EPOCH = datetime.fromtimestamp(0, tz=timezone.utc)


@dataclass
class JobDef:
    function: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    job_try: int
    enqueue_time: datetime
    score: Optional[int]
    job_id: Optional[str]
    # priority defaults so existing constructor sites that predate the priority feature keep working;
    # dataclass inheritance forces every JobResult-only field below to also carry a default.
    priority: int = DEFAULT_PRIORITY

    def __post_init__(self) -> None:
        if isinstance(self.score, float):
            self.score = int(self.score)


@dataclass
class JobResult(JobDef):
    success: bool = False
    result: Any = None
    start_time: datetime = field(default_factory=lambda: _EPOCH)
    finish_time: datetime = field(default_factory=lambda: _EPOCH)
    queue_name: str = ''


class Job:
    """
    Holds data a reference to a job.
    """

    __slots__ = 'job_id', '_redis', '_queue_name', '_deserializer'

    def __init__(
        self,
        job_id: str,
        redis: 'Redis[bytes]',
        _queue_name: str = default_queue_name,
        _deserializer: Optional[Deserializer] = None,
    ):
        self.job_id = job_id
        self._redis = redis
        self._queue_name = _queue_name
        self._deserializer = _deserializer

    async def result(
        self, timeout: Optional[float] = None, *, poll_delay: float = 0.5, pole_delay: Optional[float] = None
    ) -> Any:
        """
        Get the result of the job or, if the job raised an exception, reraise it.

        This function waits for the result if it's not yet available and the job is
        present in the queue. Otherwise ``ResultNotFound`` is raised.

        :param timeout: maximum time to wait for the job result before raising ``TimeoutError``, will wait forever
        :param poll_delay: how often to poll redis for the job result
        :param pole_delay: deprecated, use poll_delay instead
        """
        if pole_delay is not None:
            warnings.warn(
                '"pole_delay" is deprecated, use the correct spelling "poll_delay" instead', DeprecationWarning
            )
            poll_delay = pole_delay

        async for delay in poll(poll_delay):
            async with self._redis.pipeline(transaction=True) as tr:
                tr.get(result_key_prefix + self.job_id)
                tr.zscore(ready_queue_key(self._queue_name), self.job_id)
                tr.zscore(deferred_queue_key(self._queue_name), self.job_id)
                v, ready_score, deferred_score = await tr.execute()

            if v:
                info = deserialize_result(v, deserializer=self._deserializer)
                if info.success:
                    return info.result
                elif isinstance(info.result, (Exception, asyncio.CancelledError)):
                    raise info.result
                else:
                    raise SerializationError(info.result)
            elif ready_score is None and deferred_score is None:
                raise ResultNotFound(
                    'Not waiting for job result because the job is not in queue. '
                    'Is the worker function configured to keep result?'
                )

            if timeout is not None and delay > timeout:
                raise asyncio.TimeoutError()

    async def info(self) -> Optional[JobDef]:
        """
        All information on a job, including its result if it's available, does not wait for the result.
        """
        info: Optional[JobDef] = await self.result_info()
        if not info:
            v = await self._redis.get(job_key_prefix + self.job_id)
            if v:
                info = deserialize_job(v, deserializer=self._deserializer)
        if info:
            score, _ = await self._lookup_queue_score()
            info.score = score
        return info

    async def _lookup_queue_score(self) -> tuple[Optional[int], bool]:
        """
        Return ``(score, is_deferred)`` for this job. ``score`` is ``None`` when the job
        isn't in either sub-zset.
        """
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.zscore(ready_queue_key(self._queue_name), self.job_id)
            pipe.zscore(deferred_queue_key(self._queue_name), self.job_id)
            ready_score, deferred_score = await pipe.execute()
        if ready_score is not None:
            return int(ready_score), False
        if deferred_score is not None:
            return int(deferred_score), True
        return None, False

    async def result_info(self) -> Optional[JobResult]:
        """
        Information about the job result if available, does not wait for the result. Does not raise an exception
        even if the job raised one.
        """
        v = await self._redis.get(result_key_prefix + self.job_id)
        if v:
            return deserialize_result(v, deserializer=self._deserializer)
        else:
            return None

    async def status(self) -> JobStatus:
        """
        Status of the job.
        """
        async with self._redis.pipeline(transaction=True) as tr:
            tr.exists(result_key_prefix + self.job_id)
            tr.exists(in_progress_key_prefix + self.job_id)
            tr.zscore(ready_queue_key(self._queue_name), self.job_id)
            tr.zscore(deferred_queue_key(self._queue_name), self.job_id)
            is_complete, is_in_progress, ready_score, deferred_score = await tr.execute()

        if is_complete:
            return JobStatus.complete
        elif is_in_progress:
            return JobStatus.in_progress
        elif ready_score is not None:
            return JobStatus.queued
        elif deferred_score is not None:
            return JobStatus.deferred
        else:
            return JobStatus.not_found

    async def abort(self, *, timeout: Optional[float] = None, poll_delay: float = 0.5) -> bool:
        """
        Abort the job.

        :param timeout: maximum time to wait for the job result before raising ``TimeoutError``,
            will wait forever on None
        :param poll_delay: how often to poll redis for the job result
        :return: True if the job aborted properly, False otherwise
        """
        # if the job is currently sitting in the deferred sub-zset, hoist it into ready at
        # top priority so the worker picks it up on the next poll and observes the abort flag.
        _, is_deferred = await self._lookup_queue_score()
        if is_deferred:
            async with self._redis.pipeline(transaction=True) as tr:
                tr.zrem(deferred_queue_key(self._queue_name), self.job_id)
                tr.zadd(
                    ready_queue_key(self._queue_name),
                    {self.job_id: compute_ready_score(MIN_PRIORITY, timestamp_ms())},
                )
                await tr.execute()

        await self._redis.zadd(abort_jobs_ss, {self.job_id: timestamp_ms()})

        try:
            await self.result(timeout=timeout, poll_delay=poll_delay)
        except asyncio.CancelledError:
            return True
        except ResultNotFound:
            # We do not know if the job was cancelled or not
            return False
        else:
            return False

    def __repr__(self) -> str:
        return f'<arq job {self.job_id}>'


class SerializationError(RuntimeError):
    pass


class DeserializationError(SerializationError):
    pass


def serialize_job(
    function_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    job_try: Optional[int],
    enqueue_time_ms: int,
    *,
    priority: int = DEFAULT_PRIORITY,
    serializer: Optional[Serializer] = None,
) -> bytes:
    data = {
        't': job_try,
        'f': function_name,
        'a': args,
        'k': kwargs,
        'et': enqueue_time_ms,
        'priority': priority,
    }
    if serializer is None:
        serializer = pickle.dumps
    try:
        return serializer(data)
    except Exception as e:
        raise SerializationError(f'unable to serialize job "{function_name}"') from e


def serialize_result(
    function: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    job_try: int,
    enqueue_time_ms: int,
    success: bool,
    result: Any,
    start_ms: int,
    finished_ms: int,
    ref: str,
    queue_name: str,
    job_id: str,
    *,
    priority: int = DEFAULT_PRIORITY,
    serializer: Optional[Serializer] = None,
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
        'q': queue_name,
        'id': job_id,
        'priority': priority,
    }
    if serializer is None:
        serializer = pickle.dumps
    try:
        return serializer(data)
    except Exception:
        logger.warning('error serializing result of %s', ref, exc_info=True)

    # use string in case serialization fails again
    data.update(r='unable to serialize result', s=False)
    try:
        return serializer(data)
    except Exception:
        logger.critical('error serializing result of %s even after replacing result', ref, exc_info=True)
    return None


def deserialize_job(r: bytes, *, deserializer: Optional[Deserializer] = None) -> JobDef:
    if deserializer is None:
        deserializer = pickle.loads
    try:
        d = deserializer(r)
        return JobDef(
            function=d['f'],
            args=d['a'],
            kwargs=d['k'],
            job_try=d['t'],
            enqueue_time=ms_to_datetime(d['et']),
            score=None,
            job_id=None,
            priority=d.get('priority', DEFAULT_PRIORITY),
        )
    except Exception as e:
        raise DeserializationError('unable to deserialize job') from e


def deserialize_job_raw(
    r: bytes, *, deserializer: Optional[Deserializer] = None
) -> tuple[str, tuple[Any, ...], dict[str, Any], int, int, int]:
    if deserializer is None:
        deserializer = pickle.loads
    try:
        d = deserializer(r)
        return d['f'], d['a'], d['k'], d['t'], d['et'], d.get('priority', DEFAULT_PRIORITY)
    except Exception as e:
        raise DeserializationError('unable to deserialize job') from e


def deserialize_result(r: bytes, *, deserializer: Optional[Deserializer] = None) -> JobResult:
    if deserializer is None:
        deserializer = pickle.loads
    try:
        d = deserializer(r)
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
            queue_name=d.get('q', '<unknown>'),
            job_id=d.get('id', '<unknown>'),
            priority=d.get('priority', DEFAULT_PRIORITY),
        )
    except Exception as e:
        raise DeserializationError('unable to deserialize job result') from e

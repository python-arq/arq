import asyncio
import functools
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from operator import attrgetter
from typing import Any, List, Optional, Union
from uuid import uuid4

import aioredis
from aioredis import MultiExecError, Redis

from .constants import default_queue_name, job_key_prefix, result_key_prefix
from .jobs import Deserializer, Job, JobDef, JobResult, Serializer, deserialize_job, serialize_job
from .utils import timestamp_ms, to_ms, to_unix_ms

logger = logging.getLogger('arq.connections')


@dataclass
class RedisSettings:
    """
    No-Op class used to hold redis connection redis_settings.

    Used by :func:`arq.connections.create_pool` and :class:`arq.worker.Worker`.
    """

    host: str = 'localhost'
    port: int = 6379
    database: int = 0
    password: str = None
    conn_timeout: int = 1
    conn_retries: int = 5
    conn_retry_delay: int = 1

    def __repr__(self):
        return '<RedisSettings {}>'.format(' '.join(f'{k}={v}' for k, v in self.__dict__.items()))


# extra time after the job is expected to start when the job key should expire, 1 day in ms
expires_extra_ms = 86_400_000


class ArqRedis(Redis):
    """
    Thin subclass of ``aioredis.Redis`` which adds :func:`arq.connections.enqueue_job`.

    :param redis_settings: an instance of ``arq.connections.RedisSettings``.
    :param job_serializer: a function that serializes Python objects to bytes, defaults to pickle.dumps
    :param job_deserializer: a function that deserializes bytes into Python objects, defaults to pickle.loads
    :param kwargs: keyword arguments directly passed to ``aioredis.Redis``.
    """

    def __init__(
        self,
        pool_or_conn,
        job_serializer: Optional[Serializer] = None,
        job_deserializer: Optional[Deserializer] = None,
        **kwargs,
    ) -> None:
        self.job_serializer = job_serializer
        self.job_deserializer = job_deserializer
        super().__init__(pool_or_conn, **kwargs)

    async def enqueue_job(
        self,
        function: str,
        *args: Any,
        _job_id: Optional[str] = None,
        _queue_name: str = default_queue_name,
        _defer_until: Optional[datetime] = None,
        _defer_by: Union[None, int, float, timedelta] = None,
        _expires: Union[None, int, float, timedelta] = None,
        _job_try: Optional[int] = None,
        **kwargs: Any,
    ) -> Optional[Job]:
        """
        Enqueue a job.

        :param function: Name of the function to call
        :param args: args to pass to the function
        :param _job_id: ID of the job, can be used to enforce job uniqueness
        :param _queue_name: queue of the job, can be used to create job in different queue
        :param _defer_until: datetime at which to run the job
        :param _defer_by: duration to wait before running the job
        :param _expires: if the job still hasn't started after this duration, do not run it
        :param _job_try: useful when re-enqueueing jobs within a job
        :param kwargs: any keyword arguments to pass to the function
        :return: :class:`arq.jobs.Job` instance or ``None`` if a job with this ID already exists
        """
        job_id = _job_id or uuid4().hex
        job_key = job_key_prefix + job_id
        assert not (_defer_until and _defer_by), "use either 'defer_until' or 'defer_by' or neither, not both"

        defer_by_ms = to_ms(_defer_by)
        expires_ms = to_ms(_expires)

        with await self as conn:
            pipe = conn.pipeline()
            pipe.unwatch()
            pipe.watch(job_key)
            job_exists = pipe.exists(job_key)
            job_result_exists = pipe.exists(result_key_prefix + job_id)
            await pipe.execute()
            if await job_exists or await job_result_exists:
                return

            enqueue_time_ms = timestamp_ms()
            if _defer_until is not None:
                score = to_unix_ms(_defer_until)
            elif defer_by_ms:
                score = enqueue_time_ms + defer_by_ms
            else:
                score = enqueue_time_ms

            expires_ms = expires_ms or score - enqueue_time_ms + expires_extra_ms

            job = serialize_job(function, args, kwargs, _job_try, enqueue_time_ms, serializer=self.job_serializer)
            tr = conn.multi_exec()
            tr.psetex(job_key, expires_ms, job)
            tr.zadd(_queue_name, score, job_id)
            try:
                await tr.execute()
            except MultiExecError:
                # job got enqueued since we checked 'job_exists'
                return
        return Job(job_id, redis=self, _deserializer=self.job_deserializer)

    async def _get_job_result(self, key) -> JobResult:
        job_id = key[len(result_key_prefix) :]
        job = Job(job_id, self, _deserializer=self.job_deserializer)
        r = await job.result_info()
        r.job_id = job_id
        return r

    async def all_job_results(self) -> List[JobResult]:
        """
        Get results for all jobs in redis.
        """
        keys = await self.keys(result_key_prefix + '*')
        results = await asyncio.gather(*[self._get_job_result(k) for k in keys])
        return sorted(results, key=attrgetter('enqueue_time'))

    async def _get_job_def(self, job_id, score) -> JobDef:
        v = await self.get(job_key_prefix + job_id, encoding=None)
        jd = deserialize_job(v, deserializer=self.job_deserializer)
        jd.score = score
        return jd

    async def queued_jobs(self, *, queue_name: str = default_queue_name) -> List[JobDef]:
        """
        Get information about queued, mostly useful when testing.
        """
        jobs = await self.zrange(queue_name, withscores=True)
        return await asyncio.gather(*[self._get_job_def(job_id, score) for job_id, score in jobs])


async def create_pool(
    settings: RedisSettings = None,
    *,
    retry: int = 0,
    job_serializer: Optional[Serializer] = None,
    job_deserializer: Optional[Deserializer] = None,
) -> ArqRedis:
    """
    Create a new redis pool, retrying up to ``conn_retries`` times if the connection fails.

    Similar to ``aioredis.create_redis_pool`` except it returns a :class:`arq.connections.ArqRedis` instance,
    thus allowing job enqueuing.
    """
    settings = settings or RedisSettings()
    addr = settings.host, settings.port
    try:
        pool = await aioredis.create_redis_pool(
            addr,
            db=settings.database,
            password=settings.password,
            timeout=settings.conn_timeout,
            encoding='utf8',
            commands_factory=functools.partial(
                ArqRedis, job_serializer=job_serializer, job_deserializer=job_deserializer
            ),
        )
    except (ConnectionError, OSError, aioredis.RedisError, asyncio.TimeoutError) as e:
        if retry < settings.conn_retries:
            logger.warning(
                'redis connection error %s:%s %s %s, %d retries remaining...',
                settings.host,
                settings.port,
                e.__class__.__name__,
                e,
                settings.conn_retries - retry,
            )
            await asyncio.sleep(settings.conn_retry_delay)
        else:
            raise
    else:
        if retry > 0:
            logger.info('redis connection successful')
        return pool

    # recursively attempt to create the pool outside the except block to avoid
    # "During handling of the above exception..." madness
    return await create_pool(
        settings, retry=retry + 1, job_serializer=job_serializer, job_deserializer=job_deserializer
    )


async def log_redis_info(redis, log_func):
    with await redis as r:
        info, key_count = await asyncio.gather(r.info(), r.dbsize())
    log_func(
        f'redis_version={info["server"]["redis_version"]} '
        f'mem_usage={info["memory"]["used_memory_human"]} '
        f'clients_connected={info["clients"]["connected_clients"]} '
        f'db_keys={key_count}'
    )

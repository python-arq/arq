import asyncio
import logging
import pickle
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional, Union
from uuid import uuid4

import aioredis
from aioredis import MultiExecError, Redis

from .constants import job_key_prefix, queue_name
from .jobs import Job
from .utils import timestamp_ms, to_ms, to_unix_ms

logger = logging.getLogger('arq.connections')


@dataclass
class RedisSettings:
    """
    No-Op class used to hold redis connection redis_settings.
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


default_expires = timedelta(seconds=3600)


class ArqRedis(Redis):
    async def enqueue_job(
        self,
        function: str,
        *args: Any,
        _job_id: Optional[str] = None,
        _defer_until: Optional[datetime] = None,
        _defer_by: Union[None, int, float, timedelta] = None,
        _expires: timedelta = default_expires,
        **kwargs: Any,
    ):
        job_id = _job_id or uuid4().hex
        job_key = job_key_prefix + job_id
        assert not (_defer_until and _defer_by), "use either 'defer_until' or 'defer_by' or neither, not both"

        defer_by_ms = to_ms(_defer_by)

        with await self as conn:
            _, _, job_exists = await asyncio.gather(conn.unwatch(), conn.watch(job_key), conn.exists(job_key))
            if job_exists:
                return

            enqueue_time_ms = timestamp_ms()
            if _defer_until is not None:
                score = to_unix_ms(_defer_until)
            elif defer_by_ms:
                score = enqueue_time_ms + defer_by_ms
            else:
                score = enqueue_time_ms

            job = pickle.dumps((enqueue_time_ms, function, args, kwargs))
            tr = conn.multi_exec()
            tr.setex(job_key, _expires.total_seconds(), job)
            tr.zadd(queue_name, score, job_id)
            try:
                await tr.execute()
            except MultiExecError:
                # job got enqueued since we got 'job_exists'
                return
        return Job(job_id, self)


async def create_pool(settings: RedisSettings = None, *, _retry: int = 0) -> ArqRedis:
    """
    Create a new redis pool, retrying up to conn_retries times if the connection fails.
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
            commands_factory=ArqRedis,
        )
    except (ConnectionError, OSError, aioredis.RedisError, asyncio.TimeoutError) as e:
        if _retry < settings.conn_retries:
            logger.warning(
                'redis connection error %s %s, %d retries remaining...',
                e.__class__.__name__,
                e,
                settings.conn_retries - _retry,
            )
            await asyncio.sleep(settings.conn_retry_delay)
        else:
            raise
    else:
        if _retry > 0:
            logger.info('redis connection successful')
        return pool

    # recursively attempt to create the pool outside the except block to avoid
    # "During handling of the above exception..." madness
    return await create_pool(settings, _retry=_retry + 1)


async def log_redis_info(redis, log_func):
    with await redis as r:
        info, key_count = await asyncio.gather(r.info(), r.dbsize())
    log_func(
        f'redis_version={info["server"]["redis_version"]} '
        f'mem_usage={info["memory"]["used_memory_human"]} '
        f'clients_connected={info["clients"]["connected_clients"]} '
        f'db_keys={key_count}'
    )

from redis.cluster import RedisCluster, ClusterNode
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
import arq
import redis.connection as conn
import asyncio
from arq.worker import Retry, Worker, func





def arq_from_settings() -> arq.connections.RedisSettings:
    """Return arq RedisSettings from a settings section"""
    return arq.connections.RedisSettings(
        host="test-cluster.aqtke6.clustercfg.use2.cache.amazonaws.com",
        port="6379",
        conn_timeout=5,
        cluster_mode=True

    )


_arq_pool: arq.ArqRedis | None = None
worker_: Worker = None

async def open_arq_pool() -> arq.ArqRedis:
    """Opens a shared ArqRedis pool for this process"""
    global _arq_pool
    if not _arq_pool:
        _arq_pool = await arq.create_pool(arq_from_settings())
        await _arq_pool.__aenter__()
    return _arq_pool


async def close_arq_pool() -> None:
    """Closes the shared ArqRedis pool for this process"""
    if _arq_pool:
        await _arq_pool.__aexit__(None, None, None)


async def arq_pool() -> arq.ArqRedis:
    if not _arq_pool:
        raise Exception("The global pool was not opened for this process")
    return _arq_pool


async def get_queued_jobs_ids(arq_pool: arq.ArqRedis, queue_name: str) -> set[str]:
    return {job_id.decode() for job_id in await arq_pool.zrange(queue_name, 0, -1)}


def print_job():
    print("job started")

async def create_worker(arq_redis:arq.ArqRedis, functions=[], burst=True, poll_delay=0, max_jobs=10,  **kwargs):
        global worker_
        worker_ = Worker(
            functions=functions, redis_pool=arq_redis, burst=burst, poll_delay=poll_delay, max_jobs=max_jobs, **kwargs
        )
        return worker_



async def qj():
    """Schedule an arq task to remove the access grant from the database at the time of expiration."""
    await open_arq_pool()
    arq = await arq_pool()

    async def foobar(ctx):
        return 42

    j = await arq.enqueue_job('foobar')

    worker: Worker = await create_worker(arq,functions=[func(foobar, name='foobar')],)
    await worker.main()
    r = await j.result(poll_delay=0)
    print(r)


if __name__ == "__main__":


    asyncio.run(qj())

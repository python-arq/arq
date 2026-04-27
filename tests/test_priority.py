"""End-to-end tests for priority-aware scheduling and the split ready/deferred sub-zsets."""

import asyncio
import pickle

import pytest

from arq.connections import ArqRedis
from arq.constants import (
    DEFAULT_PRIORITY,
    MIN_PRIORITY,
    PRIORITY_FACTOR,
    compute_ready_score,
    default_queue_name,
    deferred_queue_key,
    job_key_prefix,
    ready_queue_key,
)
from arq.jobs import JobStatus, deserialize_job
from arq.utils import timestamp_ms
from arq.worker import Retry, Worker, func


async def _enqueue(arq_redis: ArqRedis, fn: str, *args, **kwargs) -> str:
    """Enqueue, then sleep 2ms so subsequent enqueues land at strictly larger timestamps."""
    j = await arq_redis.enqueue_job(fn, *args, **kwargs)
    await asyncio.sleep(0.002)
    return j.job_id


async def test_priority_ordering(arq_redis: ArqRedis, worker):
    order: list[str] = []

    async def record(ctx, tag):
        order.append(tag)

    await _enqueue(arq_redis, 'record', 'a', _priority=5)
    await _enqueue(arq_redis, 'record', 'b', _priority=1)
    await _enqueue(arq_redis, 'record', 'c', _priority=5)
    await _enqueue(arq_redis, 'record', 'd', _priority=10)
    await _enqueue(arq_redis, 'record', 'e', _priority=1)

    w: Worker = worker(functions=[func(record, name='record')], max_jobs=1)
    await w.main()

    # priority-1 jobs first (FIFO between b and e), then the priority-5 jobs (FIFO between a and c),
    # then priority-10.
    assert order == ['b', 'e', 'a', 'c', 'd']


async def test_priority_default_is_5(arq_redis: ArqRedis):
    j = await arq_redis.enqueue_job('foobar')
    info = await j.info()
    assert info.priority == DEFAULT_PRIORITY
    # ready zset score = PRIORITY_FACTOR * 5 + enqueue_time_ms
    assert info.score >= PRIORITY_FACTOR * DEFAULT_PRIORITY
    assert info.score < PRIORITY_FACTOR * (DEFAULT_PRIORITY + 1)


@pytest.mark.parametrize('bad_priority', [0, 11, -1, 100])
async def test_priority_validation(arq_redis: ArqRedis, bad_priority):
    with pytest.raises(ValueError, match='_priority must be between'):
        await arq_redis.enqueue_job('foobar', _priority=bad_priority)
    assert await arq_redis.queue_size(default_queue_name) == 0


async def test_priority_in_blob_round_trips(arq_redis: ArqRedis):
    j = await arq_redis.enqueue_job('foobar', _priority=3)
    blob = await arq_redis.get(job_key_prefix + j.job_id)
    assert blob is not None
    restored = deserialize_job(blob)
    assert restored.priority == 3


async def test_deferred_lives_in_deferred_zset(arq_redis: ArqRedis):
    before = timestamp_ms()
    j = await arq_redis.enqueue_job('foobar', _defer_by=60, _priority=2)
    after = timestamp_ms()

    assert await arq_redis.zcard(deferred_queue_key(default_queue_name)) == 1
    assert await arq_redis.zcard(ready_queue_key(default_queue_name)) == 0

    assert await j.status() == JobStatus.deferred

    # deferred score is the raw run-at timestamp (no priority bucket).
    score = await arq_redis.zscore(deferred_queue_key(default_queue_name), j.job_id)
    assert before + 60_000 <= score <= after + 60_000


async def test_promote_deferred_to_ready_uses_priority(arq_redis: ArqRedis, worker):
    async def noop(ctx):
        pass

    # Tiny defer so promotion happens after one short sleep.
    await arq_redis.enqueue_job('noop', _job_id='lo', _defer_by=0.005, _priority=5)
    await arq_redis.enqueue_job('noop', _job_id='hi', _defer_by=0.005, _priority=1)

    assert await arq_redis.zcard(deferred_queue_key(default_queue_name)) == 2
    assert await arq_redis.zcard(ready_queue_key(default_queue_name)) == 0

    await asyncio.sleep(0.05)

    w: Worker = worker(functions=[func(noop, name='noop')])
    await w._promote_deferred_jobs()

    assert await arq_redis.zcard(deferred_queue_key(default_queue_name)) == 0
    assert await arq_redis.zcard(ready_queue_key(default_queue_name)) == 2

    entries = await arq_redis.zrange(ready_queue_key(default_queue_name), 0, -1, withscores=True)
    by_id = {jid.decode(): int(s) for jid, s in entries}
    # priority=1 has lower score than priority=5 -> picked first.
    assert by_id['hi'] < by_id['lo']
    # bucket sanity: scores live in their priority bucket.
    assert PRIORITY_FACTOR * 1 <= by_id['hi'] < PRIORITY_FACTOR * 2
    assert PRIORITY_FACTOR * 5 <= by_id['lo'] < PRIORITY_FACTOR * 6


async def test_retry_with_defer_moves_to_deferred_zset(arq_redis: ArqRedis, worker):
    attempts = {'n': 0}

    async def sometimes_retries(ctx):
        attempts['n'] += 1
        if attempts['n'] == 1:
            raise Retry(defer=0.05)
        return 'done'

    j = await arq_redis.enqueue_job('sometimes_retries', _job_id='retry-job')
    w: Worker = worker(functions=[func(sometimes_retries, name='sometimes_retries')])

    # First poll: run -> Retry(defer) -> moved into deferred zset.
    await w._poll_iteration()
    await asyncio.gather(*w.tasks.values())

    assert attempts['n'] == 1
    assert await arq_redis.zcard(ready_queue_key(default_queue_name)) == 0
    assert await arq_redis.zcard(deferred_queue_key(default_queue_name)) == 1

    # After the defer window, second poll promotes + runs the job to completion.
    await asyncio.sleep(0.1)
    await w._poll_iteration()
    await asyncio.gather(*w.tasks.values())

    assert attempts['n'] == 2
    assert await arq_redis.queue_size(default_queue_name) == 0
    assert await j.status() == JobStatus.complete


async def test_abort_deferred_job_moves_to_ready(arq_redis: ArqRedis):
    j = await arq_redis.enqueue_job('noop', _defer_by=600, _priority=8)
    assert await arq_redis.zcard(deferred_queue_key(default_queue_name)) == 1

    # Job.abort does its zrem/zadd up front, then blocks waiting for the worker to mark the
    # job complete. We don't have a worker running, so kick off abort as a task, give it a
    # moment to perform the zset moves, then cancel.
    abort_task = asyncio.create_task(j.abort(poll_delay=0.01))
    try:
        await asyncio.sleep(0.05)

        assert await arq_redis.zcard(deferred_queue_key(default_queue_name)) == 0
        assert await arq_redis.zcard(ready_queue_key(default_queue_name)) == 1

        score = int(await arq_redis.zscore(ready_queue_key(default_queue_name), j.job_id))
        # promoted at top priority regardless of original priority
        assert PRIORITY_FACTOR * MIN_PRIORITY <= score < PRIORITY_FACTOR * (MIN_PRIORITY + 1)
    finally:
        abort_task.cancel()
        # Job.abort swallows CancelledError and returns True; either outcome is fine here.
        try:
            await abort_task
        except asyncio.CancelledError:
            pass


async def test_status_deferred_vs_queued(arq_redis: ArqRedis):
    ready_job = await arq_redis.enqueue_job('foobar')
    deferred_job = await arq_redis.enqueue_job('foobar', _defer_by=60)

    assert await ready_job.status() == JobStatus.queued
    assert await deferred_job.status() == JobStatus.deferred


async def test_queue_size_helper_sums_subzsets(arq_redis: ArqRedis):
    for _ in range(2):
        await arq_redis.enqueue_job('foobar')
    for _ in range(3):
        await arq_redis.enqueue_job('foobar', _defer_by=60)

    assert await arq_redis.zcard(ready_queue_key(default_queue_name)) == 2
    assert await arq_redis.zcard(deferred_queue_key(default_queue_name)) == 3
    assert await arq_redis.queue_size(default_queue_name) == 5


async def test_legacy_blob_deserializes_with_default_priority():
    # blobs written by old arq versions don't have a 'priority' key
    legacy = pickle.dumps({'f': 'foo', 'a': (), 'k': {}, 't': None, 'et': timestamp_ms()})
    jd = deserialize_job(legacy)
    assert jd.priority == DEFAULT_PRIORITY
    # sanity: round-trip score helper agrees with the formula
    assert compute_ready_score(jd.priority, 100) == PRIORITY_FACTOR * DEFAULT_PRIORITY + 100

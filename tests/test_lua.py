import pytest
from redis.commands.core import AsyncScript

from arq import ArqRedis
from arq.lua import get_job_from_stream_lua, publish_delayed_job_lua, publish_job_lua


@pytest.fixture()
def publish_delayed_job(arq_redis: ArqRedis) -> AsyncScript:
    return arq_redis.register_script(publish_delayed_job_lua)


@pytest.fixture()
def publish_job(arq_redis: ArqRedis) -> AsyncScript:
    return arq_redis.register_script(publish_job_lua)


@pytest.fixture()
def get_job_from_stream(arq_redis: ArqRedis) -> AsyncScript:
    return arq_redis.register_script(get_job_from_stream_lua)


async def test_publish_delayed_job(arq_redis: ArqRedis, publish_delayed_job: AsyncScript) -> None:
    await arq_redis.zadd('delayed_queue_key', {'job_id': 1000})
    await publish_delayed_job(
        keys=[
            'delayed_queue_key',
            'stream_key',
            'job_message_id_key',
        ],
        args=[
            'job_id',
            '1000',
        ],
    )

    stream_msgs = await arq_redis.xrange('stream_key', '-', '+')
    assert len(stream_msgs) == 1

    saved_msg_id = await arq_redis.get('job_message_id_key')

    msg_id, msg = stream_msgs[0]
    assert msg == {b'job_id': b'job_id', b'score': b'1000'}
    assert saved_msg_id == msg_id

    assert await arq_redis.zrange('delayed_queue_key', '-inf', '+inf', byscore=True) == []

    await publish_delayed_job(
        keys=[
            'delayed_queue_key',
            'stream_key',
            'job_message_id_key',
        ],
        args=[
            'job_id',
            '1000',
        ],
    )

    stream_msgs = await arq_redis.xrange('stream_key', '-', '+')
    assert len(stream_msgs) == 1

    saved_msg_id = await arq_redis.get('job_message_id_key')
    assert saved_msg_id == msg_id


async def test_publish_job(arq_redis: ArqRedis, publish_job: AsyncScript) -> None:
    msg_id = await publish_job(
        keys=[
            'stream_key',
            'job_message_id_key',
        ],
        args=[
            'job_id',
            '1000',
            '1000',
        ],
    )

    stream_msgs = await arq_redis.xrange('stream_key', '-', '+')
    assert len(stream_msgs) == 1

    saved_msg_id = await arq_redis.get('job_message_id_key')
    assert saved_msg_id == msg_id

    msg_id, msg = stream_msgs[0]
    assert msg == {b'job_id': b'job_id', b'score': b'1000'}
    assert saved_msg_id == msg_id


async def test_get_job_from_stream(
    arq_redis: ArqRedis, publish_job: AsyncScript, get_job_from_stream: AsyncScript
) -> None:
    msg_id = await publish_job(
        keys=[
            'stream_key',
            'job_message_id_key',
        ],
        args=[
            'job_id',
            '1000',
            '1000',
        ],
    )

    job = await get_job_from_stream(
        keys=[
            'stream_key',
            'job_message_id_key',
        ],
    )

    assert job == [msg_id, [b'job_id', b'job_id', b'score', b'1000']]

    await arq_redis.delete('job_message_id_key')
    job = await get_job_from_stream(
        keys=[
            'stream_key',
            'job_message_id_key',
        ],
    )
    assert job is None

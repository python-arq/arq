import asyncio

import msgpack  # installable with "pip install msgpack"

from arq import create_pool
from arq.connections import RedisSettings


async def the_task(ctx):
    return 42


async def main():
    redis = await create_pool(
        RedisSettings(),
        job_serializer=msgpack.packb,
        job_deserializer=lambda b: msgpack.unpackb(b, raw=False),
    )
    await redis.enqueue_job('the_task')


class WorkerSettings:
    functions = [the_task]
    job_serializer = msgpack.packb
    # refer to MsgPack's documentation as to why raw=False is required
    job_deserializer = lambda b: msgpack.unpackb(b, raw=False)


if __name__ == '__main__':
    asyncio.run(main())

import asyncio

from arq import create_pool
from arq.connections import RedisSettings

async def the_task(ctx):
    await asyncio.sleep(5)

async def main():
    redis = await create_pool(RedisSettings())
    await redis.enqueue_job('the_task')

class WorkerSettings:
    functions = [the_task]

if __name__ == '__main__':
    asyncio.run(main())

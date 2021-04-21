import asyncio
from datetime import datetime, timedelta

from arq import create_pool
from arq.connections import RedisSettings

async def the_task(ctx):
    print('this is the tasks, delay since enqueueing:', datetime.now() - ctx['enqueue_time'])

async def main():
    redis = await create_pool(RedisSettings())

    # deferred by 10 seconds
    await redis.enqueue_job('the_task', _defer_by=10)

    # deferred by 1 minute
    await redis.enqueue_job('the_task', _defer_by=timedelta(minutes=1))

    # deferred until jan 28th 2032, you'll be waiting a long time for this...
    await redis.enqueue_job('the_task', _defer_until=datetime(2032, 1, 28))

class WorkerSettings:
    functions = [the_task]

if __name__ == '__main__':
    asyncio.run(main())

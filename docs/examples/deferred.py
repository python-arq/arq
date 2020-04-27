import asyncio
from datetime import datetime, timedelta, timezone

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
    # when defininig a datetime make sure it includes time zone information otherwise it will be assumed to be utc without conversion
    await redis.enqueue_job('the_task', _defer_until=datetime(2032, 1, 28, tzinfo=timezone.utc))

class WorkerSettings:
    functions = [the_task]

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

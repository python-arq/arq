import asyncio

from datetime import datetime

from arq import create_pool, cron, func
from arq.connections import RedisSettings
# requires `pip install devtools`, used for pretty printing of job info
from devtools import debug


async def the_task(ctx):
    print('running the task')
    return 42

async def cron_the_task(ctx, crontab, funcname):
    crn = cron(cron_the_task, **crontab)
    redis = ctx['redis']
    while True:
        n = datetime.now()
        crn.set_next(n)
        wait = (crn.next_run - n).total_seconds()
        print(f'{n}: need to wait till {wait} for next activation')
        await asyncio.sleep(wait)
        await redis.enqueue_job(funcname)

async def main():
    redis = await create_pool(RedisSettings())

    job = await redis.enqueue_job(
        'cron_a_task', {'hour': 19, 'minute': 23}, 'the_task'
    )
    await asyncio.sleep(30)
    # stop the crontab task scheduler
    success = await job.abort()
    if success:
        print('crontab cancelled successfully')
    else:
        print('crontab not cancelled successfully')
    """
    >  19:22:43: Starting worker for 2 functions: cron_a_task, the_task
    >  19:22:43: redis_version=5.0.5 mem_usage=1.16M clients_connected=18 db_keys=0
    >  19:22:44:   0.15s → 754fb45d0aeb49ca8f295e1895de8c45:cron_a_task({'hour': 19, 'minute': 23}, 'the_task')
    >  2020-03-26 19:22:44.039359: need to wait till 16.084097 for next activation
    >  2020-03-26 19:23:00.129139: need to wait till 86399.994317 for next activation
    >  19:23:00:   0.47s → ad77622cf26c4b38b52a757f635e0c70:the_task()
    >  running the task
    >  19:23:00:   0.00s ← ad77622cf26c4b38b52a757f635e0c70:the_task ● 42
    >  19:23:14:  30.10s 🛇  754fb45d0aeb49ca8f295e1895de8c45:cron_a_task aborted
    """

class WorkerSettings:
    # default timeout is 300s, timeout=0 disable it and wait till job.abort() is called
    functions = [
        func(cron_the_task, name='cron_a_task', timeout=0), 
        the_task
    ]

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

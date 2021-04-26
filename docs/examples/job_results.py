import asyncio

from arq import create_pool
from arq.connections import RedisSettings
# requires `pip install devtools`, used for pretty printing of job info
from devtools import debug

async def the_task(ctx):
    print('running the task')
    return 42

async def main():
    redis = await create_pool(RedisSettings())

    job = await redis.enqueue_job('the_task')

    # get the job's id
    print(job.job_id)
    """
    >  68362958a244465b9be909db4b7b5ab4 (or whatever)
    """

    # get information about the job, will include results if the job has finished, but
    # doesn't await the job's result
    debug(await job.info())
    """
    >   docs/examples/job_results.py:23 main
    JobDef(
        function='the_task',
        args=(),
        kwargs={},
        job_try=None,
        enqueue_time=datetime.datetime(2019, 4, 23, 13, 58, 56, 781000),
        score=1556027936781
    ) (JobDef)
    """

    # get the Job's status
    print(await job.status())
    """
    >  JobStatus.queued
    """

    # poll redis for the job result, if the job raised an exception,
    # it will be raised here
    # (You'll need the worker running at the same time to get a result here)
    print(await job.result(timeout=5))
    """
    >  42
    """

class WorkerSettings:
    functions = [the_task]

if __name__ == '__main__':
    asyncio.run(main())

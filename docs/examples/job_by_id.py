import asyncio

from arq import create_pool
from arq.connections import RedisSettings
from arq.jobs import Job


async def main():
    redis = await create_pool(RedisSettings())

    # if the job_id is already known, instantiate Job directly to query it
    job = Job(job_id='68362958a244465b9be909db4b7b5ab4', redis=redis)

    print(await job.status())
    """
    >  JobStatus.not_found  (if no job with this id exists)
    """

    print(await job.info())
    """
    >  None  (if the job is absent or its key has expired)
    """


if __name__ == '__main__':
    asyncio.run(main())

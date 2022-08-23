import asyncio
from httpx import AsyncClient
from arq import create_pool, Retry
from arq.connections import RedisSettings

async def download_content(ctx, url):
    session: AsyncClient = ctx['session']
    response = await session.get(url)
    if response.status_code != 200:
        # retry the job with increasing back-off
        # delays will be 5s, 10s, 15s, 20s
        # after max_tries (default 5) the job will permanently fail
        raise Retry(defer=ctx['job_try'] * 5)
    return len(response.text)

async def startup(ctx):
    ctx['session'] = AsyncClient()

async def shutdown(ctx):
    await ctx['session'].aclose()

async def main():
    redis = await create_pool(RedisSettings())
    await redis.enqueue_job('download_content', 'https://httpbin.org/status/503')

class WorkerSettings:
    functions = [download_content]
    on_startup = startup
    on_shutdown = shutdown

if __name__ == '__main__':
    asyncio.run(main())

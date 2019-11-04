import asyncio
from aiohttp import ClientSession
from arq import create_pool, Retry
from arq.connections import RedisSettings

async def download_content(ctx, url):
    session: ClientSession = ctx['session']
    async with session.get(url) as response:
        if response.status != 200:
            # retry the job with increasing back-off
            # delays will be 5s, 10s, 15s, 20s
            # after max_tries (default 5) the job will permanently fail
            raise Retry(defer=ctx['job_try'] * 5)
        content = await response.text()
    return len(content)

async def startup(ctx):
    ctx['session'] = ClientSession()

async def shutdown(ctx):
    await ctx['session'].close()

async def main():
    redis = await create_pool(RedisSettings())
    await redis.enqueue_job('download_content', 'https://httpbin.org/status/503')

class WorkerSettings:
    functions = [download_content]
    on_startup = startup
    on_shutdown = shutdown

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

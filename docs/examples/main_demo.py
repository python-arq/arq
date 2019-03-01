import asyncio
from aiohttp import ClientSession
from arq import create_pool
from arq.connections import RedisSettings


async def download_content(ctx, url):
    session: ClientSession = ctx['session']
    async with session.get(url) as response:
        content = await response.text()
        print(f'{url}: {content:.80}...')
    return len(content)


async def startup(ctx):
    ctx['session'] = ClientSession()


async def shutdown(ctx):
    await ctx['session'].close()


# WorkerSettings defines the settings to use when creating the work,
# it's used by the arq cli
class WorkerSettings:
    functions = [download_content]
    on_startup = startup
    on_shutdown = shutdown


async def main():
    redis = await create_pool(RedisSettings())
    for url in ('https://facebook.com', 'https://microsoft.com', 'https://github.com'):
        await redis.enqueue_job('download_content', url)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

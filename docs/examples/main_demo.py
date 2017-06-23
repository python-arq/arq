import asyncio
from aiohttp import ClientSession
from arq import Actor, BaseWorker, concurrent


class Downloader(Actor):
    async def startup(self):
        self.session = ClientSession(loop=self.loop)

    @concurrent
    async def download_content(self, url):
        async with self.session.get(url) as response:
            content = await response.read()
            print('{}: {:.80}...'.format(url, content.decode()))
        return len(content)

    async def shutdown(self):
        self.session.close()


class Worker(BaseWorker):
    shadows = [Downloader]


async def download_lots():
    d = Downloader()
    for url in ('https://facebook.com', 'https://microsoft.com', 'https://github.com'):
        await d.download_content(url)
    await d.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_lots())

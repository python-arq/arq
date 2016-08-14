Usage
-----

Usage is best described with an example, ``demo.py``:

.. code:: python

    import asyncio
    from aiohttp import ClientSession
    from arq import Actor, BaseWorker, concurrent


    class Downloader(Actor):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.session = ClientSession(loop=self.loop)

        @concurrent
        async def download_content(self, url):
            async with self.session.get(url) as response:
                assert response.status == 200
                content = await response.read()
                print('{}: {:.80}...'.format(url, content.decode()))
            return len(content)

        async def close(self):
            await super().close()
            self.session.close()


    class Worker(BaseWorker):
        shadows = [Downloader]


    async def download_lots(loop):
        d = Downloader(loop=loop)
        for url in ('https://facebook.com', 'https://microsoft.com', 'https://github.com'):
            await d.download_content(url)
        await d.close()

    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(download_lots(loop))

You can then enqueue the jobs with just ``python demo.py``, and run the
worker to do the jobs with ``arq demo.py``.

To enqueue the jobs, simply run the script::

    python demo.py

To execute the jobs, either after running ``demo.py`` or before/during::

    arq demo.py

For details on the ``arq`` CLI::

    arq --help

Still to be documented but working fine: \* multiple queues \* multiple
actors \* worker ``max_concurrency`` \* worker job timeout \* advanced
worker logging \* ``.testing`` py.test plugins.

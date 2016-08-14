Usage
-----

Usage is best described by example.

Simple Usage
............

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
                content = await response.read()
                print('{}: {:.80}...'.format(url, content.decode()))
            return len(content)

        async def close(self):
            await super().close()
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

(This script is complete, it should run "as is" both to enqueue jobs and run them)

To enqueue the jobs, simply run the script::

    python demo.py

To execute the jobs, either after running ``demo.py`` or before/during::

    arq demo.py

For details on the ``arq`` CLI::

    arq --help

Multiple Queues
...............

Functions can be assigned to different queues, by default arq defines three queues:
``HIGH_QUEUE``, ``DEFAULT_QUEUE`` and ``LOW_QUEUE`` which are prioritised by the worker in the order.

.. code:: python

    from arq import Actor, concurrent

    class RegistrationEmail(Actor):
        @concurrent
        async def email_standard_user(self, user_id):
            send_user_email(user_id)

        @concurrent(Actor.HIGH_QUEUE)
        async def email_premium_user(self, user_id):
            send_user_email(user_id)

(Just a snippet, won't run as is)

Direct Enqueuing
................

Functions can we enqueued direction whether or no they're decorated with ``@concurrent``.

.. code:: python

    from arq import Actor, concurrent

    class FooBar(Actor):
        async def foo(self, a, b, c):
            print(a + b + c)

    async def main():
        foobar = FooBar()
        await foobar.enqueue_job('foo', 1, 2, c=48, queue=Actor.LOW_QUEUE)
        await foobar.enqueue_job('foo', 1, 2, c=48)  # this will be queued in DEFAULT_QUEUE


(This script is almost complete except for ``loop.run_until_complete(main())`` as above to run ``main``,
you would also need to define a worker to run the jobs)

See :meth:`arq.main.Actor.enqueue_job` for more details.

Worer Customisation
...................

Workers can be customised in numerous ways, this is preferred to command line arguments as it's easier to
document and record.

.. code:: python

    from arq import BaseWorker

    class Worker(BaseWorker):
        # execute jobs from both Downloader and FooBar above
        shadows = [Downloader, FooBar]

        # allow lots and lots of jobs to run simultaniously, default 50
        max_concurrent_tasks = 500

        # force the worker to close quickly after a termination signal is received, default 6
        shutdown_delay = 2

        # jobs may not take more than 10 seconds, default 60
        timeout_seconds = 10

        def logging_config(self, verbose):
            conf = super().logging_config(verbose)
            # alter logging setup to set arq.jobs level to WARNING
            conf['loggers']['arq.jobs']['level'] = 'WARNING'
            return conf

(This script is more-or-less complete,
provided ``Downloader`` and ``FooBar`` are defined and import it should run "as is")

See :meth:`arq.worker.BaseWorker` for more customisation options.

For more information on logging see :meth:`arq.logs.default_log_config`.

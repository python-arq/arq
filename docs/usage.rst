Usage
-----

Usage is best described by example.

Simple Usage
............

.. literalinclude:: demo.py

(This script is complete, it should run "as is" both to enqueue jobs and run them)

To enqueue the jobs, simply run the script::

    python demo.py

To execute the jobs, either after running ``demo.py`` or before/during::

    arq demo.py

For details on the *arq* CLI::

    arq --help

Startup & Shutdown coroutines
.............................

The ``startup`` and ``shutdown`` are provided as a convenient way to run logic as actors start and finish,
however it's important to not that these methods **are not called by default when actors are initialised or closed**.
They are however called when the actor started and closed on the worker, eg. in "shadow" mode, see above.
In other words: if you need these coroutines to be called when using an actor in your code, that's your responsibility.

For example, in the above code there's no need for ``self.session`` when using the actor in "default" mode, eg. called
with ``python demo.py``, so neither ``startup`` or ``shutdown`` are called.

Health checks
.............

*arq* will automatically record some info about it's current state in redis every ``health_check_interval`` seconds,
see :attr:`arq.worker.BaseWorker.health_check_interval`. That key/value will expire after ``health_check_interval + 1``
so you can be sure if the variable exists you can be sure *arq* is alive and kicking (technically you can be sure it
was alive and kicking ``health_check_interval`` seconds ago).

You can run a health check with the CLI using (assuming you're using the above example)::

    arq --check demo.py

The command will output the value of the health check if found,
then exit ``0`` if the key was found and ``1`` if it was not.

A health check value takes the following form::

    Feb-20_11:02:40 j_complete=0 j_failed=0 j_timedout=0 j_ongoing=0 q_high=0 q_dft=0 q_low=0

Where the values have the following meaning:

* ``j_complete`` the number of jobs completed
* ``j_failed`` the number of jobs which have failed eg. raised an exception
* ``j_timedout`` the number of jobs which have timed out, eg. exceeded :attr:`arq.worker.BaseWorker.timeout_seconds`
  and been cancelled
* ``j_ongoing`` the number of jobs currently being performed.
* ``q_*`` the number of pending jobs in each queue.

Multiple Queues
...............

Functions can be assigned to different queues, by default arq defines three queues:
``HIGH_QUEUE``, ``DEFAULT_QUEUE`` and ``LOW_QUEUE`` which are prioritised by the worker in that order.

.. code:: python

    from arq import Actor, concurrent

    class RegistrationEmail(Actor):
        @concurrent
        async def email_standard_user(self, user_id):
            send_user_email(user_id)

        @concurrent(Actor.HIGH_QUEUE)
        async def email_premium_user(self, user_id):
            send_user_email(user_id)

(Just a snippet, won't run "as is")

Direct Enqueuing
................

Functions can we enqueued directly whether or no they're decorated with ``@concurrent``.

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

Worker Customisation
....................

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

        # number of seconds between health checks, default 60
        health_check_interval = 30

        def logging_config(self, verbose):
            conf = super().logging_config(verbose)
            # alter logging setup to set arq.jobs level to WARNING
            conf['loggers']['arq.jobs']['level'] = 'WARNING'
            return conf

(This script is more-or-less complete,
provided ``Downloader`` and ``FooBar`` are defined and imported it should run "as is")

See :meth:`arq.worker.BaseWorker` for more customisation options.

For more information on logging see :meth:`arq.logs.default_log_config`.

Usage
-----

Usage is best described by example.

Simple Usage
............

.. literalinclude:: examples/main_demo.py

(This script is complete, it should run "as is" both to enqueue jobs and run them)

To enqueue the jobs, simply run the script::

    python demo.py

To execute the jobs, either after running ``demo.py`` or before/during::

    arq demo.py

For details on the *arq* CLI::

    arq --help

Startup & Shutdown coroutines
.............................

The ``startup`` and ``shutdown`` coroutines are provided as a convenient way to run logic as actors start and finish,
however it's important to not that these methods **are not called by default when actors are initialised or closed**.
They are however called when the actor was started and closed on the worker, eg. in "shadow" mode, see above.
In other words: if you need these coroutines to be called when using an actor in your code; that's your responsibility.

For example, in the above example there's no need for ``self.session`` when using the actor in "default" mode,
eg. called with ``python demo.py``, so neither ``startup`` or ``shutdown`` are called.

Health checks
.............

*arq* will automatically record some info about it's current state in redis every ``health_check_interval`` seconds,
see :attr:`arq.worker.BaseWorker.health_check_interval`. That key/value will expire after ``health_check_interval + 1``
seconds so you can be sure if the variable exists *arq* is alive and kicking (technically you can be sure it
was alive and kicking ``health_check_interval`` seconds ago).

You can run a health check with the CLI (assuming you're using the above example)::

    arq --check demo.py

The command will output the value of the health check if found;
then exit ``0`` if the key was found and ``1`` if it was not.

A health check value takes the following form::

    Feb-20_11:02:40 j_complete=0 j_failed=0 j_timedout=0 j_ongoing=0 q_high=0 q_dft=0 q_low=0

Where the items have the following meaning:

* ``j_complete`` the number of jobs completed
* ``j_failed`` the number of jobs which have failed eg. raised an exception
* ``j_timedout`` the number of jobs which have timed out, eg. exceeded :attr:`arq.worker.BaseWorker.timeout_seconds`
  and been cancelled
* ``j_ongoing`` the number of jobs currently being performed
* ``q_*`` the number of pending jobs in each queue

Cron Jobs
.........

Functions can be scheduled to be run periodically at specific times

.. literalinclude:: examples/cron.py

See :meth:`arq.main.cron` for details on the available arguments and how how cron works.

Usage roughly shadows `cron <https://helpmanual.io/man8/cron/>`_ except ``None`` is equivilent on ``*`` in crontab.
As per the example sets can be used to run at multiple of the given unit.

Note that ``second`` defaults to ``0`` so you don't in inadvertently run jobs every second and ``microsecond``
defaults to ``123456`` so you don't inadvertently run jobs every microsecond and so *arq* avoids enqueuing jobs
at the top of a second when the world is generally slightly busier.

Multiple Queues
...............

Functions can be assigned to different queues, by default arq defines three queues:
``HIGH_QUEUE``, ``DEFAULT_QUEUE`` and ``LOW_QUEUE`` which are prioritised by the worker in that order.

.. literalinclude:: examples/multiple_queues.py

(Just a snippet, won't run "as is")

Direct Enqueuing
................

Functions can we enqueued directly whether or no they're decorated with ``@concurrent``.

.. literalinclude:: examples/direct_enqueuing.py


(This script is almost complete except for ``loop.run_until_complete(main())`` as above to run ``main``,
you would also need to define a worker to run the jobs)

See :meth:`arq.main.Actor.enqueue_job` for more details.

Worker Customisation
....................

Workers can be customised in numerous ways, this is preferred to command line arguments as it's easier to
document and record.

.. literalinclude:: examples/worker_customisation.py

(This script is more-or-less complete,
provided ``Downloader`` and ``FooBar`` are defined and imported it should run "as is")

See :meth:`arq.worker.BaseWorker` for more customisation options.

For more information on logging see :meth:`arq.logs.default_log_config`.

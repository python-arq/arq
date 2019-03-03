arq
===

.. toctree::
   :maxdepth: 2

|pypi| |license|

Current Version: |version|

Job queues and RPC in python with asyncio and redis.

*arq* was conceived as a simple, modern and performant successor to rq_.

.. warning::

   In ``v0.16`` *arq* was **COMPLETELY REWRITTEN** to use an entirely different approach to registering workers,
   enqueueing jobs and processing jobs. You will need to either keep using ``v0.15`` or entirely rewrite you *arq*
   integration to use ``v0.16``.

   See `here <./old/index.html>`_ for old docs.

Why use *arq*?

**non-blocking**
    *arq* is built using python 3's asyncio_ allowing
    non-blocking job enqueuing and execution. Multiple jobs (potentially hundreds) can be run simultaneously
    using a pool of *asyncio* ``Tasks``.

**powerful-features**
    Deferred execution, easy retrying of jobs, and pessimistic execution (:ref:`see below <usage>`)
    means *arq* is great for critical jobs that **must** be completed.

**fast**
    Asyncio and no forking make *arq* around 7x faster than
    *rq* for short jobs with no io. With io that might increase to around 40x
    faster. (TODO)

**elegant**
    I'm a long time contributor to and user of `rq`_, *arq* is designed to be simpler, clearer and more powerful.

**small**
    and easy to reason with - currently *arq* is only about 700 lines, that won't change significantly.

Install
-------

Just::

    pip install arq

Redesigned to be less elegant?
------------------------------

The approach used in *arq* ``v0.16`` of enqueueing jobs by name rather than "just calling a function" and knowing it
will be called on the worker (as used in *arq* ``<= v0.15``, rq, celery et al.) might seem less elegant,
but it's for good reason.

This approach means your frontend (calling the worker) doesn't need access to the worker code,
meaning better code separation and possibly smaller images etc.

.. _usage:

Usage
-----

.. warning::

   **Jobs may be called more than once!**

   *arq* v0.16 has what I'm calling "pessimistic execution": jobs aren't removed from the queue until they've either
   succeeded or failed. If the worker shuts down, the job will be cancelled immediately and will remain in the queue
   to be run again when the worker starts up again (or run by another worker which is still running).

   (This differs from other similar libraries like *arq* ``<= v0.15``, rq, celery et al. where jobs generally don't get
   rerun when a worker shuts down. This in turn requires complex logic to try and let jobs finish before
   shutting down (I wrote the ``HerokuWorker`` for rq), however this never really works unless either: all jobs take
   less than 6 seconds or your worker never shuts down when a job is running (impossible).)

   All *arq* jobs should therefore be designed to cope with being called repeatedly if they're cancelled,
   eg. use database transactions, idempotency keys or redis to mark when an API request or similar has succeeded
   to avoid making it twice.

   **In summary:** sometimes *exactly once* can be hard or impossible, *arq* favours multiple times over zero times.

Simple Usage
............

.. literalinclude:: examples/main_demo.py

(This script is complete, it should run "as is" both to enqueue jobs and run them)

To enqueue the jobs, simply run the script::

    python demo.py

To execute the jobs, either after running ``demo.py`` or before/during::

    arq demo.WorkerSettings

Append (``--burst``) to stop the worker once all jobs have finished.

For details on the *arq* CLI::

    arq --help

Startup & Shutdown coroutines
.............................

The ``on_startup`` and ``on_startup`` coroutines are provided as a convenient way to run logic as the worker
starts and finishes.

For example, in the above example ``session`` is created once when the work starts up and is then used in subsequent
jobs.

Deferring Jobs
..............

By default, when a job is enqueued it will run as soon as possible (provided a worker is running). However
you can schedule jobs to run in the future, either by a given time delta (``_defer_by``) or
at a particular time ``_defer_until``.

.. literalinclude:: examples/deferred.py

TODO:

* job ids and uniqueness
* job results
* RetryJob and cancellation
* all the arguments to Worker, func and cron and enqueue_job

Health checks
.............

*arq* will automatically record some info about it's current state in redis every ``health_check_interval`` seconds.
That key/value will expire after ``health_check_interval + 1`` seconds so you can be sure if the variable exists *arq*
is alive and kicking (technically you can be sure it was alive and kicking ``health_check_interval`` seconds ago).

You can run a health check with the CLI (assuming you're using the above example)::

    arq --check demo.WorkerSettings

The command will output the value of the health check if found;
then exit ``0`` if the key was found and ``1`` if it was not.

A health check value takes the following form::

    Mar-01 17:41:22 j_complete=0 j_failed=0 j_retried=0 j_ongoing=0 queued=0

Where the items have the following meaning:

* ``j_complete`` the number of jobs completed
* ``j_failed`` the number of jobs which have failed eg. raised an exception
* ``j_ongoing`` the number of jobs currently being performed
* ``j_retried`` the number of jobs retries run

Cron Jobs
.........

Functions can be scheduled to be run periodically at specific times

.. literalinclude:: examples/cron.py

Usage roughly shadows `cron <https://helpmanual.io/man8/cron/>`_ except ``None`` is equivalent on ``*`` in crontab.
As per the example sets can be used to run at multiple of the given unit.

Note that ``second`` defaults to ``0`` so you don't in inadvertently run jobs every second and ``microsecond``
defaults to ``123456`` so you don't inadvertently run jobs every microsecond and so *arq* avoids enqueuing jobs
at the top of a second when the world is generally slightly busier.

.. include:: ../HISTORY.rst

.. |pypi| image:: https://img.shields.io/pypi/v/arq.svg
   :target: https://pypi.python.org/pypi/arq
.. |license| image:: https://img.shields.io/pypi/l/arq.svg
   :target: https://github.com/samuelcolvin/arq
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _rq: http://python-rq.org/

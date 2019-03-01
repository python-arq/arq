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

    arq demo.WorkerSettings

For details on the *arq* CLI::

    arq --help

Startup & Shutdown coroutines
.............................

The ``on_startup`` and ``on_startup`` coroutines are provided as a convenient way to run logic as the worker
starts and finishes.

For example, in the above example ``session`` is created once when the work starts up and is then used in subsiquent
jobs.

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

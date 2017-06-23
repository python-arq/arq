arq
===

.. toctree::
   :maxdepth: 2

|pypi| |license|

Current Version: |version|

Job queues and RPC in python with asyncio, redis and msgpack.

*arq* was conceived as a simple, modern and performant successor to `rq`_.

Why use *arq*?

**non-blocking**
    *arq* is built using python 3's `asyncio`_ allowing
    non-blocking job enqueuing and execution. Multiple jobs (potentially hundreds) can be run simultaneously
    using a pool of *asyncio* ``Tasks``.

**pre-forked**
    The worker starts two processes and uses the subprocess to execute
    all jobs, there's no overhead in forking a process for each job.

**fast**
    Asyncio, pre-forking and use of `msgpack`_ for job encoding make
    *arq* around 7x faster (see `benchmarks`_) than
    *rq* for short jobs with no io. With io that might increase to around 40x
    faster. (TODO)

**elegant**
    *arq* uses a novel approach to variable scope with the ``@concurrent`` decorator being applied to bound
    methods of ``Actor`` classes which hold the connection pool. This works well with `aiohttp`_, allows for easier
    testing and avoids extended head scratching over how variables like connections are defined (is this attached
    to the request? or thread local? or truly global? where am I, hell, what does global even mean?).

**small**
    and easy to reason with - currently *arq* is only about 700 lines, that won't change significantly.

Dependencies
------------

Required **before pip install**:

* `Python 3.6.0+`_ *asyncio* is used throughout with new style ``async/await`` syntax and ``async yield``.
* `Redis`_ Redis lists are used to communication between the front end and worker, redis can also be used to store job results.

Installed as dependencies by pip:

* `msgpack`_ is used for its simplicity and performance to encode and decode job information.
* `aioredis`_ is used as the non-block *asyncio* interface to redis.
* `click`_ is used for the CLI interface *"arq"*.


Install
-------

Just::

    pip install arq

Terminology
-----------

The old computer science proverb/joke goes:

    There are only two challenges in computer science: cache invalidation, naming things and the n + 1 problem.

*arq* tries to avoid confusion over what's named what by using generally accepted terminology as much as possible,
however a few terms (like "actors" and "shadows") are not so standard and bear describing:

An **Actor** is a class with some concurrent methods, you can define and use multiple actors. Actors hold a
reference to a redis pool for enqueuing jobs and are generally singletons.

The **Worker** is the class which is responsible for running jobs for one or more actors. Workers should inherit
from ``BaseWorker``, your application will generally have just one worker.

Actors are therefore used in two distinctly different modes:

* **default** mode where you initialise, then use and abuse the actor including calling concurrent methods and
  thereby enqueuing jobs
* **shadow** mode where the actor was initialised by the worker in order to perform jobs enqueued by the actor in
  default (or even shadow) mode.

It's possible to check what mode an actor is in by checking the ``is_shadow`` variable.

.. include:: usage.rst

API Reference
-------------

.. automodule:: arq.main
   :members:

.. automodule:: arq.worker
   :members:

.. automodule:: arq.drain
   :members:

.. automodule:: arq.jobs
   :members: Job, DatetimeJob

.. automodule:: arq.logs
   :members:

.. automodule:: arq.utils
   :members: RedisSettings, RedisMixin, create_tz, timestamp, timestamp, to_unix_ms_tz, to_unix_ms, from_unix_ms, gen_random, truncate

.. automodule:: arq.testing
   :members:

.. include:: ../HISTORY.rst

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |pypi| image:: https://img.shields.io/pypi/v/arq.svg
   :target: https://pypi.python.org/pypi/arq
.. |license| image:: https://img.shields.io/pypi/l/arq.svg
   :target: https://github.com/samuelcolvin/arq
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _rq: http://python-rq.org/
.. _msgpack: http://msgpack.org/index.html
.. _benchmarks: https://github.com/samuelcolvin/arq/tree/master/performance_benchmarks
.. _aiohttp: http://aiohttp.readthedocs.io/en/stable/
.. _Python 3.6.0+: https://docs.python.org/3/whatsnew/3.6.html
.. _Redis: http://redis.io/
.. _aioredis: http://aioredis.readthedocs.io/
.. _click: http://click.pocoo.org/6/

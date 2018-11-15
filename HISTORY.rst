.. :changelog:

History
-------

v0.15.0 (2018-11-15)
....................
* update dependencies
* reconfigure ``Job``, return a job instance when enqueuing tasks #93
* tweaks to docs #106

v0.14.0 (2018-05-28)
....................
* package updates, particularly compatibility for ``msgpack 0.5.6``

v0.13.0 (2017-11-27)
....................
* **Braking Change:** integration with aioredis >= 1.0, basic usage hasn't changed but
  look at aioredis's migration docs for changes in redis API #76

v0.12.0 (2017-11-16)
....................
* better signal handling, support ``uvloop`` #73
* drain pending tasks and drain task cancellation #74
* add aiohttp and docker demo ``/demo`` #75

v0.11.0 (2017-08-25)
....................
* extract ``create_pool_lenient`` from ``RedixMixin``
* improve redis connection traceback

v0.10.4 (2017-08-22)
....................
* ``RedisSettings`` repr method
* add ``create_connection_timeout`` to connection pool

v0.10.3 (2017-08-19)
....................
* fix bug with ``RedisMixin.get_redis_pool`` creating multiple queues
* tweak drain logs

v0.10.2 (2017-08-17)
....................
* only save job on task in drain if re-enqueuing
* add semaphore timeout to drains
* add key count to ``log_redis_info``

v0.10.1 (2017-08-16)
....................
* correct format of ``log_redis_info``

v0.10.0 (2017-08-16)
....................
* log redis version when starting worker, fix #64
* log "connection success" when connecting to redis after connection failures, fix #67
* add job ids, for now they're just used in logging, fix #53

v0.9.0 (2017-06-23)
...................
* allow set encoding in msgpack for jobs #49
* cron tasks allowing scheduling of functions in the future #50
* **Breaking change:** switch ``to_unix_ms`` to just return the timestamp int, add ``to_unix_ms_tz`` to
  return tz offset too

v0.8.1 (2017-06-05)
...................
* uprev setup requires
* correct setup arguments

v0.8.0 (2017-06-05)
...................
* add ``async-timeout`` dependency
* use async-timeout around ``shadow_factory``
* change logger name for control process log messages
* use ``Semaphore`` rather than ``asyncio.wait(...return_when=asyncio.FIRST_COMPLETED)`` for improved performance
* improve log display
* add timeout and retry logic to ``RedisMixin.create_redis_pool``

v0.7.0 (2017-06-01)
...................
* implementing reusable ``Drain`` which takes tasks from a redis list and allows them to be execute asynchronously.
* Drain uses python 3.6 ``async yield``, therefore **python 3.5 is no longer supported**.
* prevent repeated identical health check log messages

v0.6.1 (2017-05-06)
...................
* mypy at last passing, #30
* adding trove classifiers, #29

v0.6.0 (2017-04-14)
...................
* add ``StopJob`` exception for cleaning ending jobs, #21
* add ``flushdb`` to ``MockRedis``, #23
* allow configurable length job logging via ``log_curtail`` on ``Worker``, #28

v0.5.2 (2017-02-25)
...................
* add ``shadow_kwargs`` method to ``BaseWorker`` to make customising actors easier.

v0.5.1 (2017-02-25)
...................
* reimplement worker reuse as it turned out to be useful in tests.

v0.5.0 (2017-02-20)
...................
* use ``gather`` rather than ``wait`` for startup and shutdown so exceptions propagate.
* add ``--check`` option to confirm arq worker is running.

v0.4.1 (2017-02-11)
...................
* fix issue with ``Concurrent`` class binding with multiple actor instances.

v0.4.0 (2017-02-10)
...................
* improving naming of log handlers and formatters
* upgrade numerous packages, nothing significant
* add ``startup`` and ``shutdown`` methods to actors
* switch ``@concurrent`` to return a ``Concurrent`` instance so the direct method is accessible via ``<func>.direct``

v0.3.2 (2017-01-24)
...................
* improved solution for preventing new jobs starting when the worker is about to stop
* switch ``SIGRTMIN`` > ``SIGUSR1`` to work with mac

v0.3.1 (2017-01-20)
...................
* fix main process signal handling so the worker shuts down when just the main process receives a signal
* re-enqueue un-started jobs popped from the queue if the worker is about to exit

v0.3.0 (2017-01-19)
...................
* rename settings class to ``RedisSettings`` and simplify significantly

v0.2.0 (2016-12-09)
...................
* add ``concurrency_enabled`` argument to aid in testing
* fix conflict with unitest.mock

v0.1.0 (2016-12-06)
...................
* prevent logs disabling other logs

v0.0.6 (2016-08-14)
...................
* first proper release

.. :changelog:

History
-------

v0.5.1 (2017-02-25)
...................
* reimplement worker reuse as it turned out to be useful in tests.

v0.5.0 (2017-02-20)
...................
* use ``gather`` rather than ``wait`` for startup and shutdown so exceptions propagate.
* add ``--check`` option to confirm arq worker is running.

v0.4.1 (2017-02-11)
...................
* fix issue with ``Concurrent`` class binding with multiple actor instances

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

arq
===

.. toctree::
   :maxdepth: 2

|Build Status| |Coverage| |pypi| |license|


Job queues in python with asyncio, redis and msgpack.


**non-blocking**
    arq is built using python's `asyncio`_ allowing
    non-blocking job enqueuing and job execution.

**pre-forked**
    In other works the worker starts two processes and uses the subprocess to execute
    all jobs, there's no overhead in forking a process for each job.

**fast**
    Asyncio, pre-forking and use of msgpack for job encoding make
    arq around 7x faster (see `benchmarks`_) than
    rq for small jobs with no io, with io that might increase to around 40x
    faster. TODO

**elegant**
    arq uses a novel approach to variable scope
    with the ``@concurrent`` decorator being applied to bound methods of
    "Actor" classes which hold the connection pool. This works well with
    `aiohttp`_, avoids extended
    head scratching over how variables like connections are defined (is this
    attached to the request? or thread local? or truly global? where am I,
    what does global mean?) and allows for easier testing. See below.

**small**
    and easy to reason with - currently arq is only about 500
    lines, that won't change significantly.


Install
-------

**Python >=3.5** and **redis** are required. After that:

::

    pip install arq

Should install everything you need.

.. include:: usage.rst

API Reference
-------------

.. automodule:: arq.main
   :members:

.. automodule:: arq.worker
   :members:

.. automodule:: arq.jobs
   :members:

.. automodule:: arq.logs
   :members:

.. automodule:: arq.utils
   :members:

.. automodule:: arq.testing
   :members:

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |Build Status| image:: https://travis-ci.org/samuelcolvin/arq.svg?branch=master
   :target: https://travis-ci.org/samuelcolvin/arq
.. |Coverage| image:: https://codecov.io/gh/samuelcolvin/arq/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/samuelcolvin/arq
.. |pypi| image:: https://img.shields.io/pypi/v/arq.svg
   :target: https://pypi.python.org/pypi/arq
.. |license| image:: https://img.shields.io/pypi/l/arq.svg
   :target: https://github.com/samuelcolvin/arq
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _benchmarks: https://github.com/samuelcolvin/arq/tree/master/performance_benchmarks
.. _aiohttp: http://aiohttp.readthedocs.io/en/stable/

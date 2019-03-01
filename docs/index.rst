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
    Deferred execution, and pessimistic execution (jobs are automatically re-enqueued when the worker shuts down)
    means *arq* is great for critical jobs that can't simply be cancelled when a worker is stopped.

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

.. include:: usage.rst

.. include:: ../HISTORY.rst


.. |pypi| image:: https://img.shields.io/pypi/v/arq.svg
   :target: https://pypi.python.org/pypi/arq
.. |license| image:: https://img.shields.io/pypi/l/arq.svg
   :target: https://github.com/samuelcolvin/arq
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _rq: http://python-rq.org/

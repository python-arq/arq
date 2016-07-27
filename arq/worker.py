import asyncio
from collections import namedtuple
from importlib import import_module
from multiprocessing import Process
import logging
from traceback import format_exception
import sys

import msgpack

from .utils import RedisMixin, timestamp, cached_property

__all__ = [
    'AbstractWorkManager',
    'Job',
    'run_job'
]

logger = logging.getLogger('arq.work')

Job = namedtuple('Job', ['queue', 'class_name', 'func_name', 'args', 'kwargs'])


def _log_job_def(queue_time, j):
    if not logger.isEnabledFor(logging.INFO):
        return
    arguments = ''
    if j.args:
        arguments = ', '.join(map(str, j.args))
    if j.kwargs:
        if arguments:
            arguments += ', '
        arguments += ', '.join('{}={}'.format(*kv) for kv in j.kwargs.items())

    if len(arguments) > 80:
        arguments = arguments[:77] + '...'
    logger.info('%-4s queued =%7.3fs → %s.%s(%s)', j.queue, queue_time, j.class_name, j.func_name, arguments)


def _log_job_result(started_at, result, j):
    if not logger.isEnabledFor(logging.INFO):
        return
    job_time = timestamp() - started_at
    sr = str(result)
    if len(sr) > 80:
        sr = sr[:77] + '...'
    logger.info('%-4s ran in =%7.3fs ← %s.%s ● %s', j.queue, job_time, j.class_name, j.func_name, sr)


async def run_job(queue_name, data, get_worker, exc_handler=None):
    data = msgpack.unpackb(data, encoding='utf8')
    queued_at, *extra = data
    j = Job(queue_name, *extra)
    queued_at /= 1000

    worker = get_worker(j)
    func = getattr(worker, j.func_name)

    started_at = timestamp()
    queue_time = started_at - queued_at
    _log_job_def(queue_time, j)
    unbound_func = getattr(func, 'unbound_original', None)
    try:
        if unbound_func:
            result = await unbound_func(worker, *j.args, **j.kwargs)
        else:
            result = await func(*j.args, **j.kwargs)
    except Exception as e:
        if exc_handler:
            await exc_handler(started_at, e, j)
        else:
            raise e
    else:
        _log_job_result(started_at, result, j)


QUIT = b'quit'


class AbstractWorkManager(RedisMixin):
    max_concurrent_tasks = 1000
    _workers = None

    def __init__(self, batch_mode=False, **kwargs):
        self._batch_mode = batch_mode
        self._pending_tasks = set()
        self._task_count = 0
        super().__init__(**kwargs)

    async def worker_factory(self):
        raise NotImplementedError

    @cached_property
    def queues(self):
        from .main import Dispatch
        return Dispatch.DEFAULT_QUEUES

    async def run(self):
        # TODO these two statements could go to a "INFO_HIGH" log level
        logger.warning('Initialising work manager, batch mode: %s', self._batch_mode)
        self._workers = {w.__class__.__name__: w for w in await self.worker_factory()}

        logger.warning('Running worker with %s worker classes listening to %d queues',
                       len(self._workers), len(self.queues))
        logger.debug('workers: %s, queues: %s', self._worker_names, self._queue_names)

        self.redis_pool = await self.init_redis_pool()
        start = timestamp()
        try:
            await self.work()
        finally:
            logger.warning('shutting down worker after %0.3fs, %d jobs done', timestamp() - start, self._task_count)
            await self.close()

    @cached_property
    def _worker_names(self):
        return ', '.join(self._workers.keys())

    @cached_property
    def _queue_names(self):
        return ', '.join(q.decode() for q in self.queues)

    async def work(self):
        timeout = 0
        queues = list(self.queues)
        async with self.redis_pool.get() as redis:
            if self._batch_mode:
                timeout = 1  # in case another worker gets the QUIT first
                redis.lpush(QUIT, b'1')
                queues.append(QUIT)
            while True:
                msg = await redis.blpop(*queues, timeout=timeout)
                if msg is None:
                    logger.debug('msg None, stopping work')
                    break
                _queue, data = msg
                if self._batch_mode and _queue == QUIT:
                    logger.debug('msg No Op., stopping work')
                    break
                queue = _queue.decode()
                logger.debug('scheduling job from queue %s', queue)
                await self.schedule_job(queue, data)

    async def schedule_job(self, queue, data):
        if len(self._pending_tasks) > self.max_concurrent_tasks:
            _, self._pending_tasks = await asyncio.wait(self._pending_tasks, return_when=asyncio.FIRST_COMPLETED)
        task = self.loop.create_task(self.run_task(queue, data))
        task.add_done_callback(self.job_callback)
        self._pending_tasks.add(task)

    async def run_task(self, queue, data):
        await run_job(queue, data, self.get_worker, self.handle_exc)

    def get_worker(self, job):
        return self._workers[job.class_name]

    @classmethod
    async def handle_exc(cls, started_at, exc, j):
        job_time = timestamp() - started_at
        logger.error('%-4s ran in =%7.3fs ! %s.%s: %s',
                     j.queue, job_time, j.class_name, j.func_name, exc.__class__.__name__)
        tb = format_exception(*sys.exc_info())
        logger.error(''.join(tb).strip('\n'))

    def job_callback(self, task):
        self._task_count += 1
        task.result()


def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed. Stolen from django.
    """
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError as e:
        raise ImportError("%s doesn't look like a module path" % dotted_path) from e

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError('Module "%s" does not define a "%s" attribute/class' % (module_path, class_name)) from e


def start_worker(manager_path, batch_mode):
    loop = asyncio.get_event_loop()
    worker_manager = import_string(manager_path)(batch_mode, loop=loop)
    loop.run_until_complete(worker_manager.run())


def start_worker_process(manager_path, batch_mode=False):
    p = Process(target=start_worker, args=(manager_path, batch_mode))
    p.start()
    # TODO monitor p, restart it and kill it when required

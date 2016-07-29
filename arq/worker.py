import asyncio
from importlib import import_module
from multiprocessing import Process
import logging

from .main import Actor, Job
from .utils import RedisMixin, timestamp, cached_property

__all__ = ['AbstractWorkManager']

logger = logging.getLogger('arq.work')

QUIT = b'quit'


class AbstractWorkManager(RedisMixin):
    max_concurrent_tasks = 1000

    def __init__(self, batch_mode=False, **kwargs):
        self._batch_mode = batch_mode
        self._pending_tasks = set()
        self._task_count = 0
        self._shadows = {}
        super().__init__(**kwargs)

    async def shadow_factory(self):
        raise NotImplementedError

    @cached_property
    def queues(self):
        return Actor.QUEUES

    async def run(self):
        # TODO these two statements could go to a "INFO_HIGH" log level
        logger.warning('Initialising work manager, batch mode: %s', self._batch_mode)
        self._shadows = {w.__class__.__name__: w for w in await self.shadow_factory()}

        logger.warning('Running worker with %s shadow%s listening to %d queues',
                       len(self._shadows), '' if len(self._shadows) == 1 else 's', len(self.queues))
        logger.debug('shadows: %s, queues: %s', ', '.join(self._shadows.keys()), ', '.join(self.queues))

        self.redis_pool = await self.init_redis_pool()
        start = timestamp()
        try:
            await self.work()
        finally:
            logger.warning('shutting down worker after %0.3fs, %d jobs done', timestamp() - start, self._task_count)
            await self.close()

    def get_redis_queues(self):
        q_lookup = {}
        for s in self._shadows.values():
            q_lookup.update(s.queue_lookup)
        queues = [(q_lookup[q], q) for q in self.queues]
        return [r for r, q in queues], dict(queues)

    async def work(self):
        timeout = 0
        redis_queues, queue_lookup = self.get_redis_queues()
        async with self.redis_pool.get() as redis:
            if self._batch_mode:
                timeout = 1  # in case another worker gets the QUIT first
                redis.lpush(QUIT, b'1')
                redis_queues.append(QUIT)
            while True:
                msg = await redis.blpop(*redis_queues, timeout=timeout)
                if msg is None:
                    logger.debug('msg None, stopping work')
                    break
                _queue, data = msg
                if self._batch_mode and _queue == QUIT:
                    logger.debug('Quit msg , stopping work')
                    break
                queue = queue_lookup[_queue]
                logger.debug('scheduling job from queue %s', queue)
                await self.schedule_job(queue, data)

    async def schedule_job(self, queue, data):
        if len(self._pending_tasks) > self.max_concurrent_tasks:
            _, self._pending_tasks = await asyncio.wait(self._pending_tasks, return_when=asyncio.FIRST_COMPLETED)
        task = self.loop.create_task(self.run_task(queue, data))
        task.add_done_callback(self.job_callback)
        self._pending_tasks.add(task)

    async def run_task(self, queue, data):
        j = Job(queue, data)
        worker = self.get_worker(j)
        await worker.run_job(j)

    def get_worker(self, job):
        return self._shadows[job.class_name]

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

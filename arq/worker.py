import asyncio
import traceback
from importlib import import_module, reload
import logging
from multiprocessing import Process
import os
import signal
import sys
import time


from .main import Actor, Job
from .utils import RedisMixin, timestamp, cached_property

__all__ = ['AbstractWorker']

logger = logging.getLogger('arq.work')

QUIT = b'quit'


class HandledExit(Exception):
    pass


class AbstractWorker(RedisMixin):
    max_concurrent_tasks = 100
    shutdown_delay = 6

    def __init__(self, batch_mode=False, **kwargs):
        self._batch_mode = batch_mode
        self._pending_tasks = set()
        self._task_count = 0
        self._shadows = {}
        self.start = None
        signal.signal(signal.SIGINT, self.handle_sig)
        signal.signal(signal.SIGTERM, self.handle_sig)
        super().__init__(**kwargs)

    async def shadow_factory(self):
        raise NotImplementedError

    @cached_property
    def queues(self):
        return Actor.QUEUES

    def handle_sig(self, signum, frame):
        logger.warning('%d, got signal: %s, stopping...', os.getpid(), signal.Signals(signum).name)
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)
        signal.signal(signal.SIGALRM, self.handle_sig_force)
        signal.alarm(self.shutdown_delay)
        raise HandledExit()

    def handle_sig_force(self, signum, frame):
        logger.error('%d, got signal: %s again, forcing exit', os.getpid(), signal.Signals(signum).name)
        raise SystemError('force exit')

    def run_forever(self):
        self.loop.run_until_complete(self.run())

    async def run(self):
        # TODO these two statements could go to a "INFO_HIGH" log level
        logger.info('Initialising work manager, batch mode: %s', self._batch_mode)
        self._shadows = {w.__class__.__name__: w for w in await self.shadow_factory()}

        logger.info('Running worker with %s shadow%s listening to %d queues',
                    len(self._shadows), '' if len(self._shadows) == 1 else 's', len(self.queues))
        logger.debug('shadows: %s, queues: %s', ', '.join(self._shadows.keys()), ', '.join(self.queues))

        self.start = timestamp()
        try:
            await self.work()
        finally:
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
        pool = await self.get_redis_pool()
        async with pool.get() as redis:
            if self._batch_mode:
                timeout = 1  # in case another worker gets the QUIT first
                await redis.rpush(QUIT, b'1')
                redis_queues.append(QUIT)
            logger.debug('starting main blpop loop')
            while True:
                msg = await redis.blpop(*redis_queues, timeout=timeout)
                if msg is None:
                    logger.debug('msg None, stopping work')
                    break
                _queue, data = msg
                if self._batch_mode and _queue == QUIT:
                    logger.debug('Quit msg, stopping work')
                    break
                queue = queue_lookup[_queue]
                logger.debug('scheduling job from queue %s', queue)
                await self.schedule_job(queue, data)

    async def schedule_job(self, queue, data):
        if len(self._pending_tasks) > self.max_concurrent_tasks:
            _, self._pending_tasks = await asyncio.wait(self._pending_tasks, loop=self.loop,
                                                        return_when=asyncio.FIRST_COMPLETED)
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

    async def close(self):
        logger.info('shutting down worker after %0.3fs, %d jobs done', timestamp() - self.start, self._task_count)
        if self._pending_tasks:
            logger.info('waiting for %d jobs to finish', len(self._pending_tasks))
            await asyncio.wait(self._pending_tasks, loop=self.loop)
        await super().close()


def import_string(file_path, attr_name):
    """
    Import attribute/class from from a python module. Raise ImportError if the import failed.
    Approximately stolen from django.
    :param file_path: path to python module
    :param attr_name: attribute to get from module
    :return: attribute
    """
    module_path = file_path.replace('.py', '').replace('/', '.')
    p = os.getcwd()
    sys.path = [p] + sys.path

    module = import_module(module_path)
    reload(module)

    try:
        attr = getattr(module, attr_name)
    except AttributeError as e:
        raise ImportError('Module "%s" does not define a "%s" attribute/class' % (module_path, attr_name)) from e
    return attr


def start_worker(worker_path, worker_class, batch_mode):
    worker = import_string(worker_path, worker_class)
    worker_manager = worker(batch_mode)
    try:
        worker_manager.run_forever()
    except HandledExit:
        worker_manager.loop.run_until_complete(worker_manager.close())
        pass


class RunWorkerProcess:
    def __init__(self, worker_path, worker_class, batch_mode=False):
        signal.signal(signal.SIGINT, self.handle_sig)
        signal.signal(signal.SIGTERM, self.handle_sig)
        self.process = None
        self.run_worker(worker_path, worker_class, batch_mode)

    def handle_sig(self, signum, frame):
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)
        logger.warning('got signal: %s, waiting for worker %d to finish...',
                       signal.Signals(signum).name, self.process.pid)
        for i in range(100):
            if not self.process or not self.process.is_alive():
                return
            time.sleep(0.1)

    def run_worker(self, worker_path, worker_class, batch_mode):
        self.process = Process(target=start_worker, args=(worker_path, worker_class, batch_mode), name='WorkProcess')
        self.process.start()
        self.process.join()
        if self.process.exitcode == 0:
            logger.info('worker process exited ok')
            return
        logger.error('worker process exited badly, exit code %s', self.process.exitcode)
        # TODO could restart worker here, but better to leave it to the real manager

    def handle_sig_force(self, signum, frame):
        logger.error('got signal: %s again, forcing exit', signal.Signals(signum).name)
        if self.process and self.process.is_alive():
            logger.error('sending worker %d SIGTERM', self.process.pid)
            os.kill(self.process.pid, signal.SIGTERM)
        raise SystemError('force exit')

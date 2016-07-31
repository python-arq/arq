import asyncio
from importlib import import_module, reload
import logging
from multiprocessing import Process
import os
import signal
from signal import Signals
import sys
import time


from .logs import default_log_config
from .main import Actor, Job
from .utils import RedisMixin, timestamp, cached_property, gen_random

__all__ = ['BaseWorker', 'import_string', 'RunWorkerProcess']

logger = logging.getLogger('arq.work')


class HandledExit(Exception):
    pass


class ImmediateExit(Exception):
    pass


class BadJob(Exception):
    pass


class BaseWorker(RedisMixin):
    max_concurrent_tasks = 50
    shutdown_delay = 6
    job_class = Job
    shadows = None

    def __init__(self, *, batch=False, shadows=None, queues=None, **kwargs):
        self._batch_mode = batch
        if self.shadows is None and shadows is None:
            raise TypeError('shadows not defined on worker')
        if shadows:
            self.shadows = shadows
        self._queues = queues
        self._pending_tasks = set()
        self.jobs_complete = 0
        self.jobs_failed = 0
        self._task_exception = None
        self._shadows = {}
        self.start = None
        self.running = True
        self._closed = False
        signal.signal(signal.SIGINT, self.handle_sig)
        signal.signal(signal.SIGTERM, self.handle_sig)
        super().__init__(**kwargs)

    async def shadow_factory(self):
        rp = await self.get_redis_pool()
        shadows = [s(is_shadow=True, loop=self.loop, existing_pool=rp) for s in self.shadows]
        return {w.name: w for w in shadows}

    @classmethod
    def logging_config(cls, verbose):
        return default_log_config(verbose)

    @cached_property
    def queues(self):
        return self._queues or Actor.QUEUES

    @cached_property
    def shadow_names(self):
        return ', '.join(self._shadows.keys())

    def get_redis_queues(self):
        q_lookup = {}
        for s in self._shadows.values():
            q_lookup.update(s.queue_lookup)
        try:
            queues = [(q_lookup[q], q) for q in self.queues]
        except KeyError as e:
            raise KeyError('queue not found in queue lookups from shadows, '
                           'queues: {}, combined shadow queue lookups: {}'.format(self.queues, q_lookup)) from e
        return [r for r, q in queues], dict(queues)

    def run_until_complete(self):
        self.loop.run_until_complete(self.run())

    async def run(self):
        logger.info('Initialising work manager, batch mode: %s', self._batch_mode)

        self._shadows = await self.shadow_factory()
        assert isinstance(self._shadows, dict), 'shadow_factory should return a dict not %s' % type(self._shadows)

        logger.info('Running worker with %s shadow%s listening to %d queues',
                    len(self._shadows), '' if len(self._shadows) == 1 else 's', len(self.queues))
        logger.info('shadows: %s | queues: %s', self.shadow_names, ', '.join(self.queues))

        self.start = timestamp()
        try:
            await self.work()
        finally:
            await self.close()
            if self._task_exception:
                logger.error('Found task exception "%s"', self._task_exception)
                raise self._task_exception

    async def work(self):
        redis_queues, queue_lookup = self.get_redis_queues()
        quit_queue = None
        async with await self.get_redis_conn() as redis:
            if self._batch_mode:
                quit_queue = b'QUIT-%s' % gen_random()
                logger.debug('adding random quit queue for faster batch exit: %s', quit_queue.decode())
                await redis.rpush(quit_queue, b'1')
                redis_queues.append(quit_queue)
            logger.debug('starting main blpop loop')
            while self.running:
                msg = await redis.blpop(*redis_queues, timeout=1)
                if msg is None:
                    continue
                _queue, data = msg
                if self._batch_mode and _queue == quit_queue:
                    logger.debug('got job from the quit queue, stopping')
                    break
                queue = queue_lookup[_queue]
                logger.debug('scheduling job from queue %s', queue)
                await self.schedule_job(queue, data)

    async def schedule_job(self, queue, data):
        job = self.job_class(queue, data)

        if len(self._pending_tasks) >= self.max_concurrent_tasks:
            logger.debug('%d pending tasks, waiting for one to finish before creating task for %s',
                         len(self._pending_tasks), job)
            _, self._pending_tasks = await asyncio.wait(self._pending_tasks, loop=self.loop,
                                                        return_when=asyncio.FIRST_COMPLETED)

        task = self.loop.create_task(self.run_job(job))
        task.add_done_callback(self.job_callback)
        self._pending_tasks.add(task)

    async def run_job(self, j):
        try:
            shadow = self._shadows[j.class_name]
        except KeyError:
            self.jobs_failed += 1
            logger.error('Job Error: unable to find shadow for %r', j)
            # exit with zero so we don't increment jobs_failed twice
            return 0
        try:
            func = getattr(shadow, j.func_name + '_direct')
        except AttributeError:
            # try the method name directly for causes where enqueue_job is called manually
            try:
                func = getattr(shadow, j.func_name)
            except AttributeError:
                self.jobs_failed += 1
                logger.error('Job Error: shadow class "%s" has not function "%s"', shadow.name, j.func_name)
                return 0

        started_at = timestamp()
        queue_time = started_at - j.queued_at
        self.log_job_start(queue_time, j)
        try:
            result = await func(*j.args, **j.kwargs)
        except Exception as e:
            await self.handle_exc(started_at, e, j)
            return 1
        else:
            self.log_job_result(started_at, result, j)
            return 0

    def job_callback(self, task):
        self.jobs_complete += 1
        task_exception = task.exception()
        if task_exception:
            self.running = False
            self._task_exception = task_exception
        elif task.result():
            self.jobs_failed += 1
        logger.debug('task complete, %d jobs done, %d failed', self.jobs_complete, self.jobs_failed)

    @classmethod
    def log_job_start(cls, queue_time, j):
        if logger.isEnabledFor(logging.INFO):
            logger.info('%-4s queued%7.3fs → %s', j.queue, queue_time, j)

    @classmethod
    def log_job_result(cls, started_at, result, j):
        if not logger.isEnabledFor(logging.INFO):
            return
        job_time = timestamp() - started_at
        if result is None:
            sr = ''
        else:
            sr = str(result)
            if len(sr) > 80:
                sr = sr[:77] + '...'
        logger.info('%-4s ran in%7.3fs ← %s.%s ● %s', j.queue, job_time, j.class_name, j.func_name, sr)

    @classmethod
    async def handle_exc(cls, started_at, exc, j):
        job_time = timestamp() - started_at
        exc_type = exc.__class__.__name__
        logger.exception('%-4s ran in =%7.3fs ! %s: %s', j.queue, job_time, j, exc_type)

    async def close(self):
        if self._closed:
            return
        if self._pending_tasks:
            logger.info('waiting for %d jobs to finish', len(self._pending_tasks))
            await asyncio.wait(self._pending_tasks, loop=self.loop)
        t = (timestamp() - self.start) if self.start else 0
        logger.warning('shutting down worker after %0.3fs, %d jobs done, %d failed',
                       t, self.jobs_complete, self.jobs_failed)

        if self._shadows:
            await asyncio.wait([s.close() for s in self._shadows.values()], loop=self.loop)
        await super().close()
        self._closed = True

    def handle_sig(self, signum, frame):
        self.running = False
        logger.warning('pid=%d, got signal: %s, stopping...', os.getpid(), Signals(signum).name)
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)
        signal.signal(signal.SIGALRM, self.handle_sig_force)
        signal.alarm(self.shutdown_delay)
        raise HandledExit()

    def handle_sig_force(self, signum, frame):
        logger.error('pid=%d, got signal: %s again, forcing exit', os.getpid(), Signals(signum).name)
        raise ImmediateExit('force exit')


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


def start_worker(worker_path, worker_class, batch):
    worker_cls = import_string(worker_path, worker_class)
    worker = worker_cls(batch=batch)
    try:
        worker.run_until_complete()
    except HandledExit:
        pass
    except Exception as e:
        logger.exception('Worker exiting after an unhandled error: %s', e.__class__.__name__)
        raise
    finally:
        worker.loop.run_until_complete(worker.close())


class RunWorkerProcess:
    def __init__(self, worker_path, worker_class, batch=False):
        signal.signal(signal.SIGINT, self.handle_sig)
        signal.signal(signal.SIGTERM, self.handle_sig)
        self.process = None
        self.run_worker(worker_path, worker_class, batch)

    def run_worker(self, worker_path, worker_class, batch):
        name = 'WorkProcess'
        logger.info('starting work process "%s"', name)
        self.process = Process(target=start_worker, args=(worker_path, worker_class, batch), name=name)
        self.process.start()
        self.process.join()
        if self.process.exitcode == 0:
            logger.info('worker process exited ok')
            return
        logger.critical('worker process %s exited badly with exit code %s', self.process.pid, self.process.exitcode)
        sys.exit(3)
        # TODO could restart worker here, but better to leave it up to the real manager

    def handle_sig(self, signum, frame):
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)
        logger.warning('got signal: %s, waiting for worker pid=%s to finish...', Signals(signum).name,
                       self.process and self.process.pid)
        for i in range(100):
            if not self.process or not self.process.is_alive():
                return
            time.sleep(0.1)

    def handle_sig_force(self, signum, frame):
        logger.error('got signal: %s again, forcing exit', Signals(signum).name)
        if self.process and self.process.is_alive():
            logger.error('sending worker %d SIGTERM', self.process.pid)
            os.kill(self.process.pid, signal.SIGTERM)
        raise ImmediateExit('force exit')

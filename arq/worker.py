import asyncio
import logging
import os
import signal
import sys
import time
from importlib import import_module, reload
from multiprocessing import Process
from signal import Signals

from .logs import default_log_config
from .main import Actor, Job
from .utils import RedisMixin, cached_property, ellipsis, gen_random, timestamp

__all__ = ['BaseWorker', 'import_string', 'RunWorkerProcess']

work_logger = logging.getLogger('arq.work')
jobs_logger = logging.getLogger('arq.jobs')


class HandledExit(Exception):
    pass


class ImmediateExit(Exception):
    pass


class BadJob(Exception):
    pass


class BaseWorker(RedisMixin):
    max_concurrent_tasks = 50
    shutdown_delay = 6
    timeout_seconds = 60
    job_class = Job
    shadows = None

    def __init__(self, *, batch=False, shadows=None, queues=None, timeout_seconds=None, **kwargs):
        self._batch_mode = batch
        if self.shadows is None and shadows is None:
            raise TypeError('shadows not defined on worker')
        if shadows:
            self.shadows = shadows
        self.timeout_seconds = timeout_seconds or self.timeout_seconds
        self._queues = queues
        self._pending_tasks = set()
        self.jobs_complete = 0
        self.jobs_failed = 0
        self.jobs_timed_out = 0
        self._task_exception = None
        self._shadows = {}
        self.start = None
        self.running = True
        self._closed = False
        signal.signal(signal.SIGINT, self.handle_sig)
        signal.signal(signal.SIGTERM, self.handle_sig)
        super().__init__(**kwargs)
        self._closing_lock = asyncio.Lock(loop=self.loop)

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
        work_logger.info('Initialising work manager, batch mode: %s', self._batch_mode)

        self._shadows = await self.shadow_factory()
        assert isinstance(self._shadows, dict), 'shadow_factory should return a dict not %s' % type(self._shadows)

        work_logger.info('Running worker with %s shadow%s listening to %d queues',
                         len(self._shadows), '' if len(self._shadows) == 1 else 's', len(self.queues))
        work_logger.info('shadows: %s | queues: %s', self.shadow_names, ', '.join(self.queues))

        self.start = timestamp()
        try:
            await self.work()
        finally:
            await self.close()
            if self._task_exception:
                work_logger.error('Found task exception "%s"', self._task_exception)
                raise self._task_exception

    async def work(self):
        redis_queues, queue_lookup = self.get_redis_queues()
        quit_queue = None
        async with await self.get_redis_conn() as redis:
            if self._batch_mode:
                quit_queue = b'QUIT-%s' % gen_random()
                work_logger.debug('populating quit queue to prompt exit: %s', quit_queue.decode())
                await redis.rpush(quit_queue, b'1')
                redis_queues.append(quit_queue)
            work_logger.debug('starting main blpop loop')
            while self.running:
                msg = await redis.blpop(*redis_queues, timeout=1)
                if msg is None:
                    continue
                _queue, data = msg
                if self._batch_mode and _queue == quit_queue:
                    work_logger.debug('got job from the quit queue, stopping')
                    break
                queue = queue_lookup[_queue]
                work_logger.debug('scheduling job from queue %s', queue)
                await self.schedule_job(queue, data)

    async def schedule_job(self, queue, data):
        job = self.job_class(queue, data)

        pt_cnt = len(self._pending_tasks)
        if pt_cnt >= self.max_concurrent_tasks:
            work_logger.debug('%d pending tasks, waiting for one to finish before creating task for %s', pt_cnt, job)
            _, self._pending_tasks = await asyncio.wait(self._pending_tasks, loop=self.loop,
                                                        return_when=asyncio.FIRST_COMPLETED)

        task = self.loop.create_task(self.run_job(job))
        task.add_done_callback(self.job_callback)
        self.loop.call_later(self.timeout_seconds, self.cancel_job, task, job)
        self._pending_tasks.add(task)

    def cancel_job(self, task, job):
        if not task.cancel():
            return
        self.jobs_timed_out += 1
        jobs_logger.error('job timed out %r', job)

    async def run_job(self, j):
        try:
            shadow = self._shadows[j.class_name]
        except KeyError:
            self.jobs_failed += 1
            jobs_logger.error('Job Error: unable to find shadow for %r', j)
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
                jobs_logger.error('Job Error: shadow class "%s" has not function "%s"', shadow.name, j.func_name)
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
        jobs_logger.debug('task complete, %d jobs done, %d failed', self.jobs_complete, self.jobs_failed)

    @classmethod
    def log_job_start(cls, queue_time, j):
        if jobs_logger.isEnabledFor(logging.INFO):
            jobs_logger.info('%-4s queued%7.3fs → %s', j.queue, queue_time, j)

    @classmethod
    def log_job_result(cls, started_at, result, j):
        if not jobs_logger.isEnabledFor(logging.INFO):
            return
        job_time = timestamp() - started_at
        sr = '' if result is None else ellipsis(repr(result))
        jobs_logger.info('%-4s ran in%7.3fs ← %s.%s ● %s', j.queue, job_time, j.class_name, j.func_name, sr)

    @classmethod
    async def handle_exc(cls, started_at, exc, j):
        job_time = timestamp() - started_at
        exc_type = exc.__class__.__name__
        jobs_logger.exception('%-4s ran in%7.3fs ! %s: %s', j.queue, job_time, j, exc_type)

    async def close(self):
        with await self._closing_lock:
            if self._closed:
                return
            if self._pending_tasks:
                work_logger.info('shutting down worker, waiting for %d jobs to finish', len(self._pending_tasks))
                await asyncio.wait(self._pending_tasks, loop=self.loop)
            t = (timestamp() - self.start) if self.start else 0
            work_logger.info('shutting down worker after %0.3fs ◆ %d jobs done ◆ %d failed ◆ %d timed out',
                             t, self.jobs_complete, self.jobs_failed, self.jobs_timed_out)

            if self._shadows:
                await asyncio.wait([s.close() for s in self._shadows.values()], loop=self.loop)
            await super().close()
            self._closed = True

    def handle_sig(self, signum, frame):
        self.running = False
        work_logger.warning('pid=%d, got signal: %s, stopping...', os.getpid(), Signals(signum).name)
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)
        signal.signal(signal.SIGALRM, self.handle_sig_force)
        signal.alarm(self.shutdown_delay)
        raise HandledExit()

    def handle_sig_force(self, signum, frame):
        work_logger.error('pid=%d, got signal: %s again, forcing exit', os.getpid(), Signals(signum).name)
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


def start_worker(worker_path, worker_class, batch, loop=None):
    worker_cls = import_string(worker_path, worker_class)
    worker = worker_cls(batch=batch, loop=loop)
    work_logger.info('Starting %s on worker process pid=%d', worker_cls.__name__, os.getpid())
    try:
        worker.run_until_complete()
    except HandledExit:
        work_logger.debug('worker exited with well handled exception')
        pass
    except Exception as e:
        work_logger.exception('Worker exiting after an unhandled error: %s', e.__class__.__name__)
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
        work_logger.info('starting work process "%s"', name)
        self.process = Process(target=start_worker, args=(worker_path, worker_class, batch), name=name)
        self.process.start()
        self.process.join()
        if self.process.exitcode == 0:
            work_logger.info('worker process exited ok')
            return
        work_logger.critical('worker process %s exited badly with exit code %s',
                             self.process.pid, self.process.exitcode)
        sys.exit(3)
        # could restart worker here, but better to leave it up to the real manager

    def handle_sig(self, signum, frame):
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)
        work_logger.warning('got signal: %s, waiting for worker pid=%s to finish...', Signals(signum).name,
                            self.process and self.process.pid)
        for i in range(100):
            if not self.process or not self.process.is_alive():
                return
            time.sleep(0.1)

    def handle_sig_force(self, signum, frame):
        work_logger.error('got signal: %s again, forcing exit', Signals(signum).name)
        if self.process and self.process.is_alive():
            work_logger.error('sending worker %d SIGTERM', self.process.pid)
            os.kill(self.process.pid, signal.SIGTERM)
        raise ImmediateExit('force exit')

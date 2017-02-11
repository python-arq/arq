"""
:mod:`worker`
=============

Responsible for executing jobs on the worker.
"""
import asyncio
import logging
import os
import signal
import sys
import time
from importlib import import_module, reload
from multiprocessing import Process
from signal import Signals  # type: ignore

from .logs import default_log_config
from .utils import RedisMixin, ellipsis, gen_random, timestamp

__all__ = ['BaseWorker', 'import_string', 'RunWorkerProcess']

work_logger = logging.getLogger('arq.work')
jobs_logger = logging.getLogger('arq.jobs')


class HandledExit(Exception):
    pass


class ImmediateExit(Exception):
    pass


class BadJob(Exception):
    pass


# special signal sent by the main process in case the worker process hasn't received a signal (eg. SIGTERM or SIGINT)
SIG_PROXY = signal.SIGUSR1


class BaseWorker(RedisMixin):
    """
    Base class for Workers to inherit from.
    """
    #: maximum number of jobs which can be execute at the same time by the event loop
    max_concurrent_tasks = 50

    #: number of seconds after a termination signal (SIGINT or SIGTERM) is received to force quit the worker
    shutdown_delay = 6

    #: default maximum duration of a job
    timeout_seconds = 60

    #: shadow classes, a list of Actor classes for the Worker to run
    shadows = None

    def __init__(self, *,
                 burst: bool=False,
                 shadows: list=None,
                 queues: list=None,
                 timeout_seconds: int=None,
                 **kwargs) -> None:
        """
        :param burst: if true the worker will close as soon as no new jobs are found in the queue lists
        :param shadows: list of :class:`arq.main.Actor` classes for the worker to run,
            overrides shadows already defined in the class definition
        :param queues: list of queue names for the worker to listen on, if None queues is taken from the shadows
        :param timeout_seconds: maximum duration of a job, after that the job will be cancelled by the event loop
        :param kwargs: other keyword arguments, see :class:`arq.utils.RedisMixin` for all available options
        """
        self._burst_mode = burst
        self.shadows = shadows or self.shadows
        self.queues = queues
        self.timeout_seconds = timeout_seconds or self.timeout_seconds
        self._pending_tasks = set()  # type: Set[asyncio.futures.Future]

        self.jobs_complete, self.jobs_failed, self.jobs_timed_out = 0, 0, 0
        self._task_exception = None  # type: Exception
        self._shadow_lookup = {}  # type: Dict[str, object] # TODO
        self.start = None  # type: float
        self.running = True
        self._closed = False
        self.job_class = None  # type: type # TODO
        signal.signal(signal.SIGINT, self.handle_sig)
        signal.signal(signal.SIGTERM, self.handle_sig)
        signal.signal(SIG_PROXY, self.handle_proxy_signal)
        super().__init__(**kwargs)  # type: ignore # TODO
        self._closing_lock = asyncio.Lock(loop=self.loop)

    async def shadow_factory(self) -> list:
        """
        Initialise list of shadows and return them.

        Override to customise the way shadows are initialised.
        """
        if self.shadows is None:
            raise TypeError('shadows not defined on worker')
        rp = await self.get_redis_pool()
        shadows = [s(redis_settings=self.redis_settings, is_shadow=True, loop=self.loop, existing_pool=rp)
                   for s in self.shadows]
        await asyncio.wait([s.startup() for s in shadows], loop=self.loop)
        return shadows

    @classmethod
    def logging_config(cls, verbose) -> dict:
        """
        Override to customise the logging setup for the arq worker.
        By default just uses :func:`arq.logs.default_log_config`.

        :param verbose: verbose flag from cli, by default log level is INFO if false and DEBUG if true
        :return: dict suitable for ``logging.config.dictConfig``
        """
        return default_log_config(verbose)

    @property
    def shadow_names(self):
        return ', '.join(self._shadow_lookup.keys())

    def get_redis_queues(self):
        q_lookup = {}
        for s in self._shadow_lookup.values():
            q_lookup.update(s.queue_lookup)
        try:
            queues = [(q_lookup[q], q) for q in self.queues]
        except KeyError as e:
            raise KeyError('queue not found in queue lookups from shadows, '
                           'queues: {}, combined shadow queue lookups: {}'.format(self.queues, q_lookup)) from e
        return [r for r, q in queues], dict(queues)

    def run_until_complete(self):
        self.loop.run_until_complete(self.run())

    async def run(self, reuse=False):
        work_logger.info('Initialising work manager, burst mode: %s', self._burst_mode)

        shadows = await self.shadow_factory()
        assert isinstance(shadows, list), 'shadow_factory should return a list not %s' % type(shadows)
        self.job_class = shadows[0].job_class
        work_logger.debug('Using first shadows job class "%s"', self.job_class.__name__)
        for shadow in shadows[1:]:
            if shadow.job_class != self.job_class:
                raise TypeError("shadows don't match: {s} has a different job class to the first shadow, "
                                "{s.job_class} != {self.job_class}".format(s=shadow, self=self))

        if not self.queues:
            self.queues = shadows[0].queues
            for shadow in shadows[1:]:
                if shadow.queues != self.queues:
                    raise TypeError('{s} has a different list of queues to the first shadow: '
                                    '{s.queues} != {self.queues}'.format(s=shadow, self=self))

        self._shadow_lookup = {w.name: w for w in shadows}

        work_logger.info('Running worker with %s shadow%s listening to %d queues',
                         len(self._shadow_lookup), '' if len(self._shadow_lookup) == 1 else 's', len(self.queues))
        work_logger.info('shadows: %s | queues: %s', self.shadow_names, ', '.join(self.queues))

        self.start = timestamp()
        try:
            await self.work()
        finally:
            if reuse:
                work_logger.info('waiting for %d jobs to finish', len(self._pending_tasks))
                await asyncio.wait(self._pending_tasks, loop=self.loop)
            else:
                await self.close()
            if self._task_exception:
                work_logger.error('Found task exception "%s"', self._task_exception)
                raise self._task_exception

    async def work(self):
        redis_queues, queue_lookup = self.get_redis_queues()
        quit_queue = None
        async with await self.get_redis_conn() as redis:
            if self._burst_mode:
                quit_queue = b'QUIT-%s' % gen_random()
                work_logger.debug('populating quit queue to prompt exit: %s', quit_queue.decode())
                await redis.rpush(quit_queue, b'1')
                redis_queues.append(quit_queue)
            work_logger.debug('starting main blpop loop')
            while self.running:
                msg = await redis.blpop(*redis_queues, timeout=1)
                if msg is None:
                    continue
                raw_queue, data = msg
                if self._burst_mode and raw_queue == quit_queue:
                    work_logger.debug('got job from the quit queue, stopping')
                    break
                queue = queue_lookup[raw_queue]
                self.schedule_job(data, queue)
                await self.below_concurrency_limit()

    def schedule_job(self, data, queue):
        work_logger.debug('scheduling job from queue %s', queue)
        job = self.job_class(queue, data)

        task = self.loop.create_task(self.run_job(job))
        task.add_done_callback(self.job_callback)
        self.loop.call_later(self.timeout_seconds, self.cancel_job, task, job)
        self._pending_tasks.add(task)

    async def below_concurrency_limit(self):
        pt_cnt = len(self._pending_tasks)
        while True:
            if pt_cnt < self.max_concurrent_tasks:
                return
            work_logger.debug('%d pending tasks, waiting for one to finish', pt_cnt)
            _, self._pending_tasks = await asyncio.wait(self._pending_tasks, loop=self.loop,
                                                        return_when=asyncio.FIRST_COMPLETED)
            pt_cnt = len(self._pending_tasks)

    def cancel_job(self, task, job):
        if not task.cancel():
            return
        self.jobs_timed_out += 1
        jobs_logger.error('job timed out %r', job)

    async def run_job(self, j):
        try:
            shadow = self._shadow_lookup[j.class_name]
        except KeyError:
            return self.handle_prepare_exc('Job Error: unable to find shadow for {!r}'.format(j))
        try:
            func = getattr(shadow, j.func_name)
        except AttributeError:
            msg = 'Job Error: shadow class "{}" has no function "{}"'.format(shadow.name, j.func_name)
            return self.handle_prepare_exc(msg)

        try:
            func = func.direct
        except AttributeError:
            # allow for cases where enqueue_job is called manually
            pass

        started_at = timestamp()
        self.log_job_start(started_at, j)
        try:
            result = await func(*j.args, **j.kwargs)
        except Exception as e:
            self.handle_execute_exc(started_at, e, j)
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
    def log_job_start(cls, started_at, j):
        if jobs_logger.isEnabledFor(logging.INFO):
            jobs_logger.info('%-4s queued%7.3fs → %s', j.queue, started_at - j.queued_at, j)

    @classmethod
    def log_job_result(cls, started_at, result, j):
        if not jobs_logger.isEnabledFor(logging.INFO):
            return
        job_time = timestamp() - started_at
        sr = '' if result is None else ellipsis(repr(result))
        jobs_logger.info('%-4s ran in%7.3fs ← %s.%s ● %s', j.queue, job_time, j.class_name, j.func_name, sr)

    def handle_prepare_exc(self, msg):
        self.jobs_failed += 1
        jobs_logger.error(msg)
        # exit with zero so we don't increment jobs_failed twice
        return 0

    @classmethod
    def handle_execute_exc(cls, started_at, exc, j):
        exc_type = exc.__class__.__name__
        jobs_logger.exception('%-4s ran in%7.3fs ! %s: %s', j.queue, timestamp() - started_at, j, exc_type)

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

            if self._shadow_lookup:
                await asyncio.wait([s.close() for s in self._shadow_lookup.values()], loop=self.loop)
            await super().close()
            self._closed = True

    def handle_proxy_signal(self, signum, frame):
        self.running = False
        work_logger.info('pid=%d, got signal proxied from main process, stopping...', os.getpid())
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)
        signal.signal(signal.SIGALRM, self.handle_sig_force)
        signal.alarm(self.shutdown_delay)
        raise HandledExit()

    def handle_sig(self, signum, frame):
        self.running = False
        work_logger.info('pid=%d, got signal: %s, stopping...', os.getpid(), Signals(signum).name)
        signal.signal(SIG_PROXY, signal.SIG_IGN)
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)
        signal.signal(signal.SIGALRM, self.handle_sig_force)
        signal.alarm(self.shutdown_delay)
        raise HandledExit()

    def handle_sig_force(self, signum, frame):
        work_logger.warning('pid=%d, got signal: %s again, forcing exit', os.getpid(), Signals(signum).name)
        raise ImmediateExit('force exit')


def import_string(file_path, attr_name):
    """
    Import attribute/class from from a python module. Raise ``ImportError`` if the import failed.
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


def start_worker(worker_path: str, worker_class: str, burst: bool, loop: asyncio.AbstractEventLoop=None):
    """
    Run from within the subprocess to load the worker class and execute jobs.

    :param worker_path: full path to the python file containing the worker definition
    :param worker_class: name of the worker class to be loaded and used
    :param burst: whether or not to run in burst mode
    :param loop: asyncio loop use to or None
    """
    worker_cls = import_string(worker_path, worker_class)
    worker = worker_cls(burst=burst, loop=loop)
    work_logger.info('Starting %s on worker process pid=%d', worker_cls.__name__, os.getpid())
    try:
        worker.run_until_complete()
    except HandledExit:
        work_logger.debug('worker exited with well handled exception')
        pass
    except Exception as e:
        work_logger.exception('Worker exiting after an unhandled error: %s', e.__class__.__name__)
        # could raise here instead of sys.exit but that causes the traceback to be printed twice,
        # if it's needed "raise_exc" would need to be added a new argument to the function
        sys.exit(1)
    finally:
        worker.loop.run_until_complete(worker.close())


class RunWorkerProcess:
    """
    Responsible for starting a process to run the worker, monitoring it and possibly killing it.
    """
    def __init__(self, worker_path, worker_class, burst=False):
        signal.signal(signal.SIGINT, self.handle_sig)
        signal.signal(signal.SIGTERM, self.handle_sig)
        self.process = None
        self.run_worker(worker_path, worker_class, burst)

    def run_worker(self, worker_path, worker_class, burst):
        name = 'WorkProcess'
        work_logger.info('starting work process "%s"', name)
        self.process = Process(target=start_worker, args=(worker_path, worker_class, burst), name=name)
        self.process.start()
        self.process.join()
        if self.process.exitcode == 0:
            work_logger.info('worker process exited ok')
            return
        work_logger.critical('worker process %s exited badly with exit code %s',
                             self.process.pid, self.process.exitcode)
        sys.exit(3)
        # could restart worker here, but better to leave it up to the real manager eg. docker restart: always

    def handle_sig(self, signum, frame):
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)
        work_logger.info('got signal: %s, waiting for worker pid=%s to finish...', Signals(signum).name,
                         self.process and self.process.pid)
        # sleep to make sure worker.handle_sig above has executed if it's going to and detached handle_proxy_signal
        time.sleep(0.01)
        if self.process and self.process.is_alive():
            work_logger.debug("sending custom shutdown signal to worker in case it didn't receive the signal")
            os.kill(self.process.pid, SIG_PROXY)

    def handle_sig_force(self, signum, frame):
        work_logger.warning('got signal: %s again, forcing exit', Signals(signum).name)
        if self.process and self.process.is_alive():
            work_logger.error('sending worker %d SIGTERM', self.process.pid)
            os.kill(self.process.pid, signal.SIGTERM)
        raise ImmediateExit('force exit')

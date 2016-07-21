import asyncio
from multiprocessing import Process

import msgpack

from .utils import *
from .main import Dispatch

__all__ = [
    'AbstractWorkManager'
]


class AbstractWorkManager(RedisMixin):
    queues = Dispatch.DEFAULT_QUEUES
    log_job_defs = True
    log_job_results = True

    async def worker_cls_factory(self):
        raise NotImplementedError

    async def run(self):
        aq_logger.info('Initialising worker classes')
        worker_classes = await self.worker_cls_factory()
        worker_classes = {w.__class__.__name__: w for w in worker_classes}
        aq_logger.info('Running worker with %s worker classes listening to %d queues',
                       len(worker_classes), len(self.queues))

        aq_logger.debug('workers: %s, queues: %s', ', '.join(worker_classes.keys()),
                        ', '.join(q.decode() for q in self.queues))
        self.redis_pool = await self.init_redis_pool()
        try:
            await self.work(worker_classes)
        finally:
            await self.clear()

    async def work(self, worker_classes):
        async with self.redis_pool.get() as redis:
            while True:
                msg = await redis.blpop(*self.queues)

                queue, data = msg
                data = msgpack.unpackb(data, encoding='utf8')
                class_name, func_name, args, kwargs = data

                cls = worker_classes[class_name]
                func = getattr(cls, func_name)

                self.log_job_def(queue, class_name, func_name, args, kwargs)
                unbound_original = getattr(func, 'unbound_original', None)
                if unbound_original:
                    r = await unbound_original(cls, *args, **kwargs)
                else:
                    r = await func(*args, **kwargs)
                self.log_job_result(queue, class_name, func_name, r)

    def log_job_def(self, queue, class_name, func_name, args, kwargs):
        if not self.log_job_defs:
            return
        arguments = ''
        if args:
            arguments = ', '.join(map(str, args))
        if kwargs:
            if arguments:
                arguments += ', '
            arguments += ', '.join('{}={}'.format(*kv) for kv in kwargs.items())

        if len(arguments) > 80:
            arguments = arguments[:77] + '...'
        aq_logger.info('%-4s ◤ %s.%s(%s)', queue.decode(), class_name, func_name, arguments)

    def log_job_result(self, queue, class_name, func_name, result):
        if not self.log_job_results:
            return
        sresult = str(result)
        if len(sresult) > 80:
            sresult = sresult[:77] + '...'
        aq_logger.info('%-4s ◣ %s.%s > %s', queue.decode(), class_name, func_name, sresult)


def start_worker(manager_path):
    loop = asyncio.get_event_loop()
    WorkManager = import_string(manager_path)
    worker_manager = WorkManager(loop=loop)
    loop.run_until_complete(worker_manager.run())


def start_worker_process(manager_path):
    p = Process(target=start_worker, args=(manager_path,))
    p.start()
    # TODO monitor p, restart it and kill it when required

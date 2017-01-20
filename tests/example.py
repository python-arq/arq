from arq import Actor, BaseWorker, concurrent


class ActorTest(Actor):
    @concurrent
    async def foo(self, a, b=0):
        with open('foo', 'w') as f:
            r = a + b
            f.write('{}'.format(r))


class Worker(BaseWorker):
    signature = 'foobar'
    shadows = [ActorTest]


class WorkerSignalQuit(Worker):
    """
    worker which simulates receiving sigint after 2 jobs
    """
    max_concurrent_tasks = 1

    def schedule_job(self, *args):
        super().schedule_job(*args)
        if self.jobs_complete >= 2:
            self.handle_sig(2, None)


class WorkerSignalTwiceQuit(Worker):
    """
    worker which simulates receiving sigint twice after 2 jobs
    """
    def schedule_job(self, *args):
        super().schedule_job(*args)
        if self.jobs_complete >= 2:
            self.handle_sig_force(2, None)

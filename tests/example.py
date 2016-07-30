from arq import concurrent, AbstractWorker, Actor


class ActorTest(Actor):
    @concurrent
    async def foo(self, a, b):
        with open('add_numbers', 'w') as f:
            r = a + b
            f.write('{}'.format(r))


class Worker(AbstractWorker):
    signature = 'foobar'
    async def shadow_factory(self):
        return [ActorTest(loop=self.loop)]

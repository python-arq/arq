from arq import concurrent, Actor, AbstractWorkManager
from arq.mock_redis import MockRedisMixin


class Demo(Actor):
    @concurrent
    async def add_numbers(self, a, b):
        with open('add_numbers', 'w') as f:
            r = a + b
            f.write('{}'.format(r))

    @concurrent(Actor.HIGH_QUEUE)
    async def high_add_numbers(self, a, b, c=4):
        with open('high_add_numbers', 'w') as f:
            r = a + b + c
            f.write('{}'.format(r))
        return r

    @concurrent
    async def concat(self, a, b):
        return a + ' + ' + b

    @concurrent
    async def boom(self):
        raise RuntimeError('boom')


class MockRedisDemo(MockRedisMixin, Demo):
    pass


class Worker(AbstractWorkManager):
    async def shadow_factory(self):
        return {Demo()}


class MockRedisWorker(MockRedisMixin, AbstractWorkManager):
    async def shadow_factory(self):
        return {MockRedisDemo()}

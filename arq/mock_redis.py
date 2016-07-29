import asyncio
from arq import RedisMixin, timestamp


class MockRedis:
    def __init__(self, *, loop=None, data=None):
        self.loop = loop or asyncio.get_event_loop()
        self.data = {} if data is None else data

    async def rpush(self, list_name, data):
        self.data[list_name] = self.data.get(list_name, []) + [data]

    async def blpop(self, *list_names, timeout=0):
        assert isinstance(timeout, int)
        start = timestamp() if timeout > 0 else None
        while True:
            v = await self.lpop(*list_names)
            if v:
                return v
            if start and (timestamp() - start) > timeout:
                return
            await asyncio.sleep(0.1, loop=self.loop)

    async def lpop(self, *list_names):
        for list_name in list_names:
            data_list = self.data.get(list_name)
            if data_list is None:
                continue
            assert isinstance(data_list, list)
            if data_list:
                return list_name, data_list.pop(0)


class MockRedisPoolContextManager:
    def __init__(self, loop, data):
        self.loop = loop
        self.data = data

    async def __aenter__(self):
        return MockRedis(loop=self.loop, data=self.data)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MockRedisPool:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.data = {}

    def get(self):
        return MockRedisPoolContextManager(self.loop, self.data)

    async def clear(self):
        self.data = {}


class MockRedisMixin(RedisMixin):
    async def create_redis_pool(self):
        return self._redis_pool or MockRedisPool(self.loop)

    @property
    def mock_data(self):
        self._redis_pool = self._redis_pool or MockRedisPool(self.loop)
        return self._redis_pool.data

    @mock_data.setter
    def mock_data(self, data):
        self._redis_pool = self._redis_pool or MockRedisPool(self.loop)
        self._redis_pool.data = data

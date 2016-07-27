from arq import concurrent, Dispatch


class Demo(Dispatch):
    @concurrent
    async def add_numbers(self, a, b):
        with open('add_numbers', 'w') as f:
            r = a + b
            f.write('{}'.format(r))

    @concurrent(Dispatch.HIGH_QUEUE)
    async def high_add_numbers(self, a, b, c=4):
        with open('high_add_numbers', 'w') as f:
            r = a + b + c
            f.write('{}'.format(r))

    @concurrent
    async def boom(self, a, b):
        raise RuntimeError('boom')

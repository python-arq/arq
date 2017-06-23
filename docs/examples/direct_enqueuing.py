from arq import Actor


class FooBar(Actor):
    async def foo(self, a, b, c):
        print(a + b + c)


async def main():
    foobar = FooBar()
    await foobar.enqueue_job('foo', 1, 2, c=48, queue=Actor.LOW_QUEUE)
    await foobar.enqueue_job('foo', 1, 2, c=48)  # this will be queued in DEFAULT_QUEUE
    await foobar.close()

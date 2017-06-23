from arq import Actor, cron


class FooBar(Actor):

    @cron(hour={9, 12, 18}, minute=12)
    async def foo(self):
        print('run foo job at 9.12am, 12.12pm and 6.12pm')

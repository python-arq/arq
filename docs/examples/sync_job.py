import time
import functools
import asyncio
from concurrent import futures

def sync_task(t):
    return time.sleep(t)

async def the_task(ctx, t):
    blocking = functools.partial(sync_task, t)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(ctx['pool'], blocking)

async def startup(ctx):
    ctx['pool'] = futures.ProcessPoolExecutor()

class WorkerSettings:
    functions = [the_task]
    on_startup = startup

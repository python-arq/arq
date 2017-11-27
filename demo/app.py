#!/usr/bin/env python3.6
import os
import asyncio
from time import time

import chevron
import uvloop
from aiohttp import web, ClientError, ClientSession
from aiohttp_session import SimpleCookieStorage, get_session
from aiohttp_session import setup as session_setup
from arq import Actor, BaseWorker, RedisSettings, concurrent

R_OUTPUT = 'output'

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class Downloader(Actor):
    re_enqueue_jobs = True

    async def startup(self):
        self.session = ClientSession(loop=self.loop)

    @concurrent
    async def download_content(self, url, count):
        total_size = 0
        errors = []
        start = time()
        for _ in range(count):
            try:
                async with self.session.get(url) as r:
                    content = await r.read()
                    total_size += len(content)
                    if r.status != 200:
                        errors.append(f'{r.status} length: {len(content)}')
            except ClientError as e:
                errors.append(f'{e.__class__.__name__}: {e}')
        output = f'{time() - start:0.2f}s, {count} downloads, total size: {total_size}'
        if errors:
            output += ', errors: ' + ', '.join(errors)
        await self.redis.rpush(R_OUTPUT, output.encode())
        return total_size

    async def shutdown(self):
        self.session.close()


html_template = """
<h1>arq demo</h1>

{{#message}}
<div>{{ message }}</div>
{{/message}}

<form method="post" action="/start-job/">
  <p>
  <label for="url">Url to download</label>
  <input type="url" name="url" id="url" value="https://httpbin.org/get" required/>
  </p>
  <p>
  <label for="count">Download count</label>
  <input type="number" step="1" name="count" id="count" value="10" required/>
  </p>
  <p>
  <input type="submit" value="Download"/>
  </p>
</form>

<h2>Results:</h2>
{{#results}}
<p>{{ . }}</p>
{{/results}}
"""


async def index(request):
    redis = await request.app['downloader'].get_redis()
    data = await redis.lrange(R_OUTPUT, 0, -1)
    results = [r.decode() for r in data]

    session = await get_session(request)
    html = chevron.render(html_template, {'message': session.get('message'), 'results': results})
    session.invalidate()
    return web.Response(text=html, content_type='text/html')


async def start_job(request):
    data = await request.post()
    session = await get_session(request)
    try:
        url = data['url']
        count = int(data['count'])
    except (KeyError, ValueError) as e:
        session['message'] = f'Invalid input, {e.__class__.__name__}: {e}'
    else:
        await request.app['downloader'].download_content(url, count)
        session['message'] = f'Downloading "{url}" ' + (f'{count} times.' if count > 1 else 'once.')
    raise web.HTTPFound(location='/')


redis_settings = RedisSettings(host=os.getenv('REDIS_HOST', 'localhost'))


async def shutdown(app):
    await app['downloader'].close()


def create_app():
    app = web.Application()
    app.router.add_get('/', index)
    app.router.add_post('/start-job/', start_job)
    app['downloader'] = Downloader(redis_settings=redis_settings)
    app.on_shutdown.append(shutdown)
    session_setup(app, SimpleCookieStorage())
    return app


class Worker(BaseWorker):
    # used by `arq app.py` command
    shadows = [Downloader]
    # set to small value so we can play with timeouts
    timeout_seconds = 10

    def __init__(self, *args, **kwargs):
        kwargs['redis_settings'] = redis_settings
        super().__init__(*args, **kwargs)


if __name__ == '__main__':
    # when called directly run the webserver
    app = create_app()
    web.run_app(app, port=8000)

from aiohttp import web


async def start_job(request):
    data = await request.post()
    # this will enqueue the download_content job
    await request.app['downloader'].download_content(data['url'])
    raise web.HTTPFound(location='/wherever/')


async def shutdown(app):
    await app['downloader'].close()


def create_app():
    app = web.Application()
    ...
    app.router.add_post('/start-job/', start_job)
    app['downloader'] = Downloader()
    # use aiohttp's on_shutdown trigger to close downloader
    app.on_shutdown.append(shutdown)
    return app


if __name__ == '__main__':
    app = create_app()
    web.run_app(app, port=8000)

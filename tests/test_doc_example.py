from importlib.machinery import SourceFileLoader
from pathlib import Path

THIS_DIR = Path(__file__).parent


async def test_run_job_burst(redis_conn, loop, caplog):
    demo = SourceFileLoader('demo', str(THIS_DIR / '../docs/examples/main_demo.py')).load_module()
    worker = demo.Worker(burst=True, loop=loop)

    downloader = demo.Downloader(loop=loop)

    await downloader.download_content('http://example.com')
    await worker.run()
    await downloader.close()
    assert 's → Downloader.download_content(http://example.com)' in caplog
    assert 's ← Downloader.download_content ● 1' in caplog

import importlib
import sys
from types import ModuleType

from arq.cli import watch_reload


async def test_watch_reload_runs_edited_job(tmp_path, monkeypatch):
    """--watch must import job functions again after the settings module changes."""
    module_name = 'arq_watch_demo_429'
    module_path = tmp_path / f'{module_name}.py'
    source = 'async def my_task(ctx):\n    return "HELLO-1"\n\nclass WorkerSettings:\n    functions = [my_task]\n'
    module_path.write_text(source, encoding='utf-8')
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module(module_name)

    async def awatch(path, stop_event=None):
        module_path.write_text(source.replace('HELLO-1', 'HELLO-2'), encoding='utf-8')
        yield {'changed'}

    watchfiles = ModuleType('watchfiles')
    watchfiles.awatch = awatch
    monkeypatch.setitem(sys.modules, 'watchfiles', watchfiles)

    created = []

    class _FakeWorker:
        def __init__(self, settings):
            self.settings = settings
            self.on_stop = None
            self.tasks = {}
            self.main_task = None

        def handle_sig(self, sig):
            return None

        async def close(self):
            return None

        async def async_run(self):
            return None

    def fake_create(settings, **kwargs):
        worker = _FakeWorker(settings)
        created.append(worker)
        return worker

    monkeypatch.setattr('arq.cli.create_worker', fake_create)
    try:
        await watch_reload(str(tmp_path), module.WorkerSettings)
    finally:
        sys.modules.pop(module_name, None)

    assert len(created) == 2
    assert await created[0].settings.functions[0]({}) == 'HELLO-1'
    assert await created[1].settings.functions[0]({}) == 'HELLO-2'

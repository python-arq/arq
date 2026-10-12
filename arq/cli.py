import asyncio
import importlib
import logging.config
import os
import sys
from signal import Signals
from typing import TYPE_CHECKING, Any, cast

import click

from .logs import default_log_config
from .utils import import_string
from .version import VERSION
from .worker import Worker, check_health, create_worker, run_worker

if TYPE_CHECKING:
    from .typing import WorkerSettingsType

burst_help = 'Batch mode: exit once no jobs are found in any queue.'
health_check_help = 'Health Check: run a health check and exit.'
watch_help = 'Watch a directory and reload the worker upon changes.'
verbose_help = 'Enable verbose output.'
logdict_help = "Import path for a dictionary in logdict form, to configure Arq's own logging."


@click.command('arq')
@click.version_option(VERSION, '-V', '--version', prog_name='arq')
@click.argument('worker-settings', type=str, required=True)
@click.option('--burst/--no-burst', default=None, help=burst_help)
@click.option('--check', is_flag=True, help=health_check_help)
@click.option('--watch', type=click.Path(exists=True, dir_okay=True, file_okay=False), help=watch_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
@click.option('--custom-log-dict', type=str, help=logdict_help)
def cli(*, worker_settings: str, burst: bool, check: bool, watch: str, verbose: bool, custom_log_dict: str) -> None:
    """
    Job queues in python with asyncio and redis.

    CLI to run the arq worker.
    """
    sys.path.append(os.getcwd())
    worker_settings_ = cast('WorkerSettingsType', import_string(worker_settings))
    if custom_log_dict:
        log_config = import_string(custom_log_dict)
    else:
        log_config = default_log_config(verbose)
    logging.config.dictConfig(log_config)

    if check:
        exit(check_health(worker_settings_))
    else:
        kwargs = {} if burst is None else {'burst': burst}
        if watch:
            asyncio.run(watch_reload(watch, worker_settings_))
        else:
            run_worker(worker_settings_, **kwargs)


def _reload_worker_settings(worker_settings: 'WorkerSettingsType') -> 'WorkerSettingsType':
    """Re-import worker settings so --watch runs the edited job functions."""
    module_name = getattr(worker_settings, '__module__', None)
    qualname = getattr(worker_settings, '__qualname__', None)
    if not module_name or not qualname:
        return worker_settings
    module = sys.modules.get(module_name)
    if module is None:
        return worker_settings
    # A save in the same mtime second still has to recompile; otherwise reload
    # keeps the bytecode from startup and --watch runs the old function.
    cached = getattr(module, '__cached__', None)
    if isinstance(cached, str):
        try:
            os.remove(cached)
        except OSError:
            pass
    importlib.invalidate_caches()
    reloaded = importlib.reload(module)
    obj: Any = reloaded
    for part in qualname.split('.'):
        obj = getattr(obj, part)
    return cast('WorkerSettingsType', obj)


def _stop_worker_for_reload(worker: Worker) -> None:
    """Cancel the running worker without leaving --watch.

    Unix uses SIGUSR1 so ``worker_on_stop`` does not set the watch stop event.
    Windows has no SIGUSR1; cancel the tasks directly.
    """
    sigusr1 = getattr(Signals, 'SIGUSR1', None)
    if sigusr1 is not None:
        worker.handle_sig(sigusr1)
        return
    for task in list(worker.tasks.values()):
        if not task.done():
            task.cancel()
    if worker.main_task is not None and not worker.main_task.done():
        worker.main_task.cancel()


async def watch_reload(path: str, worker_settings: 'WorkerSettingsType') -> None:
    try:
        from watchfiles import awatch
    except ImportError as e:  # pragma: no cover
        raise ImportError('watchfiles not installed, use `pip install watchfiles`') from e

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def worker_on_stop(s: Signals) -> None:
        if s != getattr(Signals, 'SIGUSR1', None):  # pragma: no cover
            stop_event.set()

    worker = create_worker(worker_settings)
    try:
        worker.on_stop = worker_on_stop
        loop.create_task(worker.async_run())
        async for _ in awatch(path, stop_event=stop_event):
            print('\nfiles changed, reloading arq worker...')
            _stop_worker_for_reload(worker)
            await worker.close()
            worker_settings = _reload_worker_settings(worker_settings)
            worker = create_worker(worker_settings)
            worker.on_stop = worker_on_stop
            loop.create_task(worker.async_run())
    finally:
        await worker.close()

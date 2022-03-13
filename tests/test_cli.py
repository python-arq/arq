import pytest
from click.testing import CliRunner

from arq.cli import cli


async def foobar(ctx):
    return 42


class WorkerSettings:
    burst = True
    functions = [foobar]


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert result.output.startswith('Usage: arq [OPTIONS] WORKER_SETTINGS\n')


def test_run(cancel_remaining_task, mocker, loop):
    mocker.patch('asyncio.get_event_loop', lambda: loop)
    runner = CliRunner()
    result = runner.invoke(cli, ['tests.test_cli.WorkerSettings'])
    assert result.exit_code == 0
    assert 'Starting worker for 1 functions: foobar' in result.output


def test_check(loop):
    runner = CliRunner()
    result = runner.invoke(cli, ['tests.test_cli.WorkerSettings', '--check'])
    assert result.exit_code == 1
    assert 'Health check failed: no health check sentinel value found' in result.output


async def mock_awatch():
    yield [1]


@pytest.mark.filterwarnings('ignore::DeprecationWarning')
def test_run_watch(mocker, cancel_remaining_task):
    mocker.patch('watchgod.awatch', return_value=mock_awatch())
    runner = CliRunner()
    result = runner.invoke(cli, ['tests.test_cli.WorkerSettings', '--watch', 'tests'])
    assert result.exit_code == 0
    assert '1 files changes, reloading arq worker...'

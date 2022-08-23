import pytest
from click.testing import CliRunner

from arq import logs
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


def test_run_watch(mocker, cancel_remaining_task):
    mocker.patch('watchfiles.awatch', return_value=mock_awatch())
    runner = CliRunner()
    result = runner.invoke(cli, ['tests.test_cli.WorkerSettings', '--watch', 'tests'])
    assert result.exit_code == 0
    assert '1 files changes, reloading arq worker...'


custom_log_dict = {
    'version': 1,
    'handlers': {'custom': {'level': 'ERROR', 'class': 'logging.StreamHandler', 'formatter': 'custom'}},
    'formatters': {'custom': {'format': '%(asctime)s: %(message)s', 'datefmt': '%H:%M:%S'}},
    'loggers': {'arq': {'handlers': ['custom'], 'level': 'ERROR'}},
}


@pytest.mark.parametrize(
    'cli_argument,log_dict_to_use',
    [
        (None, logs.default_log_config(verbose=False)),
        ('--custom-log-dict=tests.test_cli.custom_log_dict', custom_log_dict),
    ],
)
def test_custom_log_dict(mocker, loop, cli_argument, log_dict_to_use):
    mocker.patch('asyncio.get_event_loop', lambda: loop)
    mock_dictconfig = mocker.MagicMock()
    mocker.patch('logging.config.dictConfig', mock_dictconfig)
    arq_arguments = ['tests.test_cli.WorkerSettings']
    if cli_argument is not None:
        arq_arguments.append(cli_argument)

    runner = CliRunner()
    result = runner.invoke(cli, arq_arguments)
    assert result.exit_code == 0
    mock_dictconfig.assert_called_with(log_dict_to_use)

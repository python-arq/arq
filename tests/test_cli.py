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


def test_run():
    runner = CliRunner()
    result = runner.invoke(cli, ['tests.test_cli.WorkerSettings'])
    assert result.exit_code == 0
    assert 'Starting worker for 1 functions: foobar' in result.output


def test_check():
    runner = CliRunner()
    result = runner.invoke(cli, ['tests.test_cli.WorkerSettings', '--check'])
    assert result.exit_code == 1
    assert 'Health check failed: no health check sentinel value found' in result.output

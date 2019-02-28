from click.testing import CliRunner

from arq.cli import cli


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert result.output.startswith('Usage: arq [OPTIONS] WORKER_SETTINGS\n')

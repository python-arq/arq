import re
from unittest.mock import MagicMock

from click.testing import CliRunner

from arq.cli import cli
import arq.worker

from .fixtures import EXAMPLE_FILE


def test_simple_batch_mode(tmpworkdir, monkeypatch):
    # we have to prevent RunWorkerProcess actually starting another process
    # TODO remove after https://bitbucket.org/ned/coveragepy/issues/512
    monkeypatch.setattr(arq.worker.Process, 'start', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'join', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'exitcode', 0)
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    runner = CliRunner()
    result = runner.invoke(cli, ['--batch', 'test.py'])
    assert result.exit_code == 0
    output = re.sub('\d+:\d+:\d+', 'TIME', result.output)
    assert output == ('TIME MainProcess: starting work process "WorkProcess"\n'
                      'TIME MainProcess: worker process exited ok\n')


def test_worker_exited_badly(tmpworkdir, monkeypatch):
    # we have to prevent RunWorkerProcess actually starting another process
    # TODO remove after https://bitbucket.org/ned/coveragepy/issues/512
    monkeypatch.setattr(arq.worker.Process, 'start', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'join', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'exitcode', 42)
    monkeypatch.setattr(arq.worker.Process, 'pid', 123)
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    runner = CliRunner()
    result = runner.invoke(cli, ['--batch', 'test.py'])
    assert result.exit_code == 3
    output = re.sub('\d+:\d+:\d+', 'TIME', result.output)
    assert ('TIME MainProcess: starting work process "WorkProcess"\n'
            'TIME MainProcess: worker process 123 exited badly with exit code 42\n') == output

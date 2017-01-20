import re
import signal
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

import arq.worker
from arq.cli import cli

from .fixtures import EXAMPLE_FILE


def test_simple_burst(tmpworkdir, monkeypatch):
    # we have to prevent RunWorkerProcess actually starting another process
    # TODO remove after https://bitbucket.org/ned/coveragepy/issues/512
    monkeypatch.setattr(arq.worker.Process, 'start', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'join', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'exitcode', 0)
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    runner = CliRunner()
    result = runner.invoke(cli, ['--burst', 'test.py'])
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
    result = runner.invoke(cli, ['--burst', 'test.py'])
    assert result.exit_code == 3
    output = re.sub('\d+:\d+:\d+', 'TIME', result.output)
    assert ('TIME MainProcess: starting work process "WorkProcess"\n'
            'TIME MainProcess: worker process 123 exited badly with exit code 42\n') == output


def test_main_process_sigint(tmpworkdir, monkeypatch, caplog):
    monkeypatch.setattr(arq.worker.Process, 'start', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'join', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'is_alive', MagicMock(return_value=True))
    monkeypatch.setattr(arq.worker.Process, 'exitcode', 0)
    monkeypatch.setattr(arq.worker.Process, 'pid', 123)

    os_kill = MagicMock()
    monkeypatch.setattr(arq.worker.os, 'kill', os_kill)

    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    work_runner = arq.worker.RunWorkerProcess('test.py', 'Worker')
    work_runner.handle_sig(signal.SIGINT, None)
    assert 'got signal: SIGINT, waiting for worker pid=123 to finish...' in caplog
    os_kill.assert_called_once_with(123, arq.worker.SIG_PROXY)


def test_main_process_sigint_worker_stopped(tmpworkdir, monkeypatch, caplog):
    monkeypatch.setattr(arq.worker.Process, 'start', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'join', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'is_alive', MagicMock(return_value=False))
    monkeypatch.setattr(arq.worker.Process, 'exitcode', 0)
    monkeypatch.setattr(arq.worker.Process, 'pid', 123)

    os_kill = MagicMock()
    monkeypatch.setattr(arq.worker.os, 'kill', os_kill)

    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    work_runner = arq.worker.RunWorkerProcess('test.py', 'Worker')
    work_runner.handle_sig(signal.SIGINT, None)
    assert os_kill.called is False


def test_main_process_sigint_twice(tmpworkdir, monkeypatch, caplog):
    monkeypatch.setattr(arq.worker.Process, 'start', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'join', MagicMock())
    is_alive = MagicMock(return_value=False)
    monkeypatch.setattr(arq.worker.Process, 'is_alive', is_alive)
    monkeypatch.setattr(arq.worker.Process, 'exitcode', 0)
    monkeypatch.setattr(arq.worker.Process, 'pid', 123)
    os_kill = MagicMock()
    monkeypatch.setattr(arq.worker.os, 'kill', os_kill)
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    work_runner = arq.worker.RunWorkerProcess('test.py', 'Worker')
    with pytest.raises(arq.worker.ImmediateExit):
        work_runner.handle_sig_force(signal.SIGINT, None)
    assert is_alive.call_count == 1
    assert not os_kill.called
    assert 'got signal: SIGINT again, forcing exit' in caplog


def test_main_process_sigint_twice_worker_running(tmpworkdir, monkeypatch, caplog):
    monkeypatch.setattr(arq.worker.Process, 'start', MagicMock())
    monkeypatch.setattr(arq.worker.Process, 'join', MagicMock())
    is_alive = MagicMock(return_value=True)
    monkeypatch.setattr(arq.worker.Process, 'is_alive', is_alive)
    monkeypatch.setattr(arq.worker.Process, 'exitcode', 0)
    monkeypatch.setattr(arq.worker.Process, 'pid', 123)
    os_kill = MagicMock()
    monkeypatch.setattr(arq.worker.os, 'kill', os_kill)
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    work_runner = arq.worker.RunWorkerProcess('test.py', 'Worker')
    with pytest.raises(arq.worker.ImmediateExit):
        work_runner.handle_sig_force(signal.SIGINT, None)
    assert is_alive.call_count == 1
    os_kill.assert_called_once_with(123, signal.SIGTERM)
    assert 'got signal: SIGINT again, forcing exit' in caplog

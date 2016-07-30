import re

from click.testing import CliRunner

from arq.cli import cli

TEST_PY = """\
from arq import concurrent, AbstractWorker, Actor

class ActorTest(Actor):
    @concurrent
    async def foo(self, a, b):
        return a + b

class Worker(AbstractWorker):
    async def shadow_factory(self):
        return [ActorTest(loop=self.loop)]"""


def test_simple_batch_mode(tmpworkdir):
    tmpworkdir.join('test.py').write(TEST_PY)
    runner = CliRunner()
    result = runner.invoke(cli, ['--batch', 'test.py'])
    assert result.exit_code == 0
    output = re.sub('\d+:\d+:\d+', 'TIME', result.output)
    assert output == ('TIME MainProcess arq.work: starting work process "WorkProcess"\n'
                      'TIME MainProcess arq.work: worker process exited ok\n')

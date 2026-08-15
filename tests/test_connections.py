import pytest

from arq.connections import ArqRedis


def test_decode_responses_rejected():
    with pytest.raises(RuntimeError, match='decode_responses'):
        ArqRedis(host='localhost', port=6379, decode_responses=True)


def test_decode_responses_false_is_fine():
    redis_ = ArqRedis(host='localhost', port=6379, decode_responses=False)
    assert redis_.connection_pool.connection_kwargs.get('decode_responses') is False


def test_decode_responses_unset_is_fine():
    redis_ = ArqRedis(host='localhost', port=6379)
    assert not redis_.connection_pool.connection_kwargs.get('decode_responses')

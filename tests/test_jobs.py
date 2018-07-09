from functools import partial

import pytest

from arq.jobs import ArqError, Job

# Job init params
queued_at = 10000000 * 1000  # in mili seconds
class_name = 'MockClass'
func_name = 'mock_method'
args = ('first', 2)
kwargs = {'key0': '0', 'key1': 1, 'key2': True}
job_id = 'qwertyuiop1234'
queue_name = 'HIGH'
raw_queue = queue_name.encode()

create_job_with_queue = partial(
    Job,
    queued_at=queued_at,
    class_name=class_name,
    func_name=func_name,
    args=args,
    kwargs=kwargs,
    job_id=job_id,
)


def test_job_creation():
    job = create_job_with_queue(
        queue_name=queue_name,
        raw_queue=raw_queue,
    )
    assert job.class_name == class_name
    assert job.func_name == func_name
    assert job.args == args
    assert job.kwargs == kwargs
    assert job.id == job_id
    assert job.queue == queue_name
    assert job.raw_queue == raw_queue

    # input miliseconds, but store in seconds
    assert job.queued_at == queued_at / 1000


def test_job_creation_with_queue_name_none():
    job = create_job_with_queue(
        queue_name=None,
        raw_queue=raw_queue,
    )
    assert job.queue == raw_queue.decode()
    assert job.raw_queue == raw_queue


def test_job_creation_with_raw_queue_none():
    job = create_job_with_queue(
        queue_name=queue_name,
        raw_queue=None,
    )
    assert job.queue == queue_name
    assert job.raw_queue == queue_name.encode()


def test_job_raise_if_both_queue_name_is_none():
    with pytest.raises(ArqError) as exc_info:
        create_job_with_queue()
    assert 'either queue_name or raw_queue are required' in str(exc_info)


def test_job_encode_then_decode_the_same():
    job = create_job_with_queue(
        queue_name=queue_name,
        raw_queue=raw_queue,
    )

    decoded_job = Job.decode(job.encode(), queue_name, raw_queue)

    assert job.class_name == decoded_job.class_name
    assert job.func_name == decoded_job.func_name
    # TODO: fix this strange behavior(job.args is tuple and decoded_job.args is list)
    assert list(job.args) == list(decoded_job.args)
    assert job.kwargs == decoded_job.kwargs
    assert job.id == decoded_job.id
    assert job.queue == decoded_job.queue
    assert job.raw_queue == decoded_job.raw_queue
    assert job.queued_at == decoded_job.queued_at

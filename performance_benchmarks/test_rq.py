import sys

from rq.decorators import job
from redis import Redis
from rq import Queue, Worker

from jobs import fast_job, generate_big_dict, big_argument_job

q = Queue(connection=Redis())


@job(q, connection=q.connection)
def fast():
    fast_job()


@job(q, connection=q.connection)
def big_argument(v):
    return big_argument_job(v)


def start_jobs():
    for i in range(1000):
        fast.delay()
        v = generate_big_dict()
        big_argument.delay(v)

if __name__ == '__main__':
    if 'work' in sys.argv:
        # easier than faffing around with rq worker's dumb import system
        worker = Worker([q], connection=q.connection)
        worker.work(burst=True)
    else:
        start_jobs()

import os


def fast_job():
    a = 1
    for i in range(10):
        a *= i
    return a


def generate_big_dict():
    d = {}
    for i in range(100):
        d[i] = os.urandom(i)
    for i in range(100):
        d[str(i)] = list(range(i))
    return d


def big_argument_job(v):
    return {str(v): k for k, v in v.items()}

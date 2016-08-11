import msgpack

from .utils import ellipsis, timestamp

__all__ = ['ArqSerialiseError', 'Job']


class ArqSerialiseError(Exception):
    pass


class Job:
    __slots__ = ('queue', 'queued_at', 'class_name', 'func_name', 'args', 'kwargs')

    def __init__(self, queue, data):
        self.queue = queue
        data = msgpack.unpackb(data, encoding='utf8')
        self.queued_at, self.class_name, self.func_name, self.args, self.kwargs = data
        self.queued_at /= 1000

    @classmethod
    def encode(cls, *, queued_at=None, class_name, func_name, args, kwargs):
        queued_at = queued_at or int(timestamp() * 1000)
        return msgpack.packb([queued_at, class_name, func_name, args, kwargs], use_bin_type=True)

    def __str__(self):
        arguments = ''
        if self.args:
            arguments = ', '.join(map(str, self.args))
        if self.kwargs:
            if arguments:
                arguments += ', '
            arguments += ', '.join('{}={!r}'.format(*kv) for kv in sorted(self.kwargs.items()))

        return '{s.class_name}.{s.func_name}({args})'.format(s=self, args=ellipsis(arguments))

    def __repr__(self):
        return '<Job {} on {}>'.format(self, self.queue)

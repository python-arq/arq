from datetime import datetime, timedelta

import msgpack

from .utils import EPOCH, EPOCH_TZ, create_tz, ellipsis, timestamp

__all__ = ['JobSerialisationError', 'Job']


class JobSerialisationError(Exception):
    pass


class Job:
    __slots__ = ('queue', 'queued_at', 'class_name', 'func_name', 'args', 'kwargs')
    msgpack_encoder = None
    msgpack_object_hook = None

    def __init__(self, queue, data):
        self.queue = queue
        self.queued_at, self.class_name, self.func_name, self.args, self.kwargs = self._decode(data)
        self.queued_at /= 1000

    @classmethod
    def encode(cls, *, queued_at=None, class_name, func_name, args, kwargs):
        queued_at = queued_at or int(timestamp() * 1000)
        return cls._encode([queued_at, class_name, func_name, args, kwargs])

    @classmethod
    def _encode(cls, data):
        return msgpack.packb(data, default=cls.msgpack_encoder, use_bin_type=True)

    @classmethod
    def _decode(cls, data):
        return msgpack.unpackb(data, object_hook=cls.msgpack_object_hook, encoding='utf8')

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


DATETIME = '_dt_'


class DatetimeJob(Job):
    @staticmethod
    def msgpack_encoder(obj):
        if isinstance(obj, datetime):
            utcoffset = obj.utcoffset()
            if utcoffset is not None:
                utcoffset = utcoffset.total_seconds()
                unix = (obj - EPOCH_TZ).total_seconds() + utcoffset
                return {DATETIME: unix, 'o': int(utcoffset)}
            else:
                return {DATETIME: (obj - EPOCH).total_seconds()}
        return obj

    @staticmethod
    def msgpack_object_hook(obj):
        if DATETIME in obj:
            dt = EPOCH + timedelta(seconds=obj[DATETIME])
            utcoffset = obj.get('o')
            if utcoffset is not None:
                dt = dt.replace(tzinfo=create_tz(utcoffset))
            return dt
        return obj

"""
:mod:`jobs`
===========

Defines the ``Job`` class and descendants which deal with encoding and decoding job data.
"""
from datetime import datetime

import msgpack

from .utils import ellipsis, from_unix_ms, timestamp, to_unix_ms

__all__ = ['JobSerialisationError', 'Job', 'DatetimeJob']


class JobSerialisationError(Exception):
    pass


class Job:
    """
    Main Job class responsible for encoding and decoding jobs as they go
    into and come out of redis.
    """
    __slots__ = ('queue', 'queued_at', 'class_name', 'func_name', 'args', 'kwargs')

    #: custom encoder for msgpack, see :class:`arq.jobs.DatetimeJob` for an example of usage
    msgpack_encoder = None  # type: function

    #: custom object hook for msgpack, see :class:`arq.jobs.DatetimeJob` for an example of usage
    msgpack_object_hook = None  # type: function

    def __init__(self, queue: str, data: bytes) -> None:
        """
        Create a job instance be decoding a job definition eg. from redis.

        :param queue: name of the queue the job was dequeued from
        :param data: data to decode, as created by :meth:`arq.jobs.Job.encode`
        """
        self.queue = queue
        self.queued_at, self.class_name, self.func_name, self.args, self.kwargs = self._decode(data)
        self.queued_at /= 1000

    @classmethod
    def encode(cls, *, queued_at: int=None, class_name: str, func_name: str,
               args: tuple, kwargs: dict) -> bytes:
        """
        Create a byte string suitable for pushing into redis which contains all
        required information about a job to be performed.

        :param queued_at: time in ms unix time when the job was queue, if None now is used
        :param class_name: name (see :attr:`arq.main.Actor.name`) of the actor class where the job is defined
        :param func_name: name of the function be called
        :param args: arguments to pass to the function
        :param kwargs: key word arguments to pass to the function
        """
        queued_at = queued_at or int(timestamp() * 1000)
        try:
            return cls._encode([queued_at, class_name, func_name, args, kwargs])
        except TypeError as e:
            raise JobSerialisationError(str(e)) from e

    @classmethod
    def _encode(cls, data) -> bytes:
        return msgpack.packb(data, default=cls.msgpack_encoder, use_bin_type=True)

    @classmethod
    def _decode(cls, data: bytes):
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


# unicode clock is small to encode and should be fairly unlikely to clash with another dict key
DATETIME = '⌚'
TIMEZONE = 'O'


class DatetimeJob(Job):
    """
    Alternative Job which copes with datetimes. None timezone naïve dates are supported but
    the returned datetimes will use a :mod:`datetime.timezone` class to define the timezone
    regardless of the timezone class originally used on the datetime object (eg. ``pytz``).
    """
    @staticmethod
    def msgpack_encoder(obj):
        if isinstance(obj, datetime):
            ts, tz = to_unix_ms(obj)
            result = {DATETIME: ts}
            if tz is not None:
                result[TIMEZONE] = tz
            return result
        else:
            return obj

    @staticmethod
    def msgpack_object_hook(obj):
        if DATETIME in obj and len(obj) <= 2:
            return from_unix_ms(obj[DATETIME], utcoffset=obj.get(TIMEZONE))
        return obj

"""
:mod:`jobs`
===========

Defines the ``Job`` class and descendants which deal with encoding and decoding job data.
"""
import base64
import os
from datetime import datetime
from typing import Tuple

import msgpack

from .utils import DEFAULT_CURTAIL, from_unix_ms, timestamp, to_unix_ms_tz, truncate

__all__ = ['JobSerialisationError', 'Job', 'DatetimeJob']


class ArqError(Exception):
    pass


class JobSerialisationError(ArqError):
    pass


def gen_random():
    """
    generate a lowercase alpha-numeric random string of length 24.

    Should have more randomness for its size thank uuid
    """
    return base64.b32encode(os.urandom(10))[:16].decode().lower()


# "device control one" should be fairly unique as a dict key and only one byte
DEVICE_CONTROL_ONE = '\x11'


class Job:
    """
    Main Job class responsible for encoding and decoding jobs as they go
    into and come out of redis.
    """
    __slots__ = 'id', 'queue', 'queued_at', 'class_name', 'func_name', 'args', 'kwargs', 'raw_queue'

    def __init__(
        self,
        class_name: str,
        func_name: str,
        args: tuple,
        kwargs: dict,
        *,
        queued_at: float = None,
        job_id: str = None,
        queue_name: str = None,
        raw_queue: bytes = None,
    ) -> None:
        """
        Create a job instance be decoding a job definition eg. from redis.

        :param raw_data: data to decode, as created by :meth:`arq.jobs.Job.encode`
        :param raw_queue: raw name of the queue the job was taken from
        :param queue_name: name of the queue the job was dequeued from
        """
        self.queue, self.raw_queue = self.init_queue_name(queue_name, raw_queue)

        self.queued_at = queued_at or int(timestamp() * 1000)
        self.queued_at = self.queued_at / 1000
        self.class_name = class_name
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs
        self.id = self.generate_id(job_id)

    @staticmethod
    def init_queue_name(queue_name: str, raw_queue: bytes) -> Tuple[str, bytes]:
        if queue_name is None and raw_queue is None:
            raise ArqError('either queue_name or raw_queue are required')

        final_queue = queue_name or raw_queue.decode()
        final_raw_queue = raw_queue or queue_name.encode()

        return final_queue, final_raw_queue

    @property
    def raw_data(self):
        return self.encode()

    def encode(self) -> bytes:
        """
        Create a byte string suitable for pushing into redis which contains all
        required information about a job to be performed.

        :param job_id: id to use for the job, leave blank to generate a uuid
        :param queued_at: time in ms unix time when the job was queue, if None now is used
        :param class_name: name (see :attr:`arq.main.Actor.name`) of the actor class where the job is defined
        :param func_name: name of the function be called
        :param args: arguments to pass to the function
        :param kwargs: key word arguments to pass to the function
        """
        to_be_encoded = [
            self.queued_at * 1000,
            self.class_name,
            self.func_name,
            self.args,
            self.kwargs,
            self.id,
        ]
        try:
            return self.encode_raw(to_be_encoded)
        except TypeError as e:
            raise JobSerialisationError(str(e)) from e

    @classmethod
    def decode(cls, raw_data: bytes, queue_name: str=None, raw_queue: bytes=None):
        queue, raw_queue = cls.init_queue_name(queue_name, raw_queue)
        queued_at, class_name, func_name, args, kwargs, job_id = cls.decode_raw(raw_data)
        return cls(
            queued_at=queued_at,
            class_name=class_name,
            func_name=func_name,
            args=args,
            kwargs=kwargs,
            job_id=job_id,
            queue_name=queue_name,
            raw_queue=raw_queue,
        )

    @classmethod
    def generate_id(cls, given_id):
        return given_id or gen_random()

    @classmethod
    def msgpack_encoder(cls, obj):
        """
        The default msgpack encoder, adds support for encoding sets.
        """
        if isinstance(obj, set):
            return {DEVICE_CONTROL_ONE: list(obj)}
        else:
            return obj

    @classmethod
    def msgpack_object_hook(cls, obj):
        if len(obj) == 1 and DEVICE_CONTROL_ONE in obj:
            return set(obj[DEVICE_CONTROL_ONE])
        return obj

    @classmethod
    def encode_raw(cls, data) -> bytes:
        return msgpack.packb(data, default=cls.msgpack_encoder, use_bin_type=True)

    @classmethod
    def decode_raw(cls, data: bytes):
        return msgpack.unpackb(data, object_hook=cls.msgpack_object_hook, raw=False)

    def to_string(self, args_curtail=DEFAULT_CURTAIL):
        arguments = ''
        if self.args:
            arguments = ', '.join(map(str, self.args))
        if self.kwargs:
            if arguments:
                arguments += ', '
            arguments += ', '.join(f'{k}={v!r}' for k, v in sorted(self.kwargs.items()))

        return '{s.id:.6} {s.class_name}.{s.func_name}({args})'.format(s=self, args=truncate(arguments, args_curtail))

    def short_ref(self):
        return '{s.id:.6} {s.class_name}.{s.func_name}'.format(s=self)

    def __str__(self):
        return self.to_string()

    def __repr__(self):
        return f'<Job {self} on {self.queue}>'


DEVICE_CONTROL_TWO = '\x12'
TIMEZONE = 'O'


class DatetimeJob(Job):
    """
    Alternative Job which copes with datetimes. None timezone naïve dates are supported but
    the returned datetimes will use a :mod:`datetime.timezone` class to define the timezone
    regardless of the timezone class originally used on the datetime object (eg. ``pytz``).
    """
    @classmethod
    def msgpack_encoder(cls, obj):
        if isinstance(obj, datetime):
            ts, tz = to_unix_ms_tz(obj)
            result = {DEVICE_CONTROL_TWO: ts}
            if tz is not None:
                result[TIMEZONE] = tz
            return result
        else:
            return super().msgpack_encoder(obj)

    @classmethod
    def msgpack_object_hook(cls, obj):
        if len(obj) <= 2 and DEVICE_CONTROL_TWO in obj:
            return from_unix_ms(obj[DEVICE_CONTROL_TWO], utcoffset=obj.get(TIMEZONE))
        else:
            return super().msgpack_object_hook(obj)

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Optional, Union

from pydantic.utils import import_string

from arq.utils import SecondsTimedelta, to_seconds


class D(str, Enum):
    month = 'month'
    day = 'day'
    weekday = 'weekday'
    hour = 'hour'
    minute = 'minute'
    second = 'second'
    microsecond = 'microsecond'


dt_fields = D.month, D.day, D.weekday, D.hour, D.minute, D.second, D.microsecond
weekdays = 'mon', 'tues', 'wed', 'thurs', 'fri', 'sat', 'sun'


def _get_next_dt(dt_, options):  # noqa: C901
    for field in dt_fields:
        v = options[field]
        if v is None:
            continue
        if field == D.weekday:
            next_v = dt_.weekday()
        else:
            next_v = getattr(dt_, field)
        if isinstance(v, int):
            mismatch = next_v != v
        else:
            assert isinstance(v, (set, list, tuple))
            mismatch = next_v not in v
        # print(field, v, next_v, mismatch)
        if mismatch:
            micro = max(dt_.microsecond - options[D.microsecond], 0)
            if field == D.month:
                if dt_.month == 12:
                    return datetime(dt_.year + 1, 1, 1)
                else:
                    return datetime(dt_.year, dt_.month + 1, 1)
            elif field in (D.day, D.weekday):
                return (
                    dt_
                    + timedelta(days=1)
                    - timedelta(hours=dt_.hour, minutes=dt_.minute, seconds=dt_.second, microseconds=micro)
                )
            elif field == D.hour:
                return dt_ + timedelta(hours=1) - timedelta(minutes=dt_.minute, seconds=dt_.second, microseconds=micro)
            elif field == D.minute:
                return dt_ + timedelta(minutes=1) - timedelta(seconds=dt_.second, microseconds=micro)
            elif field == D.second:
                return dt_ + timedelta(seconds=1) - timedelta(microseconds=micro)
            else:
                assert field == D.microsecond, field
                return dt_ + timedelta(microseconds=options['microsecond'] - dt_.microsecond)


def next_cron(
    previous_dt: datetime,
    *,
    month: Union[None, set, int] = None,
    day: Union[None, set, int] = None,
    weekday: Union[None, set, int, str] = None,
    hour: Union[None, set, int] = None,
    minute: Union[None, set, int] = None,
    second: Union[None, set, int] = 0,
    microsecond: int = 123_456,
):
    """
    Find the next datetime matching the given parameters.
    """
    dt = previous_dt + timedelta(seconds=1)
    if isinstance(weekday, str):
        weekday = weekdays.index(weekday.lower())
    options = dict(
        month=month, day=day, weekday=weekday, hour=hour, minute=minute, second=second, microsecond=microsecond
    )

    while True:
        next_dt = _get_next_dt(dt, options)
        # print(dt, next_dt)
        if next_dt is None:
            return dt
        dt = next_dt


@dataclass
class CronJob:
    name: str
    coroutine: Callable
    month: Union[None, set, int]
    day: Union[None, set, int]
    weekday: Union[None, set, int, str]
    hour: Union[None, set, int]
    minute: Union[None, set, int]
    second: Union[None, set, int]
    microsecond: int
    run_at_startup: bool
    unique: bool
    timeout_s: Optional[float]
    keep_result_s: Optional[float]
    max_tries: Optional[int]
    next_run: datetime = None

    def set_next(self, dt: datetime):
        self.next_run = next_cron(
            dt,
            month=self.month,
            day=self.day,
            weekday=self.weekday,
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            microsecond=self.microsecond,
        )

    def __repr__(self):
        return '<CronJob {}>'.format(' '.join(f'{k}={v}' for k, v in self.__dict__.items()))


def cron(
    coroutine: Union[str, Callable],
    *,
    name: Optional[str] = None,
    month: Union[None, set, int] = None,
    day: Union[None, set, int] = None,
    weekday: Union[None, set, int, str] = None,
    hour: Union[None, set, int] = None,
    minute: Union[None, set, int] = None,
    second: Union[None, set, int] = 0,
    microsecond: int = 123_456,
    run_at_startup: bool = False,
    unique: bool = True,
    timeout: Optional[SecondsTimedelta] = None,
    keep_result: Optional[float] = 0,
    max_tries: Optional[int] = 1,
) -> CronJob:
    """
    Create a cron job, eg. it should be executed at specific times.

    Workers will enqueue this job at or just after the set times. If ``unique`` is true (the default) the
    job will only be run once even if multiple workers are running.

    :param coroutine: coroutine function to run
    :param name: name of the job, if None, the name of the coroutine is used
    :param month: month(s) to run the job on, 1 - 12
    :param day: day(s) to run the job on, 1 - 31
    :param weekday: week day(s) to run the job on, 0 - 6 or mon - sun
    :param hour: hour(s) to run the job on, 0 - 23
    :param minute: minute(s) to run the job on, 0 - 59
    :param second: second(s) to run the job on, 0 - 59
    :param microsecond: microsecond(s) to run the job on,
        defaults to 123456 as the world is busier at the top of a second, 0 - 1e6
    :param run_at_startup: whether to run as worker starts
    :param unique: whether the job should be only be executed once at each time
    :param timeout: job timeout
    :param keep_result: how long to keep the result for
    :param max_tries: maximum number of tries for the job
    """

    if isinstance(coroutine, str):
        name = name or 'cron:' + coroutine
        coroutine = import_string(coroutine)

    assert asyncio.iscoroutinefunction(coroutine), f'{coroutine} is not a coroutine function'
    timeout = to_seconds(timeout)
    keep_result = to_seconds(keep_result)

    return CronJob(
        name or 'cron:' + coroutine.__qualname__,
        coroutine,
        month,
        day,
        weekday,
        hour,
        minute,
        second,
        microsecond,
        run_at_startup,
        unique,
        timeout,
        keep_result,
        max_tries,
    )

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from time import time
from typing import Optional, Union

logger = logging.getLogger('arq.utils')


epoch = datetime(1970, 1, 1)
epoch_tz = epoch.replace(tzinfo=timezone.utc)
SecondsTimedelta = Union[int, float, timedelta]


def as_int(f: float) -> int:
    return int(round(f))


def timestamp_ms() -> int:
    return as_int(time() * 1000)


def to_unix_ms(dt: datetime) -> int:
    """
    convert a datetime to number of milliseconds since 1970 and calculate timezone offset
    """
    utcoffset = dt.utcoffset()
    ep = epoch if utcoffset is None else epoch_tz
    return as_int((dt - ep).total_seconds() * 1000)


def ms_to_datetime(unix_ms: int) -> datetime:
    return epoch + timedelta(seconds=unix_ms / 1000)


def to_ms(td: Optional[SecondsTimedelta]) -> Optional[int]:
    if td is None:
        return td
    elif isinstance(td, timedelta):
        td = td.total_seconds()
    return as_int(td * 1000)


def to_seconds(td: Optional[SecondsTimedelta]) -> Optional[float]:
    if td is None:
        return td
    elif isinstance(td, timedelta):
        return td.total_seconds()
    return td


async def poll(step: float = 0.5):
    loop = asyncio.get_event_loop()
    start = loop.time()
    while True:
        before = loop.time()
        yield before - start
        after = loop.time()
        wait = max([0, step - after + before])
        await asyncio.sleep(wait)


DEFAULT_CURTAIL = 80


def truncate(s: str, length: int = DEFAULT_CURTAIL) -> str:
    """
    Truncate a string and add an ellipsis (three dots) to the end if it was too long

    :param s: string to possibly truncate
    :param length: length to truncate the string to
    """
    if len(s) > length:
        s = s[: length - 1] + '…'
    return s


def args_to_string(args, kwargs):
    arguments = ''
    if args:
        arguments = ', '.join(map(repr, args))
    if kwargs:
        if arguments:
            arguments += ', '
        arguments += ', '.join(f'{k}={v!r}' for k, v in sorted(kwargs.items()))
    return truncate(arguments)

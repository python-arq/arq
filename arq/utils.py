import asyncio
import logging
from datetime import datetime, timedelta, timezone
from time import time
from typing import Union

logger = logging.getLogger('arq.utils')


EPOCH = datetime(1970, 1, 1)
EPOCH_TZ = EPOCH.replace(tzinfo=timezone.utc)


def as_int(f: float) -> int:
    return int(round(f))


def timestamp() -> int:
    return as_int(time() * 1000)


def to_unix_ms(dt: datetime) -> int:
    """
    convert a datetime to number of milliseconds since 1970 and calculate timezone offset
    """
    utcoffset = dt.utcoffset()
    ep = EPOCH if utcoffset is None else EPOCH_TZ
    return as_int((dt - ep).total_seconds() * 1000)


def to_datetime(unix_ms: int) -> datetime:
    return EPOCH + timedelta(seconds=unix_ms / 1000)


async def poll(step: Union[int, float] = 1):
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
        s = s[:length - 1] + '…'
    return s

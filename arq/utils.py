import asyncio
import logging
from datetime import datetime, timedelta, timezone
from time import time
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, Optional, Sequence, overload

logger = logging.getLogger('arq.utils')

if TYPE_CHECKING:
    from .typing import SecondsTimedelta


def as_int(f: float) -> int:
    return int(round(f))


def timestamp_ms() -> int:
    return as_int(time() * 1000)


def to_unix_ms(dt: datetime) -> int:
    """
    convert a datetime to epoch with milliseconds as int
    """
    return as_int(dt.timestamp() * 1000)


def ms_to_datetime(unix_ms: int) -> datetime:
    return datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc)


@overload
def to_ms(td: None) -> None:
    pass


@overload
def to_ms(td: 'SecondsTimedelta') -> int:
    pass


def to_ms(td: Optional['SecondsTimedelta']) -> Optional[int]:
    if td is None:
        return td
    elif isinstance(td, timedelta):
        td = td.total_seconds()
    return as_int(td * 1000)


@overload
def to_seconds(td: None) -> None:
    pass


@overload
def to_seconds(td: 'SecondsTimedelta') -> float:
    pass


def to_seconds(td: Optional['SecondsTimedelta']) -> Optional[float]:
    if td is None:
        return td
    elif isinstance(td, timedelta):
        return td.total_seconds()
    return td


async def poll(step: float = 0.5) -> AsyncGenerator[float, None]:
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


def args_to_string(args: Sequence[Any], kwargs: Dict[str, Any]) -> str:
    arguments = ''
    if args:
        arguments = ', '.join(map(repr, args))
    if kwargs:
        if arguments:
            arguments += ', '
        arguments += ', '.join(f'{k}={v!r}' for k, v in sorted(kwargs.items()))
    return truncate(arguments)

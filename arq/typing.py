from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Protocol, Sequence, Set, Type, Union

__all__ = (
    'OptionType',
    'WeekdayOptionType',
    'WEEKDAYS',
    'SecondsTimedelta',
    'WorkerCoroutine',
    'StartupShutdown',
    'WorkerSettingsType',
)


if TYPE_CHECKING:
    from .cron import CronJob
    from .worker import Function

OptionType = Union[None, Set[int], int]
WEEKDAYS = 'mon', 'tues', 'wed', 'thurs', 'fri', 'sat', 'sun'
WeekdayOptionType = Union[OptionType, Literal['mon', 'tues', 'wed', 'thurs', 'fri', 'sat', 'sun']]
SecondsTimedelta = Union[int, float, timedelta]


class WorkerCoroutine(Protocol):
    __qualname__: str

    async def __call__(self, ctx: Dict[Any, Any], *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        pass


class StartupShutdown(Protocol):
    __qualname__: str

    async def __call__(self, ctx: Dict[Any, Any]) -> Any:  # pragma: no cover
        pass


class WorkerSettingsBase(Protocol):
    functions: Sequence[Union[WorkerCoroutine, 'Function']]
    cron_jobs: Optional[Sequence['CronJob']] = None
    on_startup: Optional[StartupShutdown] = None
    on_shutdown: Optional[StartupShutdown] = None
    # and many more...


WorkerSettingsType = Union[Dict[str, Any], Type[WorkerSettingsBase]]

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, Optional, Protocol, Sequence, Set, Type, Union

__all__ = (
    'OptionType',
    'WeekdayOptionType',
    'SecondsTimedelta',
    'WorkerCoroutine',
    'StartupShutdown',
    'WorkerSettingsType',
)


if TYPE_CHECKING:
    from .worker import Function  # noqa F401
    from .cron import CronJob  # noqa F401

OptionType = Union[None, Set[int], int]
WeekdayOptionType = Union[OptionType, str]
SecondsTimedelta = Union[int, float, timedelta]


class WorkerCoroutine(Protocol):
    __qualname__: str

    async def __call__(self, ctx: Dict[Any, Any], *args: Any, **kwargs: Any) -> Any:
        pass


class StartupShutdown(Protocol):
    __qualname__: str

    async def __call__(self, ctx: Dict[Any, Any]) -> Any:
        pass


class WorkerSettingsBase(Protocol):
    functions: Sequence[Union[WorkerCoroutine, 'Function']]
    cron_jobs: Optional[Sequence['CronJob']] = None
    on_startup: Optional[StartupShutdown] = None
    on_shutdown: Optional[StartupShutdown] = None
    # and many more...


WorkerSettingsType = Union[Dict[str, Any], Type[WorkerSettingsBase]]

"""Code to add trace information producing jobs."""

from datetime import datetime, timedelta
from time import time_ns
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Mapping,
    Optional,
    ParamSpec,
    Union,
    cast,
)
from uuid import uuid4

from opentelemetry import context, metrics, trace
from opentelemetry.propagate import inject
from redis.asyncio.connection import ConnectKwargs

from arq import ArqRedis
from arq.jobs import Job
from arq.opentelemetry.shared import shared_messaging_attributes

P = ParamSpec("P")
EnqueueJobType = Callable[P, Awaitable[Optional[Job]]]

meter = metrics.get_meter(__name__)


def span_name_producer(*enqueue_args: Any, **enqueue_kwargs: Mapping[str, Any]) -> str:
    """Get the span name to use when enqueuing a job. Name is based on conventions in
    https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#conventions
    and is derived from the name of the job that was passed as an argument to enqueue_job().
    """
    job_name = enqueue_kwargs.get("function", enqueue_args[0])
    return f"{job_name[0]} send"


def get_producer_attributes(*args: Any, **kwargs: Mapping[str, Any]) -> Dict[str, Any]:
    """Get attributes that apply when enqueuing a job."""
    attributes = {}
    attributes["span.name"] = span_name_producer(args, kwargs)
    attributes["messaging.operation"] = "publish"
    if "_queue_name" in kwargs:
        attributes["messaging.destination.name"] = kwargs["_queue_name"]

    return attributes


def get_message_attributes(*args: Any, **kwargs: Mapping[str, Any]) -> Dict[str, Any]:
    """Get attributes specific to a message when enqueuing a job."""
    attributes = {}
    if "_job_id" in kwargs:
        attributes["messaging.message.id"] = kwargs["_job_id"]

    return attributes


TRACE_CONTEXT_PARAM_NAME = "_trace_context"

job_counter = meter.create_counter(
    name="arq.jobs.counter",
    unit="1",
    description="Counts the number of jobs created with Arq.",
)


def get_wrap_enqueue_job(redis_host: str, redis_port: int) -> EnqueueJobType:
    shared_attributes = shared_messaging_attributes(redis_host, redis_port)

    async def wrap_enqueue_job(
        enqueue_job_func: EnqueueJobType,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Optional[Job]:
        """Add an extra parameter into the job we're enqueueing, which holds
        trace context information."""
        token = context.attach(context.get_current())

        attributes = get_producer_attributes(enqueue_job_func, list(args), kwargs)
        attributes.update(shared_attributes)
        tracer = trace.get_tracer(__name__)
        span = tracer.start_span(
            span_name_producer(args, kwargs),
            kind=trace.SpanKind.PRODUCER,
            start_time=time_ns(),
        )
        if span.is_recording():
            message_attributes = get_message_attributes(
                enqueue_job_func, list(args), kwargs
            )
            message_attributes.update(attributes)
            span.set_attributes(message_attributes)

        job_counter.add(1, attributes=attributes)

        # Inject our context into the job definition.
        kwargs.setdefault(TRACE_CONTEXT_PARAM_NAME, {})
        inject(carrier=kwargs[TRACE_CONTEXT_PARAM_NAME])

        with trace.use_span(span, end_on_exit=True):
            result = await enqueue_job_func(*args, **kwargs)
            # If we were given an arq.jobs.Job instance as a result, put its job ID into our
            # span attributes.
            if result is not None and isinstance(result, Job):
                span.set_attribute("messaging.message.id", result.job_id)

        context.detach(token)
        return result

    return wrap_enqueue_job


class InstrumentedArqRedis(ArqRedis):
    """InstrumentedArqRedis is an ArqRedis instance that adds tracing
    information to the jobs it enqueues.
    """

    wrapper: Callable[..., Awaitable[Optional[Job]]]

    def __init__(
        self,
        arq_redis: ArqRedis,
    ) -> None:
        connection_kwargs = cast(
            ConnectKwargs, arq_redis.connection_pool.connection_kwargs
        )
        self.wrapper = get_wrap_enqueue_job(
            connection_kwargs.get("host", "localhost"),
            connection_kwargs.get("port", 6379),
        )
        super().__init__(
            connection_pool=arq_redis.connection_pool,
            job_serializer=arq_redis.job_serializer,
            job_deserializer=arq_redis.job_deserializer,
            default_queue_name=arq_redis.default_queue_name,
            expires_extra_ms=arq_redis.expires_extra_ms,
        )

    async def enqueue_job(
        self,
        function: str,
        *args: Any,
        _job_id: Optional[str] = None,
        _queue_name: Optional[str] = None,
        _defer_until: Optional[datetime] = None,
        _defer_by: Union[None, int, float, timedelta] = None,
        _expires: Union[None, int, float, timedelta] = None,
        _job_try: Optional[int] = None,
        **kwargs: Any,
    ) -> Optional[Job]:
        # Allow _queue_name to be included in trace.
        if _queue_name is None:
            _queue_name = self.default_queue_name
        _job_id = _job_id or uuid4().hex

        return await self.wrapper(
            super().enqueue_job,
            function,
            *args,
            _job_id=_job_id,
            _queue_name=_queue_name,
            _defer_until=_defer_until,
            _defer_by=_defer_by,
            _expires=_expires,
            _job_try=_job_try,
            **kwargs,
        )

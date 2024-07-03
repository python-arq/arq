"""Module that instruments tracing on enqueue_job() and the various
job coroutines."""

from functools import wraps
from time import time_ns
from typing import Any, Awaitable, Callable, Dict, List, Optional, ParamSpec, TypeVar

from opentelemetry import context, metrics, trace
from opentelemetry.propagate import extract, inject

from arq.jobs import Job
from arq.opentelemetry.shared import shared_messaging_attributes
from arq.worker import Retry

meter = metrics.get_meter(__name__)
job_counter = meter.create_counter(
    name="arq.jobs.counter",
    unit="1",
    description="Counts the number of jobs created with Arq.",
)


def span_name_consumer(func: Callable) -> str:
    """Get the span name to use when running a job. Name is based on conventions in
    https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#conventions

    There's a difference between 'receive' and 'process', which can be elided when they happen
    close together, but is relevant for batch processing. But for now, we just use "process".
    """
    return f"{func.__qualname__} process"


def get_consumer_attributes(
    _func: Callable[..., Awaitable[Any]], _args: List[Any], kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    """Get attributes that apply when running a job."""
    attributes = {}
    attributes["span.name"] = span_name_consumer(_func)
    attributes["messaging.operation"] = "process"
    if "_queue_name" in kwargs:
        attributes["messaging.source.name"] = kwargs["_queue_name"]

    return attributes


TRACE_CONTEXT_PARAM_NAME = "_trace_context"
JobArgs = ParamSpec("JobArgs")
JobReturnType = TypeVar("JobReturnType")


def instrument_job(
    wrapped: Callable[JobArgs, Awaitable[JobReturnType]]
) -> Callable[JobArgs, Awaitable[JobReturnType]]:
    """Decorate a job definition such that it can be traced."""
    shared_attributes = shared_messaging_attributes(redis_host, redis_port)

    @wraps(wrapped)
    async def wrapper(*args: JobArgs.args, **kwargs: JobArgs.kwargs) -> Any:
        if TRACE_CONTEXT_PARAM_NAME in kwargs:
            # The .extract() method gets us the trace data...
            token = context.attach(
                extract(
                    # IMPORTANT! Manually remove TRACE_CONTEXT_PARAM_NAME
                    # to prevent it being passed as an argument to the job.
                    carrier=kwargs.pop(TRACE_CONTEXT_PARAM_NAME, {}),
                )
            )
        else:
            # We're running a job coroutine but not as a job - perhaps it was awaited
            # by something else. So we have no trace data in its kwargs.
            #
            # In that case, we'll stick with the current context.
            token = None

        tracer = trace.get_tracer(__name__)
        span = tracer.start_span(
            span_name_consumer(wrapped),
            kind=trace.SpanKind.CONSUMER,
            start_time=time_ns(),
        )
        if span.is_recording():
            attributes = get_consumer_attributes(wrapped, list(args), kwargs)
            for key, value in attributes.items():
                span.set_attribute(key, value)

        with trace.use_span(
            span, end_on_exit=True, set_status_on_exception=False
        ) as span:
            try:
                result = await wrapped(*args, **kwargs)
            except Retry as exc:
                span.set_status(trace.Status(trace.StatusCode.OK))
                raise exc
            except Exception as exc:
                span.set_status(
                    trace.Status(
                        trace.StatusCode.ERROR,
                        description=f"{type(exc).__name__}: {exc}",
                    )
                )
                raise exc

        if token is not None:
            context.detach(token)
        return result

    return wrapper

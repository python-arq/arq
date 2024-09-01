"""A TextMapPropagator implementation for tracing between Arq jobs."""

from typing import Dict, List, Optional, Set

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import (
    CarrierT,
    Getter,
    Setter,
    TextMapPropagator,
    default_getter,
    default_setter,
)
from opentelemetry.trace import INVALID_SPAN_ID, INVALID_TRACE_ID, TraceFlags

JobCarrierType = Dict[str, str]
ARQ_TRACE_ID_HEADER = "arq-tracer-trace-id"
ARQ_SPAN_ID_HEADER = "arq-tracer-span-id"
ARQ_TRACE_FLAGS_HEADER = "arq-tracer-trace-flags"


class ArqJobTextMapPropagator(TextMapPropagator):
    """Used to convert between the SpanContext objects that OpenTelemetry uses, and the format in
    which it is passed in job parameters (in an extra _trace_context parameter, as a dictionary).

    The actual writing to / reading from job parameters is done by other code.

    The span context parameters are:
    * trace_id (an integer)
    * span_id (an integer)
    * trace_flags (an integer that represents a bitmask) - has a default of 0

    It should meet the OpenTelemetry Propagators API specification.
    https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/context/api-propagators.md
    """

    def extract(
        self,
        carrier: CarrierT,
        context: Optional[Context] = None,
        getter: Getter[CarrierT] = default_getter,
    ) -> Context:
        """Extracts SpanContext from the carrier.

        For this TextMapPropagator, the carrier is the parameters of an arq job."""
        if context is None:
            context = Context()

        trace_id: int = _extract_identifier(
            getter.get(carrier, ARQ_TRACE_ID_HEADER), INVALID_TRACE_ID
        )
        span_id: int = _extract_identifier(
            getter.get(carrier, ARQ_SPAN_ID_HEADER), INVALID_SPAN_ID
        )
        trace_flags_str: Optional[str] = _extract_first_element(
            getter.get(carrier, ARQ_TRACE_FLAGS_HEADER)
        )

        if trace_id == INVALID_TRACE_ID or span_id == INVALID_SPAN_ID:
            return context

        if trace_flags_str is None:
            trace_flags = TraceFlags.DEFAULT
        else:
            trace_flags = int(trace_flags_str)

        span_context = trace.SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=True,
            trace_flags=trace.TraceFlags(trace_flags),
        )
        return trace.set_span_in_context(trace.NonRecordingSpan(span_context), context)

    def inject(
        self,
        carrier: CarrierT,
        context: Optional[Context] = None,
        setter: Setter[CarrierT] = default_setter,
    ) -> None:
        """Injects SpanContext into the carrier.

        For this TextMapPropagator, the carrier is the parameters of an arq job."""
        span = trace.get_current_span(context)
        span_context = span.get_span_context()
        if span_context == trace.INVALID_SPAN_CONTEXT:
            return

        setter.set(carrier, ARQ_TRACE_ID_HEADER, hex(span_context.trace_id))
        setter.set(carrier, ARQ_SPAN_ID_HEADER, hex(span_context.span_id))
        setter.set(carrier, ARQ_TRACE_FLAGS_HEADER, str(span_context.trace_flags))

    @property
    def fields(self) -> Set[str]:
        return {
            ARQ_TRACE_ID_HEADER,
            ARQ_SPAN_ID_HEADER,
            ARQ_TRACE_FLAGS_HEADER,
        }


def _extract_first_element(
    items: Optional[List[str]], default: Optional[str] = None
) -> Optional[str]:
    if items is None:
        return default
    return next(iter(items), None)


def _extract_identifier(items: Optional[List[str]], default: int) -> int:
    header = _extract_first_element(items)
    if header is None:
        return default

    try:
        return int(header, 16)
    except ValueError:
        return default

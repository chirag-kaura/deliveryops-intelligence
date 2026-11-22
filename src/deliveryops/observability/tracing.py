"""OpenTelemetry tracing setup with automatic structlog trace_id correlation."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from deliveryops.observability.logging import bind_context

_ACTIVE_EXPORTER: InMemorySpanExporter | None = None


def configure_tracing(
    service_name: str = "deliveryops-intelligence",
    capture_in_memory: bool = True,
) -> InMemorySpanExporter | None:
    """Configure OpenTelemetry TracerProvider with an optional in-memory span exporter."""
    global _ACTIVE_EXPORTER
    existing = trace.get_tracer_provider()
    if isinstance(existing, TracerProvider):
        if capture_in_memory:
            if _ACTIVE_EXPORTER is None:
                _ACTIVE_EXPORTER = InMemorySpanExporter()
                existing.add_span_processor(SimpleSpanProcessor(_ACTIVE_EXPORTER))
            else:
                _ACTIVE_EXPORTER.clear()
            return _ACTIVE_EXPORTER
        return None

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    exporter: InMemorySpanExporter | None = None
    if capture_in_memory:
        exporter = InMemorySpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        _ACTIVE_EXPORTER = exporter

    trace.set_tracer_provider(provider)
    return exporter


def get_in_memory_exporter() -> InMemorySpanExporter | None:
    """Return the active in-memory span exporter if configured."""
    return _ACTIVE_EXPORTER


def get_tracer(name: str = "deliveryops") -> trace.Tracer:
    """Return an OpenTelemetry tracer instance."""
    return trace.get_tracer(name)


@contextmanager
def traced_span(span_name: str, **attributes: Any) -> Iterator[trace.Span]:
    """Start an OpenTelemetry span and bind trace_id/span_id into structlog contextvars."""
    tracer = get_tracer()
    with tracer.start_as_current_span(span_name) as span:
        for key, val in attributes.items():
            if val is not None:
                span.set_attribute(key, val)
        ctx = span.get_span_context()
        if ctx and ctx.is_valid:
            bind_context(
                trace_id=f"{ctx.trace_id:032x}",
                span_id=f"{ctx.span_id:016x}",
            )
        yield span

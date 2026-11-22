"""OpenTelemetry tracing setup with structlog correlation and JSON trace export."""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
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


def serialize_recorded_spans() -> list[dict[str, Any]]:
    """Return all recorded spans from the active in-memory exporter as dictionaries."""
    if _ACTIVE_EXPORTER is None:
        return []
    records: list[dict[str, Any]] = []
    for span in _ACTIVE_EXPORTER.get_finished_spans():
        ctx = span.get_span_context()
        duration_ms = (
            round((span.end_time - span.start_time) / 1_000_000.0, 2)
            if span.end_time is not None and span.start_time is not None
            else 0.0
        )
        records.append(
            {
                "name": span.name,
                "trace_id": f"{ctx.trace_id:032x}" if ctx else "",
                "span_id": f"{ctx.span_id:016x}" if ctx else "",
                "duration_ms": duration_ms,
                "attributes": dict(span.attributes) if span.attributes else {},
            }
        )
    return records


def export_spans_to_file(output_path: Path | str) -> Path:
    """Persist recorded OpenTelemetry spans as an OTLP-style JSON trace bundle."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "span_count": len(serialize_recorded_spans()),
        "spans": serialize_recorded_spans(),
    }
    target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return target

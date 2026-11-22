"""Unit tests for OpenTelemetry span instrumentation and context binding."""

from deliveryops.observability import configure_tracing, traced_span


def test_traced_span_records_attributes_in_memory() -> None:
    exporter = configure_tracing(
        service_name="deliveryops-test",
        capture_in_memory=True,
    )
    assert exporter is not None

    with traced_span("test.operation", batch_id="b900", count=15):
        pass

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "test.operation"
    assert spans[0].attributes is not None
    assert spans[0].attributes["batch_id"] == "b900"
    assert spans[0].attributes["count"] == 15

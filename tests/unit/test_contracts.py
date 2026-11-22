"""Unit tests for DeliveryOps Pydantic v2 data contracts and DLQ gate."""

from datetime import UTC, datetime

from deliveryops.contracts import EventType, validate_event_batch, validate_event_payload
from deliveryops.ingestion import DeliveryEventGenerator


def test_valid_payload_passes_contract() -> None:
    gen = DeliveryEventGenerator(seed=10)
    payload = gen.generate_valid_payload(base_time=datetime(2026, 11, 22, 10, 0, tzinfo=UTC))
    event, dlq = validate_event_payload(payload)
    assert event is not None
    assert dlq is None
    assert event.order_id.startswith("ORD-")


def test_missing_courier_on_pickup_routes_to_dlq() -> None:
    gen = DeliveryEventGenerator(seed=11)
    payload = gen.generate_valid_payload(base_time=datetime(2026, 11, 22, 10, 0, tzinfo=UTC))
    payload["event_type"] = EventType.PICKUP_COMPLETED.value
    payload["courier_id"] = None

    event, dlq = validate_event_payload(payload)
    assert event is None
    assert dlq is not None
    assert "courier_id is required" in dlq.error_summary


def test_missing_actual_duration_on_completed_routes_to_dlq() -> None:
    gen = DeliveryEventGenerator(seed=12)
    payload = gen.generate_valid_payload(base_time=datetime(2026, 11, 22, 10, 0, tzinfo=UTC))
    payload["event_type"] = EventType.DELIVERY_COMPLETED.value
    payload["courier_id"] = "CR-101"
    payload["actual_duration_minutes"] = None

    event, dlq = validate_event_payload(payload)
    assert event is None
    assert dlq is not None
    assert "actual_duration_minutes is required" in dlq.error_summary


def test_naive_timestamp_routes_to_dlq() -> None:
    gen = DeliveryEventGenerator(seed=13)
    payload = gen.generate_valid_payload()
    payload["event_timestamp"] = "2026-11-22T10:00:00"

    event, dlq = validate_event_payload(payload)
    assert event is None
    assert dlq is not None
    assert "timezone-aware" in dlq.error_summary


def test_batch_validation_splits_valid_and_dlq() -> None:
    gen = DeliveryEventGenerator(seed=99)
    batch = gen.generate_batch(batch_size=20, corrupt_ratio=0.25)
    result = validate_event_batch(batch)
    assert result.total_processed == 20
    assert len(result.valid_events) == 15
    assert len(result.dead_letter_records) == 5

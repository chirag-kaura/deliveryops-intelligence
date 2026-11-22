"""Integration test verifying synthetic generation -> contract gate -> local Bronze lake."""

from datetime import UTC, datetime
from pathlib import Path

from deliveryops.contracts import validate_event_batch
from deliveryops.ingestion import DeliveryEventGenerator
from deliveryops.observability import bind_context, clear_context, configure_logging, get_logger
from deliveryops.storage import LocalLakeStorage


def test_end_to_end_generation_validation_and_lake_persistence(tmp_path: Path) -> None:
    configure_logging(log_level="INFO", json_output=True)
    bind_context(pipeline_run_id="run-test-001")
    logger = get_logger("test_pipeline")

    base_time = datetime(2026, 11, 22, 12, 0, tzinfo=UTC)
    generator = DeliveryEventGenerator(seed=2026)
    raw_batch = generator.generate_batch(
        batch_size=30,
        corrupt_ratio=0.20,
        base_time=base_time,
    )

    validation_result = validate_event_batch(raw_batch)
    logger.info(
        "batch_validated",
        valid_count=len(validation_result.valid_events),
        dlq_count=len(validation_result.dead_letter_records),
    )

    lake = LocalLakeStorage(base_dir=tmp_path)
    events_path = lake.write_bronze_events(
        validation_result.valid_events,
        batch_id="b001",
        partition_date="2026-11-22",
    )
    dlq_path = lake.write_dlq_records(
        validation_result.dead_letter_records,
        batch_id="b001",
        partition_date="2026-11-22",
    )

    assert events_path is not None and events_path.exists()
    assert dlq_path is not None and dlq_path.exists()
    assert "dt=2026-11-22" in str(events_path)
    assert "dt=2026-11-22" in str(dlq_path)

    stored_valid = lake.read_jsonl_dataset("bronze", "events")
    stored_dlq = lake.read_jsonl_dataset("bronze", "dlq")

    assert len(stored_valid) == 24
    assert len(stored_dlq) == 6
    assert len(stored_valid) + len(stored_dlq) == 30
    clear_context()

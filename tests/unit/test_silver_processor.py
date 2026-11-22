"""Unit tests for Bronze-to-Silver Polars processor and Pandera tabular contract."""

from datetime import UTC, datetime
from pathlib import Path

import pandera.errors
import pytest

from deliveryops.contracts import validate_event_batch, validate_silver_dataframe
from deliveryops.ingestion import DeliveryEventGenerator
from deliveryops.processing import BronzeToSilverProcessor
from deliveryops.storage import LocalLakeStorage


def test_silver_deduplicates_and_enriches_sla_fields(tmp_path: Path) -> None:
    gen = DeliveryEventGenerator(seed=55)
    base_time = datetime(2026, 11, 22, 10, 0, tzinfo=UTC)
    raw = gen.generate_batch(batch_size=10, corrupt_ratio=0.0, base_time=base_time)
    valid_dicts = [e.model_dump(mode="json") for e in validate_event_batch(raw).valid_events]

    # Duplicate the first record with a later timestamp and higher ETA to test deduplication
    dup = dict(valid_dicts[0])
    dup["event_timestamp"] = "2026-11-22T11:59:59+00:00"
    dup["estimated_eta_minutes"] = 50.0
    dup["actual_duration_minutes"] = 50.0
    valid_dicts.append(dup)

    processor = BronzeToSilverProcessor(lake=LocalLakeStorage(tmp_path))
    silver_df = processor.transform_records(valid_dicts)

    assert silver_df.height == 10
    row0 = silver_df.filter(silver_df["event_id"] == dup["event_id"]).to_dicts()[0]
    assert row0["estimated_eta_minutes"] == 50.0
    assert row0["is_sla_breached"] is True
    assert row0["sla_variance_minutes"] == 15.0


def test_pandera_rejects_invalid_latitude_in_silver(tmp_path: Path) -> None:
    gen = DeliveryEventGenerator(seed=56)
    base_time = datetime(2026, 11, 22, 10, 0, tzinfo=UTC)
    raw = gen.generate_batch(batch_size=2, corrupt_ratio=0.0, base_time=base_time)
    valid_dicts = [e.model_dump(mode="json") for e in validate_event_batch(raw).valid_events]

    processor = BronzeToSilverProcessor(lake=LocalLakeStorage(tmp_path))
    silver_df = processor.transform_records(valid_dicts)
    corrupted_df = silver_df.with_columns(latitude=silver_df["latitude"] + 200.0)

    with pytest.raises(pandera.errors.SchemaError):
        validate_silver_dataframe(corrupted_df)

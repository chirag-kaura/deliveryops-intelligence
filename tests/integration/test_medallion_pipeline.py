"""End-to-end integration test for the Bronze -> Silver -> Gold Medallion pipeline."""

from datetime import UTC, datetime
from pathlib import Path

import duckdb

from deliveryops.pipelines import MedallionPipelineRunner


def test_medallion_pipeline_runner_end_to_end(tmp_path: Path) -> None:
    runner = MedallionPipelineRunner(lake_dir=tmp_path)
    metrics = runner.run_batch(
        batch_size=40,
        corrupt_ratio=0.15,
        seed=101,
        batch_id="b101",
        base_time=datetime(2026, 11, 22, 14, 0, tzinfo=UTC),
    )

    assert metrics.total_generated == 40
    assert metrics.bronze_valid_count == 34
    assert metrics.bronze_dlq_count == 6
    assert metrics.silver_row_count == 34
    assert metrics.gold_zone_rows > 0
    assert metrics.gold_courier_rows > 0

    gold_zone_files = [str(p) for p in runner.lake.list_parquet_files("gold", "zone_kpi_daily")]
    assert len(gold_zone_files) == 1

    conn = duckdb.connect(":memory:")
    try:
        row = conn.execute(
            "SELECT SUM(total_events) FROM read_parquet($1);",
            [gold_zone_files],
        ).fetchone()
        assert row is not None
        assert int(row[0]) == 34
    finally:
        conn.close()

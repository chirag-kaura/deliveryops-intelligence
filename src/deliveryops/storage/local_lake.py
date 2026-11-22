"""Local filesystem Medallion data lake adapter with Hive date partitioning."""

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from deliveryops.contracts.events import DeadLetterRecord, DeliveryEvent


class LocalLakeStorage:
    """Manages partitioned JSONL and Parquet storage across Bronze, Silver, and Gold layers."""

    def __init__(self, base_dir: Path | str) -> None:
        self.base_dir = Path(base_dir)

    def _partition_dir(self, layer: str, dataset: str, partition_date: str) -> Path:
        target = self.base_dir / layer / dataset / f"dt={partition_date}"
        target.mkdir(parents=True, exist_ok=True)
        return target

    def write_bronze_events(
        self,
        events: Sequence[DeliveryEvent],
        batch_id: str,
        partition_date: str | None = None,
    ) -> Path | None:
        """Write validated DeliveryEvent objects to Bronze JSONL partition."""
        if not events:
            return None
        dt_str = partition_date or events[0].event_timestamp.astimezone(UTC).strftime("%Y-%m-%d")
        part_dir = self._partition_dir("bronze", "events", dt_str)
        file_path = part_dir / f"events_{batch_id}.jsonl"
        lines = [event.model_dump_json() for event in events]
        file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return file_path

    def write_dlq_records(
        self,
        records: Sequence[DeadLetterRecord],
        batch_id: str,
        partition_date: str | None = None,
    ) -> Path | None:
        """Write DeadLetterRecord objects to Bronze DLQ JSONL partition."""
        if not records:
            return None
        dt_str = partition_date or datetime.now(UTC).strftime("%Y-%m-%d")
        part_dir = self._partition_dir("bronze", "dlq", dt_str)
        file_path = part_dir / f"dlq_{batch_id}.jsonl"
        lines = [record.model_dump_json() for record in records]
        file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return file_path

    def read_jsonl_dataset(self, layer: str, dataset: str) -> list[dict[str, Any]]:
        """Read all JSONL records across all date partitions for a given layer and dataset."""
        dataset_root = self.base_dir / layer / dataset
        if not dataset_root.exists():
            return []
        records: list[dict[str, Any]] = []
        for file_path in sorted(dataset_root.rglob("*.jsonl")):
            for line in file_path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    records.append(json.loads(line))
        return records

    def write_parquet_dataset(
        self,
        df: pl.DataFrame,
        layer: str,
        dataset: str,
        partition_date: str,
        filename: str = "part-0000.parquet",
    ) -> Path:
        """Write a Polars DataFrame to a Hive date-partitioned Parquet file."""
        part_dir = self._partition_dir(layer, dataset, partition_date)
        file_path = part_dir / filename
        df.write_parquet(file_path)
        return file_path

    def list_parquet_files(self, layer: str, dataset: str) -> list[Path]:
        """Return all Parquet files under a layer/dataset directory."""
        dataset_root = self.base_dir / layer / dataset
        if not dataset_root.exists():
            return []
        return sorted(dataset_root.rglob("*.parquet"))

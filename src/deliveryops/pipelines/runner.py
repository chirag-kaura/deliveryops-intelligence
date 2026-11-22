"""Lightweight custom Python pipeline runner orchestrating Bronze -> Silver -> Gold."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from deliveryops.config import AppSettings, get_settings
from deliveryops.contracts import validate_event_batch
from deliveryops.ingestion import DeliveryEventGenerator
from deliveryops.observability import bind_context, clear_context, get_logger
from deliveryops.processing import BronzeToSilverProcessor, SilverToGoldProcessor
from deliveryops.storage import LocalLakeStorage


class PipelineRunMetrics(BaseModel):
    """Execution summary metrics for a single Medallion pipeline run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str
    batch_id: str
    total_generated: int
    bronze_valid_count: int
    bronze_dlq_count: int
    silver_row_count: int
    gold_zone_rows: int
    gold_courier_rows: int


class MedallionPipelineRunner:
    """End-to-end local Medallion runner (Ingestion -> Bronze -> Silver -> Gold)."""

    def __init__(
        self,
        lake_dir: Path | str | None = None,
        settings: AppSettings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        target_dir = Path(lake_dir) if lake_dir is not None else self.settings.local_lake_dir
        self.lake = LocalLakeStorage(base_dir=target_dir)
        self.silver_processor = BronzeToSilverProcessor(lake=self.lake)
        self.gold_processor = SilverToGoldProcessor(
            lake=self.lake,
            memory_limit=self.settings.duckdb_memory_limit,
            threads=self.settings.duckdb_threads,
        )
        self.logger = get_logger("medallion_runner")

    def run_batch(
        self,
        batch_size: int = 50,
        corrupt_ratio: float = 0.10,
        seed: int = 42,
        batch_id: str = "b001",
        base_time: datetime | None = None,
    ) -> PipelineRunMetrics:
        """Execute an end-to-end batch run from event generation through Gold KPI tables."""
        run_id = f"run-{uuid4().hex[:8]}"
        bind_context(pipeline_run_id=run_id, batch_id=batch_id)
        try:
            effective_time = base_time or datetime(2026, 11, 22, 12, 0, tzinfo=UTC)
            partition_date = effective_time.astimezone(UTC).strftime("%Y-%m-%d")

            generator = DeliveryEventGenerator(seed=seed)
            raw_batch = generator.generate_batch(
                batch_size=batch_size,
                corrupt_ratio=corrupt_ratio,
                base_time=effective_time,
            )

            validation = validate_event_batch(raw_batch)
            self.lake.write_bronze_events(
                validation.valid_events,
                batch_id=batch_id,
                partition_date=partition_date,
            )
            self.lake.write_dlq_records(
                validation.dead_letter_records,
                batch_id=batch_id,
                partition_date=partition_date,
            )

            silver_df, _ = self.silver_processor.process_all(batch_id=batch_id)
            zone_df, courier_df, _ = self.gold_processor.build_gold_tables(batch_id=batch_id)

            metrics = PipelineRunMetrics(
                run_id=run_id,
                batch_id=batch_id,
                total_generated=len(raw_batch),
                bronze_valid_count=len(validation.valid_events),
                bronze_dlq_count=len(validation.dead_letter_records),
                silver_row_count=silver_df.height,
                gold_zone_rows=zone_df.height,
                gold_courier_rows=courier_df.height,
            )
            self.logger.info("medallion_pipeline_completed", **metrics.model_dump())
            return metrics
        finally:
            clear_context()

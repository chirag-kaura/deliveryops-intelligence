"""Dagster-inspired declarative software-defined asset graph orchestrator."""

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from deliveryops.config import AppSettings, get_settings
from deliveryops.ingestion import (
    DeliveryEventGenerator,
    InMemoryEventBroker,
    StreamIngestionConsumer,
)
from deliveryops.observability import bind_context, clear_context, get_logger, traced_span
from deliveryops.processing import BronzeToSilverProcessor, DbtDuckDBRunner
from deliveryops.storage import GoldWarehouseLoader, LocalLakeStorage


class AssetNode(BaseModel):
    """Metadata definition for a software-defined data asset in the platform DAG."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    key: str
    group: str
    compute_kind: str
    upstream_keys: list[str]
    description: str


class AssetMaterialization(BaseModel):
    """Execution record produced when an asset node is materialized."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    asset_key: str
    row_count: int
    duration_ms: float
    metadata: dict[str, Any]


class OrchestrationRunResult(BaseModel):
    """Complete result of materializing the DeliveryOps Intelligence asset graph."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str
    batch_id: str
    status: str
    materializations: list[AssetMaterialization]
    dbt_tests_passed: dict[str, bool]
    warehouse_tables: dict[str, int]


PLATFORM_ASSETS: tuple[AssetNode, ...] = (
    AssetNode(
        key="stream_raw_topic",
        group="ingestion",
        compute_kind="redpanda_broker",
        upstream_keys=[],
        description="Raw synthetic delivery lifecycle events published to Redpanda/Kafka topic.",
    ),
    AssetNode(
        key="bronze_events_and_dlq",
        group="bronze",
        compute_kind="pydantic_v2",
        upstream_keys=["stream_raw_topic"],
        description="Contract-validated Bronze events JSONL and Dead-Letter Queue partition.",
    ),
    AssetNode(
        key="silver_validated_events",
        group="silver",
        compute_kind="polars_pandera",
        upstream_keys=["bronze_events_and_dlq"],
        description="Deduplicated, SLA-enriched Silver Parquet validated by Pandera schema.",
    ),
    AssetNode(
        key="gold_dbt_marts",
        group="gold",
        compute_kind="dbt_duckdb",
        upstream_keys=["silver_validated_events"],
        description="Gold Zone Daily KPIs and Courier Performance marts built via dbt SQL models.",
    ),
    AssetNode(
        key="warehouse_serving_tables",
        group="serving",
        compute_kind="duckdb_postgres_warehouse",
        upstream_keys=["gold_dbt_marts", "bronze_events_and_dlq"],
        description="Relational SQL warehouse tables serving FastAPI and Streamlit.",
    ),
)


class AssetGraphOrchestrator:
    """Executes the software-defined asset DAG in topological order with OTel tracing."""

    def __init__(
        self,
        lake_dir: Path | str | None = None,
        settings: AppSettings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        target_dir = Path(lake_dir) if lake_dir is not None else self.settings.local_lake_dir
        self.lake = LocalLakeStorage(base_dir=target_dir)
        self.broker = InMemoryEventBroker()
        self.consumer = StreamIngestionConsumer(
            broker=self.broker,
            lake=self.lake,
            raw_topic=self.settings.redpanda_topic_events,
            dlq_topic=self.settings.redpanda_topic_dlq,
        )
        self.silver_processor = BronzeToSilverProcessor(lake=self.lake)
        self.dbt_runner = DbtDuckDBRunner(
            lake=self.lake,
            memory_limit=self.settings.duckdb_memory_limit,
            threads=self.settings.duckdb_threads,
        )
        self.warehouse = GoldWarehouseLoader(
            lake=self.lake,
            memory_limit=self.settings.duckdb_memory_limit,
            threads=self.settings.duckdb_threads,
        )
        self.logger = get_logger("asset_orchestrator")

    @staticmethod
    def get_asset_lineage() -> list[dict[str, Any]]:
        """Return the platform software-defined asset graph definition."""
        return [asset.model_dump() for asset in PLATFORM_ASSETS]

    def materialize_all(
        self,
        batch_size: int = 50,
        corrupt_ratio: float = 0.10,
        seed: int = 42,
        batch_id: str = "dag-b001",
        base_time: datetime | None = None,
    ) -> OrchestrationRunResult:
        """Materialize all 5 platform assets in topological dependency order."""
        run_id = f"dagrun-{uuid4().hex[:8]}"
        bind_context(pipeline_run_id=run_id, batch_id=batch_id)
        materializations: list[AssetMaterialization] = []

        try:
            with traced_span("orchestrator.materialize_all", run_id=run_id, batch_id=batch_id):
                effective_time = base_time or datetime(2026, 11, 22, 12, 0, tzinfo=UTC)
                partition_date = effective_time.astimezone(UTC).strftime("%Y-%m-%d")

                # Asset 1: stream_raw_topic
                t0 = time.perf_counter()
                with traced_span("asset.materialize.stream_raw_topic"):
                    generator = DeliveryEventGenerator(seed=seed)
                    raw_batch = generator.generate_batch(
                        batch_size=batch_size,
                        corrupt_ratio=corrupt_ratio,
                        base_time=effective_time,
                    )
                    published = self.broker.publish_batch(
                        topic=self.settings.redpanda_topic_events,
                        payloads=raw_batch,
                    )
                materializations.append(
                    AssetMaterialization(
                        asset_key="stream_raw_topic",
                        row_count=published,
                        duration_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                        metadata={"topic": self.settings.redpanda_topic_events},
                    )
                )

                # Asset 2: bronze_events_and_dlq
                t1 = time.perf_counter()
                with traced_span("asset.materialize.bronze_events_and_dlq"):
                    valid_events, dlq_records = self.consumer.consume_and_sink(
                        batch_id=batch_id,
                        partition_date=partition_date,
                    )
                materializations.append(
                    AssetMaterialization(
                        asset_key="bronze_events_and_dlq",
                        row_count=len(valid_events),
                        duration_ms=round((time.perf_counter() - t1) * 1000.0, 2),
                        metadata={
                            "valid_events": len(valid_events),
                            "dlq_records": len(dlq_records),
                            "dlq_topic_size": self.broker.topic_size(
                                self.settings.redpanda_topic_dlq
                            ),
                        },
                    )
                )

                # Asset 3: silver_validated_events
                t2 = time.perf_counter()
                with traced_span("asset.materialize.silver_validated_events"):
                    silver_df, silver_paths = self.silver_processor.process_all(
                        batch_id=batch_id
                    )
                materializations.append(
                    AssetMaterialization(
                        asset_key="silver_validated_events",
                        row_count=silver_df.height,
                        duration_ms=round((time.perf_counter() - t2) * 1000.0, 2),
                        metadata={"files_written": len(silver_paths)},
                    )
                )

                # Asset 4: gold_dbt_marts
                t3 = time.perf_counter()
                with traced_span("asset.materialize.gold_dbt_marts"):
                    zone_df, courier_df, dbt_tests = self.dbt_runner.run_models_and_tests(
                        batch_id=batch_id
                    )
                materializations.append(
                    AssetMaterialization(
                        asset_key="gold_dbt_marts",
                        row_count=zone_df.height + courier_df.height,
                        duration_ms=round((time.perf_counter() - t3) * 1000.0, 2),
                        metadata={
                            "mart_zone_kpi_daily_rows": zone_df.height,
                            "mart_courier_performance_rows": courier_df.height,
                            "dbt_tests": dbt_tests,
                        },
                    )
                )

                # Asset 5: warehouse_serving_tables
                t4 = time.perf_counter()
                with traced_span("asset.materialize.warehouse_serving_tables"):
                    wh_counts = self.warehouse.sync_from_lake()
                materializations.append(
                    AssetMaterialization(
                        asset_key="warehouse_serving_tables",
                        row_count=sum(wh_counts.values()),
                        duration_ms=round((time.perf_counter() - t4) * 1000.0, 2),
                        metadata=wh_counts,
                    )
                )

                result = OrchestrationRunResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    status="SUCCESS",
                    materializations=materializations,
                    dbt_tests_passed=dbt_tests,
                    warehouse_tables=wh_counts,
                )
                self.logger.info("asset_graph_materialized", run_id=run_id, status="SUCCESS")
                return result
        finally:
            clear_context()

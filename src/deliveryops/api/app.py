"""FastAPI operational and analytical serving endpoints for DeliveryOps Intelligence."""

from pathlib import Path
from typing import Any

import polars as pl
from fastapi import FastAPI, Query
from pydantic import BaseModel, ConfigDict, Field

import deliveryops
from deliveryops.config import AppSettings, get_settings
from deliveryops.observability import traced_span
from deliveryops.pipelines import MedallionPipelineRunner, PipelineRunMetrics
from deliveryops.storage import LocalLakeStorage


class PipelineRunRequest(BaseModel):
    """Request body for triggering a synthetic Medallion pipeline run."""

    model_config = ConfigDict(extra="forbid")

    batch_size: int = Field(default=50, ge=1, le=5000)
    corrupt_ratio: float = Field(default=0.10, ge=0.0, le=1.0)
    seed: int = Field(default=42)
    batch_id: str = Field(default="api-b001", min_length=1, max_length=64)


def _read_parquet_records(files: list[Path]) -> list[dict[str, Any]]:
    if not files:
        return []
    df = pl.read_parquet([str(p) for p in files])
    return df.to_dicts()


def create_app(
    lake_dir: Path | str | None = None,
    settings: AppSettings | None = None,
) -> FastAPI:
    """Create and configure the DeliveryOps Intelligence FastAPI application."""
    cfg = settings or get_settings()
    target_lake_dir = Path(lake_dir) if lake_dir is not None else cfg.local_lake_dir
    lake = LocalLakeStorage(base_dir=target_lake_dir)

    api = FastAPI(
        title="DeliveryOps Intelligence API",
        version=deliveryops.__version__,
        description="Operational and Gold analytical serving API for DeliveryOps Intelligence",
    )

    @api.get("/health")
    def health_check() -> dict[str, str]:
        return {
            "status": "healthy",
            "service": cfg.otel_service_name,
            "version": deliveryops.__version__,
            "storage_backend": cfg.storage_backend,
        }

    @api.post("/pipeline/run", response_model=PipelineRunMetrics)
    def trigger_pipeline_run(request: PipelineRunRequest) -> PipelineRunMetrics:
        with traced_span("api.trigger_pipeline_run", batch_id=request.batch_id):
            runner = MedallionPipelineRunner(lake_dir=target_lake_dir, settings=cfg)
            return runner.run_batch(
                batch_size=request.batch_size,
                corrupt_ratio=request.corrupt_ratio,
                seed=request.seed,
                batch_id=request.batch_id,
            )

    @api.get("/analytics/zones")
    def get_zone_kpis(
        zone_id: str | None = Query(default=None, description="Optional zone_id filter"),
    ) -> dict[str, Any]:
        with traced_span("api.get_zone_kpis", zone_id=zone_id or "all"):
            files = lake.list_parquet_files("gold", "zone_kpi_daily")
            records = _read_parquet_records(files)
            if zone_id is not None:
                records = [r for r in records if r.get("zone_id") == zone_id]
            return {"count": len(records), "items": records}

    @api.get("/analytics/couriers")
    def get_courier_performance(
        vehicle_type: str | None = Query(
            default=None, description="Optional vehicle_type filter"
        ),
    ) -> dict[str, Any]:
        with traced_span("api.get_courier_performance", vehicle_type=vehicle_type or "all"):
            files = lake.list_parquet_files("gold", "courier_performance")
            records = _read_parquet_records(files)
            if vehicle_type is not None:
                records = [r for r in records if r.get("vehicle_type") == vehicle_type]
            return {"count": len(records), "items": records}

    @api.get("/analytics/dlq")
    def get_dead_letter_records(
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict[str, Any]:
        with traced_span("api.get_dead_letter_records", limit=limit):
            dlq_records = lake.read_jsonl_dataset("bronze", "dlq")
            return {
                "count": len(dlq_records),
                "items": dlq_records[:limit],
            }

    return api


app = create_app()

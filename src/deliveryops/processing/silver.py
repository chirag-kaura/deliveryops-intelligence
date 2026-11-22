"""Bronze-to-Silver Polars transformation, deduplication, SLA enrichment, and Pandera gate."""

from pathlib import Path
from typing import Any

import polars as pl

from deliveryops.contracts.tabular import validate_silver_dataframe
from deliveryops.storage.local_lake import LocalLakeStorage


class BronzeToSilverProcessor:
    """Transforms raw Bronze JSONL events into deduplicated, SLA-enriched Silver Parquet."""

    def __init__(self, lake: LocalLakeStorage) -> None:
        self.lake = lake

    def transform_records(self, raw_records: list[dict[str, Any]]) -> pl.DataFrame:
        """Flatten, deduplicate, enrich, and validate Bronze records into a Silver DataFrame."""
        if not raw_records:
            return pl.DataFrame()

        flattened: list[dict[str, Any]] = []
        for rec in raw_records:
            loc = rec.get("location") or {}
            ts_str = str(rec["event_timestamp"])
            flattened.append(
                {
                    "event_id": str(rec["event_id"]),
                    "event_type": str(rec["event_type"]),
                    "event_timestamp": ts_str,
                    "partition_date": ts_str[:10],
                    "order_id": str(rec["order_id"]),
                    "customer_id": str(rec["customer_id"]),
                    "restaurant_id": str(rec["restaurant_id"]),
                    "courier_id": rec.get("courier_id"),
                    "vehicle_type": rec.get("vehicle_type"),
                    "zone_id": str(loc.get("zone_id", "")),
                    "latitude": float(loc.get("latitude", 0.0)),
                    "longitude": float(loc.get("longitude", 0.0)),
                    "estimated_eta_minutes": float(rec["estimated_eta_minutes"]),
                    "actual_duration_minutes": (
                        float(rec["actual_duration_minutes"])
                        if rec.get("actual_duration_minutes") is not None
                        else None
                    ),
                    "sla_target_minutes": float(rec.get("sla_target_minutes", 35.0)),
                    "order_value_inr": float(rec["order_value_inr"]),
                    "weather_condition": str(rec.get("weather_condition", "CLEAR")),
                    "traffic_index": float(rec.get("traffic_index", 1.0)),
                }
            )

        df = pl.DataFrame(
            flattened,
            schema={
                "event_id": pl.String,
                "event_type": pl.String,
                "event_timestamp": pl.String,
                "partition_date": pl.String,
                "order_id": pl.String,
                "customer_id": pl.String,
                "restaurant_id": pl.String,
                "courier_id": pl.String,
                "vehicle_type": pl.String,
                "zone_id": pl.String,
                "latitude": pl.Float64,
                "longitude": pl.Float64,
                "estimated_eta_minutes": pl.Float64,
                "actual_duration_minutes": pl.Float64,
                "sla_target_minutes": pl.Float64,
                "order_value_inr": pl.Float64,
                "weather_condition": pl.String,
                "traffic_index": pl.Float64,
            },
        )

        # Sort by timestamp and deduplicate on event_id keeping the latest entry
        df = df.sort("event_timestamp").unique(subset=["event_id"], keep="last")

        # Compute SLA variance and SLA breach indicator
        effective_duration = pl.coalesce(
            [pl.col("actual_duration_minutes"), pl.col("estimated_eta_minutes")]
        )
        df = df.with_columns(
            (effective_duration - pl.col("sla_target_minutes"))
            .round(2)
            .alias("sla_variance_minutes"),
            (
                (pl.col("event_type") == "SLA_BREACH_DETECTED")
                | (effective_duration > pl.col("sla_target_minutes"))
            ).alias("is_sla_breached"),
        ).sort("event_id")

        return validate_silver_dataframe(df)

    def process_all(self, batch_id: str = "latest") -> tuple[pl.DataFrame, list[Path]]:
        """Read all Bronze events from the lake, transform to Silver, and persist Parquet."""
        raw_records = self.lake.read_jsonl_dataset("bronze", "events")
        silver_df = self.transform_records(raw_records)
        written_paths: list[Path] = []
        if silver_df.is_empty():
            return silver_df, written_paths

        for part_tuple, part_df in silver_df.partition_by("partition_date", as_dict=True).items():
            part_date = str(part_tuple[0])
            path = self.lake.write_parquet_dataset(
                df=part_df,
                layer="silver",
                dataset="events",
                partition_date=part_date,
                filename=f"silver_{batch_id}.parquet",
            )
            written_paths.append(path)

        return silver_df, written_paths

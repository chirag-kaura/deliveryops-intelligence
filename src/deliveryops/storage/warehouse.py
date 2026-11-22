"""Relational serving warehouse loader backed by DuckDB and PostgreSQL DDL compatibility."""

from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from deliveryops.storage.local_lake import LocalLakeStorage


class GoldWarehouseLoader:
    """Syncs Gold marts and DLQ operational summaries into a relational SQL warehouse."""

    def __init__(
        self,
        lake: LocalLakeStorage,
        warehouse_path: Path | str | None = None,
        memory_limit: str = "1GB",
        threads: int = 2,
    ) -> None:
        self.lake = lake
        self.warehouse_path = (
            Path(warehouse_path)
            if warehouse_path is not None
            else (self.lake.base_dir / "warehouse" / "deliveryops_serving.duckdb")
        )
        self.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
        self.memory_limit = memory_limit
        self.threads = threads

    def _connect(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(str(self.warehouse_path))
        conn.execute(f"SET memory_limit = '{self.memory_limit}';")
        conn.execute(f"SET threads = {self.threads};")
        return conn

    def sync_from_lake(self) -> dict[str, int]:
        """Materialize Gold zone KPIs, courier performance, and DLQ tables into SQL warehouse."""
        zone_files = [str(p) for p in self.lake.list_parquet_files("gold", "zone_kpi_daily")]
        courier_files = [
            str(p) for p in self.lake.list_parquet_files("gold", "courier_performance")
        ]
        dlq_records = self.lake.read_jsonl_dataset("bronze", "dlq")

        conn = self._connect()
        try:
            zone_rows = 0
            if zone_files:
                conn.read_parquet(zone_files).create_view("v_zone_gold")
                conn.execute(
                    "CREATE OR REPLACE TABLE wh_zone_kpi_daily AS SELECT * FROM v_zone_gold;"
                )
                res = conn.sql("SELECT COUNT(*) FROM wh_zone_kpi_daily;").fetchall()
                zone_rows = int(res[0][0]) if res else 0

            courier_rows = 0
            if courier_files:
                conn.read_parquet(courier_files).create_view("v_courier_gold")
                conn.execute(
                    "CREATE OR REPLACE TABLE wh_courier_performance AS "
                    "SELECT * FROM v_courier_gold;"
                )
                res = conn.sql("SELECT COUNT(*) FROM wh_courier_performance;").fetchall()
                courier_rows = int(res[0][0]) if res else 0

            dlq_rows = len(dlq_records)
            if dlq_records:
                dlq_df = pl.DataFrame(
                    [
                        {
                            "dlq_id": str(r.get("dlq_id", "")),
                            "failed_at": str(r.get("failed_at", "")),
                            "error_summary": str(r.get("error_summary", "")),
                            "source_stage": str(r.get("source_stage", "")),
                        }
                        for r in dlq_records
                    ]
                )
                conn.register("v_dlq_df", dlq_df)
                conn.execute(
                    "CREATE OR REPLACE TABLE wh_dlq_audit AS SELECT * FROM v_dlq_df;"
                )
            else:
                conn.execute(
                    """
                    CREATE OR REPLACE TABLE wh_dlq_audit (
                        dlq_id VARCHAR,
                        failed_at VARCHAR,
                        error_summary VARCHAR,
                        source_stage VARCHAR
                    );
                    """
                )

            return {
                "wh_zone_kpi_daily": zone_rows,
                "wh_courier_performance": courier_rows,
                "wh_dlq_audit": dlq_rows,
            }
        finally:
            conn.close()

    def query_summary(self) -> dict[str, Any]:
        """Query aggregated operational metrics from the relational serving warehouse."""
        if not self.warehouse_path.exists():
            return {
                "warehouse_initialized": False,
                "zone_rows": 0,
                "courier_rows": 0,
                "dlq_rows": 0,
                "top_zones": [],
            }

        conn = self._connect()
        try:
            tables = {
                row[0]
                for row in conn.sql("SHOW TABLES;").fetchall()
            }
            zone_rows = (
                int(conn.sql("SELECT COUNT(*) FROM wh_zone_kpi_daily;").fetchall()[0][0])
                if "wh_zone_kpi_daily" in tables
                else 0
            )
            courier_rows = (
                int(conn.sql("SELECT COUNT(*) FROM wh_courier_performance;").fetchall()[0][0])
                if "wh_courier_performance" in tables
                else 0
            )
            dlq_rows = (
                int(conn.sql("SELECT COUNT(*) FROM wh_dlq_audit;").fetchall()[0][0])
                if "wh_dlq_audit" in tables
                else 0
            )
            top_zones: list[dict[str, Any]] = []
            if "wh_zone_kpi_daily" in tables and zone_rows > 0:
                top_arrow = conn.sql(
                    """
                    SELECT
                        zone_id,
                        partition_date,
                        total_events,
                        completed_deliveries,
                        sla_breach_count,
                        sla_breach_rate_pct,
                        avg_order_value_inr
                    FROM wh_zone_kpi_daily
                    ORDER BY total_events DESC, zone_id ASC;
                    """
                ).to_arrow_table()
                top_df = pl.from_arrow(top_arrow)
                assert isinstance(top_df, pl.DataFrame)
                top_zones = top_df.to_dicts()

            return {
                "warehouse_initialized": True,
                "zone_rows": zone_rows,
                "courier_rows": courier_rows,
                "dlq_rows": dlq_rows,
                "top_zones": top_zones,
            }
        finally:
            conn.close()

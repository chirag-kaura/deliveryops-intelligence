"""Silver-to-Gold analytical aggregations powered by resource-capped DuckDB."""

from pathlib import Path

import duckdb
import polars as pl

from deliveryops.storage.local_lake import LocalLakeStorage


class SilverToGoldProcessor:
    """Runs in-process DuckDB OLAP queries over Silver Parquet to build Gold KPI datasets."""

    def __init__(
        self,
        lake: LocalLakeStorage,
        memory_limit: str = "1GB",
        threads: int = 2,
    ) -> None:
        self.lake = lake
        self.memory_limit = memory_limit
        self.threads = threads

    def _connect(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(database=":memory:")
        conn.execute(f"SET memory_limit = '{self.memory_limit}';")
        conn.execute(f"SET threads = {self.threads};")
        return conn

    def build_gold_tables(
        self,
        batch_id: str = "latest",
    ) -> tuple[pl.DataFrame, pl.DataFrame, list[Path]]:
        """Aggregate Silver Parquet partitions into Gold Zone KPI and Courier Performance tables."""
        silver_files = [str(p) for p in self.lake.list_parquet_files("silver", "events")]
        if not silver_files:
            return pl.DataFrame(), pl.DataFrame(), []

        conn = self._connect()
        try:
            conn.read_parquet(silver_files).create_view("silver_events")

            zone_kpi_arrow = conn.sql(
                """
                SELECT
                    zone_id,
                    partition_date,
                    CAST(COUNT(*) AS INTEGER) AS total_events,
                    CAST(COUNT(DISTINCT order_id) AS INTEGER) AS distinct_orders,
                    CAST(
                        SUM(CASE WHEN event_type = 'DELIVERY_COMPLETED' THEN 1 ELSE 0 END)
                        AS INTEGER
                    ) AS completed_deliveries,
                    CAST(
                        SUM(CASE WHEN event_type = 'DELIVERY_FAILED' THEN 1 ELSE 0 END)
                        AS INTEGER
                    ) AS failed_deliveries,
                    CAST(
                        SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END)
                        AS INTEGER
                    ) AS sla_breach_count,
                    ROUND(
                        100.0 * SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END) / COUNT(*),
                        2
                    ) AS sla_breach_rate_pct,
                    ROUND(AVG(order_value_inr), 2) AS avg_order_value_inr,
                    ROUND(AVG(estimated_eta_minutes), 2) AS avg_eta_minutes,
                    ROUND(AVG(traffic_index), 2) AS avg_traffic_index
                FROM silver_events
                GROUP BY zone_id, partition_date
                ORDER BY partition_date, zone_id;
                """
            ).to_arrow_table()

            courier_perf_arrow = conn.sql(
                """
                SELECT
                    courier_id,
                    COALESCE(vehicle_type, 'UNKNOWN') AS vehicle_type,
                    partition_date,
                    CAST(COUNT(*) AS INTEGER) AS total_events,
                    CAST(
                        SUM(CASE WHEN event_type = 'DELIVERY_COMPLETED' THEN 1 ELSE 0 END)
                        AS INTEGER
                    ) AS completed_deliveries,
                    CAST(
                        SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END)
                        AS INTEGER
                    ) AS sla_breach_count,
                    ROUND(AVG(estimated_eta_minutes), 2) AS avg_eta_minutes
                FROM silver_events
                WHERE courier_id IS NOT NULL
                GROUP BY courier_id, COALESCE(vehicle_type, 'UNKNOWN'), partition_date
                ORDER BY partition_date, courier_id;
                """
            ).to_arrow_table()
        finally:
            conn.close()

        zone_df = pl.from_arrow(zone_kpi_arrow)
        courier_df = pl.from_arrow(courier_perf_arrow)
        assert isinstance(zone_df, pl.DataFrame)
        assert isinstance(courier_df, pl.DataFrame)

        written_paths: list[Path] = []
        if not zone_df.is_empty():
            for part_tuple, part_df in zone_df.partition_by("partition_date", as_dict=True).items():
                dt_str = str(part_tuple[0])
                written_paths.append(
                    self.lake.write_parquet_dataset(
                        part_df,
                        layer="gold",
                        dataset="zone_kpi_daily",
                        partition_date=dt_str,
                        filename=f"zone_kpi_{batch_id}.parquet",
                    )
                )

        if not courier_df.is_empty():
            for part_tuple, part_df in courier_df.partition_by(
                "partition_date", as_dict=True
            ).items():
                dt_str = str(part_tuple[0])
                written_paths.append(
                    self.lake.write_parquet_dataset(
                        part_df,
                        layer="gold",
                        dataset="courier_performance",
                        partition_date=dt_str,
                        filename=f"courier_perf_{batch_id}.parquet",
                    )
                )

        return zone_df, courier_df, written_paths

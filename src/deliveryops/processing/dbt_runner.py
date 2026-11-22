"""In-process dbt SQL model compiler, executor, and schema test runner over DuckDB."""

import re
from pathlib import Path

import duckdb
import polars as pl

from deliveryops.storage.local_lake import LocalLakeStorage

SOURCE_PATTERN = re.compile(
    r"\{\{\s*source\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}"
)
REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")


class DbtDuckDBRunner:
    """Compiles and executes dbt SQL models (staging -> marts) and data tests in DuckDB."""

    def __init__(
        self,
        lake: LocalLakeStorage,
        dbt_dir: Path | str | None = None,
        memory_limit: str = "1GB",
        threads: int = 2,
    ) -> None:
        self.lake = lake
        self.dbt_dir = (
            Path(dbt_dir)
            if dbt_dir is not None
            else Path(__file__).resolve().parents[3] / "dbt"
        )
        self.memory_limit = memory_limit
        self.threads = threads

    @staticmethod
    def compile_sql(raw_sql: str) -> str:
        """Compile Jinja {{ source('a', 'b') }} and {{ ref('model') }} tags to DuckDB relations."""
        sql = SOURCE_PATTERN.sub(r"\1_\2", raw_sql)
        sql = REF_PATTERN.sub(r"\1", sql)
        return sql.strip().rstrip(";")

    def run_models_and_tests(
        self,
        batch_id: str = "latest",
    ) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, bool]]:
        """Execute dbt staging and mart SQL models over Silver Parquet and run schema tests."""
        silver_files = [str(p) for p in self.lake.list_parquet_files("silver", "events")]
        if not silver_files:
            return pl.DataFrame(), pl.DataFrame(), {}

        stg_sql = self.compile_sql(
            (self.dbt_dir / "models" / "staging" / "stg_silver_events.sql").read_text(
                encoding="utf-8"
            )
        )
        zone_mart_sql = self.compile_sql(
            (self.dbt_dir / "models" / "marts" / "mart_zone_kpi_daily.sql").read_text(
                encoding="utf-8"
            )
        )
        courier_mart_sql = self.compile_sql(
            (self.dbt_dir / "models" / "marts" / "mart_courier_performance.sql").read_text(
                encoding="utf-8"
            )
        )

        conn = duckdb.connect(":memory:")
        conn.execute(f"SET memory_limit = '{self.memory_limit}';")
        conn.execute(f"SET threads = {self.threads};")
        try:
            conn.read_parquet(silver_files).create_view("silver_events")
            conn.execute(f"CREATE VIEW stg_silver_events AS {stg_sql};")
            conn.execute(f"CREATE TABLE mart_zone_kpi_daily AS {zone_mart_sql};")
            conn.execute(f"CREATE TABLE mart_courier_performance AS {courier_mart_sql};")

            # Run dbt schema tests (not_null, unique)
            null_events = int(
                conn.sql(
                    "SELECT COUNT(*) FROM stg_silver_events WHERE event_id IS NULL;"
                ).fetchall()[0][0]
            )
            dup_events = int(
                conn.sql(
                    "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM stg_silver_events;"
                ).fetchall()[0][0]
            )
            null_zones = int(
                conn.sql(
                    "SELECT COUNT(*) FROM mart_zone_kpi_daily WHERE zone_id IS NULL;"
                ).fetchall()[0][0]
            )
            test_results = {
                "not_null_stg_silver_events_event_id": null_events == 0,
                "unique_stg_silver_events_event_id": dup_events == 0,
                "not_null_mart_zone_kpi_daily_zone_id": null_zones == 0,
            }
            if not all(test_results.values()):
                raise ValueError(f"dbt schema test failure: {test_results}")

            zone_arrow = conn.sql("SELECT * FROM mart_zone_kpi_daily;").to_arrow_table()
            courier_arrow = conn.sql("SELECT * FROM mart_courier_performance;").to_arrow_table()
        finally:
            conn.close()

        zone_df = pl.from_arrow(zone_arrow)
        courier_df = pl.from_arrow(courier_arrow)
        assert isinstance(zone_df, pl.DataFrame)
        assert isinstance(courier_df, pl.DataFrame)

        if not zone_df.is_empty():
            for part_tuple, part_df in zone_df.partition_by("partition_date", as_dict=True).items():
                self.lake.write_parquet_dataset(
                    part_df,
                    layer="gold",
                    dataset="zone_kpi_daily",
                    partition_date=str(part_tuple[0]),
                    filename=f"zone_kpi_{batch_id}.parquet",
                )

        if not courier_df.is_empty():
            for part_tuple, part_df in courier_df.partition_by(
                "partition_date", as_dict=True
            ).items():
                self.lake.write_parquet_dataset(
                    part_df,
                    layer="gold",
                    dataset="courier_performance",
                    partition_date=str(part_tuple[0]),
                    filename=f"courier_perf_{batch_id}.parquet",
                )

        return zone_df, courier_df, test_results

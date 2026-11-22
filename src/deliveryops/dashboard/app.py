"""Streamlit DeliveryOps Intelligence operational and analytical dashboard."""

from pathlib import Path
from typing import Any

import polars as pl

from deliveryops.config import get_settings
from deliveryops.pipelines import MedallionPipelineRunner
from deliveryops.storage import LocalLakeStorage


def load_dashboard_snapshot(lake_dir: Path | str | None = None) -> dict[str, Any]:
    """Load Bronze DLQ, Silver events, and Gold KPI summaries from the local lake."""
    settings = get_settings()
    target_dir = Path(lake_dir) if lake_dir is not None else settings.local_lake_dir
    lake = LocalLakeStorage(base_dir=target_dir)

    zone_files = lake.list_parquet_files("gold", "zone_kpi_daily")
    courier_files = lake.list_parquet_files("gold", "courier_performance")
    silver_files = lake.list_parquet_files("silver", "events")
    dlq_records = lake.read_jsonl_dataset("bronze", "dlq")

    zone_df = pl.read_parquet([str(p) for p in zone_files]) if zone_files else pl.DataFrame()
    courier_df = (
        pl.read_parquet([str(p) for p in courier_files]) if courier_files else pl.DataFrame()
    )
    silver_df = (
        pl.read_parquet([str(p) for p in silver_files]) if silver_files else pl.DataFrame()
    )

    total_valid = silver_df.height if not silver_df.is_empty() else 0
    sla_breaches = (
        int(silver_df["is_sla_breached"].sum())
        if not silver_df.is_empty() and "is_sla_breached" in silver_df.columns
        else 0
    )
    breach_rate_pct = round((100.0 * sla_breaches / total_valid), 2) if total_valid > 0 else 0.0

    return {
        "total_valid_events": total_valid,
        "total_dlq_records": len(dlq_records),
        "sla_breach_count": sla_breaches,
        "sla_breach_rate_pct": breach_rate_pct,
        "zone_kpis": zone_df.to_dicts() if not zone_df.is_empty() else [],
        "courier_performance": courier_df.to_dicts() if not courier_df.is_empty() else [],
        "dlq_records": dlq_records,
    }


def render_dashboard() -> None:
    """Render the interactive Streamlit DeliveryOps Intelligence UI."""
    import streamlit as st

    st.set_page_config(page_title="DeliveryOps Intelligence", layout="wide")
    st.title("DeliveryOps Intelligence — Medallion Control Plane")

    settings = get_settings()
    with st.sidebar:
        st.header("Pipeline Controls")
        batch_size = int(st.number_input("Batch Size", min_value=10, max_value=1000, value=60))
        corrupt_ratio = float(
            st.slider("Fault Injection (DLQ Ratio)", min_value=0.0, max_value=0.5, value=0.10)
        )
        seed = int(st.number_input("Random Seed", min_value=1, max_value=99999, value=42))
        if st.button("Run Medallion Pipeline Batch"):
            runner = MedallionPipelineRunner(lake_dir=settings.local_lake_dir, settings=settings)
            metrics = runner.run_batch(
                batch_size=batch_size,
                corrupt_ratio=corrupt_ratio,
                seed=seed,
            )
            st.success(
                f"Run {metrics.run_id} complete: {metrics.silver_row_count} Silver rows, "
                f"{metrics.bronze_dlq_count} DLQ records."
            )

    snapshot = load_dashboard_snapshot(settings.local_lake_dir)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Silver Valid Events", snapshot["total_valid_events"])
    col2.metric("Dead-Letter (DLQ) Count", snapshot["total_dlq_records"])
    col3.metric("SLA Breaches", snapshot["sla_breach_count"])
    col4.metric("SLA Breach Rate %", f"{snapshot['sla_breach_rate_pct']}%")

    st.subheader("Gold Layer — Zone Daily KPIs")
    st.dataframe(snapshot["zone_kpis"], use_container_width=True)

    st.subheader("Gold Layer — Courier Performance")
    st.dataframe(snapshot["courier_performance"], use_container_width=True)

    st.subheader("Bronze DLQ — Contract Violation Triage")
    st.dataframe(snapshot["dlq_records"], use_container_width=True)


if __name__ == "__main__":
    render_dashboard()

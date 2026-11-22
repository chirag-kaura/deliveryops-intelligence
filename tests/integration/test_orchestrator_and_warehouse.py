"""End-to-end integration tests for the 5-asset orchestrator, dbt marts, and SQL warehouse."""

from pathlib import Path

from fastapi.testclient import TestClient

from deliveryops.api import create_app
from deliveryops.observability import configure_tracing


def test_full_asset_graph_materialization_and_warehouse_via_api(tmp_path: Path) -> None:
    exporter = configure_tracing(service_name="deliveryops-phase5-test", capture_in_memory=True)
    assert exporter is not None

    app = create_app(lake_dir=tmp_path)
    client = TestClient(app)

    # 1. Inspect software-defined asset lineage DAG
    assets_resp = client.get("/orchestration/assets")
    assert assets_resp.status_code == 200
    assets_data = assets_resp.json()
    assert assets_data["count"] == 5
    keys = [a["key"] for a in assets_data["assets"]]
    assert keys == [
        "stream_raw_topic",
        "bronze_events_and_dlq",
        "silver_validated_events",
        "gold_dbt_marts",
        "warehouse_serving_tables",
    ]

    # 2. Materialize full 5-asset DAG via POST /orchestration/materialize
    mat_resp = client.post(
        "/orchestration/materialize",
        json={
            "batch_size": 50,
            "corrupt_ratio": 0.12,
            "seed": 505,
            "batch_id": "phase5-b01",
        },
    )
    assert mat_resp.status_code == 200
    mat_data = mat_resp.json()
    assert mat_data["status"] == "SUCCESS"
    assert len(mat_data["materializations"]) == 5
    assert all(mat_data["dbt_tests_passed"].values())
    assert mat_data["warehouse_tables"]["wh_dlq_audit"] == 6

    # 3. Query relational serving warehouse summary
    wh_resp = client.get("/warehouse/summary")
    assert wh_resp.status_code == 200
    wh_data = wh_resp.json()
    assert wh_data["warehouse_initialized"] is True
    assert wh_data["zone_rows"] > 0
    assert wh_data["courier_rows"] > 0
    assert wh_data["dlq_rows"] == 6
    assert sum(z["total_events"] for z in wh_data["top_zones"]) == 44

    # 4. Verify OpenTelemetry spans for every materialized asset
    span_names = [s.name for s in exporter.get_finished_spans()]
    for key in keys:
        assert f"asset.materialize.{key}" in span_names

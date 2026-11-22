"""Integration tests for FastAPI serving endpoints and Streamlit dashboard data loader."""

from pathlib import Path

from fastapi.testclient import TestClient

from deliveryops.api import create_app
from deliveryops.dashboard import load_dashboard_snapshot
from deliveryops.observability import configure_tracing


def test_fastapi_endpoints_and_dashboard_snapshot(tmp_path: Path) -> None:
    exporter = configure_tracing(service_name="deliveryops-api-test", capture_in_memory=True)
    assert exporter is not None

    app = create_app(lake_dir=tmp_path)
    client = TestClient(app)

    # 1. Health check
    health_resp = client.get("/health")
    assert health_resp.status_code == 200
    assert health_resp.json()["status"] == "healthy"
    assert health_resp.json()["version"] == "0.6.0"

    # 2. Trigger Medallion pipeline batch via API
    run_resp = client.post(
        "/pipeline/run",
        json={
            "batch_size": 30,
            "corrupt_ratio": 0.10,
            "seed": 77,
            "batch_id": "api-test-01",
        },
    )
    assert run_resp.status_code == 200
    run_data = run_resp.json()
    assert run_data["total_generated"] == 30
    assert run_data["bronze_valid_count"] == 27
    assert run_data["bronze_dlq_count"] == 3
    assert run_data["silver_row_count"] == 27

    # 3. Query Gold Zone KPIs
    zones_resp = client.get("/analytics/zones")
    assert zones_resp.status_code == 200
    zones_data = zones_resp.json()
    assert zones_data["count"] > 0
    assert sum(item["total_events"] for item in zones_data["items"]) == 27

    # 4. Query Gold Courier Performance
    couriers_resp = client.get("/analytics/couriers")
    assert couriers_resp.status_code == 200
    assert couriers_resp.json()["count"] > 0

    # 5. Query Bronze DLQ records
    dlq_resp = client.get("/analytics/dlq")
    assert dlq_resp.status_code == 200
    assert dlq_resp.json()["count"] == 3

    # 6. Verify Streamlit dashboard snapshot loader against the same lake directory
    snapshot = load_dashboard_snapshot(lake_dir=tmp_path)
    assert snapshot["total_valid_events"] == 27
    assert snapshot["total_dlq_records"] == 3
    assert len(snapshot["zone_kpis"]) == zones_data["count"]

    # 7. Confirm OpenTelemetry spans were recorded across API and pipeline layers
    span_names = [s.name for s in exporter.get_finished_spans()]
    assert "api.trigger_pipeline_run" in span_names
    assert "medallion.run_batch" in span_names
    assert "api.get_zone_kpis" in span_names

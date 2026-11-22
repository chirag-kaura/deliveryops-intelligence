"""Unit tests for infrastructure probes, S3/MinIO URI mapper, and OTLP trace file export."""

from pathlib import Path

from deliveryops.observability import (
    check_infrastructure_health,
    configure_tracing,
    export_spans_to_file,
    traced_span,
)
from deliveryops.storage import LocalLakeStorage, S3LakeAdapter


def test_s3_lake_adapter_builds_expected_uris(tmp_path: Path) -> None:
    lake = LocalLakeStorage(tmp_path)
    s3 = S3LakeAdapter(local_lake=lake)

    uri = s3.build_s3_uri(
        layer="silver",
        dataset="events",
        partition_date="2026-11-22",
        filename="silver_b001.parquet",
    )
    assert uri == "s3://deliveryops-silver/events/dt=2026-11-22/silver_b001.parquet"


def test_infra_health_probe_and_trace_file_export(tmp_path: Path) -> None:
    configure_tracing(service_name="deliveryops-phase6-unit", capture_in_memory=True)
    with traced_span("phase6.unit_check", layer="gold"):
        pass

    trace_path = export_spans_to_file(tmp_path / "traces" / "bundle.json")
    assert trace_path.exists()
    content = trace_path.read_text(encoding="utf-8")
    assert "phase6.unit_check" in content

    report = check_infrastructure_health(timeout_seconds=0.05)
    assert report.services_total == 4
    assert len(report.probes) == 4

"""Sync local Medallion lake, warehouse, topics, OTel traces, and LocalStack AWS."""

import json
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import duckdb
from minio import Minio
from minio.error import S3Error

from deliveryops.config import get_settings


def wait_for_container_ports(ports: tuple[int, ...], timeout_seconds: float = 15.0) -> None:
    """Wait briefly until container TCP ports are accepting connections."""
    deadline = time.time() + timeout_seconds
    pending = set(ports)
    while pending and time.time() < deadline:
        for port in list(pending):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    pending.discard(port)
            except OSError:
                pass
        if pending:
            time.sleep(0.5)


def _latest_trace_file(traces_dir: Path) -> Path | None:
    if not traces_dir.exists():
        return None
    files = sorted(traces_dir.glob("otel_*.json"), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def sync_jaeger_traces(
    lake_dir: Path,
    otlp_http_url: str = "http://127.0.0.1:4318/v1/traces",
) -> str:
    """Push recorded OpenTelemetry spans to Jaeger's OTLP/HTTP JSON collector."""
    trace_file = _latest_trace_file(lake_dir / "traces")
    if trace_file is None:
        return "No trace bundle found"
    data = json.loads(trace_file.read_text(encoding="utf-8"))
    raw_spans = data.get("spans", [])
    if not raw_spans:
        return "No spans in bundle"

    now_ns = time.time_ns()
    otlp_spans: list[dict[str, Any]] = []
    for idx, sp in enumerate(raw_spans):
        dur_ns = max(1_000_000, int(float(sp.get("duration_ms", 1.0)) * 1_000_000))
        end_ns = now_ns - (len(raw_spans) - idx) * 500_000
        start_ns = end_ns - dur_ns
        attrs = [
            {"key": str(k), "value": {"stringValue": str(v)}}
            for k, v in (sp.get("attributes") or {}).items()
        ]
        otlp_spans.append(
            {
                "traceId": sp.get("trace_id", ""),
                "spanId": sp.get("span_id", ""),
                "name": sp.get("name", "deliveryops.span"),
                "kind": 1,
                "startTimeUnixNano": str(start_ns),
                "endTimeUnixNano": str(end_ns),
                "attributes": attrs,
                "status": {"code": 1},
            }
        )

    payload = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "deliveryops-intelligence"},
                        }
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "deliveryops"},
                        "spans": otlp_spans,
                    }
                ],
            }
        ]
    }
    req = urllib.request.Request(
        otlp_http_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            msg = f"Jaeger OTLP HTTP {resp.status}: pushed {len(otlp_spans)} spans."
            print(f"[SYNC] {msg}")
            return msg
    except (urllib.error.URLError, OSError) as exc:
        msg = f"Jaeger sync skipped ({exc})"
        print(f"[SKIP] {msg}")
        return msg


def sync_minio_buckets(lake_dir: Path, root_user: str, root_password: str) -> str:
    """Create Medallion buckets in MinIO and upload Bronze, Silver, and Gold datasets via SDK."""
    try:
        client = Minio(
            "127.0.0.1:9000",
            access_key=root_user,
            secret_key=root_password,
            secure=False,
        )
        uploaded = 0
        for layer in ("bronze", "silver", "gold"):
            bucket = f"deliveryops-{layer}"
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
            layer_dir = lake_dir / layer
            if not layer_dir.exists():
                continue
            for file_path in layer_dir.rglob("*"):
                if file_path.is_file():
                    object_name = file_path.relative_to(layer_dir).as_posix()
                    client.fput_object(bucket, object_name, str(file_path))
                    uploaded += 1
        msg = f"MinIO buckets populated ({uploaded} objects)."
        print(f"[SYNC] {msg}")
        return msg
    except (S3Error, OSError, Exception) as exc:
        msg = f"MinIO sync skipped ({exc})"
        print(f"[SKIP] {msg}")
        return msg


def sync_redpanda_topics(lake_dir: Path) -> str:
    """Ensure Redpanda topics exist and publish recent Bronze & DLQ sample events."""
    try:
        subprocess.run(
            [
                "docker",
                "exec",
                "deliveryops-redpanda",
                "rpk",
                "topic",
                "create",
                "deliveryops.events.raw",
                "deliveryops.events.dlq",
                "-p",
                "1",
            ],
            check=False,
            capture_output=True,
        )
        produced = 0
        for sub_dir, topic in (
            ("events", "deliveryops.events.raw"),
            ("dlq", "deliveryops.events.dlq"),
        ):
            files = sorted((lake_dir / "bronze" / sub_dir).rglob("*.jsonl"))
            if files:
                lines = files[-1].read_text(encoding="utf-8").splitlines()[-10:]
                if lines:
                    subprocess.run(
                        [
                            "docker",
                            "exec",
                            "-i",
                            "deliveryops-redpanda",
                            "rpk",
                            "topic",
                            "produce",
                            topic,
                        ],
                        input="\n".join(lines) + "\n",
                        text=True,
                        check=True,
                        capture_output=True,
                    )
                    produced += len(lines)
        msg = f"Redpanda topics synced ({produced} messages produced)."
        print(f"[SYNC] {msg}")
        return msg
    except (subprocess.SubprocessError, OSError) as exc:
        msg = f"Redpanda sync skipped ({exc})"
        print(f"[SKIP] {msg}")
        return msg


def sync_postgres_warehouse(lake_dir: Path, pg_user: str, pg_db: str) -> str:
    """Apply DDL and sync Gold warehouse tables from DuckDB into PostgreSQL 16."""
    ddl_path = Path(__file__).resolve().parents[1] / "infra" / "postgres" / "001_init_warehouse.sql"
    db_path = lake_dir / "warehouse" / "deliveryops_serving.duckdb"
    if not ddl_path.exists() or not db_path.exists():
        return "PostgreSQL sync skipped (DDL or DuckDB warehouse missing)"

    try:
        ddl_sql = ddl_path.read_text(encoding="utf-8")
        subprocess.run(
            ["docker", "exec", "-i", "deliveryops-postgres", "psql", "-U", pg_user, "-d", pg_db],
            input=ddl_sql,
            text=True,
            check=True,
            capture_output=True,
        )

        conn = duckdb.connect(str(db_path))
        try:
            zones = conn.sql("SELECT * FROM wh_zone_kpi_daily;").fetchall()
            couriers = conn.sql("SELECT * FROM wh_courier_performance;").fetchall()
            dlqs = conn.sql("SELECT * FROM wh_dlq_audit;").fetchall()
        finally:
            conn.close()

        def esc(val: Any) -> str:
            return str(val).replace("'", "''")

        stmts = [
            "TRUNCATE TABLE wh_zone_kpi_daily, wh_courier_performance, wh_dlq_audit;",
        ]
        for z in zones:
            stmts.append(
                "INSERT INTO wh_zone_kpi_daily VALUES "
                f"('{esc(z[0])}', '{esc(z[1])}', {int(z[2])}, {int(z[3])}, "
                f"{int(z[4])}, {int(z[5])}, {int(z[6])}, {float(z[7])}, "
                f"{float(z[8])}, {float(z[9])}, {float(z[10])});"
            )
        for c in couriers:
            stmts.append(
                "INSERT INTO wh_courier_performance VALUES "
                f"('{esc(c[0])}', '{esc(c[1])}', '{esc(c[2])}', {int(c[3])}, "
                f"{int(c[4])}, {int(c[5])}, {float(c[6])});"
            )
        for d in dlqs:
            stmts.append(
                "INSERT INTO wh_dlq_audit VALUES "
                f"('{esc(d[0])}', '{esc(d[1])}', '{esc(d[2])}', '{esc(d[3])}');"
            )

        subprocess.run(
            ["docker", "exec", "-i", "deliveryops-postgres", "psql", "-U", pg_user, "-d", pg_db],
            input="\n".join(stmts),
            text=True,
            check=True,
            capture_output=True,
        )
        msg = f"PostgreSQL synced (zones={len(zones)}, couriers={len(couriers)}, dlq={len(dlqs)})."
        print(f"[SYNC] {msg}")
        return msg
    except (subprocess.SubprocessError, OSError) as exc:
        msg = f"PostgreSQL sync skipped ({exc})"
        print(f"[SKIP] {msg}")
        return msg


def sync_localstack_aws(lake_dir: Path) -> str:
    """Create AWS S3 archive bucket and SQS queue inside LocalStack (:4566)."""
    try:
        client = Minio(
            "127.0.0.1:4566",
            access_key="test",
            secret_key="test",
            secure=False,
        )
        bucket = "deliveryops-cloud-archive"
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        uploaded = 0
        gold_dir = lake_dir / "gold"
        if gold_dir.exists():
            for file_path in gold_dir.rglob("*.parquet"):
                obj_name = file_path.relative_to(gold_dir).as_posix()
                client.fput_object(bucket, obj_name, str(file_path))
                uploaded += 1

        sqs_url = (
            "http://127.0.0.1:4566/?Action=CreateQueue"
            "&QueueName=deliveryops-orders-sqs&Version=2012-11-05"
        )
        with urllib.request.urlopen(sqs_url, timeout=3.0) as resp:
            sqs_status = int(resp.status)

        msg = (
            f"LocalStack AWS S3 ({uploaded} Gold objects) & SQS "
            f"(HTTP {sqs_status}) synced."
        )
        print(f"[SYNC] {msg}")
        return msg
    except (S3Error, urllib.error.URLError, OSError, Exception) as exc:
        msg = f"LocalStack AWS sync skipped ({exc})"
        print(f"[SKIP] {msg}")
        return msg


def sync_all_containers(wait_timeout: float = 15.0) -> list[str]:
    """Sync all 5 infrastructure containers and return human-readable status lines."""
    settings = get_settings()
    wait_for_container_ports(
        (settings.postgres_port, 9000, 19092, 4318, 4566),
        timeout_seconds=wait_timeout,
    )
    lake_dir = settings.local_lake_dir
    return [
        sync_postgres_warehouse(lake_dir, settings.postgres_user, settings.postgres_db),
        sync_redpanda_topics(lake_dir),
        sync_minio_buckets(lake_dir, settings.minio_root_user, settings.minio_root_password),
        sync_jaeger_traces(lake_dir),
        sync_localstack_aws(lake_dir),
    ]


def main() -> None:
    """CLI entrypoint to sync all live containers."""
    sync_all_containers(wait_timeout=15.0)


if __name__ == "__main__":
    main()

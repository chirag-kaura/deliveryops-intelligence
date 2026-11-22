"""Infrastructure TCP reachability probes and hybrid fallback status reporter."""

import socket
import time
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict

from deliveryops.config import AppSettings, get_settings


class ServiceProbeResult(BaseModel):
    """Health and fallback status for a single infrastructure backing service."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    service: str
    host: str
    port: int
    reachable: bool
    latency_ms: float | None
    active_mode: str
    fallback_target: str


class InfrastructureHealthReport(BaseModel):
    """Consolidated health report across all DeliveryOps infrastructure services."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    overall_execution_mode: str
    services_reachable: int
    services_total: int
    probes: list[ServiceProbeResult]


def probe_tcp_endpoint(
    host: str,
    port: int,
    timeout_seconds: float = 0.25,
) -> tuple[bool, float | None]:
    """Attempt a fast TCP connection to (host, port) and return (reachable, latency_ms)."""
    t0 = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            elapsed = round((time.perf_counter() - t0) * 1000.0, 2)
            return True, elapsed
    except OSError:
        return False, None


def _parse_host_port(endpoint: str, default_port: int) -> tuple[str, int]:
    if "://" in endpoint:
        parsed = urlparse(endpoint)
        return parsed.hostname or "localhost", parsed.port or default_port
    if ":" in endpoint:
        host, port_str = endpoint.rsplit(":", 1)
        return host, int(port_str)
    return endpoint, default_port


def check_infrastructure_health(
    settings: AppSettings | None = None,
    timeout_seconds: float = 0.25,
) -> InfrastructureHealthReport:
    """Probe Postgres, Redpanda, MinIO, and Jaeger and report active/fallback modes."""
    cfg = settings or get_settings()
    rp_host, rp_port = _parse_host_port(cfg.redpanda_brokers, 19092)
    minio_host, minio_port = _parse_host_port(cfg.minio_endpoint, 9000)
    otel_host, otel_port = _parse_host_port(cfg.otel_exporter_otlp_endpoint, 4317)

    targets = [
        ("postgres", cfg.postgres_host, cfg.postgres_port, "local_duckdb_warehouse"),
        ("redpanda", rp_host, rp_port, "in_memory_event_broker"),
        ("minio", minio_host, minio_port, "local_lake_filesystem"),
        ("jaeger_otlp", otel_host, otel_port, "local_json_trace_exporter"),
    ]

    probes: list[ServiceProbeResult] = []
    for name, host, port, fallback in targets:
        ok, latency = probe_tcp_endpoint(host, port, timeout_seconds=timeout_seconds)
        probes.append(
            ServiceProbeResult(
                service=name,
                host=host,
                port=port,
                reachable=ok,
                latency_ms=latency,
                active_mode="container_live" if ok else "local_fallback",
                fallback_target=fallback,
            )
        )

    reachable_count = sum(1 for p in probes if p.reachable)
    overall_mode = (
        "full_container"
        if reachable_count == len(probes)
        else ("hybrid_mixed" if reachable_count > 0 else "local_first_fallback")
    )
    return InfrastructureHealthReport(
        overall_execution_mode=overall_mode,
        services_reachable=reachable_count,
        services_total=len(probes),
        probes=probes,
    )

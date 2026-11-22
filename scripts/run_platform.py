"""End-to-end CLI entrypoint to materialize the full DeliveryOps Intelligence platform."""

import json

from deliveryops.config import get_settings
from deliveryops.observability import (
    check_infrastructure_health,
    configure_logging,
    configure_tracing,
    export_spans_to_file,
)
from deliveryops.pipelines import AssetGraphOrchestrator


def main() -> None:
    """Run an end-to-end platform materialization, export traces, and print summary metrics."""
    settings = get_settings()
    configure_logging(log_level="INFO", json_output=True)
    exporter = configure_tracing(service_name="deliveryops-cli", capture_in_memory=True)

    infra_report = check_infrastructure_health(settings=settings)
    orchestrator = AssetGraphOrchestrator(settings=settings)
    result = orchestrator.materialize_all(
        batch_size=100,
        corrupt_ratio=0.10,
        seed=2026,
        batch_id="cli-b001",
    )

    trace_file = export_spans_to_file(
        settings.local_lake_dir / "traces" / f"otel_{result.run_id}.json"
    )
    spans_recorded = len(exporter.get_finished_spans()) if exporter else 0

    summary = {
        "run_id": result.run_id,
        "status": result.status,
        "execution_mode": infra_report.overall_execution_mode,
        "services_reachable": f"{infra_report.services_reachable}/{infra_report.services_total}",
        "assets_materialized": len(result.materializations),
        "dbt_tests_passed": result.dbt_tests_passed,
        "warehouse_tables": result.warehouse_tables,
        "otel_spans_recorded": spans_recorded,
        "trace_bundle_path": str(trace_file),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

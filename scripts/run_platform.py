"""End-to-end CLI entrypoint to materialize the full DeliveryOps Intelligence platform."""

import json

from deliveryops.observability import configure_logging, configure_tracing
from deliveryops.pipelines import AssetGraphOrchestrator


def main() -> None:
    """Run an end-to-end platform materialization and print summary metrics."""
    configure_logging(log_level="INFO", json_output=True)
    exporter = configure_tracing(service_name="deliveryops-cli", capture_in_memory=True)

    orchestrator = AssetGraphOrchestrator()
    result = orchestrator.materialize_all(
        batch_size=100,
        corrupt_ratio=0.10,
        seed=2026,
        batch_id="cli-b001",
    )

    spans_recorded = len(exporter.get_finished_spans()) if exporter else 0
    summary = {
        "run_id": result.run_id,
        "status": result.status,
        "assets_materialized": len(result.materializations),
        "dbt_tests_passed": result.dbt_tests_passed,
        "warehouse_tables": result.warehouse_tables,
        "otel_spans_recorded": spans_recorded,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

"""Live interactive E-Commerce checkout lifecycle runner across all Medallion stages."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from deliveryops.config import AppSettings, get_settings
from deliveryops.contracts import validate_event_batch
from deliveryops.observability import (
    configure_tracing,
    export_spans_to_file,
    traced_span,
)
from deliveryops.processing import BronzeToSilverProcessor, DbtDuckDBRunner
from deliveryops.storage import GoldWarehouseLoader, LocalLakeStorage, S3LakeAdapter

ZONE_COORDS: dict[str, tuple[float, float]] = {
    "BLR-KOR-01": (12.9352, 77.6245),
    "BLR-HSR-02": (12.9121, 77.6446),
    "BLR-IND-03": (12.9784, 77.6408),
    "BLR-WHT-04": (12.9698, 77.7500),
}


def submit_live_ecommerce_order(
    product_name: str,
    order_value_inr: float,
    zone_id: str,
    vehicle_type: str,
    weather_condition: str,
    actual_duration_minutes: float,
    simulate_corrupt: bool = False,
    lake_dir: Path | str | None = None,
    settings: AppSettings | None = None,
) -> dict[str, Any]:
    """Simulate a real customer checkout click and run the full Medallion pipeline."""
    cfg = settings or get_settings()
    target_dir = Path(lake_dir) if lake_dir is not None else cfg.local_lake_dir
    lake = LocalLakeStorage(base_dir=target_dir)
    s3 = S3LakeAdapter(local_lake=lake, settings=cfg)

    configure_tracing(service_name=cfg.otel_service_name, capture_in_memory=True)

    now = datetime.now(UTC)
    partition_date = now.strftime("%Y-%m-%d")
    uid = uuid4().hex[:6].upper()
    order_id = f"ORD-LIVE-{uid}"
    batch_id = f"live-{uid.lower()}"
    courier_id = f"CR-{uid[:3]}"
    lat, lon = ZONE_COORDS.get(zone_id, (12.9352, 77.6245))

    if simulate_corrupt:
        raw_payloads: list[dict[str, Any]] = [
            {
                "event_id": f"evt-{uid}-BAD",
                "event_type": "PICKUP_COMPLETED",
                "event_timestamp": now.isoformat(),
                "order_id": order_id,
                "customer_id": "CUST-WEB-01",
                "restaurant_id": "HUB-BLR-01",
                "courier_id": None,
                "vehicle_type": vehicle_type,
                "location": {"latitude": lat, "longitude": lon, "zone_id": zone_id},
                "estimated_eta_minutes": 25.0,
                "actual_duration_minutes": None,
                "sla_target_minutes": 35.0,
                "order_value_inr": -500.0,
                "weather_condition": weather_condition,
                "traffic_index": 5.0,
                "schema_version": "1.0",
            }
        ]
    else:
        stages = (
            ("ORDER_CREATED", 0, None, None, None),
            ("COURIER_ASSIGNED", 2, courier_id, vehicle_type, None),
            ("PICKUP_COMPLETED", 8, courier_id, vehicle_type, None),
            ("DELIVERY_COMPLETED", int(actual_duration_minutes), courier_id, vehicle_type,
             actual_duration_minutes),
        )
        raw_payloads = []
        for idx, (ev_type, offset_min, cr_id, veh, dur) in enumerate(stages, start=1):
            raw_payloads.append(
                {
                    "event_id": f"evt-{uid}-{idx}",
                    "event_type": ev_type,
                    "event_timestamp": (now + timedelta(minutes=offset_min)).isoformat(),
                    "order_id": order_id,
                    "customer_id": "CUST-WEB-01",
                    "restaurant_id": "HUB-BLR-01",
                    "courier_id": cr_id,
                    "vehicle_type": veh,
                    "location": {"latitude": lat, "longitude": lon, "zone_id": zone_id},
                    "estimated_eta_minutes": 28.0,
                    "actual_duration_minutes": dur,
                    "sla_target_minutes": 35.0,
                    "order_value_inr": float(order_value_inr),
                    "weather_condition": weather_condition,
                    "traffic_index": 6.5 if weather_condition != "CLEAR" else 3.2,
                    "schema_version": "1.0",
                }
            )

    trace_id_hex = ""
    with traced_span(
        "ecommerce.checkout_order",
        order_id=order_id,
        product=product_name,
        zone_id=zone_id,
        simulate_corrupt=simulate_corrupt,
    ) as span:
        ctx = span.get_span_context()
        if ctx and ctx.is_valid:
            trace_id_hex = f"{ctx.trace_id:032x}"

        validation = validate_event_batch(raw_payloads, source_stage="ecommerce_storefront")
        bronze_path = lake.write_bronze_events(
            validation.valid_events,
            batch_id=batch_id,
            partition_date=partition_date,
        )
        dlq_path = lake.write_dlq_records(
            validation.dead_letter_records,
            batch_id=batch_id,
            partition_date=partition_date,
        )

        silver_processor = BronzeToSilverProcessor(lake=lake)
        silver_df, silver_paths = silver_processor.process_all(batch_id=batch_id)

        dbt_runner = DbtDuckDBRunner(
            lake=lake,
            memory_limit=cfg.duckdb_memory_limit,
            threads=cfg.duckdb_threads,
        )
        zone_df, courier_df, dbt_tests = dbt_runner.run_models_and_tests(batch_id=batch_id)

        warehouse = GoldWarehouseLoader(
            lake=lake,
            memory_limit=cfg.duckdb_memory_limit,
            threads=cfg.duckdb_threads,
        )
        wh_counts = warehouse.sync_from_lake()

    trace_file = export_spans_to_file(target_dir / "traces" / f"otel_{batch_id}.json")
    silver_uri = (
        s3.local_path_to_s3_uri(silver_paths[0], "silver", "events")
        if silver_paths
        else ""
    )

    return {
        "order_id": order_id,
        "product_name": product_name,
        "zone_id": zone_id,
        "order_value_inr": order_value_inr,
        "actual_duration_minutes": actual_duration_minutes,
        "sla_breached": actual_duration_minutes > 35.0,
        "simulate_corrupt": simulate_corrupt,
        "valid_events_count": len(validation.valid_events),
        "dlq_count": len(validation.dead_letter_records),
        "bronze_path": str(bronze_path) if bronze_path else "",
        "dlq_path": str(dlq_path) if dlq_path else "",
        "silver_s3_uri": silver_uri,
        "silver_total_rows": silver_df.height,
        "gold_zone_rows": zone_df.height,
        "gold_courier_rows": courier_df.height,
        "dbt_tests_passed": dbt_tests,
        "warehouse_tables": wh_counts,
        "trace_id": trace_id_hex,
        "trace_file": str(trace_file),
    }

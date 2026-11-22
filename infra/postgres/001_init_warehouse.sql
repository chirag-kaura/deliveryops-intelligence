-- PostgreSQL 16 Alpine initialization schema for DeliveryOps Intelligence Serving Warehouse

CREATE TABLE IF NOT EXISTS wh_zone_kpi_daily (
    zone_id VARCHAR(32) NOT NULL,
    partition_date VARCHAR(16) NOT NULL,
    total_events INTEGER NOT NULL,
    distinct_orders INTEGER NOT NULL,
    completed_deliveries INTEGER NOT NULL,
    failed_deliveries INTEGER NOT NULL,
    sla_breach_count INTEGER NOT NULL,
    sla_breach_rate_pct NUMERIC(6, 2) NOT NULL,
    avg_order_value_inr NUMERIC(10, 2) NOT NULL,
    avg_eta_minutes NUMERIC(6, 2) NOT NULL,
    avg_traffic_index NUMERIC(5, 2) NOT NULL,
    PRIMARY KEY (zone_id, partition_date)
);

CREATE TABLE IF NOT EXISTS wh_courier_performance (
    courier_id VARCHAR(64) NOT NULL,
    vehicle_type VARCHAR(32) NOT NULL,
    partition_date VARCHAR(16) NOT NULL,
    total_events INTEGER NOT NULL,
    completed_deliveries INTEGER NOT NULL,
    sla_breach_count INTEGER NOT NULL,
    avg_eta_minutes NUMERIC(6, 2) NOT NULL,
    PRIMARY KEY (courier_id, vehicle_type, partition_date)
);

CREATE TABLE IF NOT EXISTS wh_dlq_audit (
    dlq_id VARCHAR(64) PRIMARY KEY,
    failed_at VARCHAR(64) NOT NULL,
    error_summary TEXT NOT NULL,
    source_stage VARCHAR(64) NOT NULL
);

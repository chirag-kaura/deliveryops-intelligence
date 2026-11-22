-- dbt Gold mart: daily zone operational KPIs
SELECT
    zone_id,
    partition_date,
    CAST(COUNT(*) AS INTEGER) AS total_events,
    CAST(COUNT(DISTINCT order_id) AS INTEGER) AS distinct_orders,
    CAST(
        SUM(CASE WHEN event_type = 'DELIVERY_COMPLETED' THEN 1 ELSE 0 END)
        AS INTEGER
    ) AS completed_deliveries,
    CAST(
        SUM(CASE WHEN event_type = 'DELIVERY_FAILED' THEN 1 ELSE 0 END)
        AS INTEGER
    ) AS failed_deliveries,
    CAST(
        SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END)
        AS INTEGER
    ) AS sla_breach_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS sla_breach_rate_pct,
    ROUND(AVG(order_value_inr), 2) AS avg_order_value_inr,
    ROUND(AVG(estimated_eta_minutes), 2) AS avg_eta_minutes,
    ROUND(AVG(traffic_index), 2) AS avg_traffic_index
FROM {{ ref('stg_silver_events') }}
GROUP BY zone_id, partition_date
ORDER BY partition_date, zone_id

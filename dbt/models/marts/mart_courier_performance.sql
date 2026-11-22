-- dbt Gold mart: courier performance & SLA compliance
SELECT
    courier_id,
    vehicle_type,
    partition_date,
    CAST(COUNT(*) AS INTEGER) AS total_events,
    CAST(
        SUM(CASE WHEN event_type = 'DELIVERY_COMPLETED' THEN 1 ELSE 0 END)
        AS INTEGER
    ) AS completed_deliveries,
    CAST(
        SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END)
        AS INTEGER
    ) AS sla_breach_count,
    ROUND(AVG(estimated_eta_minutes), 2) AS avg_eta_minutes
FROM {{ ref('stg_silver_events') }}
WHERE courier_id IS NOT NULL
GROUP BY courier_id, vehicle_type, partition_date
ORDER BY partition_date, courier_id

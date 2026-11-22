-- dbt staging model: normalizes Silver delivery events
SELECT
    event_id,
    event_type,
    event_timestamp,
    partition_date,
    order_id,
    customer_id,
    restaurant_id,
    courier_id,
    COALESCE(vehicle_type, 'UNKNOWN') AS vehicle_type,
    zone_id,
    latitude,
    longitude,
    estimated_eta_minutes,
    actual_duration_minutes,
    sla_target_minutes,
    sla_variance_minutes,
    is_sla_breached,
    order_value_inr,
    weather_condition,
    traffic_index
FROM {{ source('silver', 'events') }}

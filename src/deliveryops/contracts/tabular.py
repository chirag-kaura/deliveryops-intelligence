"""Pandera tabular quality contract for Silver-layer Polars DataFrames."""

import pandera.polars as pa
import polars as pl

SILVER_EVENTS_SCHEMA = pa.DataFrameSchema(
    columns={
        "event_id": pa.Column(pl.String, nullable=False, unique=True),
        "event_type": pa.Column(pl.String, nullable=False),
        "event_timestamp": pa.Column(pl.String, nullable=False),
        "partition_date": pa.Column(pl.String, nullable=False),
        "order_id": pa.Column(
            pl.String,
            checks=pa.Check.str_startswith("ORD-"),
            nullable=False,
        ),
        "customer_id": pa.Column(pl.String, nullable=False),
        "restaurant_id": pa.Column(pl.String, nullable=False),
        "courier_id": pa.Column(pl.String, nullable=True),
        "vehicle_type": pa.Column(pl.String, nullable=True),
        "zone_id": pa.Column(pl.String, nullable=False),
        "latitude": pa.Column(
            pl.Float64,
            checks=pa.Check.in_range(-90.0, 90.0),
            nullable=False,
        ),
        "longitude": pa.Column(
            pl.Float64,
            checks=pa.Check.in_range(-180.0, 180.0),
            nullable=False,
        ),
        "estimated_eta_minutes": pa.Column(
            pl.Float64,
            checks=pa.Check.in_range(0.0, 240.0),
            nullable=False,
        ),
        "actual_duration_minutes": pa.Column(
            pl.Float64,
            checks=pa.Check.in_range(0.0, 480.0),
            nullable=True,
        ),
        "sla_target_minutes": pa.Column(
            pl.Float64,
            checks=pa.Check.gt(0.0),
            nullable=False,
        ),
        "sla_variance_minutes": pa.Column(pl.Float64, nullable=False),
        "is_sla_breached": pa.Column(pl.Boolean, nullable=False),
        "order_value_inr": pa.Column(
            pl.Float64,
            checks=pa.Check.gt(0.0),
            nullable=False,
        ),
        "weather_condition": pa.Column(pl.String, nullable=False),
        "traffic_index": pa.Column(
            pl.Float64,
            checks=pa.Check.in_range(1.0, 10.0),
            nullable=False,
        ),
    },
    strict=False,
)


def validate_silver_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """Validate a Silver-layer Polars DataFrame against SILVER_EVENTS_SCHEMA."""
    return SILVER_EVENTS_SCHEMA.validate(df)

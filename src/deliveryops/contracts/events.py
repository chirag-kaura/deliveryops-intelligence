"""Pydantic v2 domain event contracts and Dead-Letter Queue (DLQ) validation gate."""

from collections.abc import Iterable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator


class EventType(StrEnum):
    """Supported lifecycle events in DeliveryOps Intelligence."""

    ORDER_CREATED = "ORDER_CREATED"
    COURIER_ASSIGNED = "COURIER_ASSIGNED"
    PICKUP_COMPLETED = "PICKUP_COMPLETED"
    IN_TRANSIT_UPDATE = "IN_TRANSIT_UPDATE"
    DELIVERY_COMPLETED = "DELIVERY_COMPLETED"
    DELIVERY_FAILED = "DELIVERY_FAILED"
    SLA_BREACH_DETECTED = "SLA_BREACH_DETECTED"


class VehicleType(StrEnum):
    """Courier vehicle classifications."""

    BIKE = "BIKE"
    SCOOTER = "SCOOTER"
    EV_VAN = "EV_VAN"
    CAR = "CAR"


class WeatherCondition(StrEnum):
    """Operating weather conditions."""

    CLEAR = "CLEAR"
    RAIN = "RAIN"
    HEAVY_RAIN = "HEAVY_RAIN"
    FOG = "FOG"


POST_ASSIGNMENT_EVENTS: frozenset[EventType] = frozenset(
    {
        EventType.COURIER_ASSIGNED,
        EventType.PICKUP_COMPLETED,
        EventType.IN_TRANSIT_UPDATE,
        EventType.DELIVERY_COMPLETED,
    }
)


class GeoLocation(BaseModel):
    """Validated geographic telemetry and hub zone identifier."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    zone_id: str = Field(..., min_length=3, max_length=32)


class DeliveryEvent(BaseModel):
    """Canonical Bronze-layer ingestion contract for a delivery lifecycle event."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(..., min_length=4, max_length=64)
    event_type: EventType
    event_timestamp: datetime
    order_id: str = Field(..., pattern=r"^ORD-[A-Z0-9-]+$")
    customer_id: str = Field(..., min_length=3, max_length=64)
    restaurant_id: str = Field(..., min_length=3, max_length=64)
    courier_id: str | None = Field(default=None, min_length=3, max_length=64)
    vehicle_type: VehicleType | None = None
    location: GeoLocation
    estimated_eta_minutes: float = Field(..., ge=0.0, le=240.0)
    actual_duration_minutes: float | None = Field(default=None, ge=0.0, le=480.0)
    sla_target_minutes: float = Field(default=35.0, gt=0.0, le=180.0)
    order_value_inr: float = Field(..., gt=0.0, le=100000.0)
    weather_condition: WeatherCondition = WeatherCondition.CLEAR
    traffic_index: float = Field(default=1.0, ge=1.0, le=10.0)
    schema_version: Literal["1.0"] = "1.0"

    @field_validator("event_timestamp")
    @classmethod
    def ensure_timezone_aware(cls, value: datetime) -> datetime:
        """Require timezone-aware timestamps and normalize to UTC."""
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("event_timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def check_lifecycle_invariants(self) -> "DeliveryEvent":
        """Enforce cross-field domain rules across delivery lifecycle stages."""
        if self.event_type in POST_ASSIGNMENT_EVENTS and not self.courier_id:
            raise ValueError(f"courier_id is required when event_type is {self.event_type}")
        if (
            self.event_type == EventType.DELIVERY_COMPLETED
            and self.actual_duration_minutes is None
        ):
            raise ValueError("actual_duration_minutes is required for DELIVERY_COMPLETED events")
        return self


class DeadLetterRecord(BaseModel):
    """Envelope capturing a payload that failed contract validation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dlq_id: str = Field(default_factory=lambda: f"dlq-{uuid4().hex[:12]}")
    failed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    raw_payload: dict[str, Any]
    error_summary: str
    error_details: list[dict[str, Any]]
    source_stage: str = "ingestion_contract_gate"


class ValidationBatchResult(BaseModel):
    """Result of running a batch of raw event payloads through the contract gate."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    valid_events: list[DeliveryEvent]
    dead_letter_records: list[DeadLetterRecord]

    @property
    def total_processed(self) -> int:
        return len(self.valid_events) + len(self.dead_letter_records)


def validate_event_payload(
    payload: dict[str, Any],
    source_stage: str = "ingestion_contract_gate",
) -> tuple[DeliveryEvent | None, DeadLetterRecord | None]:
    """Validate a single raw dict against DeliveryEvent or produce a DeadLetterRecord."""
    try:
        event = DeliveryEvent.model_validate(payload)
        return event, None
    except ValidationError as exc:
        raw_errors = exc.errors(include_url=False)
        details = [
            {
                "loc": [str(part) for part in err.get("loc", ())],
                "msg": str(err.get("msg", "")),
                "type": str(err.get("type", "")),
            }
            for err in raw_errors
        ]
        summary = "; ".join(f"{'.'.join(d['loc'])}: {d['msg']}" for d in details)
        dlq = DeadLetterRecord(
            raw_payload=payload,
            error_summary=summary or "Validation failed",
            error_details=details,
            source_stage=source_stage,
        )
        return None, dlq


def validate_event_batch(
    payloads: Iterable[dict[str, Any]],
    source_stage: str = "ingestion_contract_gate",
) -> ValidationBatchResult:
    """Validate an iterable of raw event dicts, splitting into valid events and DLQ records."""
    valid: list[DeliveryEvent] = []
    dlq_records: list[DeadLetterRecord] = []
    for payload in payloads:
        event, dlq = validate_event_payload(payload, source_stage=source_stage)
        if event is not None:
            valid.append(event)
        elif dlq is not None:
            dlq_records.append(dlq)
    return ValidationBatchResult(valid_events=valid, dead_letter_records=dlq_records)

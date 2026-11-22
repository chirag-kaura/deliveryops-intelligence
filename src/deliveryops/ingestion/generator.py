"""Deterministic synthetic DeliveryOps event stream generator with fault injection."""

import random
from datetime import UTC, datetime, timedelta
from typing import Any

from deliveryops.contracts.events import EventType, VehicleType, WeatherCondition

ZONES: tuple[tuple[str, float, float], ...] = (
    ("BLR-KOR-01", 12.9352, 77.6245),
    ("BLR-HSR-02", 12.9121, 77.6446),
    ("BLR-IND-03", 12.9784, 77.6408),
    ("BLR-WHT-04", 12.9698, 77.7500),
)


class DeliveryEventGenerator:
    """Generates realistic raw delivery event payloads with optional corrupt record injection."""

    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)
        self._seq = 0

    def _next_event_id(self) -> str:
        self._seq += 1
        return f"evt-{self._seq:06d}"

    def generate_valid_payload(self, base_time: datetime | None = None) -> dict[str, Any]:
        """Generate a single schema-compliant raw event dictionary."""
        now = (base_time or datetime.now(UTC)).astimezone(UTC)
        zone_id, base_lat, base_lon = self._rng.choice(ZONES)
        event_type = self._rng.choice(list(EventType))
        order_num = self._rng.randint(10000, 99999)

        needs_courier = event_type != EventType.ORDER_CREATED
        courier_id = f"CR-{self._rng.randint(100, 999)}" if needs_courier else None
        vehicle_type = (
            self._rng.choice(list(VehicleType)).value if needs_courier else None
        )

        eta = round(self._rng.uniform(15.0, 55.0), 1)
        actual_duration = (
            round(eta + self._rng.uniform(-5.0, 20.0), 1)
            if event_type == EventType.DELIVERY_COMPLETED
            else None
        )

        return {
            "event_id": self._next_event_id(),
            "event_type": event_type.value,
            "event_timestamp": (
                now + timedelta(seconds=self._rng.randint(0, 3600))
            ).isoformat(),
            "order_id": f"ORD-{order_num}",
            "customer_id": f"CUST-{self._rng.randint(1000, 9999)}",
            "restaurant_id": f"REST-{self._rng.randint(100, 499)}",
            "courier_id": courier_id,
            "vehicle_type": vehicle_type,
            "location": {
                "latitude": round(base_lat + self._rng.uniform(-0.01, 0.01), 6),
                "longitude": round(base_lon + self._rng.uniform(-0.01, 0.01), 6),
                "zone_id": zone_id,
            },
            "estimated_eta_minutes": eta,
            "actual_duration_minutes": actual_duration,
            "sla_target_minutes": 35.0,
            "order_value_inr": round(self._rng.uniform(150.0, 2500.0), 2),
            "weather_condition": self._rng.choice(list(WeatherCondition)).value,
            "traffic_index": round(self._rng.uniform(1.0, 9.5), 2),
            "schema_version": "1.0",
        }

    def generate_corrupt_payload(self, base_time: datetime | None = None) -> dict[str, Any]:
        """Generate an intentionally invalid payload to exercise DLQ contract routing."""
        payload = self.generate_valid_payload(base_time=base_time)
        fault_mode = self._rng.choice(
            ["bad_order_id", "missing_courier_on_pickup", "invalid_lat", "negative_value"]
        )
        if fault_mode == "bad_order_id":
            payload["order_id"] = "INVALID_ORDER_FORMAT"
        elif fault_mode == "missing_courier_on_pickup":
            payload["event_type"] = EventType.PICKUP_COMPLETED.value
            payload["courier_id"] = None
        elif fault_mode == "invalid_lat":
            payload["location"]["latitude"] = 145.0
        else:
            payload["order_value_inr"] = -50.0
        return payload

    def generate_batch(
        self,
        batch_size: int = 20,
        corrupt_ratio: float = 0.1,
        base_time: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Generate a deterministic batch of raw payloads with a controlled corrupt ratio."""
        if batch_size < 0:
            raise ValueError("batch_size must be non-negative")
        if not 0.0 <= corrupt_ratio <= 1.0:
            raise ValueError("corrupt_ratio must be between 0.0 and 1.0")

        num_corrupt = round(batch_size * corrupt_ratio)
        num_valid = batch_size - num_corrupt

        batch = [
            self.generate_valid_payload(base_time=base_time) for _ in range(num_valid)
        ] + [
            self.generate_corrupt_payload(base_time=base_time) for _ in range(num_corrupt)
        ]
        self._rng.shuffle(batch)
        return batch

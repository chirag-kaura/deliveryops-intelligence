"""Kafka/Redpanda-compatible streaming broker abstraction with topic & DLQ routing."""

from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from deliveryops.contracts.events import (
    DeadLetterRecord,
    DeliveryEvent,
    validate_event_payload,
)
from deliveryops.observability import get_logger, traced_span
from deliveryops.storage.local_lake import LocalLakeStorage


class BrokerMessage(BaseModel):
    """Canonical message envelope stored in a streaming broker topic."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    topic: str
    partition: int = 0
    offset: int
    key: str
    value: dict[str, Any]
    published_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class InMemoryEventBroker:
    """Local Redpanda/Kafka-semantics broker with topic partitions, offsets, and consumer lag."""

    def __init__(self) -> None:
        self._topics: dict[str, list[BrokerMessage]] = defaultdict(list)
        self._committed_offsets: dict[tuple[str, str], int] = defaultdict(int)

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> BrokerMessage:
        """Append a message to a topic and assign a monotonically increasing offset."""
        topic_log = self._topics[topic]
        msg = BrokerMessage(
            topic=topic,
            partition=0,
            offset=len(topic_log),
            key=key,
            value=value,
        )
        topic_log.append(msg)
        return msg

    def publish_batch(
        self,
        topic: str,
        payloads: Iterable[dict[str, Any]],
        key_field: str = "order_id",
    ) -> int:
        """Publish a batch of dictionaries to a topic."""
        count = 0
        for item in payloads:
            key = str(item.get(key_field) or f"msg-{count}")
            self.publish(topic=topic, key=key, value=item)
            count += 1
        return count

    def consume(
        self,
        topic: str,
        consumer_group: str = "deliveryops-cg",
        max_messages: int = 500,
        auto_commit: bool = True,
    ) -> list[BrokerMessage]:
        """Consume uncommitted messages from a topic for a given consumer group."""
        topic_log = self._topics[topic]
        cursor_key = (consumer_group, topic)
        start_offset = self._committed_offsets[cursor_key]
        batch = topic_log[start_offset : start_offset + max_messages]
        if auto_commit and batch:
            self._committed_offsets[cursor_key] = batch[-1].offset + 1
        return batch

    def get_consumer_lag(self, topic: str, consumer_group: str = "deliveryops-cg") -> int:
        """Return the number of unconsumed messages remaining in the topic."""
        total = len(self._topics[topic])
        committed = self._committed_offsets[(consumer_group, topic)]
        return max(0, total - committed)

    def topic_size(self, topic: str) -> int:
        """Return total messages published to a topic."""
        return len(self._topics[topic])


class StreamIngestionConsumer:
    """Consumes raw events from broker, enforces Pydantic contracts, routes DLQ, and sinks."""

    def __init__(
        self,
        broker: InMemoryEventBroker,
        lake: LocalLakeStorage,
        raw_topic: str = "deliveryops.events.raw",
        dlq_topic: str = "deliveryops.events.dlq",
    ) -> None:
        self.broker = broker
        self.lake = lake
        self.raw_topic = raw_topic
        self.dlq_topic = dlq_topic
        self.logger = get_logger("stream_consumer")

    def consume_and_sink(
        self,
        batch_id: str,
        partition_date: str | None = None,
        max_messages: int = 1000,
    ) -> tuple[list[DeliveryEvent], list[DeadLetterRecord]]:
        """Poll raw topic, validate contracts, publish invalid payloads to DLQ, and write Bronze."""
        with traced_span("stream.consume_and_sink", batch_id=batch_id, topic=self.raw_topic):
            messages = self.broker.consume(self.raw_topic, max_messages=max_messages)
            valid_events: list[DeliveryEvent] = []
            dlq_records: list[DeadLetterRecord] = []

            for msg in messages:
                event, dlq = validate_event_payload(
                    msg.value,
                    source_stage="stream_broker_consumer",
                )
                if event is not None:
                    valid_events.append(event)
                elif dlq is not None:
                    dlq_records.append(dlq)
                    self.broker.publish(
                        topic=self.dlq_topic,
                        key=dlq.dlq_id,
                        value=dlq.model_dump(mode="json"),
                    )

            self.lake.write_bronze_events(
                valid_events,
                batch_id=batch_id,
                partition_date=partition_date,
            )
            self.lake.write_dlq_records(
                dlq_records,
                batch_id=batch_id,
                partition_date=partition_date,
            )
            self.logger.info(
                "stream_batch_sunk",
                consumed=len(messages),
                valid=len(valid_events),
                dlq=len(dlq_records),
            )
            return valid_events, dlq_records

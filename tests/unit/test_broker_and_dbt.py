"""Unit tests for streaming broker semantics and dbt SQL model compiler."""

from deliveryops.ingestion import InMemoryEventBroker
from deliveryops.processing import DbtDuckDBRunner


def test_in_memory_broker_publish_consume_and_lag() -> None:
    broker = InMemoryEventBroker()
    published = broker.publish_batch(
        topic="deliveryops.events.raw",
        payloads=[{"order_id": "ORD-1"}, {"order_id": "ORD-2"}, {"order_id": "ORD-3"}],
    )
    assert published == 3
    assert broker.get_consumer_lag("deliveryops.events.raw", "cg-1") == 3

    batch1 = broker.consume("deliveryops.events.raw", consumer_group="cg-1", max_messages=2)
    assert len(batch1) == 2
    assert batch1[0].offset == 0
    assert batch1[1].offset == 1
    assert broker.get_consumer_lag("deliveryops.events.raw", "cg-1") == 1

    batch2 = broker.consume("deliveryops.events.raw", consumer_group="cg-1", max_messages=2)
    assert len(batch2) == 1
    assert batch2[0].offset == 2
    assert broker.get_consumer_lag("deliveryops.events.raw", "cg-1") == 0


def test_dbt_sql_jinja_compilation() -> None:
    raw_sql = "SELECT * FROM {{ source('silver', 'events') }} JOIN {{ ref('stg_silver_events') }};"
    compiled = DbtDuckDBRunner.compile_sql(raw_sql)
    assert compiled == "SELECT * FROM silver_events JOIN stg_silver_events"

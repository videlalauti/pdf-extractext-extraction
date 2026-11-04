import asyncio
from unittest.mock import AsyncMock

import pytest
from fakes import FakeEventPublisher
from shared.web.logging import reset_request_id, set_request_id

from events import (
    DOCUMENT_EXTRACTED_TOPIC,
    EVENT_SCHEMA_VERSION,
    EVENT_SOURCE,
    EventPublisher,
    KafkaEventPublisher,
    NoopEventPublisher,
    build_document_extracted_event,
)

DOCUMENT = {"id": "doc-1", "content": "texto", "checksum": "abc123"}


def test_document_extracted_event_has_version_and_source():
    event = build_document_extracted_event(DOCUMENT)

    assert event == {
        "schema_version": EVENT_SCHEMA_VERSION,
        "document_id": "doc-1",
        "checksum": "abc123",
        "source": EVENT_SOURCE,
    }


def test_document_extracted_event_includes_request_id_in_context():
    token = set_request_id("req-123")
    try:
        event = build_document_extracted_event(DOCUMENT)
    finally:
        reset_request_id(token)

    assert event == {
        "schema_version": EVENT_SCHEMA_VERSION,
        "document_id": "doc-1",
        "checksum": "abc123",
        "source": EVENT_SOURCE,
        "request_id": "req-123",
    }


def test_producer_publishes_to_the_topic_declared_by_infra():
    """El topic debe coincidir con el que crea infra (kafka-init).

    ``kafka-init`` declara ``documents.extracted`` y el broker corre con
    ``KAFKA_AUTO_CREATE_TOPICS_ENABLE=false``: si el productor publicara en otro
    topic, el evento nunca llegaria al consumer.
    """
    assert DOCUMENT_EXTRACTED_TOPIC == "documents.extracted"
    assert KafkaEventPublisher("localhost:9092")._topic == DOCUMENT_EXTRACTED_TOPIC


def test_noop_and_fake_implement_the_publisher_protocol():
    assert isinstance(NoopEventPublisher(), EventPublisher)
    assert isinstance(FakeEventPublisher(), EventPublisher)


def test_kafka_publisher_propagates_failure_when_producer_is_unavailable(monkeypatch):
    class OfflineProducer:
        def __init__(self, **kwargs):
            self.stop = AsyncMock()

        async def start(self):
            raise OSError("broker offline")

    monkeypatch.setattr("events.AIOKafkaProducer", OfflineProducer)
    publisher = KafkaEventPublisher("localhost:9092")

    with pytest.raises(RuntimeError, match="Kafka no disponible"):
        asyncio.run(publisher.publish({"document_id": "doc-1"}))


def test_kafka_publisher_recovers_after_failed_start(monkeypatch):
    producers = []

    class Producer:
        def __init__(self, **kwargs):
            self.start = AsyncMock(side_effect=OSError("offline") if not producers else None)
            self.send_and_wait = AsyncMock()
            self.stop = AsyncMock()
            producers.append(self)

    monkeypatch.setattr("events.AIOKafkaProducer", Producer)
    publisher = KafkaEventPublisher("localhost:9092")

    async def scenario():
        await publisher.start()
        assert publisher._producer is None
        await publisher.publish({"document_id": "doc-1"})
        await publisher.aclose()

    asyncio.run(scenario())

    assert len(producers) == 2
    producers[0].stop.assert_awaited_once()
    producers[1].send_and_wait.assert_awaited_once()


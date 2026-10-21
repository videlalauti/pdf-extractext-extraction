from fakes import FakeEventPublisher

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

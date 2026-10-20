from fakes import FakeEventPublisher

from events import (
    EVENT_SCHEMA_VERSION,
    EVENT_SOURCE,
    EventPublisher,
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


def test_noop_and_fake_implement_the_publisher_protocol():
    assert isinstance(NoopEventPublisher(), EventPublisher)
    assert isinstance(FakeEventPublisher(), EventPublisher)

"""Pruebas aisladas del caso de uso, sin FastAPI ni Redis/Kafka reales."""
import asyncio
import logging
from types import SimpleNamespace

from use_cases import ExtractionPipeline


class FakeCache:
    def __init__(self, cached=None):
        self.cached = cached
        self.saved = []

    async def get(self, checksum):
        return self.cached

    async def set(self, checksum, value):
        self.saved.append((checksum, value))


class FakeExtractor:
    def __init__(self):
        self.calls = 0

    async def extract_text_from_bytes(self, content):
        self.calls += 1
        return "texto real"


def test_cache_miss_persists_and_enqueues_event():
    cache = FakeCache()
    extractor = FakeExtractor()
    published = []

    async def persist(content, checksum):
        assert (content, checksum) == ("texto real", "sha123")
        return SimpleNamespace(
            document={"id": "id1", "checksum": "sha123", "content": content},
            summary_enqueued=True,
        )

    async def publish(doc):
        published.append(doc)
        return True

    pipeline = ExtractionPipeline(cache, persist, publish, logging.getLogger("test"))
    result = asyncio.run(pipeline.execute(b"pdf", "sha123", extractor))

    assert result.id == "id1"
    assert result.summary_status == "queued"
    assert extractor.calls == 1
    assert len(cache.saved) == 1
    assert not published  # El persist adapter ya publica el evento en cache miss.


def test_cache_hit_republishes_event_and_skips_persistence():
    cache = FakeCache({
        "id": "id-cache", "content": "anterior", "checksum": "sha123", "text": "anterior"
    })
    extractor = FakeExtractor()
    calls = []

    async def persist(*args):
        raise AssertionError("No deberia persistir en cache hit")

    async def publish(doc):
        calls.append(doc["id"])
        return False

    pipeline = ExtractionPipeline(cache, persist, publish, logging.getLogger("test"))
    result = asyncio.run(pipeline.execute(b"pdf", "sha123", extractor))

    assert result.summary_status == "unavailable"
    assert extractor.calls == 0
    assert calls == ["id-cache"]

import asyncio
import json
import logging

from fakes import FailingRedisClient, FakeRedisClient

from cache import ExtractionCache, NoopExtractionCache, RedisExtractionCache

CHECKSUM = "a" * 64
PAYLOAD = {"id": "doc-1", "content": "hola", "checksum": CHECKSUM, "text": "hola"}
KEY = f"extract:{CHECKSUM}"


def test_redis_noop_and_fake_implement_the_cache_protocol():
    redis_cache = RedisExtractionCache("redis://redis:6379/0", 3600, client=FakeRedisClient())

    assert isinstance(redis_cache, ExtractionCache)
    assert isinstance(NoopExtractionCache(), ExtractionCache)


def test_noop_cache_is_always_a_miss():
    cache = NoopExtractionCache()

    assert asyncio.run(cache.get(CHECKSUM)) is None


def test_noop_cache_set_is_a_no_op():
    asyncio.run(NoopExtractionCache().set(CHECKSUM, PAYLOAD))


def test_redis_cache_returns_none_on_miss():
    cache = RedisExtractionCache("redis://redis:6379/0", 3600, client=FakeRedisClient())

    assert asyncio.run(cache.get(CHECKSUM)) is None


def test_redis_cache_returns_payload_on_hit():
    client = FakeRedisClient(stored={KEY: json.dumps(PAYLOAD)})
    cache = RedisExtractionCache("redis://redis:6379/0", 3600, client=client)

    assert asyncio.run(cache.get(CHECKSUM)) == PAYLOAD


def test_redis_cache_set_serializes_payload_as_json_with_ttl():
    client = FakeRedisClient()
    cache = RedisExtractionCache("redis://redis:6379/0", 3600, client=client)

    asyncio.run(cache.set(CHECKSUM, PAYLOAD))

    key, value, ttl = client.sets[0]
    assert key == KEY
    assert json.loads(value) == PAYLOAD
    assert ttl == 3600


def test_redis_cache_treats_corrupted_json_as_a_miss(caplog):
    client = FakeRedisClient(stored={KEY: "{corrupto"})
    cache = RedisExtractionCache("redis://redis:6379/0", 3600, client=client)

    with caplog.at_level(logging.WARNING):
        assert asyncio.run(cache.get(CHECKSUM)) is None

    assert any(record.levelno == logging.WARNING for record in caplog.records)


def test_redis_cache_fail_open_returns_miss_and_logs_warning(caplog):
    cache = RedisExtractionCache("redis://redis:6379/0", 3600, client=FailingRedisClient())

    with caplog.at_level(logging.WARNING):
        assert asyncio.run(cache.get(CHECKSUM)) is None

    assert any(record.levelno == logging.WARNING for record in caplog.records)


def test_redis_cache_fail_open_on_set_is_a_no_op(caplog):
    cache = RedisExtractionCache("redis://redis:6379/0", 3600, client=FailingRedisClient())

    with caplog.at_level(logging.WARNING):
        asyncio.run(cache.set(CHECKSUM, PAYLOAD))

    assert any(record.levelno == logging.WARNING for record in caplog.records)


def test_factory_returns_noop_when_cache_is_disabled():
    from app import Settings, build_extraction_cache

    cache = build_extraction_cache(Settings(extraction_cache_enabled=False))

    assert isinstance(cache, NoopExtractionCache)


def test_factory_returns_redis_adapter_when_cache_is_enabled():
    from app import Settings, build_extraction_cache

    cache = build_extraction_cache(Settings())

    try:
        assert isinstance(cache, RedisExtractionCache)
    finally:
        asyncio.run(cache.aclose())

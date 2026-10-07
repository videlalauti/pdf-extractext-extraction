"""Doubles usos por los tests: implementan los puertos sin tocar librerías."""

import json


class FakeExtractionCache:
    """Implementación en memoria de :class:`ExtractionCache` que registra llamadas."""

    def __init__(self, entries: dict[str, dict] | None = None) -> None:
        self.entries = dict(entries or {})
        self.gets: list[str] = []
        self.sets: list[tuple[str, dict]] = []

    async def get(self, checksum: str) -> dict | None:
        self.gets.append(checksum)
        return self.entries.get(checksum)

    async def set(self, checksum: str, payload: dict) -> None:
        self.sets.append((checksum, payload))
        self.entries[checksum] = payload


class FakeRedisClient:
    """Cliente Redis mínimo: almacena strings en memoria, sin red."""

    def __init__(self, stored: dict[str, str] | None = None) -> None:
        self.store = dict(stored or {})
        self.sets: list[tuple[str, str, int | None]] = []

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.sets.append((key, value, ex))
        self.store[key] = value
        return True

    async def aclose(self) -> None:
        return None


class FailingRedisClient:
    """Cliente Redis que simula a Redis caído: todo comando lanza ConnectionError."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error or ConnectionError("redis caído")

    async def get(self, key: str) -> str | None:
        raise self.error

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        raise self.error

    async def aclose(self) -> None:
        raise self.error


def stored_payload(payload: dict) -> str:
    """Serializa un payload como lo haría el adaptador (JSON string)."""
    return json.dumps(payload)

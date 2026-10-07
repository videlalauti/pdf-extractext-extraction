"""Puerto de caché de la extracción y adaptadores (Redis y Noop).

El servicio depende del Protocol :class:`ExtractionCache`, nunca de ``redis``:
la dependencia se inyecta en la composición (módulo ``app``) y los tests usan
fakes. El adaptador Redis es *fail-open*: cualquier error se traduce en un
miss/no-op con un warning, porque la caché jamás puede ser motivo de un 5xx.
"""

import json
import logging
from typing import Protocol, runtime_checkable

import redis.asyncio as redis

logger = logging.getLogger(__name__)

EXTRACTION_CACHE_KEY_PREFIX = "extract:"
# Timeouts acotados: si Redis está caído, fallar rápido y seguir (fail-open)
# en lugar de dejar el request esperando el timeout de TCP por defecto.
SOCKET_TIMEOUT_SECONDS = 2.0


class _AsyncRedisCommands(Protocol):
    """Comandos mínimos de un cliente Redis asíncrono (permite fakes en tests)."""

    async def get(self, key: str) -> str | None: ...

    async def set(self, key: str, value: str, ex: int | None = None) -> bool: ...


@runtime_checkable
class ExtractionCache(Protocol):
    """Puerto de salida hacia la caché de resultados de extracción.

    Expone exactamente los métodos que usa la capa de aplicación, siguiendo
    Interface Segregation: cambiar Redis por otro store no toca ``routes``.
    """

    async def get(self, checksum: str) -> dict | None:
        """Devuelve el payload cacheado para el checksum, o None si es miss."""

    async def set(self, checksum: str, payload: dict) -> None:
        """Guarda el payload cacheado para el checksum."""


class NoopExtractionCache:
    """Caché deshabilitada: todo es miss y ``set`` no hace nada."""

    async def get(self, checksum: str) -> dict | None:
        return None

    async def set(self, checksum: str, payload: dict) -> None:
        return None

    async def aclose(self) -> None:
        return None


class RedisExtractionCache:
    """Adaptador de :class:`ExtractionCache` sobre Redis (cache-aside).

    Fail-open: un Redis caído o un payload corrupto se reporta como miss con
    warning; la extracción y la persistencia siguen su curso normal. El TTL se
    acorta (1 h) para no devolver ids stale si se borra MongoDB.
    """

    def __init__(self, url: str, ttl_seconds: int, client: _AsyncRedisCommands | None = None) -> None:
        self._owns_client = client is None
        self._client = client or redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=SOCKET_TIMEOUT_SECONDS,
            socket_timeout=SOCKET_TIMEOUT_SECONDS,
        )
        self._ttl_seconds = ttl_seconds

    async def get(self, checksum: str) -> dict | None:
        try:
            raw = await self._client.get(EXTRACTION_CACHE_KEY_PREFIX + checksum)
        except Exception as error:
            logger.warning(
                "caché Redis no disponible (get, checksum=%s); se asume miss: %s",
                checksum,
                type(error).__name__,
                exc_info=True,
            )
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as error:
            logger.warning(
                "payload de caché inválido (checksum=%s); se asume miss: %s",
                checksum,
                error,
            )
            return None

    async def set(self, checksum: str, payload: dict) -> None:
        try:
            await self._client.set(
                EXTRACTION_CACHE_KEY_PREFIX + checksum,
                json.dumps(payload),
                ex=self._ttl_seconds,
            )
        except Exception as error:
            logger.warning(
                "no se pudo escribir en la caché Redis (checksum=%s); se ignora: %s",
                checksum,
                type(error).__name__,
                exc_info=True,
            )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

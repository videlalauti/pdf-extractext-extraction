"""Extraction service: capa de aplicación, lógica de negocio encapsulada."""

import hashlib
import logging

import httpx
from fastapi import HTTPException
from pydantic_settings import BaseSettings
from shared.web.resilience import CircuitBreaker, CircuitOpenError, retry_with_backoff

from cache import ExtractionCache, NoopExtractionCache, RedisExtractionCache

logger = logging.getLogger(__name__)

SAFE_PERSISTENCE_ERROR = "No se pudo guardar el documento en persistence-service"

DEFAULT_EXTRACTION_CACHE_TTL_SECONDS = 3600


class Settings(BaseSettings):
    persistence_service_url: str = "http://persistence-service:8000"
    redis_url: str = "redis://redis:6379/0"
    extraction_cache_ttl_seconds: int = DEFAULT_EXTRACTION_CACHE_TTL_SECONDS
    extraction_cache_enabled: bool = True


settings = Settings()


def build_extraction_cache(config: Settings) -> ExtractionCache:
    """Compone la caché según config: habilitada → Redis, si no → Noop.

    El flag permite desactivar la caché por env sin tocar código
    (12-FACTOR: configuración en el entorno).
    """
    if not config.extraction_cache_enabled:
        return NoopExtractionCache()
    return RedisExtractionCache(config.redis_url, config.extraction_cache_ttl_seconds)


# Accessor módulo-global, consistente con `settings`/`persistence_breaker`:
# `routes.py` lo lee en tiempo de llamada y los tests lo monkeypatchean acá.
extraction_cache: ExtractionCache = build_extraction_cache(settings)

# Call timeout 120 s: cubre el peor caso de reintentos (3 x timeout httpx 30 s + sleeps).
persistence_breaker = CircuitBreaker(
    service="persistence",
    failure_threshold=5,
    recovery_timeout=30,
    call_timeout=120,
)


def compute_checksum(content: bytes) -> str:
    """Checksum SHA-256 del contenido binario; identifica duplicados de forma inequívoca."""
    return hashlib.sha256(content).hexdigest()


async def _post_document(content: str, checksum: str) -> dict | None:
    """POST a /documents; devuelve None ante un 409 (checksum ya existente).

    El 409 no es un error sino un resultado esperado: el llamador recupera el
    documento previo por checksum. Cualquier otra respuesta no-2xx se levanta.
    """
    url = f"{settings.persistence_service_url}/documents"
    payload = {"content": content, "checksum": checksum}
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, json=payload)
        if response.status_code == 409:
            return None
        response.raise_for_status()
        return response.json()


async def _fetch_existing_document(checksum: str) -> dict:
    """Recupera el documento ya persistido; ante cualquier fallo responde 502 sin reintentos.

    El POST que lo precedió llegó a persistir (409), así que reintentar aquí solo
    repetiría el mismo error; el cliente que recibe el 502 re-envía el PDF y el
    ciclo empieza de nuevo.
    """
    url = f"{settings.persistence_service_url}/documents/by-checksum/{checksum}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
        except (httpx.RequestError, httpx.HTTPStatusError) as error:
            logger.error(
                "no se pudo recuperar el documento duplicado (checksum=%s): %s",
                checksum,
                error,
            )
            raise HTTPException(status_code=502, detail=SAFE_PERSISTENCE_ERROR) from error


async def save_to_persistence(content: str, checksum: str) -> dict:
    """Persiste el documento; un 409 por checksum es éxito (deduplicación idempotente).

    El backoff con jitter cadena solo los errores transitorios del POST (4xx se
    reenvían tal cual); el circuit breaker corta el tráfico si persistence cae.
    La recuperación por checksum tras un 409 va fuera del breaking y no se
    reintenta: un fallo ahí responde 502 y el cliente vuelve a enviar el PDF.
    """
    try:
        result = await persistence_breaker.call(
            retry_with_backoff,
            _post_document,
            content,
            checksum,
            attempts=3,
            base_delay=0.5,
            max_delay=4,
        )
    except CircuitOpenError as error:
        logger.warning("circuito abierto para persistence-service: %s", error)
        raise HTTPException(status_code=502, detail=SAFE_PERSISTENCE_ERROR) from error
    except httpx.HTTPStatusError as error:
        status_code = error.response.status_code
        if 400 <= status_code < 500:
            logger.error("persistence-service rechazó el documento (%s)", status_code)
            raise HTTPException(status_code=status_code, detail=SAFE_PERSISTENCE_ERROR) from error
        logger.error("persistence-service respondió %s tras reintentos", status_code)
        raise HTTPException(status_code=502, detail=SAFE_PERSISTENCE_ERROR) from error
    except httpx.RequestError as error:
        logger.error(
            "persistence-service inalcanzable tras reintentos: %s", type(error).__name__
        )
        raise HTTPException(status_code=502, detail=SAFE_PERSISTENCE_ERROR) from error

    if result is None:
        return await _fetch_existing_document(checksum)
    return result
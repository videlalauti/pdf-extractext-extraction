"""Extraction service: capa de aplicación, lógica de negocio encapsulada."""

import hashlib
import logging
from dataclasses import dataclass

import httpx
from fastapi import HTTPException
from pydantic_settings import BaseSettings
from shared.web.logging import request_id_header
from shared.web.resilience import CircuitBreaker, CircuitOpenError, retry_with_backoff

from cache import ExtractionCache, NoopExtractionCache, RedisExtractionCache
from events import (
    EventPublisher,
    KafkaEventPublisher,
    NoopEventPublisher,
    build_document_extracted_event,
)

logger = logging.getLogger(__name__)

SAFE_PERSISTENCE_ERROR = "No se pudo guardar el documento en persistence-service"
SAFE_VALIDATION_ERROR = "validation-service no disponible"
VALIDATION_REJECTION_DETAIL = "El PDF no pasó la validación"

DEFAULT_EXTRACTION_CACHE_TTL_SECONDS = 3600


@dataclass(frozen=True)
class PersistResult:
    """Resultado de persistir: el documento y si el resumen quedó encolado."""

    document: dict
    summary_enqueued: bool


class Settings(BaseSettings):
    persistence_service_url: str = "http://persistence-service:8000"
    persistence_failure_threshold: int = 5
    persistence_recovery_timeout_seconds: float = 30
    persistence_call_timeout_seconds: float = 120
    validation_service_url: str = "http://validation-service:8000"
    validation_call_timeout_seconds: float = 10
    redis_url: str = "redis://redis:6379/0"
    extraction_cache_ttl_seconds: int = DEFAULT_EXTRACTION_CACHE_TTL_SECONDS
    extraction_cache_enabled: bool = True
    kafka_bootstrap_servers: str = "localhost:9092"


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


def build_event_publisher(config: Settings) -> EventPublisher:
    """Compone el publisher de eventos; el lifespan lo crea y lo cierra."""
    return KafkaEventPublisher(config.kafka_bootstrap_servers)


# Default seguro para imports y tests: el lifespan lo reemplaza por Kafka en
# producción y los tests inyectan un fake.
event_publisher: EventPublisher = NoopEventPublisher()


def build_persistence_breaker(config: Settings) -> CircuitBreaker:
    """Compone el breaker de persistence con los límites configurables.

    ``call_timeout`` y el timeout de cada llamada HTTP salen del mismo setting,
    así que cambiarlo mueve ambos límites a la vez. Con el default de 120 s el
    breaker corta la operación completa si los reintentos agotan ese presupuesto.
    """
    return CircuitBreaker(
        service="persistence",
        failure_threshold=config.persistence_failure_threshold,
        recovery_timeout=config.persistence_recovery_timeout_seconds,
        call_timeout=config.persistence_call_timeout_seconds,
    )


persistence_breaker = build_persistence_breaker(settings)


def compute_checksum(content: bytes) -> str:
    """Checksum SHA-256 del contenido binario; identifica duplicados de forma inequívoca."""
    return hashlib.sha256(content).hexdigest()


def _persistence_client() -> httpx.AsyncClient:
    """Cliente HTTP hacia persistence con el timeout configurado por entorno."""
    return httpx.AsyncClient(timeout=settings.persistence_call_timeout_seconds)


async def _post_document(content: str, checksum: str) -> dict | None:
    """POST a /documents; devuelve None ante un 409 (checksum ya existente).

    El 409 no es un error sino un resultado esperado: el llamador recupera el
    documento previo por checksum. Cualquier otra respuesta no-2xx se levanta.
    """
    url = f"{settings.persistence_service_url}/documents"
    payload = {"content": content, "checksum": checksum}
    async with _persistence_client() as client:
        response = await client.post(url, json=payload, headers=request_id_header())
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
    async with _persistence_client() as client:
        try:
            response = await client.get(url, headers=request_id_header())
            response.raise_for_status()
            return response.json()
        except (httpx.RequestError, httpx.HTTPStatusError) as error:
            logger.error(
                "no se pudo recuperar el documento duplicado (checksum=%s): %s",
                checksum,
                error,
            )
            raise HTTPException(status_code=502, detail=SAFE_PERSISTENCE_ERROR) from error


def _validation_client() -> httpx.AsyncClient:
    """Cliente HTTP hacia validation con el timeout configurado por entorno."""
    return httpx.AsyncClient(timeout=settings.validation_call_timeout_seconds)


def _validation_detail(response: httpx.Response) -> str:
    """Extrae el ``detail`` del contrato de Validation sin romper si no es JSON."""
    try:
        body = response.json()
    except ValueError:
        return VALIDATION_REJECTION_DETAIL
    detail = body.get("detail") if isinstance(body, dict) else None
    return detail if isinstance(detail, str) and detail else VALIDATION_REJECTION_DETAIL


async def validate_with_validation_service(content: bytes, filename: str) -> None:
    """Valida el PDF contra validation-service antes de extraer o persistir.

    Fail-closed: si Validation rechaza o no está disponible, se levanta la
    excepción HTTP correspondiente y el flujo no deja efectos laterales.
    """
    url = f"{settings.validation_service_url}/validate"
    try:
        async with _validation_client() as client:
            response = await client.post(
                url,
                files={"file": (filename, content, "application/pdf")},
                headers=request_id_header(),
            )
    except httpx.RequestError as error:
        logger.error("validation-service inalcanzable: %s", type(error).__name__)
        raise HTTPException(status_code=503, detail=SAFE_VALIDATION_ERROR) from error

    if response.status_code == 200:
        return

    if response.status_code in (400, 413, 415):
        raise HTTPException(
            status_code=response.status_code,
            detail=_validation_detail(response),
        )

    logger.error("validation-service respondió %s", response.status_code)
    raise HTTPException(status_code=503, detail=SAFE_VALIDATION_ERROR)


async def _publish_extracted_event(document: dict) -> None:
    """Publica un evento ya construido; deja que el error llegue al reintento."""
    await event_publisher.publish(build_document_extracted_event(document))


async def publish_document_extracted(document: dict) -> bool:
    """Publica ``document.extracted`` con reintentos y sin romper la response.

    El documento ya está persistido, así que un 5xx acá mentiría sobre el
    resultado. Se reintenta con backoff y, si se agotan los intentos, se
    devuelve ``False`` para que el cliente sepa que el resumen no quedó
    encolado y pueda pedirlo explícitamente con ``POST /summary/{id}``.
    """
    try:
        await retry_with_backoff(
            _publish_extracted_event,
            document,
            attempts=3,
            base_delay=0.5,
            max_delay=2,
            retry_on=(Exception,),
        )
    except Exception as error:
        logger.exception(
            "no se pudo publicar document.extracted tras 3 intentos "
            "(document_id=%s); el resumen no quedó encolado: %s",
            document.get("id"),
            type(error).__name__,
        )
        return False
    return True


async def save_to_persistence(content: str, checksum: str) -> PersistResult:
    """Persiste el documento; un 409 por checksum es éxito (deduplicación idempotente).

    El backoff con jitter cadena solo los errores transitorios del POST (4xx se
    reenvían tal cual); el circuit breaker corta el tráfico si persistence cae.
    La recuperación por checksum tras un 409 va fuera del breaking y no se
    reintenta: un fallo ahí responde 502 y el cliente vuelve a enviar el PDF.

    Una vez obtenido el document (creado o recuperado por 409), publica
    ``document.extracted`` y reporta si el resumen quedó encolado.
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
        document = await _fetch_existing_document(checksum)
    else:
        document = result

    summary_enqueued = await publish_document_extracted(document)
    return PersistResult(document=document, summary_enqueued=summary_enqueued)

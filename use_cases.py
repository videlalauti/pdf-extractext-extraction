"""Casos de uso de extracción: sin FastAPI ni rutas HTTP."""
from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable

from pydantic import ValidationError
from shared.domain import PyPdfTextExtractor

from cache import ExtractionCache
from schemas import ExtractionResponse


def elapsed_ms(started_at: float) -> float:
    return (time.perf_counter() - started_at) * 1000


class ExtractionPipeline:
    """Cache-aside -> extractor -> Persistence -> evento Kafka -> cache.

    Los adaptadores/funciones se inyectan para que una prueba use fakes sin
    conectar Redis, Mongo o Kafka. La traducción de fallos a HTTP vive en routes.
    """

    def __init__(
        self,
        cache: ExtractionCache,
        persist: Callable[[str, str], Awaitable[object]],
        publish: Callable[[dict], Awaitable[bool]],
        logger: logging.Logger,
    ) -> None:
        self._cache = cache
        self._persist = persist
        self._publish = publish
        self._logger = logger

    async def execute(
        self, content: bytes, checksum: str, extractor: PyPdfTextExtractor
    ) -> ExtractionResponse:
        cached = await self._cache.get(checksum)
        if cached is not None:
            try:
                response = ExtractionResponse(**cached)
            except ValidationError:
                self._logger.warning(
                    "payload de caché inválido (checksum=%s); se recalcula", checksum
                )
            else:
                enqueued = await self._publish(cached)
                response = response.model_copy(
                    update={"summary_status": "queued" if enqueued else "unavailable"}
                )
                self._logger.info(
                    "extraccion completada cache_hit=True pypdf_ms=%.2f persistence_ms=%.2f",
                    0.0, 0.0,
                )
                return response

        started = time.perf_counter()
        text = await extractor.extract_text_from_bytes(content)
        pypdf_ms = elapsed_ms(started)
        started = time.perf_counter()
        result = await self._persist(text, checksum)
        document = result.document
        persistence_ms = elapsed_ms(started)

        self._logger.info(
            "extraccion completada cache_hit=False pypdf_ms=%.2f persistence_ms=%.2f",
            pypdf_ms, persistence_ms,
        )
        response = ExtractionResponse(
            id=document.get("id"),
            content=document.get("content") or text,
            checksum=document.get("checksum") or checksum,
            text=text,
            summary_status="queued" if result.summary_enqueued else "unavailable",
        )
        await self._cache.set(checksum, response.model_dump())
        return response

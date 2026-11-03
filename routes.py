"""Extraction service: endpoints HTTP delegando en extractor y lógica de aplicación."""

import logging
import time
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from pydantic import BaseModel, ValidationError
from shared.domain import (
    MAX_PDF_SIZE_BYTES,
    PdfExtractionError,
    PyPdfTextExtractor,
    has_pdf_extension,
)

import app
from app import (
    compute_checksum,
    publish_document_extracted,
    save_to_persistence,
    validate_with_validation_service,
)

logger = logging.getLogger(__name__)

router = APIRouter()

CHUNK_SIZE = 1024 * 1024


def _elapsed_ms(started_at: float) -> float:
    """Milisegundos transcurridos desde ``started_at`` (perf_counter)."""
    return (time.perf_counter() - started_at) * 1000


class ExtractionResponse(BaseModel):
    id: str
    content: str
    checksum: str
    text: str
    # Campo aditivo: un cliente viejo lo ignora y uno nuevo sabe si debe
    # reintentar POST /summary/{id} cuando el evento no llegó a Kafka.
    summary_status: Literal["queued", "unavailable"] = "queued"


def get_extractor() -> PyPdfTextExtractor:
    return PyPdfTextExtractor()


async def _read_limited(file: UploadFile) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while chunk := await file.read(CHUNK_SIZE):
        total += len(chunk)
        if total > MAX_PDF_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail="El archivo excede el tamaño máximo permitido",
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _ensure_pdf_extension(filename: str | None) -> None:
    """Chequeo local barato: corta antes de leer el archivo si no es un PDF."""
    if not has_pdf_extension(filename):
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="El archivo debe tener extensión .pdf",
        )


async def _run_extraction(
    content: bytes,
    checksum: str,
    extractor: PyPdfTextExtractor,
) -> ExtractionResponse:
    """Cache-aside, extracción y persistencia compartidos por /extract y /upload."""
    try:
        # Si el mismo PDF ya se extrajo (TTL de 1 h), el resultado cacheado se
        # devuelve al instante sin pypdf ni persistence. El miss y el fail-open
        # viven en el adaptador; acá solo se consume el puerto.
        cached = await app.extraction_cache.get(checksum)
        if cached is not None:
            try:
                response = ExtractionResponse(**cached)
            except ValidationError:
                logger.warning(
                    "payload de caché inválido (checksum=%s); se recalcula", checksum
                )
            else:
                # Un cache hit es un reintento del cliente: si el evento original
                # se perdió, esta es la última oportunidad de re-emitirlo.
                summary_enqueued = await publish_document_extracted(cached)
                response = response.model_copy(
                    update={
                        "summary_status": "queued" if summary_enqueued else "unavailable"
                    }
                )
                logger.info(
                    "extraccion completada cache_hit=True pypdf_ms=%.2f persistence_ms=%.2f",
                    0.0,
                    0.0,
                )
                return response

        pypdf_started = time.perf_counter()
        text = await extractor.extract_text_from_bytes(content)
        pypdf_ms = _elapsed_ms(pypdf_started)

        persistence_started = time.perf_counter()
        persist_result = await save_to_persistence(text, checksum)
        document = persist_result.document
        persistence_ms = _elapsed_ms(persistence_started)

        logger.info(
            "extraccion completada cache_hit=False pypdf_ms=%.2f persistence_ms=%.2f",
            pypdf_ms,
            persistence_ms,
        )

        response = ExtractionResponse(
            id=document.get("id"),
            content=document.get("content") or text,
            checksum=document.get("checksum") or checksum,
            text=text,
            summary_status="queued" if persist_result.summary_enqueued else "unavailable",
        )
        await app.extraction_cache.set(checksum, response.model_dump())
        return response
    except PdfExtractionError as error:
        logger.error("no se pudo extraer texto del PDF: %s", error)
        raise HTTPException(
            status_code=422, detail="No se pudo extraer texto del PDF"
        ) from error
    except HTTPException:
        raise
    except Exception as error:
        logger.exception("error interno inesperado (%s)", type(error).__name__)
        raise HTTPException(status_code=500, detail="Error interno") from error


@router.post("/extract", response_model=ExtractionResponse)
async def extract_text(
    file: UploadFile,
    extractor: Annotated[PyPdfTextExtractor, Depends(get_extractor)],
) -> ExtractionResponse:
    """Extracción standalone: valida localmente con shared y no llama a Validation."""
    _ensure_pdf_extension(file.filename)
    content = await _read_limited(file)
    return await _run_extraction(content, compute_checksum(content), extractor)


@router.post("/upload", response_model=ExtractionResponse)
async def upload_document(
    file: UploadFile,
    extractor: Annotated[PyPdfTextExtractor, Depends(get_extractor)],
) -> ExtractionResponse:
    """Caso de uso compuesto: Validation → extracción → persistencia → evento.

    Fail-closed: si Validation rechaza o no responde, no se persiste ni se
    publica nada.
    """
    _ensure_pdf_extension(file.filename)
    content = await _read_limited(file)
    await validate_with_validation_service(
        content, file.filename or "documento.pdf"
    )
    return await _run_extraction(content, compute_checksum(content), extractor)
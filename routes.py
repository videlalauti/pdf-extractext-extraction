"""Extraction service: endpoints HTTP delegando en extractor y lógica de aplicación."""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
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
from schemas import ExtractionResponse
from use_cases import ExtractionPipeline

logger = logging.getLogger(__name__)

router = APIRouter()

CHUNK_SIZE = 1024 * 1024


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
    """Traducir errores a HTTP; el caso de uso real vive en use_cases.py."""
    pipeline = ExtractionPipeline(
        cache=app.extraction_cache,
        persist=save_to_persistence,
        publish=publish_document_extracted,
        logger=logger,
    )
    try:
        return await pipeline.execute(content, checksum, extractor)
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
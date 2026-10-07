"""Extraction service: endpoints HTTP delegando en extractor y lógica de aplicación."""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from pydantic import BaseModel, ValidationError
from shared.domain import (
    MAX_PDF_SIZE_BYTES,
    PdfExtractionError,
    PyPdfTextExtractor,
    has_pdf_extension,
)

import app
from app import compute_checksum, save_to_persistence

logger = logging.getLogger(__name__)

router = APIRouter()

CHUNK_SIZE = 1024 * 1024


class ExtractionResponse(BaseModel):
    id: str
    content: str
    checksum: str
    text: str


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


@router.post("/extract", response_model=ExtractionResponse)
async def extract_text(
    file: UploadFile,
    extractor: Annotated[PyPdfTextExtractor, Depends(get_extractor)],
) -> ExtractionResponse:
    if not has_pdf_extension(file.filename):
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="El archivo debe tener extensión .pdf",
        )

    try:
        content = await _read_limited(file)
        checksum = compute_checksum(content)

        # Cache-aside: si el mismo PDF ya se extrajo (TTL de 1 h), el resultado
        # cacheado se devuelve al instante sin pypdf ni persistence. El miss y
        # el fail-open viven en el adaptador; acá solo se consume el puerto.
        cached = await app.extraction_cache.get(checksum)
        if cached is not None:
            try:
                response = ExtractionResponse(**cached)
            except ValidationError:
                logger.warning(
                    "payload de caché inválido (checksum=%s); se recalcula", checksum
                )
            else:
                logger.info("cache hit (checksum=%s); se omite pypdf y persistence", checksum)
                return response

        text = await extractor.extract_text_from_bytes(content)
        persistence_response = await save_to_persistence(text, checksum)

        response = ExtractionResponse(
            id=persistence_response.get("id"),
            content=persistence_response.get("content") or text,
            checksum=persistence_response.get("checksum") or checksum,
            text=text,
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
"""Contratos de salida: usados por la capa de aplicación y por FastAPI."""
from typing import Literal

from pydantic import BaseModel


class ExtractionResponse(BaseModel):
    id: str
    content: str
    checksum: str
    text: str
    summary_status: Literal["queued", "unavailable"] = "queued"

"""Extraction service FastAPI application: bootstrap y montaje de componentes."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from shared.web.cors import add_cors
from shared.web.logging import RequestIdMiddleware, setup_logging

import app as app_state
from routes import router

SERVICE_NAME = "extraction-service"

setup_logging(SERVICE_NAME)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    try:
        yield
    finally:
        # Disposability: cierre ordenado del adaptador de caché al apagar.
        close = getattr(app_state.extraction_cache, "aclose", None)
        if close is not None:
            await close()


app = FastAPI(title="PDF Extraction Service", version="1.0.0", lifespan=lifespan)

app.add_middleware(RequestIdMiddleware)
add_cors(app)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": SERVICE_NAME}


app.include_router(router)
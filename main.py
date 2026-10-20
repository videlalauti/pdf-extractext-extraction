"""Extraction service FastAPI application: bootstrap y montaje de componentes."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from shared.web.cors import add_cors
from shared.web.logging import RequestIdMiddleware, TimingMiddleware, setup_logging

import app as app_state
from routes import router

SERVICE_NAME = "extraction-service"

setup_logging(SERVICE_NAME)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    # El publisher real se crea acá y se cierra en el shutdown; los tests usan
    # el Noop por defecto o inyectan un fake.
    app_state.event_publisher = app_state.build_event_publisher(app_state.settings)
    start = getattr(app_state.event_publisher, "start", None)
    if start is not None:
        await start()
    try:
        yield
    finally:
        # Disposability: cierre ordenado de caché y publisher al apagar.
        for resource in (app_state.extraction_cache, app_state.event_publisher):
            close = getattr(resource, "aclose", None)
            if close is not None:
                await close()


app = FastAPI(title="PDF Extraction Service", version="1.0.0", lifespan=lifespan)

# El último agregado envuelve a los anteriores, así que Timing va primero para
# quedar dentro del contexto de RequestIdMiddleware y heredar el request-id.
app.add_middleware(TimingMiddleware)
app.add_middleware(RequestIdMiddleware)
add_cors(app)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": SERVICE_NAME}


app.include_router(router)
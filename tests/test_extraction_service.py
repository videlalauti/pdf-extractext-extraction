import asyncio
import logging
import re
from functools import partial
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fakes import FailingRedisClient, FakeEventPublisher, FakeExtractionCache
from fastapi.testclient import TestClient
from shared.web.logging import RequestIdMiddleware, TimingMiddleware
from shared.web.resilience import retry_with_backoff as real_retry_with_backoff

import app as app_module
import routes
from cache import RedisExtractionCache
from main import app

client = TestClient(app)

PDF = b"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 5 0 R >>
endobj
5 0 obj
<< /Length 44 >>
stream
BT /F1 12 Tf 100 700 Td (Hello World) Tj ET
endstream
endobj
xref
0 4
0000000000 65535 f 
0000000009 00000 n 
0000000115 00000 n 
0000000266 00000 n 
trailer
<< /Size 4 /Root 1 0 R >>
startxref
0000
%%EOF"""

PERSISTED = {"id": "doc-1", "content": "", "checksum": "abc"}

# Ejercita la política real de reintentos sin esperar los backoffs en los tests.
_RETRY_WITHOUT_SLEEP = partial(real_retry_with_backoff, sleep=AsyncMock())


def _persistence_post(response):
    return patch("httpx.AsyncClient.post", new=AsyncMock(return_value=response))


def _http_response(status_code, payload=None):
    request = httpx.Request("POST", "http://persistence-service:8000/documents")
    return httpx.Response(status_code, json=payload, request=request)


def _validation_response(status_code, payload=None):
    request = httpx.Request("POST", "http://validation-service:8000/validate")
    return httpx.Response(status_code, json=payload, request=request)


def _validation_request():
    return httpx.Request("POST", "http://validation-service:8000/validate")


def _extraction_completed_messages(caplog):
    messages = []
    for record in caplog.records:
        message = record.getMessage()
        if record.name == "routes" and message.startswith("extraccion completada"):
            messages.append(message)
    return messages


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["service"] == "extraction-service"


def test_timing_middleware_is_registered_inside_request_id():
    registered = [middleware.cls for middleware in app.user_middleware]
    assert TimingMiddleware in registered
    # El menor índice es el más externo: RequestId debe correr antes que Timing
    # para que el log de duración herede el request-id.
    assert registered.index(RequestIdMiddleware) < registered.index(TimingMiddleware)


def test_extract_valid_pdf_returns_document_and_text():
    post = AsyncMock(
        return_value=MagicMock(
            status_code=201, json=lambda: PERSISTED, raise_for_status=lambda: None
        )
    )
    with patch("httpx.AsyncClient.post", new=post):
        response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"id", "content", "checksum", "text", "summary_status"}
    assert body["id"] == "doc-1"
    assert body["checksum"] == "abc"
    assert isinstance(body["text"], str)
    assert body["summary_status"] == "queued"


def test_extract_duplicate_pdf_returns_existing_document():
    post = AsyncMock(return_value=_http_response(409))
    existing = {"id": "doc-42", "content": "texto previo", "checksum": "abc"}
    get = AsyncMock(return_value=_http_response(200, existing))

    with patch("httpx.AsyncClient.post", new=post), patch("httpx.AsyncClient.get", new=get):
        response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "doc-42"
    assert body["content"] == "texto previo"
    assert body["checksum"] == "abc"
    assert isinstance(body["text"], str)


def test_extract_publishes_document_extracted_event(monkeypatch):
    publisher = FakeEventPublisher()
    monkeypatch.setattr(app_module, "event_publisher", publisher)
    post = AsyncMock(
        return_value=MagicMock(
            status_code=201, json=lambda: PERSISTED, raise_for_status=lambda: None
        )
    )

    with patch("httpx.AsyncClient.post", new=post):
        response = client.post(
            "/extract",
            files={"file": ("doc.pdf", PDF, "application/pdf")},
            headers={"X-Request-Id": "req-evento"},
        )

    assert response.status_code == 200
    assert publisher.events == [
        {
            "schema_version": 1,
            "document_id": "doc-1",
            "checksum": "abc",
            "source": "extraction",
            "request_id": "req-evento",
        }
    ]


def test_extract_duplicate_pdf_publishes_event_for_existing_document(monkeypatch):
    publisher = FakeEventPublisher()
    monkeypatch.setattr(app_module, "event_publisher", publisher)
    post = AsyncMock(return_value=_http_response(409))
    existing = {"id": "doc-42", "content": "texto previo", "checksum": "abc"}
    get = AsyncMock(return_value=_http_response(200, existing))

    with patch("httpx.AsyncClient.post", new=post), patch("httpx.AsyncClient.get", new=get):
        response = client.post(
            "/extract",
            files={"file": ("doc.pdf", PDF, "application/pdf")},
            headers={"X-Request-Id": "req-evento"},
        )

    assert response.status_code == 200
    assert publisher.events == [
        {
            "schema_version": 1,
            "document_id": "doc-42",
            "checksum": "abc",
            "source": "extraction",
            "request_id": "req-evento",
        }
    ]


def test_extract_survives_event_publisher_failure(monkeypatch):
    publisher = FakeEventPublisher(fail=True)
    monkeypatch.setattr(app_module, "event_publisher", publisher)
    monkeypatch.setattr(app_module, "retry_with_backoff", _RETRY_WITHOUT_SLEEP)
    post = AsyncMock(
        return_value=MagicMock(
            status_code=201, json=lambda: PERSISTED, raise_for_status=lambda: None
        )
    )

    with patch("httpx.AsyncClient.post", new=post):
        response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "doc-1"
    assert body["summary_status"] == "unavailable"


def test_extract_corrupt_pdf_returns_422_with_generic_detail():
    response = client.post(
        "/extract", files={"file": ("doc.pdf", b"%PDF-1.4 broken", "application/pdf")}
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "No se pudo extraer texto del PDF"


def test_extract_non_pdf_returns_415():
    response = client.post("/extract", files={"file": ("doc.txt", b"not a pdf", "text/plain")})

    assert response.status_code == 415


def test_extract_oversized_file_returns_413(monkeypatch):
    monkeypatch.setattr(routes, "MAX_PDF_SIZE_BYTES", 100)

    response = client.post(
        "/extract", files={"file": ("doc.pdf", b"x" * (200 * 1024), "application/pdf")}
    )

    assert response.status_code == 413


def test_upload_valid_pdf_validates_extracts_persists_and_publishes(monkeypatch):
    publisher = FakeEventPublisher()
    monkeypatch.setattr(app_module, "event_publisher", publisher)
    post = AsyncMock(
        side_effect=[
            _validation_response(200, {"valid": True, "error": None}),
            _http_response(201, PERSISTED),
        ]
    )

    with patch("httpx.AsyncClient.post", new=post):
        response = client.post("/upload", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "doc-1"
    assert body["summary_status"] == "queued"
    assert post.await_count == 2
    assert post.await_args_list[0].args == ("http://validation-service:8000/validate",)
    assert [event["document_id"] for event in publisher.events] == ["doc-1"]


@pytest.mark.parametrize(
    ("status_code", "detail"),
    [
        (400, "El archivo no tiene un formato PDF válido"),
        (413, "El archivo excede el tamaño máximo de 10 bytes"),
        (415, "El archivo debe tener extensión .pdf"),
    ],
)
def test_upload_forwards_validation_rejections(status_code, detail):
    publisher = FakeEventPublisher()
    post = AsyncMock(return_value=_validation_response(status_code, {"detail": detail}))

    with patch("httpx.AsyncClient.post", new=post), patch(
        "app.event_publisher", new=publisher
    ):
        response = client.post("/upload", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == status_code
    assert response.json()["detail"] == detail
    assert post.await_count == 1
    assert publisher.events == []


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ConnectError("connection refused", request=_validation_request()),
        httpx.ReadTimeout("timeout", request=_validation_request()),
        _validation_response(503, {"detail": "boom"}),
    ],
)
def test_upload_returns_503_when_validation_is_unavailable(failure):
    publisher = FakeEventPublisher()
    post = AsyncMock(side_effect=[failure])

    with patch("httpx.AsyncClient.post", new=post), patch(
        "app.event_publisher", new=publisher
    ):
        response = client.post("/upload", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 503
    assert response.json()["detail"] == "validation-service no disponible"
    assert post.await_count == 1
    assert publisher.events == []


def test_upload_duplicate_pdf_returns_existing_document(monkeypatch):
    publisher = FakeEventPublisher()
    monkeypatch.setattr(app_module, "event_publisher", publisher)
    existing = {"id": "doc-42", "content": "texto previo", "checksum": "abc"}
    post = AsyncMock(
        side_effect=[
            _validation_response(200, {"valid": True, "error": None}),
            _http_response(409),
        ]
    )
    get = AsyncMock(return_value=_http_response(200, existing))

    with patch("httpx.AsyncClient.post", new=post), patch("httpx.AsyncClient.get", new=get):
        response = client.post("/upload", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "doc-42"
    assert body["content"] == "texto previo"
    assert [event["document_id"] for event in publisher.events] == ["doc-42"]


def test_upload_non_pdf_returns_415_without_calling_validation():
    post = AsyncMock()

    with patch("httpx.AsyncClient.post", new=post):
        response = client.post(
            "/upload", files={"file": ("doc.txt", b"not a pdf", "text/plain")}
        )

    assert response.status_code == 415
    post.assert_not_awaited()


def test_upload_oversized_file_returns_413_without_calling_validation(monkeypatch):
    monkeypatch.setattr(routes, "MAX_PDF_SIZE_BYTES", 100)
    post = AsyncMock()

    with patch("httpx.AsyncClient.post", new=post):
        response = client.post(
            "/upload", files={"file": ("doc.pdf", b"x" * (200 * 1024), "application/pdf")}
        )

    assert response.status_code == 413
    post.assert_not_awaited()


class _SpyExtractor:
    """Extractor que registra cuántas veces se lo invocó."""

    def __init__(self) -> None:
        self.calls = 0

    async def extract_text_from_bytes(self, pdf_bytes: bytes) -> str:
        self.calls += 1
        return "texto extraido"


def test_extract_cache_hit_skips_extractor_and_persistence(monkeypatch):
    checksum = app_module.compute_checksum(PDF)
    payload = {"id": "doc-9", "content": "cacheado", "checksum": checksum, "text": "cacheado"}
    cache = FakeExtractionCache({checksum: payload})
    monkeypatch.setattr(app_module, "extraction_cache", cache)
    spy = _SpyExtractor()
    app.dependency_overrides[routes.get_extractor] = lambda: spy
    post = AsyncMock(return_value=_http_response(201, PERSISTED))

    with patch("httpx.AsyncClient.post", new=post):
        response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    assert response.json() == {**payload, "summary_status": "queued"}
    assert spy.calls == 0
    assert post.await_count == 0
    assert cache.gets == [checksum]


def test_extract_cache_hit_republishes_document_extracted(monkeypatch):
    checksum = app_module.compute_checksum(PDF)
    payload = {"id": "doc-9", "content": "cacheado", "checksum": checksum, "text": "cacheado"}
    cache = FakeExtractionCache({checksum: payload})
    monkeypatch.setattr(app_module, "extraction_cache", cache)
    publisher = FakeEventPublisher()
    monkeypatch.setattr(app_module, "event_publisher", publisher)

    response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    assert response.json()["summary_status"] == "queued"
    assert [event["document_id"] for event in publisher.events] == ["doc-9"]


def test_extract_cache_hit_survives_event_publisher_failure(monkeypatch):
    checksum = app_module.compute_checksum(PDF)
    payload = {"id": "doc-9", "content": "cacheado", "checksum": checksum, "text": "cacheado"}
    cache = FakeExtractionCache({checksum: payload})
    monkeypatch.setattr(app_module, "extraction_cache", cache)
    monkeypatch.setattr(app_module, "event_publisher", FakeEventPublisher(fail=True))
    monkeypatch.setattr(app_module, "retry_with_backoff", _RETRY_WITHOUT_SLEEP)

    response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "doc-9"
    assert body["summary_status"] == "unavailable"


def test_extract_cache_miss_extracts_persists_and_caches(monkeypatch):
    checksum = app_module.compute_checksum(PDF)
    cache = FakeExtractionCache()
    monkeypatch.setattr(app_module, "extraction_cache", cache)
    post = AsyncMock(return_value=_http_response(201, PERSISTED))

    with patch("httpx.AsyncClient.post", new=post):
        response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    assert post.await_count == 1
    assert cache.gets == [checksum]
    assert len(cache.sets) == 1
    cached_checksum, payload = cache.sets[0]
    assert cached_checksum == checksum
    assert payload == response.json()


def test_extract_with_failing_cache_still_returns_200(monkeypatch):
    cache = RedisExtractionCache("redis://redis:6379/0", 3600, client=FailingRedisClient())
    monkeypatch.setattr(app_module, "extraction_cache", cache)
    post = AsyncMock(return_value=_http_response(201, PERSISTED))

    with patch("httpx.AsyncClient.post", new=post):
        response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    assert post.await_count == 1


def test_extract_cache_with_corrupted_payload_recomputes(monkeypatch):
    checksum = app_module.compute_checksum(PDF)
    cache = FakeExtractionCache({checksum: {"id": "doc-roto"}})
    monkeypatch.setattr(app_module, "extraction_cache", cache)
    post = AsyncMock(return_value=_http_response(201, PERSISTED))

    with patch("httpx.AsyncClient.post", new=post):
        response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    assert post.await_count == 1


def test_extract_cache_hit_logs_zero_timings(monkeypatch, caplog):
    checksum = app_module.compute_checksum(PDF)
    payload = {"id": "doc-9", "content": "cacheado", "checksum": checksum, "text": "cacheado"}
    cache = FakeExtractionCache({checksum: payload})
    monkeypatch.setattr(app_module, "extraction_cache", cache)

    with caplog.at_level(logging.INFO, logger="routes"):
        response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    assert _extraction_completed_messages(caplog) == [
        "extraccion completada cache_hit=True pypdf_ms=0.00 persistence_ms=0.00"
    ]


def test_extract_cache_miss_logs_nonzero_timings(monkeypatch, caplog):
    """Aísla pypdf y persistence para medir los spans de instrumentación.

    Los fakes lentos hacen determinista que ambos tiempos superen 0.00; el
    retry y el 409 de persistence se cubren en los tests de app.
    """
    checksum = app_module.compute_checksum(PDF)
    cache = FakeExtractionCache()
    monkeypatch.setattr(app_module, "extraction_cache", cache)

    class _SlowExtractor:
        async def extract_text_from_bytes(self, pdf_bytes: bytes) -> str:
            await asyncio.sleep(0.001)
            return "texto extraido"

    async def _slow_save(text: str, _checksum: str) -> app_module.PersistResult:
        await asyncio.sleep(0.001)
        return app_module.PersistResult(document=dict(PERSISTED), summary_enqueued=True)

    app.dependency_overrides[routes.get_extractor] = lambda: _SlowExtractor()
    monkeypatch.setattr(routes, "save_to_persistence", _slow_save)

    with caplog.at_level(logging.INFO, logger="routes"):
        response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    messages = _extraction_completed_messages(caplog)
    assert len(messages) == 1
    message = messages[0]
    match = re.fullmatch(
        r"extraccion completada cache_hit=False pypdf_ms=(\d+\.\d{2}) "
        r"persistence_ms=(\d+\.\d{2})",
        message,
    )
    assert match is not None
    pypdf_ms, persistence_ms = (float(value) for value in match.groups())
    assert pypdf_ms > 0
    assert persistence_ms > 0
    assert "checksum=" not in message
    assert checksum not in message

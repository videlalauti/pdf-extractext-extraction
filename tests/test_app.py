"""Tests de la capa de aplicación: checksum, reintentos y duplicados ante persistence-service."""

import asyncio
import logging
from functools import partial
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fakes import FlakyEventPublisher
from fastapi import HTTPException
from shared.web.logging import reset_request_id, set_request_id
from shared.web.resilience import (
    CircuitBreaker,
    CircuitState,
)
from shared.web.resilience import (
    retry_with_backoff as real_retry_with_backoff,
)

import app
from app import (
    compute_checksum,
    publish_document_extracted,
    save_to_persistence,
    settings,
)

URL = f"{settings.persistence_service_url}/documents"
BY_CHECKSUM_URL = f"{URL}/by-checksum/abc123"
REQUEST = httpx.Request("POST", URL)

TEXT = "texto extraido del pdf"
CHECKSUM = "abc123"
PAYLOAD = {"id": "doc-1", "content": TEXT, "checksum": CHECKSUM}
EXISTING = {"id": "doc-42", "content": "texto previo", "checksum": CHECKSUM}


def _response(status_code: int, payload: dict | None = None) -> httpx.Response:
    return httpx.Response(status_code, json=payload, request=REQUEST)


def _connect_error() -> httpx.ConnectError:
    return httpx.ConnectError("connection refused", request=REQUEST)


# Ejercita la política real de reintentos sin esperar los backoffs en los tests.
_RETRY_WITHOUT_SLEEP = partial(real_retry_with_backoff, sleep=AsyncMock())


def test_compute_checksum_matches_known_sha256_digest():
    assert compute_checksum(b"abc") == (
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_compute_checksum_is_stable_for_the_same_input():
    content = b"%PDF-1.4 contenido identico"

    assert compute_checksum(content) == compute_checksum(content)


def test_compute_checksum_differs_for_different_input():
    assert compute_checksum(b"documento a") != compute_checksum(b"documento b")


def test_persistence_client_uses_configured_timeout(monkeypatch):
    monkeypatch.setattr(app.settings, "persistence_call_timeout_seconds", 7.5)
    client_factory = MagicMock()

    with patch("app.httpx.AsyncClient", new=client_factory):
        app._persistence_client()

    client_factory.assert_called_once_with(timeout=7.5)


def test_publish_document_extracted_retries_until_success(monkeypatch):
    publisher = FlakyEventPublisher(failures=2)
    monkeypatch.setattr(app, "event_publisher", publisher)
    monkeypatch.setattr(app, "retry_with_backoff", _RETRY_WITHOUT_SLEEP)

    published = asyncio.run(publish_document_extracted(PAYLOAD))

    assert published is True
    assert publisher.attempts == 3
    assert [event["document_id"] for event in publisher.events] == ["doc-1"]


def test_publish_document_extracted_returns_false_after_retries(monkeypatch, caplog):
    publisher = FlakyEventPublisher(failures=99)
    monkeypatch.setattr(app, "event_publisher", publisher)
    monkeypatch.setattr(app, "retry_with_backoff", _RETRY_WITHOUT_SLEEP)

    with caplog.at_level(logging.ERROR, logger="app"):
        published = asyncio.run(publish_document_extracted(PAYLOAD))

    assert published is False
    assert publisher.attempts == 3
    assert any(record.levelno == logging.ERROR for record in caplog.records)


def test_save_to_persistence_reports_summary_unavailable(monkeypatch):
    publisher = FlakyEventPublisher(failures=99)
    monkeypatch.setattr(app, "event_publisher", publisher)
    monkeypatch.setattr(app, "retry_with_backoff", _RETRY_WITHOUT_SLEEP)
    post = AsyncMock(return_value=_response(201, PAYLOAD))

    with patch("httpx.AsyncClient.post", new=post):
        result = asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert result.document == PAYLOAD
    assert result.summary_enqueued is False
    assert publisher.attempts == 3


def test_save_to_persistence_returns_response_on_first_attempt():
    post = AsyncMock(return_value=_response(201, PAYLOAD))

    with patch("httpx.AsyncClient.post", new=post):
        result = asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert result.document == PAYLOAD
    assert result.summary_enqueued is True
    post.assert_called_once_with(
        URL, json={"content": TEXT, "checksum": CHECKSUM}, headers={}
    )


def test_save_to_persistence_retries_until_success():
    post = AsyncMock(side_effect=[_connect_error(), _connect_error(), _response(201, PAYLOAD)])

    with patch("httpx.AsyncClient.post", new=post):
        result = asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert result.document == PAYLOAD
    assert result.summary_enqueued is True
    assert post.await_count == 3


@pytest.mark.parametrize("failure", [_connect_error(), _response(500)])
def test_save_to_persistence_raises_502_after_three_failed_attempts(failure):
    post = AsyncMock(side_effect=[failure] * 3)

    with patch("httpx.AsyncClient.post", new=post), pytest.raises(HTTPException) as exc_info:
        asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail == "No se pudo guardar el documento en persistence-service"
    assert post.await_count == 3


@pytest.mark.parametrize("status_code", [400, 422])
def test_save_to_persistence_forwards_4xx_without_retry(status_code):
    post = AsyncMock(side_effect=[_response(status_code)] * 3)

    with patch("httpx.AsyncClient.post", new=post), pytest.raises(HTTPException) as exc_info:
        asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert exc_info.value.status_code == status_code
    assert exc_info.value.detail == "No se pudo guardar el documento en persistence-service"
    assert post.await_count == 1


def test_save_to_persistence_returns_existing_document_on_conflict():
    post = AsyncMock(return_value=_response(409))
    get = AsyncMock(return_value=_response(200, EXISTING))

    with patch("httpx.AsyncClient.post", new=post), patch("httpx.AsyncClient.get", new=get):
        result = asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert result.document == EXISTING
    assert result.summary_enqueued is True
    post.assert_awaited_once_with(
        URL, json={"content": TEXT, "checksum": CHECKSUM}, headers={}
    )
    get.assert_awaited_once_with(BY_CHECKSUM_URL, headers={})


def test_save_to_persistence_skips_conflict_lookup_without_conflict():
    post = AsyncMock(return_value=_response(201, PAYLOAD))
    get = AsyncMock()

    with patch("httpx.AsyncClient.post", new=post), patch("httpx.AsyncClient.get", new=get):
        result = asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert result.document == PAYLOAD
    assert result.summary_enqueued is True
    get.assert_not_awaited()


def test_save_to_persistence_forwards_request_id_to_post():
    post = AsyncMock(return_value=_response(201, PAYLOAD))
    token = set_request_id("req-post")
    try:
        with patch("httpx.AsyncClient.post", new=post):
            result = asyncio.run(save_to_persistence(TEXT, CHECKSUM))
    finally:
        reset_request_id(token)

    assert result.document == PAYLOAD
    assert result.summary_enqueued is True
    post.assert_awaited_once_with(
        URL,
        json={"content": TEXT, "checksum": CHECKSUM},
        headers={"X-Request-Id": "req-post"},
    )


def test_save_to_persistence_forwards_request_id_to_get():
    post = AsyncMock(return_value=_response(409))
    get = AsyncMock(return_value=_response(200, EXISTING))
    token = set_request_id("req-get")
    try:
        with patch("httpx.AsyncClient.post", new=post), patch(
            "httpx.AsyncClient.get", new=get
        ):
            result = asyncio.run(save_to_persistence(TEXT, CHECKSUM))
    finally:
        reset_request_id(token)

    assert result.document == EXISTING
    assert result.summary_enqueued is True
    get.assert_awaited_once_with(
        BY_CHECKSUM_URL, headers={"X-Request-Id": "req-get"}
    )


@pytest.mark.parametrize("lookup_failure", [_connect_error(), _response(404)])
def test_save_to_persistence_raises_502_when_conflict_lookup_fails(lookup_failure):
    post = AsyncMock(return_value=_response(409))
    get = AsyncMock(side_effect=[lookup_failure])

    with (
        patch("httpx.AsyncClient.post", new=post),
        patch("httpx.AsyncClient.get", new=get),
        pytest.raises(HTTPException) as exc_info,
    ):
        asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail == "No se pudo guardar el documento en persistence-service"
    assert post.await_count == 1
    assert get.await_count == 1


def _local_breaker(**overrides) -> CircuitBreaker:
    params = {
        "service": "persistence",
        "failure_threshold": 5,
        "recovery_timeout": 30,
        "call_timeout": 120,
    }
    params.update(overrides)
    return CircuitBreaker(**params)


def test_breaker_settings_default_to_current_values(monkeypatch):
    for variable in (
        "PERSISTENCE_FAILURE_THRESHOLD",
        "PERSISTENCE_RECOVERY_TIMEOUT_SECONDS",
        "PERSISTENCE_CALL_TIMEOUT_SECONDS",
    ):
        monkeypatch.delenv(variable, raising=False)

    config = app.Settings()
    breaker = app.build_persistence_breaker(config)

    assert config.persistence_failure_threshold == 5
    assert config.persistence_recovery_timeout_seconds == 30
    assert config.persistence_call_timeout_seconds == 120
    assert breaker.failure_threshold == 5
    assert breaker.recovery_timeout == 30
    assert breaker.call_timeout == 120
    assert app.persistence_breaker.failure_threshold == 5
    assert app.persistence_breaker.recovery_timeout == 30
    assert app.persistence_breaker.call_timeout == 120


def test_breaker_settings_read_environment_overrides(monkeypatch):
    monkeypatch.setenv("PERSISTENCE_FAILURE_THRESHOLD", "2")
    monkeypatch.setenv("PERSISTENCE_RECOVERY_TIMEOUT_SECONDS", "1.5")
    monkeypatch.setenv("PERSISTENCE_CALL_TIMEOUT_SECONDS", "7.5")

    config = app.Settings()
    breaker = app.build_persistence_breaker(config)

    assert config.persistence_failure_threshold == 2
    assert config.persistence_recovery_timeout_seconds == 1.5
    assert config.persistence_call_timeout_seconds == 7.5
    assert breaker.failure_threshold == 2
    assert breaker.recovery_timeout == 1.5
    assert breaker.call_timeout == 7.5


def test_breaker_opens_and_short_circuits_without_touching_peer(monkeypatch):
    breaker = _local_breaker(failure_threshold=1)
    monkeypatch.setattr(app, "persistence_breaker", breaker)

    post = AsyncMock(side_effect=_connect_error())

    with patch("httpx.AsyncClient.post", new=post), pytest.raises(HTTPException) as exc_info:
        asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert exc_info.value.status_code == 502
    assert post.await_count == 3
    assert breaker.state is CircuitState.OPEN

    with patch("httpx.AsyncClient.post", new=post), pytest.raises(HTTPException) as exc_info:
        asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert exc_info.value.status_code == 502
    assert post.await_count == 3


def test_breaker_half_open_recovers_to_closed(monkeypatch):
    breaker = _local_breaker(failure_threshold=1, recovery_timeout=0.2)
    monkeypatch.setattr(app, "persistence_breaker", breaker)

    post = AsyncMock(side_effect=_connect_error())
    with patch("httpx.AsyncClient.post", new=post), pytest.raises(HTTPException):
        asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert breaker.failure_count == 1

    asyncio.run(asyncio.sleep(0.3))
    assert breaker.state is CircuitState.HALF_OPEN

    post.side_effect = [_response(201, PAYLOAD)]
    with patch("httpx.AsyncClient.post", new=post):
        result = asyncio.run(save_to_persistence(TEXT, CHECKSUM))

    assert result.document == PAYLOAD
    assert result.summary_enqueued is True
    assert breaker.state is CircuitState.CLOSED
    assert post.await_count == 4

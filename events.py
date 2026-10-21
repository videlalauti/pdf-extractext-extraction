"""Puerto de publicación de eventos de dominio y adaptadores (Kafka y Noop).

El servicio depende del Protocol :class:`EventPublisher`, nunca de ``aiokafka``:
la dependencia se inyecta en la composición (módulo ``app``) y los tests usan
fakes. El adaptador Kafka es *fail-open*: si el broker no está disponible, se
loguea un warning y el request de extracción sigue su curso.
"""

import contextlib
import json
import logging
from typing import Protocol, runtime_checkable

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)

DOCUMENT_EXTRACTED_TOPIC = "documents.extracted"
EVENT_SCHEMA_VERSION = 1
EVENT_SOURCE = "extraction"
# Timeout acotado: si Kafka está caído, fallar rápido y seguir (fail-open) en
# lugar de dejar el request esperando el timeout de 40 s por defecto.
KAFKA_REQUEST_TIMEOUT_MS = 2000


@runtime_checkable
class EventPublisher(Protocol):
    """Puerto de salida hacia el bus de eventos de dominio.

    Expone exactamente lo que usa la capa de aplicación: publicar un evento ya
    construido y liberar la conexión al apagar.
    """

    async def publish(self, event: dict) -> None:
        """Publica un evento; nunca debe romper el request que lo origina."""

    async def aclose(self) -> None:
        """Libera los recursos del publisher (conexión al broker)."""


class NoopEventPublisher:
    """Publicación deshabilitada: descarta los eventos."""

    async def publish(self, event: dict) -> None:
        return None

    async def aclose(self) -> None:
        return None


def build_document_extracted_event(document: dict) -> dict:
    """Arma el evento ``document.extracted`` (schema v1) del documento persistido."""
    return {
        "schema_version": EVENT_SCHEMA_VERSION,
        "document_id": document["id"],
        "checksum": document["checksum"],
        "source": EVENT_SOURCE,
    }


class KafkaEventPublisher:
    """Adaptador de :class:`EventPublisher` sobre aiokafka.

    Fail-open: un broker caído se reporta como warning y el evento se descarta;
    la extracción y la respuesta al cliente no dependen de Kafka.
    """

    def __init__(
        self, bootstrap_servers: str, topic: str = DOCUMENT_EXTRACTED_TOPIC
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Conecta el producer; si el broker no responde, sigue sin él."""
        producer: AIOKafkaProducer | None = None
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,
            )
            await producer.start()
        except Exception as error:
            logger.warning(
                "Kafka no disponible (%s); se publicará sin broker: %s",
                self._bootstrap_servers,
                type(error).__name__,
                exc_info=True,
            )
            if producer is not None:
                with contextlib.suppress(Exception):
                    await producer.stop()
            return
        self._producer = producer

    async def publish(self, event: dict) -> None:
        if self._producer is None:
            logger.warning(
                "Kafka no disponible; se descarta %s (document_id=%s)",
                DOCUMENT_EXTRACTED_TOPIC,
                event.get("document_id"),
            )
            return
        try:
            await self._producer.send_and_wait(
                self._topic, value=json.dumps(event).encode("utf-8")
            )
        except Exception as error:
            logger.warning(
                "no se pudo publicar %s (document_id=%s); se ignora: %s",
                self._topic,
                event.get("document_id"),
                type(error).__name__,
                exc_info=True,
            )

    async def aclose(self) -> None:
        if self._producer is None:
            return
        producer, self._producer = self._producer, None
        try:
            await producer.stop()
        except Exception as error:
            logger.warning(
                "no se pudo cerrar el producer de Kafka: %s",
                type(error).__name__,
                exc_info=True,
            )

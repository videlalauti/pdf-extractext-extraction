# pdf-extractext-extraction

Servicio de extracción de texto de archivos PDF. Expone una API FastAPI para extraer el contenido de documentos PDF.

## Endpoints

| Método | Ruta       | Descripción |
| ------ | ---------- | ----------- |
| `POST` | `/upload`  | **Caso de uso principal:** valida con Validation, extrae, persiste y publica `documents.extracted` en una sola llamada. |
| `POST` | `/extract` | Extracción standalone con validación local de `shared`. Mantiene la compatibilidad con clientes existentes. |
| `GET`  | `/health`  | Healthcheck del servicio. |

`POST /upload` es **fail-closed** respecto de Validation: si rechaza el PDF
(`400`/`413`/`415`) se reenvía el mismo status, y si no responde (timeout,
error de red o `5xx`) devuelve `503` sin persistir ni publicar nada. La
secuencia manual `/validate` → `/extract` → `POST /documents` sigue existiendo
como equivalente, pero el flujo recomendado es una sola llamada:

```bash
curl -F "file=@documento.pdf" http://extraction.pdf.localhost/upload
```

## Instalación

Instalar las dependencias:

```bash
pip install -r requirements.txt
```

Para desarrollo también hacen falta `pytest` y `ruff`, que no van en la imagen de producción:

```bash
pip install -r requirements-dev.txt
```

`requirements-dev.txt` ya incluye `requirements.txt`, así que alcanza con ese comando para trabajar localmente.

## Modos de ejecución

### Compose (recomendado)

Todo el stack corre por la red interna de compose y se accede solo a través de
Traefik (`http://extraction.pdf.localhost`). En este modo extraction resuelve
Kafka, Redis, persistence y validation por sus nombres de servicio, sin publicar
puertos directos.

### Local con uvicorn

Para correr extraction solo contra los backing services hace falta un override
de infra que publique `8001` (validation), `8003` (persistence), `9092` (Kafka) y
`6379` (Redis) en el host. Sin ese override, los hosts `localhost` no resuelven
y lo más simple es levantar todo con compose.

Con el override activo:

```bash
uvicorn main:app --reload --port 8002
```

El `.env.example` trae los valores de `localhost` para este modo; el bloque de
compose queda comentado para no mezclar entornos. La imagen distroless usa
`entrypoint.py`, que lee `PORT` para arrancar uvicorn.

## Tests

Correr los tests:

```bash
pytest tests/ -v
```

## Lint

Verificar estilo y errores con ruff:

```bash
ruff check .
```

Ambos comandos se ejecutan en cada push y PR mediante GitHub Actions (`.github/workflows/ci.yml`).

## Variables de entorno

| Variable                        | Descripción                                  | Valor por defecto        |
| ------------------------------- | -------------------------------------------- | ------------------------ |
| `PORT`                          | Puerto de escucha de uvicorn                 | `8000`                   |
| `PERSISTENCE_SERVICE_URL`       | URL base del servicio de persistencia        | `http://persistence-service:8000` |
| `PERSISTENCE_FAILURE_THRESHOLD` | Fallas consecutivas antes de abrir el circuito | `5`                      |
| `PERSISTENCE_RECOVERY_TIMEOUT_SECONDS` | Ventana antes de sondear la recuperación (segundos) | `30`              |
| `PERSISTENCE_CALL_TIMEOUT_SECONDS` | Tiempo máximo de espera por llamada a persistence (segundos) | `120` |
| `VALIDATION_SERVICE_URL`        | URL base del servicio de validación          | `http://validation-service:8000` |
| `VALIDATION_CALL_TIMEOUT_SECONDS` | Tiempo máximo de espera por llamada a validation (segundos) | `10` |
| `REDIS_URL`                     | URL de Redis para la caché de extracción     | `redis://redis:6379/0`   |
| `EXTRACTION_CACHE_TTL_SECONDS`  | TTL de la caché de resultados (segundos)     | `3600`                   |
| `EXTRACTION_CACHE_ENABLED`      | Habilita la caché (`false` la desactiva)     | `true`                   |
| `KAFKA_BOOTSTRAP_SERVERS`       | Bootstrap servers de Kafka para eventos      | `localhost:9092`         |

Si una llamada a persistence supera `PERSISTENCE_CALL_TIMEOUT_SECONDS`, el
circuit breaker la cuenta como falla y puede abrir el circuito. Bajarlo por
debajo de ~95 s hace que el breaker corte antes de que termine la política de
reintentos (3 x 30 s + sleeps) y convierte una persistence lenta pero viva en
falla.

La caché es *cache-aside* y **fail-open**: si Redis no está disponible, el
servicio extrae y persiste igual, solo loguea un warning. La clave es
`extract:{sha256}` del PDF.

Los eventos `document.extracted` se publican en Kafka con **3 intentos y backoff**
(0,5 s / 1 s con jitter). Si el broker sigue caído, la respuesta sigue siendo
`200` —el documento ya está persistido— pero incluye
`summary_status: "unavailable"` en vez de `"queued"`: el cliente debe reintentar
`POST /summary/{document_id}`. Un cache hit también republica el evento, porque
es la última oportunidad de recuperar un resumen perdido.
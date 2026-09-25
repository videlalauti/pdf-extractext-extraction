# pdf-extractext-extraction

Servicio de extracción de texto de archivos PDF. Expone una API FastAPI para extraer el contenido de documentos PDF.

## Instalación

Instalar las dependencias:

```bash
pip install -r requirements.txt
```

## Ejecución

Levantar el servidor en modo desarrollo:

```bash
uvicorn main:app --reload --port 8002
```

## Tests

Correr los tests:

```bash
pytest tests/ -v
```

## Variables de entorno

| Variable                  | Descripción                                  | Valor por defecto        |
| ------------------------- | -------------------------------------------- | ------------------------ |
| `PERSISTENCE_SERVICE_URL` | URL base del servicio de persistencia        | `http://persistence.localhost` |
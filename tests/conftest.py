import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _reset_persistence_breaker():
    from app import persistence_breaker

    persistence_breaker.reset()
    yield
    persistence_breaker.reset()


@pytest.fixture(autouse=True)
def _isolate_extraction_cache():
    """Caché Noop por defecto: los tests no dependen de Redis ni de la config."""
    import app as app_module
    from cache import NoopExtractionCache

    original = app_module.extraction_cache
    app_module.extraction_cache = NoopExtractionCache()
    yield
    app_module.extraction_cache = original


@pytest.fixture(autouse=True)
def _clear_dependency_overrides():
    from main import app

    app.dependency_overrides.clear()
    yield
    app.dependency_overrides.clear()
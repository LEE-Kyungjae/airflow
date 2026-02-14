"""Shared fixtures for router tests."""

import sys
from pathlib import Path

import pytest
from unittest.mock import MagicMock, patch

# Add api/ to sys.path
_api_dir = str(Path(__file__).resolve().parent.parent.parent.parent / "api")
if _api_dir not in sys.path:
    sys.path.insert(0, _api_dir)

from fastapi.testclient import TestClient


@pytest.fixture
def mock_mongo():
    """Create mock MongoService for router tests."""
    mock_instance = MagicMock()
    mock_db = MagicMock()
    mock_db.command.return_value = {"ok": 1}
    mock_db.sources = MagicMock()
    mock_db.crawlers = MagicMock()
    mock_db.error_logs = MagicMock()
    mock_db.crawl_results = MagicMock()
    mock_db.data_reviews = MagicMock()
    mock_db.schemas = MagicMock()
    mock_db.data_catalog = MagicMock()
    mock_db.data_versions = MagicMock()
    mock_db.review_sessions = MagicMock()
    mock_db.list_collection_names.return_value = []
    mock_instance.db = mock_db
    mock_instance.close = MagicMock()
    mock_instance.database_name = "test_db"
    return mock_instance


@pytest.fixture
def client(mock_mongo):
    """Create TestClient with mocked MongoDB and auth bypassed."""
    with patch("app.services.mongo_service.MongoService") as MockClass:
        MockClass.return_value = mock_mongo
        from app.main import app
        from app.auth.dependencies import require_auth, optional_auth, AuthContext

        # Bypass auth for router unit tests
        test_auth = AuthContext(
            auth_type="api_key", user_id="test-user", role="admin", scopes=["admin"]
        )
        app.dependency_overrides[require_auth] = lambda: test_auth
        app.dependency_overrides[optional_auth] = lambda: test_auth

        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

        app.dependency_overrides.clear()

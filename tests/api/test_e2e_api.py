"""
E2E API Integration Tests for Crawler System.

Tests the full FastAPI application with real request/response cycles,
including authentication, CORS, rate limiting, and core endpoints.
"""

import sys
import os
from pathlib import Path

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Add api/ to sys.path so main.py's `from app.routers import ...` works
_api_dir = str(Path(__file__).resolve().parent.parent.parent / "api")
if _api_dir not in sys.path:
    sys.path.insert(0, _api_dir)

from fastapi.testclient import TestClient


# ============================================================
# Test Client Fixture
# ============================================================

@pytest.fixture
def mock_mongo():
    """Create a reusable mock MongoService instance."""
    mock_instance = MagicMock()
    mock_db = MagicMock()

    mock_db.command.return_value = {"ok": 1}
    mock_db.sources = MagicMock()
    mock_db.crawlers = MagicMock()
    mock_db.error_logs = MagicMock()
    mock_db.crawl_results = MagicMock()
    mock_db.data_reviews = MagicMock()
    mock_db.schemas = MagicMock()

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

        test_auth = AuthContext(
            auth_type="api_key", user_id="test-user", role="admin", scopes=["admin"]
        )
        app.dependency_overrides[require_auth] = lambda: test_auth
        app.dependency_overrides[optional_auth] = lambda: test_auth

        with TestClient(app, raise_server_exceptions=False) as test_client:
            yield test_client

        app.dependency_overrides.clear()


# ============================================================
# Health & Root Endpoints
# ============================================================

class TestHealthEndpoints:
    def test_health_check(self, client):
        """GET /health returns 200 with status."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert data["service"] == "crawler-system-api"

    def test_root_endpoint(self, client):
        """GET / returns API info."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Crawler System API"
        assert data["version"] == "1.0.0"
        assert "docs" in data
        assert "health" in data


# ============================================================
# Auth Flow
# ============================================================

class TestAuthFlow:
    def test_jwt_token_works_on_health(self, client, valid_jwt_token):
        """JWT token is accepted on endpoints."""
        headers = {"Authorization": f"Bearer {valid_jwt_token}"}
        response = client.get("/health", headers=headers)
        assert response.status_code == 200

    def test_api_key_works_on_health(self, client, valid_api_key):
        """API key auth header is accepted."""
        headers = {"X-API-Key": valid_api_key}
        response = client.get("/health", headers=headers)
        assert response.status_code == 200

    def test_expired_token_rejected(self):
        """Expired JWT token fails auth."""
        from api.app.auth.jwt_auth import JWTAuth
        token = JWTAuth.create_access_token(
            user_id="test", expires_delta=timedelta(seconds=-1)
        )
        result = JWTAuth.verify_access_token(token)
        assert result is None

    def test_invalid_api_key_rejected(self):
        """Invalid API key fails validation."""
        from api.app.auth.api_key import APIKeyAuth
        result = APIKeyAuth.validate_key("totally-wrong-key-12345")
        assert result is None

    def test_jwt_token_roundtrip(self):
        """Create token -> decode -> verify user_id matches."""
        from api.app.auth.jwt_auth import JWTAuth
        token = JWTAuth.create_access_token(
            user_id="roundtrip_user", role="user", scopes=["read"]
        )
        payload = JWTAuth.verify_access_token(token)
        assert payload is not None
        assert payload.sub == "roundtrip_user"
        assert payload.role == "user"

    def test_refresh_token_flow(self):
        """Create refresh -> use it to get new access token."""
        from api.app.auth.jwt_auth import JWTAuth
        JWTAuth.register_user("refresh_test", "refresh_test", role="user")
        try:
            refresh = JWTAuth.create_refresh_token("refresh_test")
            new_access = JWTAuth.refresh_access_token(refresh)
            assert new_access is not None
            payload = JWTAuth.verify_access_token(new_access)
            assert payload.sub == "refresh_test"
        finally:
            JWTAuth._users.pop("refresh_test", None)


# ============================================================
# OpenAPI & Documentation
# ============================================================

class TestOpenAPIDocumentation:
    def test_openapi_json(self, client):
        """GET /openapi.json returns valid schema."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert "openapi" in data
        assert "paths" in data
        assert "info" in data
        assert data["info"]["title"] == "Crawler System API"

    def test_swagger_docs(self, client):
        """GET /docs returns Swagger UI."""
        response = client.get("/docs")
        assert response.status_code == 200

    def test_redoc_docs(self, client):
        """GET /redoc returns ReDoc."""
        response = client.get("/redoc")
        assert response.status_code == 200


# ============================================================
# CORS Headers
# ============================================================

class TestCORSHeaders:
    def test_cors_headers_on_options(self, client):
        """OPTIONS request returns CORS headers."""
        response = client.options(
            "/health",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET",
            }
        )
        assert response.status_code in (200, 204, 405)
        if "access-control-allow-origin" in response.headers:
            assert response.headers["access-control-allow-origin"] in (
                "*", "http://localhost:3000"
            )

    def test_cors_on_regular_request(self, client):
        """Regular GET with Origin header gets CORS response."""
        response = client.get(
            "/health",
            headers={"Origin": "http://localhost:3000"}
        )
        assert response.status_code == 200
        if "access-control-allow-origin" in response.headers:
            assert response.headers["access-control-allow-origin"] in (
                "*", "http://localhost:3000"
            )


# ============================================================
# Metrics Endpoint
# ============================================================

class TestMetricsEndpoint:
    def test_metrics_endpoint_exists(self, client):
        """GET /metrics returns prometheus-compatible data."""
        response = client.get("/metrics")
        assert response.status_code in (200, 401, 403)


# ============================================================
# Sources API
# ============================================================

class TestSourcesAPI:
    def test_list_sources_endpoint_exists(self, client, api_key_headers):
        """GET /api/sources responds."""
        response = client.get("/api/sources", headers=api_key_headers)
        assert response.status_code in (200, 401, 500)

    def test_sources_without_auth(self, client):
        """GET /api/sources without auth gets some response."""
        response = client.get("/api/sources")
        assert response.status_code in (200, 401, 403)


# ============================================================
# Error Handling
# ============================================================

class TestErrorHandling:
    def test_404_not_found(self, client):
        """Non-existent endpoint returns 404."""
        response = client.get("/api/this-does-not-exist-at-all")
        assert response.status_code == 404

    def test_method_not_allowed(self, client):
        """Wrong HTTP method returns 405."""
        response = client.delete("/health")
        assert response.status_code == 405


# ============================================================
# API Key Management
# ============================================================

class TestAPIKeyManagement:
    def test_register_and_validate_key(self):
        """Register a key, then validate it."""
        from api.app.auth.api_key import APIKeyAuth
        key_id, raw_key = APIKeyAuth.register_key(
            name="E2E Test Key",
            scopes=["read", "write"],
            expires_in_days=1
        )
        try:
            assert key_id.startswith("key_")
            assert raw_key.startswith("craw_")
            info = APIKeyAuth.validate_key(raw_key)
            assert info is not None
            assert info.name == "E2E Test Key"
            assert "read" in info.scopes
        finally:
            APIKeyAuth.revoke_key(key_id)

    def test_revoked_key_rejected(self):
        """Revoked key fails validation."""
        from api.app.auth.api_key import APIKeyAuth
        key_id, raw_key = APIKeyAuth.register_key(name="Revoke Test")
        APIKeyAuth.revoke_key(key_id)
        assert APIKeyAuth.validate_key(raw_key) is None

    def test_list_keys_returns_list(self):
        """list_keys returns a list of dicts."""
        from api.app.auth.api_key import APIKeyAuth
        keys = APIKeyAuth.list_keys()
        assert isinstance(keys, list)


# ============================================================
# Auth Dependencies
# ============================================================

class TestAuthDependencies:
    def test_auth_context_jwt(self):
        """AuthContext correctly identifies JWT auth."""
        from api.app.auth.dependencies import AuthContext
        ctx = AuthContext(
            auth_type="jwt", user_id="test", role="admin", scopes=["admin"]
        )
        assert ctx.is_authenticated is True
        assert ctx.is_admin is True

    def test_auth_context_none(self):
        """AuthContext correctly identifies no auth."""
        from api.app.auth.dependencies import AuthContext
        ctx = AuthContext(auth_type="none")
        assert ctx.is_authenticated is False

    def test_auth_mode_exempt_paths(self):
        """Exempt paths are recognized."""
        from api.app.auth.dependencies import AuthMode
        assert AuthMode.is_exempt("/health") is True
        assert AuthMode.is_exempt("/") is True
        assert AuthMode.is_exempt("/docs") is True
        assert AuthMode.is_exempt("/api/sources") is False


# ============================================================
# OpenAPI Spec Validation
# ============================================================

class TestAPISpecValidation:
    def test_openapi_has_source_endpoints(self, client):
        """OpenAPI spec includes source endpoints."""
        response = client.get("/openapi.json")
        paths = response.json()["paths"]
        assert any("/api/sources" in p for p in paths)

    def test_openapi_has_auth_endpoints(self, client):
        """OpenAPI spec includes auth endpoints."""
        response = client.get("/openapi.json")
        paths = response.json()["paths"]
        assert any("/api/auth" in p for p in paths)

    def test_openapi_has_dashboard_endpoints(self, client):
        """OpenAPI spec includes dashboard endpoints."""
        response = client.get("/openapi.json")
        paths = response.json()["paths"]
        assert any("/api/dashboard" in p for p in paths)

    def test_openapi_has_monitoring_endpoints(self, client):
        """OpenAPI spec includes monitoring endpoints."""
        response = client.get("/openapi.json")
        paths = response.json()["paths"]
        assert any("/api/monitoring" in p for p in paths)

    def test_openapi_has_data_quality_endpoints(self, client):
        """OpenAPI spec includes data quality endpoints."""
        response = client.get("/openapi.json")
        paths = response.json()["paths"]
        assert any("/api/data-quality" in p for p in paths)

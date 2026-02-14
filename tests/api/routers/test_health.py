"""Tests for health and root endpoints."""


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert data["service"] == "crawler-system-api"

    def test_health_response_schema(self, client):
        data = client.get("/health").json()
        assert {"status", "timestamp", "service"}.issubset(set(data.keys()))


class TestRootEndpoint:
    def test_root_returns_200(self, client):
        resp = client.get("/")
        assert resp.status_code == 200

    def test_root_contains_api_info(self, client):
        data = client.get("/").json()
        assert data["name"] == "Crawler System API"
        assert data["version"] == "1.0.0"
        assert data["docs"] == "/docs"
        assert data["health"] == "/health"

    def test_openapi_schema_available(self, client):
        resp = client.get("/openapi.json")
        assert resp.status_code == 200
        schema = resp.json()
        assert "openapi" in schema
        assert "paths" in schema

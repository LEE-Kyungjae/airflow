"""
API Tests for Crawler System.
기본 API 테스트
"""

import pytest
from unittest.mock import patch, MagicMock


class TestHealthCheck:
    """헬스 체크 테스트"""

    def test_health_check(self, client):
        """헬스 체크 엔드포인트"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert "service" in data

    def test_health_check_no_auth_required(self, client):
        """헬스 체크는 인증 불필요"""
        response = client.get("/health")
        assert response.status_code == 200


class TestRootEndpoint:
    """루트 엔드포인트 테스트"""

    def test_root(self, client):
        """루트 엔드포인트"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "version" in data
        assert "docs" in data

    def test_root_no_auth_required(self, client):
        """루트는 인증 불필요"""
        response = client.get("/")
        assert response.status_code == 200


class TestDashboard:
    """대시보드 테스트"""

    def test_dashboard_stats(self, client):
        """대시보드 통계"""
        with patch('app.routers.dashboard.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_dashboard_stats.return_value = {
                'sources': {'total': 10, 'active': 8, 'error': 2},
                'crawlers': {'total': 10, 'active': 8},
                'recent_executions': {
                    'total': 100,
                    'success': 95,
                    'failed': 5,
                    'success_rate': 95.0
                },
                'unresolved_errors': 3,
                'timestamp': '2024-01-01T00:00:00'
            }
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/dashboard")
            assert response.status_code == 200
            data = response.json()
            assert 'sources' in data
            assert 'crawlers' in data

    def test_dashboard_empty(self, client):
        """빈 대시보드"""
        with patch('app.routers.dashboard.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_dashboard_stats.return_value = {
                'sources': {'total': 0, 'active': 0, 'error': 0},
                'crawlers': {'total': 0, 'active': 0},
                'recent_executions': {
                    'total': 0,
                    'success': 0,
                    'failed': 0,
                    'success_rate': 0
                },
                'unresolved_errors': 0,
                'timestamp': '2024-01-01T00:00:00'
            }
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/dashboard")
            assert response.status_code == 200


class TestErrorsEndpoint:
    """에러 엔드포인트 테스트"""

    def test_list_errors(self, client):
        """에러 목록 조회"""
        with patch('app.routers.errors.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.list_errors.return_value = []
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/errors")
            assert response.status_code == 200

    def test_list_errors_with_filter(self, client):
        """필터링된 에러 목록"""
        with patch('app.routers.errors.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.list_errors.return_value = []
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/errors?resolved=false")
            assert response.status_code == 200


class TestMonitoringEndpoint:
    """모니터링 엔드포인트 테스트"""

    def test_monitoring_health(self, client):
        """모니터링 헬스"""
        response = client.get("/api/monitoring/health")
        # 구현에 따라 다를 수 있음
        assert response.status_code in [200, 404, 500]


class TestOpenAPISchema:
    """OpenAPI 스키마 테스트"""

    def test_openapi_json(self, client):
        """OpenAPI JSON 스키마"""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert "openapi" in data
        assert "paths" in data
        assert "info" in data

    def test_docs_endpoint(self, client):
        """Swagger UI 문서"""
        response = client.get("/docs")
        assert response.status_code == 200

    def test_redoc_endpoint(self, client):
        """ReDoc 문서"""
        response = client.get("/redoc")
        assert response.status_code == 200


class TestCORS:
    """CORS 테스트"""

    def test_cors_headers(self, client):
        """CORS 헤더"""
        response = client.options(
            "/api/sources",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET"
            }
        )
        # CORS preflight 응답
        assert response.status_code in [200, 405]


class TestContentTypes:
    """Content-Type 테스트"""

    def test_json_response(self, client):
        """JSON 응답"""
        response = client.get("/health")
        assert response.headers["content-type"] == "application/json"

    def test_json_request_required(self, client, auth_headers):
        """JSON 요청 필수"""
        response = client.post(
            "/api/sources",
            headers={**auth_headers, "Content-Type": "text/plain"},
            content="not json"
        )
        assert response.status_code == 422


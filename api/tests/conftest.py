"""
Pytest Configuration and Fixtures
테스트 공통 설정 및 픽스처
"""

import os
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

# 테스트 환경 설정
os.environ["ENV"] = "test"
os.environ["AUTH_MODE"] = "optional"
os.environ["API_MASTER_KEYS"] = "test-api-key-12345"
os.environ["JWT_SECRET_KEY"] = "test-jwt-secret-key"
os.environ["ADMIN_PASSWORD"] = "test-admin-password"

from app.main import app
from app.auth import APIKeyAuth, JWTAuth


@pytest.fixture(autouse=True)
def reset_circuit_breakers():
    """테스트 간 CircuitBreaker 상태 초기화"""
    yield
    try:
        from app.services.mongo_service import _connection_circuit
        _connection_circuit.reset()
    except (ImportError, AttributeError):
        pass


@pytest.fixture
def client():
    """FastAPI 테스트 클라이언트"""
    return TestClient(app)


@pytest.fixture
def client_no_raise():
    """서버 예외를 500 응답으로 반환하는 테스트 클라이언트"""
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def auth_headers():
    """인증 헤더 (API Key)"""
    return {"X-API-Key": "test-api-key-12345"}


@pytest.fixture
def admin_token():
    """관리자 JWT 토큰"""
    JWTAuth.register_user(
        user_id="admin",
        username="admin",
        role="admin",
        scopes=["admin", "read", "write", "delete"]
    )
    token = JWTAuth.create_access_token(
        user_id="admin",
        role="admin",
        scopes=["admin", "read", "write", "delete"]
    )
    return token


@pytest.fixture
def user_token():
    """일반 사용자 JWT 토큰"""
    JWTAuth.register_user(
        user_id="test_user",
        username="test_user",
        role="user",
        scopes=["read"]
    )
    token = JWTAuth.create_access_token(
        user_id="test_user",
        role="user",
        scopes=["read"]
    )
    return token


@pytest.fixture
def auth_headers_jwt(admin_token):
    """JWT 인증 헤더"""
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture
def mock_mongo():
    """MongoDB 서비스 Mock"""
    with patch('app.services.mongo_service.MongoService') as mock:
        instance = MagicMock()
        mock.return_value = instance
        yield instance


@pytest.fixture
def mock_airflow():
    """Airflow Trigger Mock"""
    with patch('app.services.airflow_trigger.AirflowTrigger') as mock:
        instance = MagicMock()
        mock.return_value = instance
        yield instance


@pytest.fixture
def sample_source():
    """샘플 소스 데이터"""
    return {
        "name": "test-source",
        "url": "https://example.com/news",
        "type": "html",
        "schedule": "0 */6 * * *",
        "fields": [
            {
                "name": "title",
                "selector": "h2.title",
                "data_type": "string"
            },
            {
                "name": "date",
                "selector": "time.published",
                "data_type": "date",
                "attribute": "datetime"
            }
        ]
    }


@pytest.fixture
def sample_source_response():
    """샘플 소스 응답 데이터"""
    return {
        "_id": "507f1f77bcf86cd799439011",
        "name": "test-source",
        "url": "https://example.com/news",
        "type": "html",
        "schedule": "0 */6 * * *",
        "fields": [
            {
                "name": "title",
                "selector": "h2.title",
                "data_type": "string"
            }
        ],
        "status": "active",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }


@pytest.fixture
def sample_crawl_result():
    """샘플 크롤링 결과"""
    return {
        "_id": "507f1f77bcf86cd799439012",
        "source_id": "507f1f77bcf86cd799439011",
        "success": True,
        "records_count": 10,
        "execution_time": 5.2,
        "data": [
            {"title": "Test Article 1", "date": "2024-01-01"},
            {"title": "Test Article 2", "date": "2024-01-02"}
        ],
        "created_at": datetime.utcnow().isoformat()
    }


@pytest.fixture
def sample_error():
    """샘플 에러 데이터"""
    return {
        "_id": "507f1f77bcf86cd799439013",
        "source_id": "507f1f77bcf86cd799439011",
        "error_code": "E002",
        "error_message": "Selector not found: h2.title",
        "stack_trace": "...",
        "resolved": False,
        "created_at": datetime.utcnow().isoformat()
    }
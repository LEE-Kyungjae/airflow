"""
API Tests for Crawler System.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from app.main import app

client = TestClient(app)


def test_health_check():
    """Test health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_root():
    """Test root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert "name" in response.json()


@patch('app.routers.sources.MongoService')
def test_list_sources_empty(mock_mongo):
    """Test listing sources when empty."""
    mock_instance = MagicMock()
    mock_instance.list_sources.return_value = []
    mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
    mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

    response = client.get("/api/sources")
    assert response.status_code == 200


@patch('app.routers.dashboard.MongoService')
def test_dashboard(mock_mongo):
    """Test dashboard endpoint."""
    mock_instance = MagicMock()
    mock_instance.get_dashboard_stats.return_value = {
        'sources': {'total': 0, 'active': 0, 'error': 0},
        'crawlers': {'total': 0, 'active': 0},
        'recent_executions': {'total': 0, 'success': 0, 'failed': 0, 'success_rate': 0},
        'unresolved_errors': 0,
        'timestamp': '2024-01-01T00:00:00'
    }
    mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
    mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

    response = client.get("/api/dashboard")
    assert response.status_code == 200

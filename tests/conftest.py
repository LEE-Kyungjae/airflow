"""
Common pytest fixtures for the airflow-crawler-system test suite.

Provides shared fixtures for:
- MongoDB mocking
- Sample test data
- Authentication helpers
- ETL pipeline components
- Schema registry components
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, AsyncMock, patch
from typing import Dict, Any, List
import os


# ============================================
# Environment Setup
# ============================================

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment variables."""
    os.environ.setdefault("ENV", "development")
    os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-jwt-minimum-32-chars")
    os.environ.setdefault("API_MASTER_KEYS", "test-api-key-123")
    os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
    os.environ.setdefault("MONGODB_DATABASE", "test_crawler_system")
    yield


# ============================================
# MongoDB Fixtures
# ============================================

@pytest.fixture
def mock_mongo_client():
    """Create a mock MongoDB client."""
    mock_client = MagicMock()
    mock_client.admin.command.return_value = {"ok": 1}
    return mock_client


@pytest.fixture
def mock_mongo_db():
    """Create a mock MongoDB database."""
    mock_db = MagicMock()

    # Set up collection mocks
    mock_db.sources = MagicMock()
    mock_db.crawlers = MagicMock()
    mock_db.crawl_results = MagicMock()
    mock_db.error_logs = MagicMock()
    mock_db.crawler_history = MagicMock()
    mock_db.schema_registry = MagicMock()
    mock_db.data_contracts = MagicMock()
    mock_db.contract_validations = MagicMock()
    mock_db.staging_news = MagicMock()
    mock_db.staging_financial = MagicMock()
    mock_db.staging_data = MagicMock()
    mock_db.data_reviews = MagicMock()

    return mock_db


@pytest.fixture
def mock_mongo_service(mock_mongo_db):
    """Create a mock MongoService instance."""
    mock_service = MagicMock()
    mock_service.db = mock_mongo_db
    mock_service.uri = "mongodb://localhost:27017"
    mock_service.database_name = "test_crawler_system"
    mock_service._client = MagicMock()
    mock_service._connection_timeout = 5000

    # Set up health check
    mock_service.health_check.return_value = {
        "status": "healthy",
        "latency_ms": 1.5,
        "circuit_state": "closed",
        "database": "test_crawler_system"
    }

    return mock_service


@pytest.fixture
def mock_collection():
    """Create a mock MongoDB collection with common operations."""
    collection = MagicMock()

    # Mock insert operations
    collection.insert_one.return_value = MagicMock(inserted_id="507f1f77bcf86cd799439011")
    collection.insert_many.return_value = MagicMock(inserted_ids=["id1", "id2", "id3"])

    # Mock find operations
    collection.find_one.return_value = None
    collection.find.return_value = MagicMock()
    collection.find.return_value.sort.return_value.skip.return_value.limit.return_value = []

    # Mock update operations
    collection.update_one.return_value = MagicMock(modified_count=1, upserted_id=None)
    collection.update_many.return_value = MagicMock(modified_count=5)

    # Mock delete operations
    collection.delete_one.return_value = MagicMock(deleted_count=1)
    collection.delete_many.return_value = MagicMock(deleted_count=10)

    # Mock count operations
    collection.count_documents.return_value = 100

    # Mock index operations
    collection.create_index.return_value = "index_name"

    # Mock aggregate
    collection.aggregate.return_value = []

    # Mock distinct
    collection.distinct.return_value = []

    return collection


# ============================================
# Sample Test Data Fixtures
# ============================================

@pytest.fixture
def sample_source_data() -> Dict[str, Any]:
    """Sample source document data."""
    return {
        "name": "Test News Source",
        "url": "https://example.com/news",
        "type": "news",
        "schedule": "0 */6 * * *",
        "status": "active",
        "error_count": 0,
        "config": {
            "selectors": {
                "title": "h1.article-title",
                "content": "div.article-body",
                "date": "time.published"
            }
        },
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }


@pytest.fixture
def sample_crawler_data() -> Dict[str, Any]:
    """Sample crawler document data."""
    return {
        "source_id": "507f1f77bcf86cd799439011",
        "name": "Test Crawler",
        "code": "async def crawl(): return []",
        "version": 1,
        "status": "active",
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }


@pytest.fixture
def sample_news_articles() -> List[Dict[str, Any]]:
    """Sample news article data for ETL testing."""
    return [
        {
            "title": "Test Article 1",
            "content": "This is the content of test article 1. " * 20,
            "link": "https://example.com/article1",
            "date": "2024-01-15 10:30:00",
            "author": "Test Author"
        },
        {
            "title": "Test Article 2",
            "content": "This is the content of test article 2. " * 15,
            "link": "https://example.com/article2",
            "date": "2024년 1월 16일",
            "author": "Another Author"
        },
        {
            "title": "Test Article 3",
            "content": "Short content",
            "link": "https://example.com/article3",
            "date": "1시간 전",
            "author": None
        }
    ]


@pytest.fixture
def sample_financial_data() -> List[Dict[str, Any]]:
    """Sample financial data for ETL testing."""
    return [
        {
            "name": "Samsung Electronics",
            "code": "005930",
            "price": "71,500",
            "change": "+1,500",
            "change_rate": "2.14%",
            "volume": "15,234,567",
            "date": "2024-01-15"
        },
        {
            "name": "SK Hynix",
            "code": "000660",
            "price": "142,000",
            "change": "-2,500",
            "change_rate": "-1.73%",
            "volume": "3,456,789",
            "date": "2024-01-15"
        },
        {
            "name": "NAVER",
            "code": "035420",
            "price": "215,500",
            "change": "0",
            "change_rate": "0.00%",
            "volume": "987,654",
            "date": "2024-01-15"
        }
    ]


@pytest.fixture
def sample_exchange_rates() -> List[Dict[str, Any]]:
    """Sample exchange rate data for ETL testing."""
    return [
        {
            "currency": "USD",
            "base_rate": "1,320.50",
            "buy_rate": "1,330.00",
            "sell_rate": "1,310.00",
            "date": "2024-01-15"
        },
        {
            "currency": "EUR",
            "base_rate": "1,450.25",
            "buy_rate": "1,462.00",
            "sell_rate": "1,438.00",
            "date": "2024-01-15"
        },
        {
            "currency": "JPY",
            "base_rate": "8.95",
            "buy_rate": "9.05",
            "sell_rate": "8.85",
            "date": "2024-01-15"
        }
    ]


@pytest.fixture
def sample_validation_data() -> List[Dict[str, Any]]:
    """Sample data for validation testing with various quality levels."""
    return [
        # Valid record
        {"title": "Valid Article", "url": "https://example.com/valid", "content": "Valid content here"},
        # Missing required field
        {"url": "https://example.com/no-title", "content": "No title here"},
        # Invalid URL
        {"title": "Bad URL", "url": "not-a-valid-url", "content": "Content"},
        # Empty content
        {"title": "Empty Content", "url": "https://example.com/empty", "content": ""},
        # All valid
        {"title": "Another Valid", "url": "https://example.com/another", "content": "More content"}
    ]


# ============================================
# Authentication Fixtures
# ============================================

@pytest.fixture
def valid_jwt_token():
    """Generate a valid JWT token for testing."""
    from api.app.auth.jwt_auth import JWTAuth
    return JWTAuth.create_access_token(
        user_id="test_user",
        role="admin",
        scopes=["admin", "read", "write", "delete"]
    )


@pytest.fixture
def expired_jwt_token():
    """Generate an expired JWT token for testing."""
    from api.app.auth.jwt_auth import JWTAuth
    return JWTAuth.create_access_token(
        user_id="test_user",
        role="user",
        scopes=["read"],
        expires_delta=timedelta(seconds=-1)  # Already expired
    )


@pytest.fixture
def valid_api_key():
    """Return a valid API key for testing."""
    return "test-api-key-123"


@pytest.fixture
def invalid_api_key():
    """Return an invalid API key for testing."""
    return "invalid-api-key-xyz"


@pytest.fixture
def auth_headers(valid_jwt_token):
    """Return headers with valid JWT token."""
    return {"Authorization": f"Bearer {valid_jwt_token}"}


@pytest.fixture
def api_key_headers(valid_api_key):
    """Return headers with valid API key."""
    return {"X-API-Key": valid_api_key}


# ============================================
# Schema Registry Fixtures
# ============================================

@pytest.fixture
def sample_field_schema():
    """Create a sample FieldSchema."""
    from api.app.services.schema_registry.models import FieldSchema, FieldType
    return FieldSchema(
        name="title",
        field_type=FieldType.STRING,
        required=True,
        nullable=False,
        description="Article title",
        min_length=5,
        max_length=500
    )


@pytest.fixture
def sample_schema():
    """Create a sample Schema."""
    from api.app.services.schema_registry.models import (
        Schema, FieldSchema, FieldType, DataCategory
    )
    return Schema(
        fields=[
            FieldSchema(name="title", field_type=FieldType.STRING, required=True),
            FieldSchema(name="content", field_type=FieldType.STRING, required=False),
            FieldSchema(name="url", field_type=FieldType.STRING, required=False),
            FieldSchema(name="published_at", field_type=FieldType.DATETIME, required=False),
            FieldSchema(name="view_count", field_type=FieldType.INTEGER, required=False),
        ],
        description="Test news article schema",
        data_category=DataCategory.NEWS_ARTICLE,
        collection_name="test_news"
    )


@pytest.fixture
def modified_schema(sample_schema):
    """Create a modified version of the sample schema."""
    from api.app.services.schema_registry.models import FieldSchema, FieldType

    schema = sample_schema.clone()
    # Add a new optional field (backward compatible)
    schema.add_field(FieldSchema(
        name="author",
        field_type=FieldType.STRING,
        required=False
    ))
    return schema


@pytest.fixture
def incompatible_schema(sample_schema):
    """Create an incompatible version of the sample schema."""
    from api.app.services.schema_registry.models import FieldSchema, FieldType

    schema = sample_schema.clone()
    # Remove a field (forward incompatible)
    schema.remove_field("content")
    # Add a required field without default (backward incompatible)
    schema.add_field(FieldSchema(
        name="mandatory_field",
        field_type=FieldType.STRING,
        required=True
    ))
    return schema


# ============================================
# Data Contract Fixtures
# ============================================

@pytest.fixture
def sample_data_contract():
    """Create a sample DataContract."""
    from api.app.services.data_contracts.contract import ContractBuilder
    from api.app.services.data_contracts.expectations import ExpectationSeverity

    return (
        ContractBuilder("test_news_contract", source_id="test_source_123")
        .with_description("Test news article data contract")
        .expect_column_not_null("title", severity=ExpectationSeverity.CRITICAL)
        .expect_column_not_null("url", severity=ExpectationSeverity.ERROR)
        .expect_column_unique("url", mostly=0.99)
        .expect_column_value_length_to_be_between("title", min_value=5, max_value=300)
        .expect_table_row_count_between(min_value=1)
        .build()
    )


@pytest.fixture
def strict_data_contract():
    """Create a strict DataContract that fails easily."""
    from api.app.services.data_contracts.contract import ContractBuilder
    from api.app.services.data_contracts.expectations import ExpectationSeverity

    return (
        ContractBuilder("strict_contract")
        .with_description("Strict validation contract")
        .fail_on_warning(True)
        .expect_column_not_null("title", severity=ExpectationSeverity.CRITICAL)
        .expect_column_not_null("content", severity=ExpectationSeverity.CRITICAL)
        .expect_column_not_null("url", severity=ExpectationSeverity.CRITICAL)
        .expect_column_values_to_match_regex("url", preset="url")
        .expect_table_row_count_between(min_value=1, max_value=100)
        .build()
    )


# ============================================
# ETL Pipeline Fixtures
# ============================================

@pytest.fixture
def etl_transform_config():
    """Create a TransformConfig for testing."""
    from airflow.dags.utils.etl_pipeline import TransformConfig, DataCategory

    return TransformConfig(
        category=DataCategory.NEWS_ARTICLE,
        required_fields=["title"],
        dedup_fields=["content_hash"],
        quality_threshold=0.6
    )


@pytest.fixture
def etl_load_config():
    """Create a LoadConfig for testing."""
    from airflow.dags.utils.etl_pipeline import LoadConfig

    return LoadConfig(
        collection_name="news_articles",
        index_fields=["published_at", "title"],
        upsert=True,
        upsert_key=["content_hash"],
        ttl_days=90
    )


# ============================================
# FastAPI Test Client Fixtures
# ============================================

@pytest.fixture
def test_app():
    """Create a test FastAPI application."""
    from fastapi import FastAPI
    from api.app.routers import auth, sources

    app = FastAPI(title="Test App")
    app.include_router(auth.router, prefix="/auth", tags=["auth"])

    return app


@pytest.fixture
def test_client(test_app):
    """Create a test client for the FastAPI app."""
    from fastapi.testclient import TestClient
    return TestClient(test_app)


# ============================================
# Async Test Helpers
# ============================================

@pytest.fixture
def async_mock():
    """Create an async mock for async function testing."""
    return AsyncMock()


@pytest.fixture
def mock_alert_dispatcher():
    """Create a mock alert dispatcher."""
    dispatcher = MagicMock()
    dispatcher.send_alert = AsyncMock(return_value=True)
    return dispatcher


# ============================================
# ObjectId Fixtures
# ============================================

@pytest.fixture
def valid_object_id():
    """Return a valid MongoDB ObjectId string."""
    return "507f1f77bcf86cd799439011"


@pytest.fixture
def invalid_object_id():
    """Return an invalid MongoDB ObjectId string."""
    return "invalid-object-id"


@pytest.fixture
def another_object_id():
    """Return another valid MongoDB ObjectId string."""
    return "507f1f77bcf86cd799439012"

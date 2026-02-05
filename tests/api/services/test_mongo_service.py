"""
Tests for MongoService CRUD operations.

Covers:
- Source CRUD operations
- Crawler operations
- Error handling and validation
- ObjectId validation
- Health check functionality
- Dashboard statistics
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock
from bson import ObjectId
from bson.errors import InvalidId


class TestObjectIdValidation:
    """Tests for ObjectId validation utilities."""

    def test_validate_object_id_with_valid_string(self):
        """Test validation with a valid ObjectId string."""
        from api.app.services.mongo_service import validate_object_id

        valid_id = "507f1f77bcf86cd799439011"
        result = validate_object_id(valid_id)

        assert isinstance(result, ObjectId)
        assert str(result) == valid_id

    def test_validate_object_id_with_object_id_instance(self):
        """Test validation with an ObjectId instance."""
        from api.app.services.mongo_service import validate_object_id

        oid = ObjectId("507f1f77bcf86cd799439011")
        result = validate_object_id(oid)

        assert result == oid

    def test_validate_object_id_with_invalid_string(self):
        """Test validation with an invalid ObjectId string."""
        from api.app.services.mongo_service import validate_object_id
        from api.app.exceptions import ObjectIdValidationError

        with pytest.raises(ObjectIdValidationError):
            validate_object_id("invalid-id")

    def test_validate_object_id_with_empty_string(self):
        """Test validation with an empty string."""
        from api.app.services.mongo_service import validate_object_id
        from api.app.exceptions import ObjectIdValidationError

        with pytest.raises(ObjectIdValidationError):
            validate_object_id("")

    def test_validate_object_id_with_none(self):
        """Test validation with None value."""
        from api.app.services.mongo_service import validate_object_id
        from api.app.exceptions import ObjectIdValidationError

        with pytest.raises(ObjectIdValidationError):
            validate_object_id(None)

    def test_safe_object_id_with_valid_string(self):
        """Test safe_object_id returns ObjectId for valid string."""
        from api.app.services.mongo_service import safe_object_id

        valid_id = "507f1f77bcf86cd799439011"
        result = safe_object_id(valid_id)

        assert isinstance(result, ObjectId)
        assert str(result) == valid_id

    def test_safe_object_id_with_invalid_string(self):
        """Test safe_object_id returns None for invalid string."""
        from api.app.services.mongo_service import safe_object_id

        result = safe_object_id("invalid-id")
        assert result is None

    def test_safe_object_id_with_context_logging(self):
        """Test safe_object_id logs context on invalid input."""
        from api.app.services.mongo_service import safe_object_id

        with patch('api.app.services.mongo_service.logger') as mock_logger:
            safe_object_id("bad-id", context="test_context")
            mock_logger.warning.assert_called_once()
            assert "test_context" in str(mock_logger.warning.call_args)


class TestMongoServiceConnection:
    """Tests for MongoService connection handling."""

    def test_context_manager_entry_exit(self):
        """Test MongoService as context manager."""
        from api.app.services.mongo_service import MongoService

        with patch.object(MongoService, 'close') as mock_close:
            with MongoService() as mongo:
                assert mongo is not None
            mock_close.assert_called_once()

    def test_close_with_client(self, mock_mongo_client):
        """Test close method when client exists."""
        from api.app.services.mongo_service import MongoService

        mongo = MongoService()
        mongo._client = mock_mongo_client

        mongo.close()

        mock_mongo_client.close.assert_called_once()
        assert mongo._client is None

    def test_close_without_client(self):
        """Test close method when no client exists."""
        from api.app.services.mongo_service import MongoService

        mongo = MongoService()
        mongo._client = None

        # Should not raise any exception
        mongo.close()
        assert mongo._client is None

    def test_health_check_healthy(self, mock_mongo_client):
        """Test health check returns healthy status."""
        from api.app.services.mongo_service import MongoService

        with patch.object(MongoService, 'client', new_callable=PropertyMock) as mock_client_prop:
            mock_client_prop.return_value = mock_mongo_client
            mongo = MongoService()

            result = mongo.health_check()

            assert result["status"] == "healthy"
            assert "latency_ms" in result
            assert "database" in result

    def test_health_check_unhealthy(self):
        """Test health check returns unhealthy status on error."""
        from api.app.services.mongo_service import MongoService

        with patch.object(MongoService, 'client', new_callable=PropertyMock) as mock_client_prop:
            mock_client_prop.side_effect = Exception("Connection failed")
            mongo = MongoService()

            result = mongo.health_check()

            assert result["status"] == "unhealthy"
            assert "error" in result


class TestMongoServiceSources:
    """Tests for source-related operations."""

    def test_create_source_success(self, mock_mongo_db, sample_source_data):
        """Test successful source creation."""
        from api.app.services.mongo_service import MongoService

        mock_mongo_db.sources.insert_one.return_value = MagicMock(
            inserted_id=ObjectId("507f1f77bcf86cd799439011")
        )

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.create_source(sample_source_data)

            assert result == "507f1f77bcf86cd799439011"
            mock_mongo_db.sources.insert_one.assert_called_once()
            call_args = mock_mongo_db.sources.insert_one.call_args[0][0]
            assert call_args["status"] == "inactive"
            assert "created_at" in call_args
            assert "updated_at" in call_args

    def test_get_source_success(self, mock_mongo_db, valid_object_id):
        """Test successful source retrieval."""
        from api.app.services.mongo_service import MongoService

        expected_doc = {
            "_id": ObjectId(valid_object_id),
            "name": "Test Source",
            "status": "active",
            "created_at": datetime.utcnow()
        }
        mock_mongo_db.sources.find_one.return_value = expected_doc

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.get_source(valid_object_id)

            assert result is not None
            assert result["_id"] == valid_object_id
            assert result["name"] == "Test Source"

    def test_get_source_not_found(self, mock_mongo_db, valid_object_id):
        """Test source retrieval when not found."""
        from api.app.services.mongo_service import MongoService

        mock_mongo_db.sources.find_one.return_value = None

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.get_source(valid_object_id)

            assert result is None

    def test_get_source_invalid_id(self, mock_mongo_db, invalid_object_id):
        """Test source retrieval with invalid ObjectId."""
        from api.app.services.mongo_service import MongoService
        from api.app.exceptions import ObjectIdValidationError

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            with pytest.raises(ObjectIdValidationError):
                mongo.get_source(invalid_object_id)

    def test_get_source_by_name_success(self, mock_mongo_db):
        """Test source retrieval by name."""
        from api.app.services.mongo_service import MongoService

        expected_doc = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "name": "Test Source",
            "status": "active"
        }
        mock_mongo_db.sources.find_one.return_value = expected_doc

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.get_source_by_name("Test Source")

            assert result is not None
            assert result["name"] == "Test Source"
            mock_mongo_db.sources.find_one.assert_called_with({"name": "Test Source"})

    def test_list_sources_with_status_filter(self, mock_mongo_db):
        """Test listing sources with status filter."""
        from api.app.services.mongo_service import MongoService

        mock_cursor = MagicMock()
        mock_cursor.sort.return_value.skip.return_value.limit.return_value = [
            {"_id": ObjectId(), "name": "Source 1", "status": "active"},
            {"_id": ObjectId(), "name": "Source 2", "status": "active"}
        ]
        mock_mongo_db.sources.find.return_value = mock_cursor

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.list_sources(status="active", skip=0, limit=10)

            assert len(result) == 2
            mock_mongo_db.sources.find.assert_called_with({"status": "active"})

    def test_list_sources_without_filter(self, mock_mongo_db):
        """Test listing sources without filter."""
        from api.app.services.mongo_service import MongoService

        mock_cursor = MagicMock()
        mock_cursor.sort.return_value.skip.return_value.limit.return_value = []
        mock_mongo_db.sources.find.return_value = mock_cursor

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            mongo.list_sources()

            mock_mongo_db.sources.find.assert_called_with({})

    def test_count_sources_with_filter(self, mock_mongo_db):
        """Test counting sources with status filter."""
        from api.app.services.mongo_service import MongoService

        mock_mongo_db.sources.count_documents.return_value = 5

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.count_sources(status="active")

            assert result == 5
            mock_mongo_db.sources.count_documents.assert_called_with({"status": "active"})

    def test_update_source_success(self, mock_mongo_db, valid_object_id):
        """Test successful source update."""
        from api.app.services.mongo_service import MongoService

        mock_mongo_db.sources.update_one.return_value = MagicMock(modified_count=1)

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.update_source(valid_object_id, {"name": "Updated Name"})

            assert result is True
            mock_mongo_db.sources.update_one.assert_called_once()

    def test_update_source_not_found(self, mock_mongo_db, valid_object_id):
        """Test source update when not found."""
        from api.app.services.mongo_service import MongoService

        mock_mongo_db.sources.update_one.return_value = MagicMock(modified_count=0)

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.update_source(valid_object_id, {"name": "Updated Name"})

            assert result is False

    def test_delete_source_success(self, mock_mongo_db, valid_object_id):
        """Test successful source deletion with cascade."""
        from api.app.services.mongo_service import MongoService

        mock_mongo_db.sources.delete_one.return_value = MagicMock(deleted_count=1)

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.delete_source(valid_object_id)

            assert result is True
            # Verify cascade deletions
            mock_mongo_db.crawlers.delete_many.assert_called_once()
            mock_mongo_db.crawl_results.delete_many.assert_called_once()
            mock_mongo_db.crawler_history.delete_many.assert_called_once()
            mock_mongo_db.error_logs.delete_many.assert_called_once()

    def test_delete_source_not_found(self, mock_mongo_db, valid_object_id):
        """Test source deletion when not found."""
        from api.app.services.mongo_service import MongoService

        mock_mongo_db.sources.delete_one.return_value = MagicMock(deleted_count=0)

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.delete_source(valid_object_id)

            assert result is False


class TestMongoServiceCrawlers:
    """Tests for crawler-related operations."""

    def test_get_crawler_success(self, mock_mongo_db, valid_object_id):
        """Test successful crawler retrieval."""
        from api.app.services.mongo_service import MongoService

        expected_doc = {
            "_id": ObjectId(valid_object_id),
            "name": "Test Crawler",
            "status": "active"
        }
        mock_mongo_db.crawlers.find_one.return_value = expected_doc

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.get_crawler(valid_object_id)

            assert result is not None
            assert result["name"] == "Test Crawler"

    def test_get_active_crawler_success(self, mock_mongo_db, valid_object_id):
        """Test getting active crawler for a source."""
        from api.app.services.mongo_service import MongoService

        expected_doc = {
            "_id": ObjectId(),
            "source_id": ObjectId(valid_object_id),
            "status": "active"
        }
        mock_mongo_db.crawlers.find_one.return_value = expected_doc

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.get_active_crawler(valid_object_id)

            assert result is not None
            mock_mongo_db.crawlers.find_one.assert_called_with({
                "source_id": ObjectId(valid_object_id),
                "status": "active"
            })

    def test_list_crawlers_with_filters(self, mock_mongo_db, valid_object_id):
        """Test listing crawlers with multiple filters."""
        from api.app.services.mongo_service import MongoService

        mock_cursor = MagicMock()
        mock_cursor.sort.return_value.skip.return_value.limit.return_value = []
        mock_mongo_db.crawlers.find.return_value = mock_cursor

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            mongo.list_crawlers(source_id=valid_object_id, status="active")

            call_args = mock_mongo_db.crawlers.find.call_args[0][0]
            assert "source_id" in call_args
            assert call_args["status"] == "active"

    def test_count_crawlers(self, mock_mongo_db):
        """Test counting crawlers."""
        from api.app.services.mongo_service import MongoService

        mock_mongo_db.crawlers.count_documents.return_value = 10

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.count_crawlers(status="active")

            assert result == 10

    def test_get_crawler_history(self, mock_mongo_db, valid_object_id):
        """Test getting crawler code history."""
        from api.app.services.mongo_service import MongoService

        mock_cursor = MagicMock()
        mock_cursor.sort.return_value.skip.return_value.limit.return_value = [
            {"_id": ObjectId(), "crawler_id": ObjectId(valid_object_id), "version": 1},
            {"_id": ObjectId(), "crawler_id": ObjectId(valid_object_id), "version": 2}
        ]
        mock_mongo_db.crawler_history.find.return_value = mock_cursor

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.get_crawler_history(valid_object_id, skip=0, limit=50)

            assert len(result) == 2


class TestMongoServiceErrors:
    """Tests for error log operations."""

    def test_list_errors_with_filters(self, mock_mongo_db, valid_object_id):
        """Test listing errors with filters."""
        from api.app.services.mongo_service import MongoService

        mock_cursor = MagicMock()
        mock_cursor.sort.return_value.skip.return_value.limit.return_value = []
        mock_mongo_db.error_logs.find.return_value = mock_cursor

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            mongo.list_errors(resolved=False, source_id=valid_object_id)

            call_args = mock_mongo_db.error_logs.find.call_args[0][0]
            assert call_args["resolved"] is False
            assert "source_id" in call_args

    def test_get_error_success(self, mock_mongo_db, valid_object_id):
        """Test getting error by ID."""
        from api.app.services.mongo_service import MongoService

        expected_doc = {
            "_id": ObjectId(valid_object_id),
            "message": "Test Error",
            "resolved": False
        }
        mock_mongo_db.error_logs.find_one.return_value = expected_doc

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.get_error(valid_object_id)

            assert result is not None
            assert result["message"] == "Test Error"

    def test_count_errors(self, mock_mongo_db):
        """Test counting errors."""
        from api.app.services.mongo_service import MongoService

        mock_mongo_db.error_logs.count_documents.return_value = 15

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.count_errors(resolved=False)

            assert result == 15
            mock_mongo_db.error_logs.count_documents.assert_called_with({"resolved": False})

    def test_resolve_error_success(self, mock_mongo_db, valid_object_id):
        """Test resolving an error."""
        from api.app.services.mongo_service import MongoService

        mock_mongo_db.error_logs.update_one.return_value = MagicMock(modified_count=1)

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db
            mongo = MongoService()

            result = mongo.resolve_error(valid_object_id, "manual", "Fixed by admin")

            assert result is True
            call_args = mock_mongo_db.error_logs.update_one.call_args[0][1]["$set"]
            assert call_args["resolved"] is True
            assert call_args["resolution_method"] == "manual"
            assert call_args["resolution_detail"] == "Fixed by admin"


class TestMongoServiceDashboard:
    """Tests for dashboard statistics."""

    def test_get_dashboard_stats(self, mock_mongo_db):
        """Test getting dashboard statistics."""
        from api.app.services.mongo_service import MongoService

        # Set up mock counts
        mock_mongo_db.sources.count_documents.side_effect = [10, 5, 2]  # total, active, error
        mock_mongo_db.crawlers.count_documents.side_effect = [8, 6]  # total, active
        mock_mongo_db.error_logs.count_documents.return_value = 3

        # Set up mock for recent results
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value.limit.return_value = [
            {"status": "success"},
            {"status": "success"},
            {"status": "failed"},
            {"status": "success"}
        ]
        mock_mongo_db.crawl_results.find.return_value = mock_cursor

        with patch.object(MongoService, 'db', new_callable=PropertyMock) as mock_db:
            mock_db.return_value = mock_mongo_db

            with patch.object(MongoService, 'health_check') as mock_health:
                mock_health.return_value = {"status": "healthy"}
                mongo = MongoService()

                result = mongo.get_dashboard_stats()

                assert result["sources"]["total"] == 10
                assert result["sources"]["active"] == 5
                assert result["sources"]["error"] == 2
                assert result["crawlers"]["total"] == 8
                assert result["crawlers"]["active"] == 6
                assert result["unresolved_errors"] == 3
                assert result["recent_executions"]["total"] == 4
                assert result["recent_executions"]["success"] == 3
                assert result["recent_executions"]["failed"] == 1


class TestMongoServiceDocumentSerialization:
    """Tests for document serialization."""

    def test_serialize_doc_with_object_id(self):
        """Test serializing document with ObjectId."""
        from api.app.services.mongo_service import MongoService

        mongo = MongoService()
        doc = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "name": "Test"
        }

        result = mongo._serialize_doc(doc)

        assert result["_id"] == "507f1f77bcf86cd799439011"
        assert result["name"] == "Test"

    def test_serialize_doc_with_datetime(self):
        """Test serializing document with datetime."""
        from api.app.services.mongo_service import MongoService

        mongo = MongoService()
        now = datetime.utcnow()
        doc = {
            "_id": ObjectId(),
            "created_at": now
        }

        result = mongo._serialize_doc(doc)

        assert result["created_at"] == now

    def test_serialize_doc_with_none(self):
        """Test serializing None document."""
        from api.app.services.mongo_service import MongoService

        mongo = MongoService()
        result = mongo._serialize_doc(None)

        assert result is None

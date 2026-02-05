"""
Database Exception Handling Tests
DB 예외 처리 테스트
"""

import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from bson import ObjectId
from bson.errors import InvalidId
from pymongo.errors import (
    ConnectionFailure,
    ServerSelectionTimeoutError,
    DuplicateKeyError as MongoDuplicateKeyError,
    OperationFailure,
    AutoReconnect,
    NetworkTimeout,
    ExecutionTimeout
)

from app.services.mongo_service import (
    MongoService,
    validate_object_id,
    safe_object_id
)
from app.exceptions import (
    ObjectIdValidationError,
    DatabaseConnectionError,
    DatabaseOperationError,
    DuplicateKeyError,
    DocumentNotFoundError
)


class TestObjectIdValidation:
    """ObjectId 검증 테스트"""

    def test_valid_object_id_string(self):
        """유효한 ObjectId 문자열"""
        valid_id = "507f1f77bcf86cd799439011"
        result = validate_object_id(valid_id)
        assert isinstance(result, ObjectId)
        assert str(result) == valid_id

    def test_valid_object_id_object(self):
        """ObjectId 객체 입력"""
        oid = ObjectId()
        result = validate_object_id(oid)
        assert result == oid

    def test_invalid_object_id_string(self):
        """유효하지 않은 ObjectId 문자열"""
        with pytest.raises(ObjectIdValidationError) as exc_info:
            validate_object_id("invalid-id")
        assert exc_info.value.error_code == "V006"
        assert "유효하지 않은 ObjectId" in str(exc_info.value)

    def test_empty_object_id(self):
        """빈 ObjectId"""
        with pytest.raises(ObjectIdValidationError):
            validate_object_id("")

    def test_none_object_id(self):
        """None ObjectId"""
        with pytest.raises(ObjectIdValidationError):
            validate_object_id(None)

    def test_object_id_with_context(self):
        """컨텍스트 포함 에러"""
        with pytest.raises(ObjectIdValidationError) as exc_info:
            validate_object_id("bad", context="source_id")
        assert "source_id" in exc_info.value.details.get("context", "")

    def test_safe_object_id_valid(self):
        """safe_object_id - 유효한 ID"""
        valid_id = "507f1f77bcf86cd799439011"
        result = safe_object_id(valid_id)
        assert isinstance(result, ObjectId)

    def test_safe_object_id_invalid(self):
        """safe_object_id - 유효하지 않은 ID"""
        result = safe_object_id("invalid")
        assert result is None


class TestMongoServiceConnection:
    """MongoDB 연결 테스트"""

    def test_connection_failure(self):
        """연결 실패 시 DatabaseConnectionError"""
        with patch('app.services.mongo_service.MongoClient') as mock_client:
            mock_client.side_effect = ConnectionFailure("Connection refused")

            service = MongoService()
            with pytest.raises(DatabaseConnectionError) as exc_info:
                _ = service.client

            assert exc_info.value.error_code == "D001"
            assert exc_info.value.recoverable is True

    def test_server_selection_timeout(self):
        """서버 선택 타임아웃 시 DatabaseConnectionError"""
        with patch('app.services.mongo_service.MongoClient') as mock_client:
            mock_client.side_effect = ServerSelectionTimeoutError("Timeout")

            service = MongoService()
            with pytest.raises(DatabaseConnectionError):
                _ = service.client

    def test_health_check_healthy(self):
        """헬스 체크 - 정상"""
        with patch('app.services.mongo_service.MongoClient') as mock_client:
            mock_instance = MagicMock()
            mock_instance.admin.command.return_value = {'ok': 1}
            mock_client.return_value = mock_instance

            service = MongoService()
            result = service.health_check()

            assert result['status'] == 'healthy'
            assert 'latency_ms' in result

    def test_health_check_unhealthy(self):
        """헬스 체크 - 비정상"""
        with patch('app.services.mongo_service.MongoClient') as mock_client:
            mock_instance = MagicMock()
            mock_instance.admin.command.side_effect = Exception("DB error")
            mock_client.return_value = mock_instance

            service = MongoService()
            result = service.health_check()

            assert result['status'] == 'unhealthy'
            assert 'error' in result


class TestMongoServiceOperations:
    """MongoDB 연산 테스트"""

    @pytest.fixture
    def mock_service(self):
        """Mock MongoDB service"""
        with patch('app.services.mongo_service.MongoClient') as mock_client:
            mock_instance = MagicMock()
            mock_instance.admin.command.return_value = {'ok': 1}
            mock_client.return_value = mock_instance

            service = MongoService()
            service._client = mock_instance
            yield service, mock_instance

    def test_get_source_invalid_id(self, mock_service):
        """get_source - 유효하지 않은 ID"""
        service, _ = mock_service

        with pytest.raises(ObjectIdValidationError):
            service.get_source("invalid-id")

    def test_get_source_valid_id(self, mock_service):
        """get_source - 유효한 ID"""
        service, mock_client = mock_service
        valid_id = "507f1f77bcf86cd799439011"

        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_db.sources.find_one.return_value = {
            '_id': ObjectId(valid_id),
            'name': 'test'
        }

        result = service.get_source(valid_id)

        assert result is not None
        assert result['_id'] == valid_id

    def test_get_source_not_found(self, mock_service):
        """get_source - 문서 없음"""
        service, mock_client = mock_service
        valid_id = "507f1f77bcf86cd799439011"

        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_db.sources.find_one.return_value = None

        result = service.get_source(valid_id)
        assert result is None

    def test_create_source_duplicate_key(self, mock_service):
        """create_source - 중복 키"""
        service, mock_client = mock_service

        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db

        error = MongoDuplicateKeyError("Duplicate key error", code=11000)
        error.details = {'keyPattern': {'name': 1}}
        mock_db.sources.insert_one.side_effect = error

        with pytest.raises(DuplicateKeyError) as exc_info:
            service.create_source({'name': 'test'})

        assert exc_info.value.error_code == "D003"
        assert exc_info.value.recoverable is False

    def test_operation_network_timeout(self, mock_service):
        """연산 중 네트워크 타임아웃"""
        service, mock_client = mock_service

        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_db.sources.find_one.side_effect = NetworkTimeout("Network timeout")

        with pytest.raises(DatabaseOperationError) as exc_info:
            service.get_source("507f1f77bcf86cd799439011")

        assert exc_info.value.error_code == "D002"
        assert "Network timeout" in str(exc_info.value)

    def test_operation_execution_timeout(self, mock_service):
        """연산 실행 타임아웃"""
        service, mock_client = mock_service

        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_db.sources.find_one.side_effect = ExecutionTimeout("Query timeout")

        with pytest.raises(DatabaseOperationError) as exc_info:
            service.get_source("507f1f77bcf86cd799439011")

        assert "Execution timeout" in exc_info.value.details.get('reason', '')

    def test_auto_reconnect_error(self, mock_service):
        """Auto Reconnect 에러"""
        service, mock_client = mock_service

        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_db.sources.find_one.side_effect = AutoReconnect("Reconnecting")

        with pytest.raises(DatabaseConnectionError):
            service.get_source("507f1f77bcf86cd799439011")


class TestMongoServiceContextManager:
    """컨텍스트 매니저 테스트"""

    def test_context_manager_closes_connection(self):
        """with 문 종료 시 연결 종료"""
        with patch('app.services.mongo_service.MongoClient') as mock_client:
            mock_instance = MagicMock()
            mock_instance.admin.command.return_value = {'ok': 1}
            mock_client.return_value = mock_instance

            with MongoService() as service:
                _ = service.client  # 연결 생성

            mock_instance.close.assert_called_once()

    def test_context_manager_exception_handling(self):
        """예외 발생 시에도 연결 종료"""
        with patch('app.services.mongo_service.MongoClient') as mock_client:
            mock_instance = MagicMock()
            mock_instance.admin.command.return_value = {'ok': 1}
            mock_client.return_value = mock_instance

            mock_db = MagicMock()
            mock_instance.__getitem__.return_value = mock_db
            mock_db.sources.find_one.side_effect = Exception("Test error")

            with pytest.raises(Exception):
                with MongoService() as service:
                    _ = service.client
                    service.get_source("507f1f77bcf86cd799439011")

            mock_instance.close.assert_called_once()


class TestExceptionToDict:
    """예외 직렬화 테스트"""

    def test_database_connection_error_to_dict(self):
        """DatabaseConnectionError 직렬화"""
        error = DatabaseConnectionError(
            reason="Connection refused",
            host="localhost:27017"
        )

        result = error.to_dict()

        assert result['error_code'] == 'D001'
        assert 'Connection refused' in result['message']
        assert result['recoverable'] is True
        assert 'timestamp' in result

    def test_object_id_validation_error_to_dict(self):
        """ObjectIdValidationError 직렬화"""
        error = ObjectIdValidationError(
            value="invalid",
            context="source_id"
        )

        result = error.to_dict()

        assert result['error_code'] == 'V006'
        assert result['details']['value'] == 'invalid'
        assert result['details']['context'] == 'source_id'


class TestListOperations:
    """목록 조회 연산 테스트"""

    @pytest.fixture
    def mock_service(self):
        """Mock MongoDB service for list operations"""
        with patch('app.services.mongo_service.MongoClient') as mock_client:
            mock_instance = MagicMock()
            mock_instance.admin.command.return_value = {'ok': 1}
            mock_client.return_value = mock_instance

            service = MongoService()
            service._client = mock_instance
            yield service, mock_instance

    def test_list_sources_with_filter(self, mock_service):
        """list_sources - 필터 적용"""
        service, mock_client = mock_service

        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db

        mock_cursor = MagicMock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = mock_cursor
        mock_cursor.__iter__ = lambda self: iter([
            {'_id': ObjectId(), 'name': 'test1', 'status': 'active'},
            {'_id': ObjectId(), 'name': 'test2', 'status': 'active'}
        ])

        mock_db.sources.find.return_value = mock_cursor

        results = service.list_sources(status='active')

        assert len(results) == 2
        mock_db.sources.find.assert_called_once_with({'status': 'active'})

    def test_list_crawlers_invalid_source_id(self, mock_service):
        """list_crawlers - 유효하지 않은 source_id"""
        service, _ = mock_service

        with pytest.raises(ObjectIdValidationError):
            service.list_crawlers(source_id="invalid")


class TestDashboardStats:
    """대시보드 통계 테스트"""

    @pytest.fixture
    def mock_service(self):
        """Mock MongoDB service"""
        with patch('app.services.mongo_service.MongoClient') as mock_client:
            mock_instance = MagicMock()
            mock_instance.admin.command.return_value = {'ok': 1}
            mock_client.return_value = mock_instance

            service = MongoService()
            service._client = mock_instance
            yield service, mock_instance

    def test_get_dashboard_stats(self, mock_service):
        """대시보드 통계 조회"""
        service, mock_client = mock_service

        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db

        # Mock count_documents
        mock_db.sources.count_documents.side_effect = [10, 8, 2]
        mock_db.crawlers.count_documents.side_effect = [5, 4]
        mock_db.error_logs.count_documents.return_value = 3

        # Mock crawl_results.find
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.limit.return_value = [
            {'status': 'success'},
            {'status': 'success'},
            {'status': 'failed'}
        ]
        mock_db.crawl_results.find.return_value = mock_cursor

        stats = service.get_dashboard_stats()

        assert stats['sources']['total'] == 10
        assert stats['sources']['active'] == 8
        assert stats['sources']['error'] == 2
        assert stats['crawlers']['total'] == 5
        assert stats['crawlers']['active'] == 4
        assert stats['unresolved_errors'] == 3
        assert 'health' in stats

"""
MongoDB Service for Airflow DAGs.

This module handles all database operations for the crawler system,
including sources, crawlers, results, history, and error logs.
Enhanced with comprehensive exception handling and retry mechanisms.
"""

import os
import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable, TypeVar
from functools import wraps
from bson import ObjectId
from bson.errors import InvalidId
from pymongo import MongoClient, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (
    ConnectionFailure,
    ServerSelectionTimeoutError,
    DuplicateKeyError as MongoDuplicateKeyError,
    OperationFailure,
    AutoReconnect,
    NetworkTimeout,
    WriteError,
    WriteConcernError,
    ExecutionTimeout
)

logger = logging.getLogger(__name__)

T = TypeVar('T')


# ============================================
# 커스텀 예외 클래스 (Airflow DAG용)
# ============================================

class MongoDBException(Exception):
    """MongoDB 연산 기본 예외"""
    def __init__(self, message: str, error_code: str, details: Dict = None,
                 recoverable: bool = False):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.recoverable = recoverable
        self.timestamp = datetime.utcnow()
        super().__init__(message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "recoverable": self.recoverable,
            "timestamp": self.timestamp.isoformat()
        }


class MongoConnectionError(MongoDBException):
    """연결 오류"""
    def __init__(self, reason: str, host: str = ""):
        super().__init__(
            message=f"MongoDB 연결 실패: {reason}",
            error_code="D001",
            details={"reason": reason, "host": host},
            recoverable=True
        )


class MongoOperationError(MongoDBException):
    """연산 오류"""
    def __init__(self, operation: str, collection: str, reason: str):
        super().__init__(
            message=f"DB 연산 실패 ({operation}): {reason}",
            error_code="D002",
            details={"operation": operation, "collection": collection, "reason": reason},
            recoverable=True
        )


class MongoDuplicateError(MongoDBException):
    """중복 키 오류"""
    def __init__(self, collection: str, key: str, value: Any):
        super().__init__(
            message=f"중복 키: {key}={value}",
            error_code="D003",
            details={"collection": collection, "key": key, "value": str(value)[:100]},
            recoverable=False
        )


class MongoNotFoundError(MongoDBException):
    """문서 없음 오류"""
    def __init__(self, collection: str, query: Dict):
        super().__init__(
            message="문서를 찾을 수 없음",
            error_code="D004",
            details={"collection": collection, "query": str(query)[:200]},
            recoverable=False
        )


class ObjectIdValidationError(MongoDBException):
    """ObjectId 검증 오류"""
    def __init__(self, value: str, context: str = ""):
        super().__init__(
            message=f"유효하지 않은 ObjectId: {value}",
            error_code="V006",
            details={"value": value, "context": context},
            recoverable=False
        )


# ============================================
# ObjectId 검증 유틸리티
# ============================================

def validate_object_id(value: str, context: str = "") -> ObjectId:
    """
    ObjectId 문자열을 검증하고 ObjectId 객체로 변환

    Args:
        value: ObjectId 문자열
        context: 에러 메시지에 포함할 컨텍스트

    Returns:
        ObjectId 객체

    Raises:
        ObjectIdValidationError: 유효하지 않은 ObjectId인 경우
    """
    if isinstance(value, ObjectId):
        return value

    if not value or not isinstance(value, str):
        raise ObjectIdValidationError(str(value), context)

    try:
        return ObjectId(value)
    except (InvalidId, TypeError) as e:
        raise ObjectIdValidationError(value, context) from e


def safe_object_id(value: str, context: str = "") -> Optional[ObjectId]:
    """ObjectId 변환 시도, 실패 시 None 반환"""
    try:
        return validate_object_id(value, context)
    except ObjectIdValidationError:
        logger.warning(f"Invalid ObjectId: {value} (context: {context})")
        return None


# ============================================
# 재시도 로직
# ============================================

def with_retry(max_retries: int = 3, base_delay: float = 1.0,
               retryable_exceptions: tuple = (AutoReconnect, NetworkTimeout, ConnectionFailure)):
    """
    재시도 데코레이터

    Args:
        max_retries: 최대 재시도 횟수
        base_delay: 기본 대기 시간 (초)
        retryable_exceptions: 재시도할 예외 타입
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e

                    if attempt >= max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded for {func.__name__}: {e}")
                        raise

                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Retry {attempt + 1}/{max_retries} for {func.__name__} after {delay}s: {e}")
                    time.sleep(delay)

            raise last_exception

        return wrapper
    return decorator


# ============================================
# DB 연산 데코레이터
# ============================================

def db_operation(collection_name: str, operation: str):
    """
    DB 연산 래퍼 데코레이터
    - 예외 처리
    - 로깅
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(self, *args, **kwargs) -> T:
            try:
                return func(self, *args, **kwargs)

            except MongoDuplicateKeyError as e:
                key_pattern = e.details.get('keyPattern', {}) if hasattr(e, 'details') else {}
                key = list(key_pattern.keys())[0] if key_pattern else 'unknown'
                raise MongoDuplicateError(
                    collection=collection_name,
                    key=key,
                    value=str(e)[:100]
                ) from e

            except (ConnectionFailure, ServerSelectionTimeoutError, AutoReconnect) as e:
                logger.error(f"DB connection error in {func.__name__}: {e}")
                raise MongoConnectionError(
                    reason=str(e),
                    host=self.uri if hasattr(self, 'uri') else ''
                ) from e

            except NetworkTimeout as e:
                logger.error(f"DB network timeout in {func.__name__}: {e}")
                raise MongoOperationError(
                    operation=operation,
                    collection=collection_name,
                    reason=f"Network timeout: {e}"
                ) from e

            except ExecutionTimeout as e:
                logger.error(f"DB execution timeout in {func.__name__}: {e}")
                raise MongoOperationError(
                    operation=operation,
                    collection=collection_name,
                    reason=f"Execution timeout: {e}"
                ) from e

            except (WriteError, WriteConcernError) as e:
                logger.error(f"DB write error in {func.__name__}: {e}")
                raise MongoOperationError(
                    operation=operation,
                    collection=collection_name,
                    reason=str(e)
                ) from e

            except OperationFailure as e:
                logger.error(f"DB operation failure in {func.__name__}: {e}")
                raise MongoOperationError(
                    operation=operation,
                    collection=collection_name,
                    reason=str(e)
                ) from e

            except (ObjectIdValidationError, MongoDBException):
                raise

            except Exception as e:
                logger.exception(f"Unexpected DB error in {func.__name__}: {e}")
                raise MongoOperationError(
                    operation=operation,
                    collection=collection_name,
                    reason=f"Unexpected error: {type(e).__name__}: {e}"
                ) from e

        return wrapper
    return decorator


# ============================================
# MongoDB 서비스 클래스
# ============================================

class MongoService:
    """
    Service for MongoDB operations with comprehensive exception handling.

    Features:
    - Automatic connection management
    - ObjectId validation
    - Retry logic for transient errors
    - Detailed error logging and reporting
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        database: Optional[str] = None
    ):
        """
        Initialize MongoDB Service.

        Args:
            uri: MongoDB connection URI. Defaults to MONGODB_URI env var.
            database: Database name. Defaults to MONGODB_DATABASE env var.
        """
        self.uri = uri or os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        self.database_name = database or os.getenv('MONGODB_DATABASE', 'crawler_system')

        self._client: Optional[MongoClient] = None
        self._db: Optional[Database] = None
        self._connection_timeout = int(os.getenv('MONGODB_TIMEOUT', '5000'))

    @property
    @with_retry(max_retries=3, base_delay=1.0)
    def client(self) -> MongoClient:
        """Get or create MongoDB client with connection validation."""
        if self._client is None:
            try:
                self._client = MongoClient(
                    self.uri,
                    serverSelectionTimeoutMS=self._connection_timeout,
                    connectTimeoutMS=self._connection_timeout,
                    socketTimeoutMS=30000,
                    retryWrites=True,
                    retryReads=True
                )
                # 연결 테스트
                self._client.admin.command('ping')
                logger.debug("MongoDB connection established")

            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                self._client = None
                raise MongoConnectionError(
                    reason=str(e),
                    host=self.uri
                ) from e

        return self._client

    @property
    def db(self) -> Database:
        """Get database instance."""
        if self._db is None:
            self._db = self.client[self.database_name]
        return self._db

    def close(self):
        """Close the MongoDB connection."""
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.warning(f"Error closing MongoDB connection: {e}")
            finally:
                self._client = None
                self._db = None

    def health_check(self) -> Dict[str, Any]:
        """Perform health check on MongoDB connection."""
        try:
            start = datetime.utcnow()
            self.client.admin.command('ping')
            latency = (datetime.utcnow() - start).total_seconds() * 1000

            return {
                "status": "healthy",
                "latency_ms": round(latency, 2),
                "database": self.database_name
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "database": self.database_name
            }

    # ==================== Sources Collection ====================

    @db_operation("sources", "create")
    def create_source(self, source_data: Dict[str, Any]) -> str:
        """
        Create a new source.

        Args:
            source_data: Source information

        Returns:
            Created source ID as string
        """
        now = datetime.utcnow()
        source_data.update({
            'status': source_data.get('status', 'active'),
            'error_count': 0,
            'created_at': now,
            'updated_at': now
        })

        result = self.db.sources.insert_one(source_data)
        logger.info(f"Created source with ID: {result.inserted_id}")
        return str(result.inserted_id)

    @db_operation("sources", "read")
    def get_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get source by ID."""
        oid = validate_object_id(source_id, "source_id")
        source = self.db.sources.find_one({'_id': oid})
        if source:
            source['_id'] = str(source['_id'])
        return source

    @db_operation("sources", "read")
    def get_source_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get source by name."""
        source = self.db.sources.find_one({'name': name})
        if source:
            source['_id'] = str(source['_id'])
        return source

    @db_operation("sources", "read")
    def list_sources(
        self,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List sources with optional filtering."""
        query = {}
        if status:
            query['status'] = status

        cursor = self.db.sources.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        sources = []
        for source in cursor:
            source['_id'] = str(source['_id'])
            sources.append(source)
        return sources

    @db_operation("sources", "update")
    def update_source(self, source_id: str, update_data: Dict[str, Any]) -> bool:
        """Update a source."""
        oid = validate_object_id(source_id, "source_id")
        update_data['updated_at'] = datetime.utcnow()
        result = self.db.sources.update_one(
            {'_id': oid},
            {'$set': update_data}
        )
        return result.modified_count > 0

    @db_operation("sources", "update")
    def update_source_status(
        self,
        source_id: str,
        status: str,
        last_run: Optional[datetime] = None,
        last_success: Optional[datetime] = None,
        increment_error: bool = False
    ) -> bool:
        """Update source execution status."""
        oid = validate_object_id(source_id, "source_id")
        update = {
            '$set': {
                'status': status,
                'updated_at': datetime.utcnow()
            }
        }

        if last_run:
            update['$set']['last_run'] = last_run
        if last_success:
            update['$set']['last_success'] = last_success
        if increment_error:
            update['$inc'] = {'error_count': 1}
        elif status == 'active':
            update['$set']['error_count'] = 0

        result = self.db.sources.update_one({'_id': oid}, update)
        return result.modified_count > 0

    @db_operation("sources", "delete")
    def delete_source(self, source_id: str) -> bool:
        """Delete a source and related data."""
        oid = validate_object_id(source_id, "source_id")

        # Delete related crawlers, results, history, and errors
        self.db.crawlers.delete_many({'source_id': oid})
        self.db.crawl_results.delete_many({'source_id': oid})
        self.db.crawler_history.delete_many({'source_id': oid})
        self.db.error_logs.delete_many({'source_id': oid})

        result = self.db.sources.delete_one({'_id': oid})
        if result.deleted_count > 0:
            logger.info(f"Deleted source and related data: {source_id}")
        return result.deleted_count > 0

    # ==================== Crawlers Collection ====================

    @db_operation("crawlers", "create")
    def create_crawler(self, crawler_data: Dict[str, Any]) -> str:
        """Create a new crawler."""
        now = datetime.utcnow()

        # Convert source_id to ObjectId if string
        if isinstance(crawler_data.get('source_id'), str):
            crawler_data['source_id'] = validate_object_id(
                crawler_data['source_id'], "crawler.source_id"
            )

        crawler_data.update({
            'version': crawler_data.get('version', 1),
            'status': crawler_data.get('status', 'testing'),
            'created_at': now
        })

        result = self.db.crawlers.insert_one(crawler_data)
        logger.info(f"Created crawler with ID: {result.inserted_id}")
        return str(result.inserted_id)

    @db_operation("crawlers", "read")
    def get_crawler(self, crawler_id: str) -> Optional[Dict[str, Any]]:
        """Get crawler by ID."""
        oid = validate_object_id(crawler_id, "crawler_id")
        crawler = self.db.crawlers.find_one({'_id': oid})
        if crawler:
            crawler['_id'] = str(crawler['_id'])
            crawler['source_id'] = str(crawler['source_id'])
        return crawler

    @db_operation("crawlers", "read")
    def get_active_crawler_for_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get the active crawler for a source."""
        oid = validate_object_id(source_id, "source_id")
        crawler = self.db.crawlers.find_one({
            'source_id': oid,
            'status': 'active'
        })
        if crawler:
            crawler['_id'] = str(crawler['_id'])
            crawler['source_id'] = str(crawler['source_id'])
        return crawler

    @db_operation("crawlers", "read")
    def get_crawler_by_dag_id(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """Get crawler by DAG ID."""
        crawler = self.db.crawlers.find_one({'dag_id': dag_id})
        if crawler:
            crawler['_id'] = str(crawler['_id'])
            crawler['source_id'] = str(crawler['source_id'])
        return crawler

    @db_operation("crawlers", "read")
    def list_crawlers(
        self,
        source_id: Optional[str] = None,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List crawlers with optional filtering."""
        query = {}
        if source_id:
            query['source_id'] = validate_object_id(source_id, "source_id")
        if status:
            query['status'] = status

        cursor = self.db.crawlers.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        crawlers = []
        for crawler in cursor:
            crawler['_id'] = str(crawler['_id'])
            crawler['source_id'] = str(crawler['source_id'])
            crawlers.append(crawler)
        return crawlers

    @db_operation("crawlers", "update")
    def update_crawler(self, crawler_id: str, update_data: Dict[str, Any]) -> bool:
        """Update a crawler."""
        oid = validate_object_id(crawler_id, "crawler_id")
        result = self.db.crawlers.update_one(
            {'_id': oid},
            {'$set': update_data}
        )
        return result.modified_count > 0

    @db_operation("crawlers", "update")
    def update_crawler_code(
        self,
        crawler_id: str,
        new_code: str,
        created_by: str = 'gpt',
        gpt_prompt: Optional[str] = None
    ) -> bool:
        """Update crawler code and increment version."""
        crawler = self.get_crawler(crawler_id)
        if not crawler:
            raise MongoNotFoundError("crawlers", {"_id": crawler_id})

        new_version = crawler['version'] + 1

        update_data = {
            'code': new_code,
            'version': new_version,
            'created_by': created_by
        }
        if gpt_prompt:
            update_data['gpt_prompt'] = gpt_prompt

        return self.update_crawler(crawler_id, update_data)

    @db_operation("crawlers", "update")
    def activate_crawler(self, crawler_id: str) -> bool:
        """Activate a crawler and deactivate others for the same source."""
        crawler = self.get_crawler(crawler_id)
        if not crawler:
            raise MongoNotFoundError("crawlers", {"_id": crawler_id})

        # Deactivate other crawlers for this source
        self.db.crawlers.update_many(
            {
                'source_id': ObjectId(crawler['source_id']),
                '_id': {'$ne': ObjectId(crawler_id)}
            },
            {'$set': {'status': 'deprecated'}}
        )

        # Activate this crawler
        return self.update_crawler(crawler_id, {'status': 'active'})

    # ==================== Crawl Results Collection ====================

    @db_operation("crawl_results", "create")
    def save_crawl_result(self, result_data: Dict[str, Any]) -> str:
        """Save crawl result."""
        # Convert IDs to ObjectId
        if isinstance(result_data.get('source_id'), str):
            result_data['source_id'] = validate_object_id(
                result_data['source_id'], "result.source_id"
            )
        if isinstance(result_data.get('crawler_id'), str):
            result_data['crawler_id'] = validate_object_id(
                result_data['crawler_id'], "result.crawler_id"
            )

        result_data['executed_at'] = result_data.get('executed_at', datetime.utcnow())

        result = self.db.crawl_results.insert_one(result_data)
        return str(result.inserted_id)

    @db_operation("crawl_results", "read")
    def get_crawl_results(
        self,
        source_id: Optional[str] = None,
        crawler_id: Optional[str] = None,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get crawl results with filtering."""
        query = {}
        if source_id:
            query['source_id'] = validate_object_id(source_id, "source_id")
        if crawler_id:
            query['crawler_id'] = validate_object_id(crawler_id, "crawler_id")
        if status:
            query['status'] = status

        cursor = self.db.crawl_results.find(query).sort('executed_at', DESCENDING).skip(skip).limit(limit)
        results = []
        for result in cursor:
            result['_id'] = str(result['_id'])
            result['source_id'] = str(result['source_id'])
            result['crawler_id'] = str(result['crawler_id'])
            results.append(result)
        return results

    @db_operation("crawl_results", "read")
    def get_latest_result(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get the latest crawl result for a source."""
        oid = validate_object_id(source_id, "source_id")
        result = self.db.crawl_results.find_one(
            {'source_id': oid},
            sort=[('executed_at', DESCENDING)]
        )
        if result:
            result['_id'] = str(result['_id'])
            result['source_id'] = str(result['source_id'])
            result['crawler_id'] = str(result['crawler_id'])
        return result

    # ==================== Crawler History Collection ====================

    @db_operation("crawler_history", "create")
    def save_crawler_history(
        self,
        crawler_id: str,
        version: int,
        code: str,
        change_reason: str,
        change_detail: str = '',
        changed_by: str = 'gpt'
    ) -> str:
        """Save crawler code history."""
        history_data = {
            'crawler_id': validate_object_id(crawler_id, "crawler_id"),
            'version': version,
            'code': code,
            'change_reason': change_reason,
            'change_detail': change_detail,
            'changed_at': datetime.utcnow(),
            'changed_by': changed_by
        }

        result = self.db.crawler_history.insert_one(history_data)
        logger.info(f"Saved crawler history: version {version}")
        return str(result.inserted_id)

    @db_operation("crawler_history", "read")
    def get_crawler_history(
        self,
        crawler_id: str,
        skip: int = 0,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get crawler code history."""
        oid = validate_object_id(crawler_id, "crawler_id")
        cursor = self.db.crawler_history.find(
            {'crawler_id': oid}
        ).sort('version', DESCENDING).skip(skip).limit(limit)

        history = []
        for h in cursor:
            h['_id'] = str(h['_id'])
            h['crawler_id'] = str(h['crawler_id'])
            history.append(h)
        return history

    @db_operation("crawler_history", "read")
    def get_crawler_version(self, crawler_id: str, version: int) -> Optional[Dict[str, Any]]:
        """Get specific version of crawler code."""
        oid = validate_object_id(crawler_id, "crawler_id")
        h = self.db.crawler_history.find_one({
            'crawler_id': oid,
            'version': version
        })
        if h:
            h['_id'] = str(h['_id'])
            h['crawler_id'] = str(h['crawler_id'])
        return h

    # ==================== Error Logs Collection ====================

    @db_operation("error_logs", "create")
    def log_error(self, error_data: Dict[str, Any]) -> str:
        """Log an error."""
        # Convert IDs to ObjectId
        if isinstance(error_data.get('source_id'), str):
            error_data['source_id'] = validate_object_id(
                error_data['source_id'], "error.source_id"
            )
        if isinstance(error_data.get('crawler_id'), str):
            error_data['crawler_id'] = validate_object_id(
                error_data['crawler_id'], "error.crawler_id"
            )

        error_data.update({
            'resolved': False,
            'created_at': datetime.utcnow()
        })

        result = self.db.error_logs.insert_one(error_data)
        logger.info(f"Logged error: {error_data.get('error_code')} - {error_data.get('message')}")
        return str(result.inserted_id)

    @db_operation("error_logs", "read")
    def get_error(self, error_id: str) -> Optional[Dict[str, Any]]:
        """Get error by ID."""
        oid = validate_object_id(error_id, "error_id")
        error = self.db.error_logs.find_one({'_id': oid})
        if error:
            error['_id'] = str(error['_id'])
            error['source_id'] = str(error['source_id'])
            if error.get('crawler_id'):
                error['crawler_id'] = str(error['crawler_id'])
        return error

    @db_operation("error_logs", "read")
    def list_errors(
        self,
        source_id: Optional[str] = None,
        resolved: Optional[bool] = None,
        error_code: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List errors with filtering."""
        query = {}
        if source_id:
            query['source_id'] = validate_object_id(source_id, "source_id")
        if resolved is not None:
            query['resolved'] = resolved
        if error_code:
            query['error_code'] = error_code

        cursor = self.db.error_logs.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        errors = []
        for error in cursor:
            error['_id'] = str(error['_id'])
            error['source_id'] = str(error['source_id'])
            if error.get('crawler_id'):
                error['crawler_id'] = str(error['crawler_id'])
            errors.append(error)
        return errors

    @db_operation("error_logs", "read")
    def get_unresolved_errors_for_source(self, source_id: str) -> List[Dict[str, Any]]:
        """Get all unresolved errors for a source."""
        return self.list_errors(source_id=source_id, resolved=False)

    @db_operation("error_logs", "update")
    def resolve_error(
        self,
        error_id: str,
        resolution_method: str,
        resolution_detail: str = ''
    ) -> bool:
        """Mark an error as resolved."""
        oid = validate_object_id(error_id, "error_id")
        result = self.db.error_logs.update_one(
            {'_id': oid},
            {
                '$set': {
                    'resolved': True,
                    'resolved_at': datetime.utcnow(),
                    'resolution_method': resolution_method,
                    'resolution_detail': resolution_detail
                }
            }
        )
        if result.modified_count > 0:
            logger.info(f"Resolved error {error_id}: {resolution_method}")
        return result.modified_count > 0

    @db_operation("error_logs", "update")
    def bulk_resolve_errors(
        self,
        source_id: str,
        resolution_method: str,
        resolution_detail: str = ''
    ) -> int:
        """Resolve all unresolved errors for a source."""
        oid = validate_object_id(source_id, "source_id")
        result = self.db.error_logs.update_many(
            {'source_id': oid, 'resolved': False},
            {
                '$set': {
                    'resolved': True,
                    'resolved_at': datetime.utcnow(),
                    'resolution_method': resolution_method,
                    'resolution_detail': resolution_detail
                }
            }
        )
        if result.modified_count > 0:
            logger.info(f"Bulk resolved {result.modified_count} errors for source {source_id}")
        return result.modified_count

    # ==================== Statistics ====================

    @db_operation("multiple", "read")
    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get dashboard statistics."""
        now = datetime.utcnow()

        # Source stats
        total_sources = self.db.sources.count_documents({})
        active_sources = self.db.sources.count_documents({'status': 'active'})
        error_sources = self.db.sources.count_documents({'status': 'error'})

        # Crawler stats
        total_crawlers = self.db.crawlers.count_documents({})
        active_crawlers = self.db.crawlers.count_documents({'status': 'active'})

        # Recent results
        recent_results = list(self.db.crawl_results.find(
            {},
            {'status': 1, 'executed_at': 1}
        ).sort('executed_at', DESCENDING).limit(100))

        success_count = sum(1 for r in recent_results if r.get('status') == 'success')
        failed_count = sum(1 for r in recent_results if r.get('status') == 'failed')

        # Unresolved errors
        unresolved_errors = self.db.error_logs.count_documents({'resolved': False})

        return {
            'sources': {
                'total': total_sources,
                'active': active_sources,
                'error': error_sources
            },
            'crawlers': {
                'total': total_crawlers,
                'active': active_crawlers
            },
            'recent_executions': {
                'total': len(recent_results),
                'success': success_count,
                'failed': failed_count,
                'success_rate': round(success_count / len(recent_results) * 100, 2) if recent_results else 0
            },
            'unresolved_errors': unresolved_errors,
            'timestamp': now.isoformat(),
            'health': self.health_check()
        }

    @db_operation("error_logs", "read")
    def get_error_statistics(self, days: int = 7) -> Dict[str, Any]:
        """Get error statistics for the last N days."""
        from datetime import timedelta

        cutoff = datetime.utcnow() - timedelta(days=days)

        # Error by code
        pipeline = [
            {'$match': {'created_at': {'$gte': cutoff}}},
            {'$group': {
                '_id': '$error_code',
                'count': {'$sum': 1},
                'resolved': {'$sum': {'$cond': ['$resolved', 1, 0]}}
            }},
            {'$sort': {'count': -1}}
        ]

        error_by_code = list(self.db.error_logs.aggregate(pipeline))

        # Error by source
        source_pipeline = [
            {'$match': {'created_at': {'$gte': cutoff}}},
            {'$group': {
                '_id': '$source_id',
                'count': {'$sum': 1},
                'unresolved': {'$sum': {'$cond': ['$resolved', 0, 1]}}
            }},
            {'$sort': {'count': -1}},
            {'$limit': 10}
        ]

        error_by_source = list(self.db.error_logs.aggregate(source_pipeline))

        # Convert ObjectIds to strings
        for item in error_by_source:
            if item.get('_id'):
                item['source_id'] = str(item['_id'])
                del item['_id']

        return {
            'period_days': days,
            'by_error_code': error_by_code,
            'by_source': error_by_source,
            'total': sum(e['count'] for e in error_by_code),
            'total_resolved': sum(e['resolved'] for e in error_by_code)
        }


# ============================================
# Wellknown Cases Collection (자가치유 학습용)
# ============================================

class WellknownCasesService(MongoService):
    """
    Service for managing wellknown recovery cases.
    Used by the auto-recovery system to learn from successful fixes.
    """

    @db_operation("wellknown_cases", "create")
    def save_wellknown_case(self, case_data: Dict[str, Any]) -> str:
        """Save a wellknown case for future reference."""
        case_data['created_at'] = datetime.utcnow()
        case_data['success_count'] = case_data.get('success_count', 1)

        result = self.db.wellknown_cases.insert_one(case_data)
        logger.info(f"Saved wellknown case: {case_data.get('error_signature')}")
        return str(result.inserted_id)

    @db_operation("wellknown_cases", "read")
    def find_similar_case(
        self,
        error_code: str,
        error_message: str,
        url_pattern: str = None
    ) -> Optional[Dict[str, Any]]:
        """Find a similar wellknown case."""
        query = {'error_code': error_code}

        if url_pattern:
            query['url_pattern'] = url_pattern

        # Find by exact error signature first
        error_signature = f"{error_code}:{error_message[:100]}"
        case = self.db.wellknown_cases.find_one(
            {'error_signature': error_signature},
            sort=[('success_count', DESCENDING)]
        )

        if not case:
            # Find by error code and similar message
            case = self.db.wellknown_cases.find_one(
                query,
                sort=[('success_count', DESCENDING)]
            )

        if case:
            case['_id'] = str(case['_id'])

        return case

    @db_operation("wellknown_cases", "update")
    def increment_success_count(self, case_id: str) -> bool:
        """Increment success count for a wellknown case."""
        oid = validate_object_id(case_id, "case_id")
        result = self.db.wellknown_cases.update_one(
            {'_id': oid},
            {
                '$inc': {'success_count': 1},
                '$set': {'last_used': datetime.utcnow()}
            }
        )
        return result.modified_count > 0

    @db_operation("wellknown_cases", "read")
    def get_top_cases(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get top wellknown cases by success count."""
        cursor = self.db.wellknown_cases.find().sort(
            'success_count', DESCENDING
        ).limit(limit)

        cases = []
        for case in cursor:
            case['_id'] = str(case['_id'])
            cases.append(case)
        return cases

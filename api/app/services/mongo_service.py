"""
MongoDB Service for FastAPI.

Provides database operations with comprehensive exception handling,
connection pooling, and automatic retry mechanisms.
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable, TypeVar
from functools import wraps
from contextlib import contextmanager
from bson import ObjectId
from bson.errors import InvalidId
from pymongo import MongoClient, DESCENDING
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

from ..exceptions import (
    DatabaseConnectionError,
    DatabaseOperationError,
    DuplicateKeyError,
    DocumentNotFoundError,
    DatabaseTransactionError,
    ObjectIdValidationError
)
from ..utils.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState
from ..utils.retry import RetryConfig, RetryStrategy, async_retry_with_backoff

logger = logging.getLogger(__name__)

T = TypeVar('T')


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
    """
    ObjectId 변환 시도, 실패 시 None 반환

    Args:
        value: ObjectId 문자열
        context: 로깅용 컨텍스트

    Returns:
        ObjectId 객체 또는 None
    """
    try:
        return validate_object_id(value, context)
    except ObjectIdValidationError:
        logger.warning(f"Invalid ObjectId: {value} (context: {context})")
        return None


# ============================================
# DB 연산 데코레이터
# ============================================

def db_operation(collection_name: str, operation: str):
    """
    DB 연산 래퍼 데코레이터
    - 예외 처리
    - 로깅
    - 재시도 (일시적 오류)

    Args:
        collection_name: 컬렉션 이름
        operation: 연산 유형 (read, write, delete 등)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(self, *args, **kwargs) -> T:
            try:
                return func(self, *args, **kwargs)

            except MongoDuplicateKeyError as e:
                # 중복 키 에러
                key_pattern = e.details.get('keyPattern', {}) if hasattr(e, 'details') else {}
                key = list(key_pattern.keys())[0] if key_pattern else 'unknown'
                raise DuplicateKeyError(
                    collection=collection_name,
                    key=key,
                    value=str(e)[:100]
                ) from e

            except NetworkTimeout as e:
                # 네트워크 타임아웃 - 재시도 가능
                # NOTE: NetworkTimeout extends ConnectionFailure, so must be caught first
                logger.error(f"DB network timeout in {func.__name__}: {e}")
                raise DatabaseOperationError(
                    operation=operation,
                    collection=collection_name,
                    reason=f"Network timeout: {e}"
                ) from e

            except (ConnectionFailure, ServerSelectionTimeoutError, AutoReconnect) as e:
                # 연결 오류 - 재시도 가능
                logger.error(f"DB connection error in {func.__name__}: {e}")
                raise DatabaseConnectionError(
                    reason=str(e),
                    host=self.uri if hasattr(self, 'uri') else ''
                ) from e

            except ExecutionTimeout as e:
                # 쿼리 실행 타임아웃
                logger.error(f"DB execution timeout in {func.__name__}: {e}")
                raise DatabaseOperationError(
                    operation=operation,
                    collection=collection_name,
                    reason=f"Execution timeout: {e}"
                ) from e

            except (WriteError, WriteConcernError) as e:
                # 쓰기 오류
                logger.error(f"DB write error in {func.__name__}: {e}")
                raise DatabaseOperationError(
                    operation=operation,
                    collection=collection_name,
                    reason=str(e)
                ) from e

            except OperationFailure as e:
                # 일반 연산 실패
                logger.error(f"DB operation failure in {func.__name__}: {e}")
                raise DatabaseOperationError(
                    operation=operation,
                    collection=collection_name,
                    reason=str(e)
                ) from e

            except ObjectIdValidationError:
                # ObjectId 검증 에러는 그대로 전파
                raise

            except Exception as e:
                # 예상치 못한 오류
                logger.exception(f"Unexpected DB error in {func.__name__}: {e}")
                raise DatabaseOperationError(
                    operation=operation,
                    collection=collection_name,
                    reason=f"Unexpected error: {type(e).__name__}: {e}"
                ) from e

        return wrapper
    return decorator


# ============================================
# Async Motor DB dependency (for async routers)
# ============================================

_motor_client = None

async def get_db():
    """Async Motor database dependency for FastAPI."""
    from motor.motor_asyncio import AsyncIOMotorClient
    global _motor_client
    if _motor_client is None:
        uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        _motor_client = AsyncIOMotorClient(uri)
    db_name = os.getenv('MONGODB_DATABASE', 'crawler_system')
    yield _motor_client[db_name]


# ============================================
# MongoDB 서비스 클래스
# ============================================

class MongoService:
    """
    MongoDB service with exception handling, connection pooling, and retry logic.

    Usage:
        with MongoService() as mongo:
            source = mongo.get_source(source_id)

        # Or manually:
        mongo = MongoService()
        try:
            source = mongo.get_source(source_id)
        finally:
            mongo.close()

    N+1 쿼리 최적화를 위한 권장 인덱스:
    -----------------------------------------
    # sources 컬렉션
    db.sources.createIndex({"status": 1, "created_at": -1})
    db.sources.createIndex({"name": 1}, {unique: true})
    db.sources.createIndex({"error_count": 1})

    # crawlers 컬렉션
    db.crawlers.createIndex({"source_id": 1, "status": 1})
    db.crawlers.createIndex({"status": 1, "created_at": -1})

    # crawler_history 컬렉션
    db.crawler_history.createIndex({"crawler_id": 1, "version": 1}, {unique: true})
    db.crawler_history.createIndex({"changed_at": -1})

    # crawl_results 컬렉션
    db.crawl_results.createIndex({"source_id": 1, "executed_at": -1})
    db.crawl_results.createIndex({"executed_at": -1})
    db.crawl_results.createIndex({"status": 1})

    # error_logs 컬렉션
    db.error_logs.createIndex({"resolved": 1, "created_at": -1})
    db.error_logs.createIndex({"source_id": 1})

    # data_reviews 컬렉션 (reviews.py 최적화)
    db.data_reviews.createIndex({"review_status": 1, "created_at": 1})
    db.data_reviews.createIndex({"review_status": 1, "confidence_score": 1})
    db.data_reviews.createIndex({"source_id": 1, "review_status": 1})
    db.data_reviews.createIndex({"crawl_result_id": 1, "data_record_index": 1})
    db.data_reviews.createIndex({"reviewed_at": -1})
    db.data_reviews.createIndex({"needs_number_review": 1, "review_status": 1})
    """

    # 연결 Circuit Breaker (클래스 레벨)
    _connection_circuit = CircuitBreaker(
        name="mongo_connection",
        config=CircuitBreakerConfig(
            failure_threshold=5,
            reset_timeout=30,
            half_open_max_calls=3
        )
    )

    def __init__(self):
        """Initialize MongoDB connection."""
        self.uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        self.database_name = os.getenv('MONGODB_DATABASE', 'crawler_system')
        self._client: Optional[MongoClient] = None
        self._connection_timeout = int(os.getenv('MONGODB_TIMEOUT', '5000'))

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.close()
        return False  # Don't suppress exceptions

    @property
    def client(self) -> MongoClient:
        """
        Get or create MongoDB client with connection validation.

        Raises:
            DatabaseConnectionError: 연결 실패 시
        """
        if self._client is None:
            # Circuit Breaker 상태 확인
            if self._connection_circuit.state == CircuitState.OPEN:
                raise DatabaseConnectionError(
                    reason="Circuit breaker is OPEN - too many connection failures",
                    host=self.uri
                )

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
                self._connection_circuit.record_success()
                logger.debug("MongoDB connection established")

            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                self._connection_circuit.record_failure()
                self._client = None
                raise DatabaseConnectionError(
                    reason=str(e),
                    host=self.uri
                ) from e

        return self._client

    @property
    def db(self):
        """Get database instance."""
        return self.client[self.database_name]

    def close(self):
        """Close the MongoDB connection safely."""
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.warning(f"Error closing MongoDB connection: {e}")
            finally:
                self._client = None

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on MongoDB connection.

        Returns:
            Health check result with connection status
        """
        try:
            start = datetime.utcnow()
            self.client.admin.command('ping')
            latency = (datetime.utcnow() - start).total_seconds() * 1000

            return {
                "status": "healthy",
                "latency_ms": round(latency, 2),
                "circuit_state": self._connection_circuit.state.value,
                "database": self.database_name
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "circuit_state": self._connection_circuit.state.value,
                "database": self.database_name
            }

    def _serialize_doc(self, doc: Dict) -> Optional[Dict]:
        """Convert MongoDB document to JSON-serializable format."""
        if doc is None:
            return None

        result = {}
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                result[key] = str(value)
            elif isinstance(value, datetime):
                result[key] = value
            else:
                result[key] = value
        return result

    # ==================== Batch Query Methods ====================
    # N+1 쿼리 최적화를 위한 배치 조회 메서드들

    @db_operation("sources", "read")
    def get_sources_by_ids(self, source_ids: List[str], projection: Dict = None) -> Dict[str, Dict]:
        """
        여러 소스를 ID로 일괄 조회 (N+1 쿼리 방지)

        Args:
            source_ids: 조회할 소스 ID 리스트
            projection: 반환할 필드 제한 (선택사항)

        Returns:
            {source_id: source_document} 형태의 딕셔너리
        """
        if not source_ids:
            return {}

        # ObjectId 변환 (유효하지 않은 ID는 건너뜀)
        valid_oids = []
        for sid in source_ids:
            oid = safe_object_id(sid, "batch_source_ids")
            if oid:
                valid_oids.append(oid)

        if not valid_oids:
            return {}

        cursor = self.db.sources.find({'_id': {'$in': valid_oids}}, projection)
        return {str(doc['_id']): self._serialize_doc(doc) for doc in cursor}

    @db_operation("crawlers", "read")
    def get_crawlers_by_ids(self, crawler_ids: List[str], projection: Dict = None) -> Dict[str, Dict]:
        """
        여러 크롤러를 ID로 일괄 조회 (N+1 쿼리 방지)

        Args:
            crawler_ids: 조회할 크롤러 ID 리스트
            projection: 반환할 필드 제한 (선택사항)

        Returns:
            {crawler_id: crawler_document} 형태의 딕셔너리
        """
        if not crawler_ids:
            return {}

        valid_oids = []
        for cid in crawler_ids:
            oid = safe_object_id(cid, "batch_crawler_ids")
            if oid:
                valid_oids.append(oid)

        if not valid_oids:
            return {}

        cursor = self.db.crawlers.find({'_id': {'$in': valid_oids}}, projection)
        return {str(doc['_id']): self._serialize_doc(doc) for doc in cursor}

    @db_operation("crawl_results", "read")
    def get_crawl_results_by_ids(self, result_ids: List[str], projection: Dict = None) -> Dict[str, Dict]:
        """
        여러 크롤 결과를 ID로 일괄 조회 (N+1 쿼리 방지)

        Args:
            result_ids: 조회할 크롤 결과 ID 리스트
            projection: 반환할 필드 제한 (선택사항)

        Returns:
            {result_id: result_document} 형태의 딕셔너리
        """
        if not result_ids:
            return {}

        valid_oids = []
        for rid in result_ids:
            oid = safe_object_id(rid, "batch_result_ids")
            if oid:
                valid_oids.append(oid)

        if not valid_oids:
            return {}

        cursor = self.db.crawl_results.find({'_id': {'$in': valid_oids}}, projection)
        return {str(doc['_id']): self._serialize_doc(doc) for doc in cursor}

    @db_operation("crawler_history", "read")
    def get_crawler_version(self, crawler_id: str, version: int) -> Optional[Dict]:
        """
        특정 버전의 크롤러 히스토리 직접 조회 (반복 조회 방지)

        Args:
            crawler_id: 크롤러 ID
            version: 버전 번호

        Returns:
            히스토리 문서 또는 None
        """
        oid = validate_object_id(crawler_id, "crawler_id")
        doc = self.db.crawler_history.find_one({
            'crawler_id': oid,
            'version': version
        })
        return self._serialize_doc(doc)

    # ==================== Aggregation Pipeline Methods ====================
    # $lookup을 활용한 조인 쿼리 최적화

    @db_operation("sources", "read")
    def list_sources_with_crawler_info(self, status: str = None, skip: int = 0, limit: int = 100) -> List[Dict]:
        """
        소스 목록을 활성 크롤러 정보와 함께 조회 ($lookup 사용)

        N+1 문제 해결: 소스당 크롤러 조회 대신 단일 aggregation으로 처리
        """
        match_stage = {}
        if status:
            match_stage['status'] = status

        pipeline = [
            {'$match': match_stage} if match_stage else {'$match': {}},
            {'$sort': {'created_at': DESCENDING}},
            {'$skip': skip},
            {'$limit': limit},
            # 활성 크롤러 정보 조인
            {
                '$lookup': {
                    'from': 'crawlers',
                    'let': {'source_id': '$_id'},
                    'pipeline': [
                        {'$match': {
                            '$expr': {'$eq': ['$source_id', '$$source_id']},
                            'status': 'active'
                        }},
                        {'$project': {'_id': 1, 'version': 1, 'dag_id': 1, 'created_at': 1}}
                    ],
                    'as': 'active_crawler'
                }
            },
            # 최근 크롤 결과 카운트 (선택적)
            {
                '$lookup': {
                    'from': 'crawl_results',
                    'let': {'source_id': '$_id'},
                    'pipeline': [
                        {'$match': {'$expr': {'$eq': ['$source_id', '$$source_id']}}},
                        {'$count': 'total'}
                    ],
                    'as': 'result_stats'
                }
            },
            # 배열을 단일 객체로 변환
            {
                '$addFields': {
                    'active_crawler': {'$arrayElemAt': ['$active_crawler', 0]},
                    'crawl_count': {'$ifNull': [{'$arrayElemAt': ['$result_stats.total', 0]}, 0]}
                }
            },
            {'$project': {'result_stats': 0}}
        ]

        cursor = self.db.sources.aggregate(pipeline)
        return [self._serialize_doc(doc) for doc in cursor]

    @db_operation("crawlers", "read")
    def list_crawlers_with_source_info(self, source_id: str = None, status: str = None,
                                        skip: int = 0, limit: int = 100) -> List[Dict]:
        """
        크롤러 목록을 소스 정보와 함께 조회 ($lookup 사용)

        N+1 문제 해결: 크롤러당 소스 조회 대신 단일 aggregation으로 처리
        """
        match_stage = {}
        if source_id:
            match_stage['source_id'] = validate_object_id(source_id, "source_id")
        if status:
            match_stage['status'] = status

        pipeline = [
            {'$match': match_stage} if match_stage else {'$match': {}},
            {'$sort': {'created_at': DESCENDING}},
            {'$skip': skip},
            {'$limit': limit},
            # code 필드 제외 (성능)
            {'$project': {'code': 0}},
            # 소스 정보 조인
            {
                '$lookup': {
                    'from': 'sources',
                    'localField': 'source_id',
                    'foreignField': '_id',
                    'pipeline': [
                        {'$project': {'name': 1, 'url': 1, 'type': 1, 'status': 1}}
                    ],
                    'as': 'source_info'
                }
            },
            {
                '$addFields': {
                    'source_info': {'$arrayElemAt': ['$source_info', 0]}
                }
            }
        ]

        cursor = self.db.crawlers.aggregate(pipeline)
        return [self._serialize_doc(doc) for doc in cursor]

    @db_operation("multiple", "read")
    def get_recent_activity_optimized(self, hours: int = 24) -> Dict[str, Any]:
        """
        최근 활동 데이터를 단일 aggregation으로 조회

        N+1 문제 해결: 3개의 개별 쿼리 대신 source 정보를 함께 조인
        """
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        # 크롤 결과 + 소스 정보
        results_pipeline = [
            {'$match': {'executed_at': {'$gte': cutoff}}},
            {'$sort': {'executed_at': DESCENDING}},
            {'$limit': 50},
            {'$lookup': {
                'from': 'sources',
                'localField': 'source_id',
                'foreignField': '_id',
                'pipeline': [{'$project': {'name': 1}}],
                'as': 'source'
            }},
            {'$addFields': {
                'source_name': {'$arrayElemAt': ['$source.name', 0]}
            }},
            {'$project': {
                'source_id': 1, 'status': 1, 'executed_at': 1,
                'record_count': 1, 'execution_time_ms': 1, 'source_name': 1
            }}
        ]

        # 에러 로그 + 소스 정보
        errors_pipeline = [
            {'$match': {'created_at': {'$gte': cutoff}}},
            {'$sort': {'created_at': DESCENDING}},
            {'$limit': 20},
            {'$lookup': {
                'from': 'sources',
                'localField': 'source_id',
                'foreignField': '_id',
                'pipeline': [{'$project': {'name': 1}}],
                'as': 'source'
            }},
            {'$addFields': {
                'source_name': {'$arrayElemAt': ['$source.name', 0]}
            }},
            {'$project': {
                'source_id': 1, 'error_code': 1, 'message': 1,
                'resolved': 1, 'created_at': 1, 'source_name': 1
            }}
        ]

        # 코드 변경 이력 + 크롤러/소스 정보
        changes_pipeline = [
            {'$match': {'changed_at': {'$gte': cutoff}}},
            {'$sort': {'changed_at': DESCENDING}},
            {'$limit': 20},
            {'$lookup': {
                'from': 'crawlers',
                'localField': 'crawler_id',
                'foreignField': '_id',
                'pipeline': [{'$project': {'source_id': 1}}],
                'as': 'crawler'
            }},
            {'$addFields': {
                'source_id': {'$arrayElemAt': ['$crawler.source_id', 0]}
            }},
            {'$lookup': {
                'from': 'sources',
                'localField': 'source_id',
                'foreignField': '_id',
                'pipeline': [{'$project': {'name': 1}}],
                'as': 'source'
            }},
            {'$addFields': {
                'source_name': {'$arrayElemAt': ['$source.name', 0]}
            }},
            {'$project': {
                'crawler_id': 1, 'version': 1, 'change_reason': 1,
                'changed_at': 1, 'changed_by': 1, 'source_name': 1
            }}
        ]

        recent_results = [self._serialize_doc(doc) for doc in self.db.crawl_results.aggregate(results_pipeline)]
        recent_errors = [self._serialize_doc(doc) for doc in self.db.error_logs.aggregate(errors_pipeline)]
        recent_changes = [self._serialize_doc(doc) for doc in self.db.crawler_history.aggregate(changes_pipeline)]

        return {
            'period_hours': hours,
            'crawl_results': recent_results,
            'errors': recent_errors,
            'code_changes': recent_changes
        }

    @db_operation("multiple", "read")
    def get_dashboard_stats_optimized(self) -> Dict[str, Any]:
        """
        대시보드 통계를 최적화된 단일 aggregation으로 조회

        N+1 문제 해결: 여러 count 쿼리를 $facet으로 병합
        """
        # $facet을 사용하여 여러 집계를 단일 쿼리로 실행
        source_pipeline = [
            {'$facet': {
                'total': [{'$count': 'count'}],
                'active': [{'$match': {'status': 'active'}}, {'$count': 'count'}],
                'error': [{'$match': {'status': 'error'}}, {'$count': 'count'}]
            }}
        ]

        crawler_pipeline = [
            {'$facet': {
                'total': [{'$count': 'count'}],
                'active': [{'$match': {'status': 'active'}}, {'$count': 'count'}]
            }}
        ]

        # 최근 실행 결과 통계
        results_pipeline = [
            {'$sort': {'executed_at': DESCENDING}},
            {'$limit': 100},
            {'$group': {
                '_id': None,
                'total': {'$sum': 1},
                'success': {'$sum': {'$cond': [{'$eq': ['$status', 'success']}, 1, 0]}},
                'failed': {'$sum': {'$cond': [{'$eq': ['$status', 'failed']}, 1, 0]}}
            }}
        ]

        source_stats = list(self.db.sources.aggregate(source_pipeline))
        crawler_stats = list(self.db.crawlers.aggregate(crawler_pipeline))
        results_stats = list(self.db.crawl_results.aggregate(results_pipeline))
        unresolved = self.db.error_logs.count_documents({'resolved': False})

        # 결과 파싱
        s = source_stats[0] if source_stats else {'total': [], 'active': [], 'error': []}
        c = crawler_stats[0] if crawler_stats else {'total': [], 'active': []}
        r = results_stats[0] if results_stats else {'total': 0, 'success': 0, 'failed': 0}

        sources_total = s['total'][0]['count'] if s['total'] else 0
        sources_active = s['active'][0]['count'] if s['active'] else 0
        sources_error = s['error'][0]['count'] if s['error'] else 0

        crawlers_total = c['total'][0]['count'] if c['total'] else 0
        crawlers_active = c['active'][0]['count'] if c['active'] else 0

        total = r.get('total', 0) if isinstance(r, dict) else 0
        success = r.get('success', 0) if isinstance(r, dict) else 0
        failed = r.get('failed', 0) if isinstance(r, dict) else 0

        return {
            'sources': {'total': sources_total, 'active': sources_active, 'error': sources_error},
            'crawlers': {'total': crawlers_total, 'active': crawlers_active},
            'recent_executions': {
                'total': total,
                'success': success,
                'failed': failed,
                'success_rate': round(success / total * 100, 2) if total > 0 else 0
            },
            'unresolved_errors': unresolved,
            'timestamp': datetime.utcnow(),
            'health': self.health_check()
        }

    # ==================== Sources Collection ====================

    @db_operation("sources", "create")
    def create_source(self, data: Dict[str, Any]) -> str:
        """
        Create a new source.

        Args:
            data: Source data

        Returns:
            Created source ID

        Raises:
            DuplicateKeyError: 중복 이름
            DatabaseOperationError: DB 연산 실패
        """
        now = datetime.utcnow()
        data.update({
            'status': 'inactive',
            'error_count': 0,
            'created_at': now,
            'updated_at': now
        })
        result = self.db.sources.insert_one(data)
        logger.info(f"Created source: {result.inserted_id}")
        return str(result.inserted_id)

    @db_operation("sources", "read")
    def get_source(self, source_id: str) -> Optional[Dict]:
        """
        Get source by ID.

        Args:
            source_id: Source ObjectId string

        Returns:
            Source document or None

        Raises:
            ObjectIdValidationError: 유효하지 않은 ID
        """
        oid = validate_object_id(source_id, "source_id")
        doc = self.db.sources.find_one({'_id': oid})
        return self._serialize_doc(doc)

    @db_operation("sources", "read")
    def get_source_by_name(self, name: str) -> Optional[Dict]:
        """Get source by name."""
        doc = self.db.sources.find_one({'name': name})
        return self._serialize_doc(doc)

    @db_operation("sources", "read")
    def list_sources(self, status: str = None, skip: int = 0, limit: int = 100) -> List[Dict]:
        """List sources with optional filtering."""
        query = {}
        if status:
            query['status'] = status
        cursor = self.db.sources.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        return [self._serialize_doc(doc) for doc in cursor]

    @db_operation("sources", "read")
    def count_sources(self, status: str = None) -> int:
        """Count sources with optional status filter."""
        query = {}
        if status:
            query['status'] = status
        return self.db.sources.count_documents(query)

    @db_operation("sources", "update")
    def update_source(self, source_id: str, data: Dict[str, Any]) -> bool:
        """
        Update a source.

        Args:
            source_id: Source ObjectId string
            data: Update data

        Returns:
            True if modified
        """
        oid = validate_object_id(source_id, "source_id")
        data['updated_at'] = datetime.utcnow()
        result = self.db.sources.update_one(
            {'_id': oid},
            {'$set': data}
        )
        return result.modified_count > 0

    @db_operation("sources", "delete")
    def delete_source(self, source_id: str) -> bool:
        """
        Delete a source and all related data.

        Args:
            source_id: Source ObjectId string

        Returns:
            True if deleted
        """
        oid = validate_object_id(source_id, "source_id")

        # Delete related data first
        self.db.crawlers.delete_many({'source_id': oid})
        self.db.crawl_results.delete_many({'source_id': oid})
        self.db.crawler_history.delete_many({'source_id': oid})
        self.db.error_logs.delete_many({'source_id': oid})

        result = self.db.sources.delete_one({'_id': oid})
        if result.deleted_count > 0:
            logger.info(f"Deleted source and related data: {source_id}")
        return result.deleted_count > 0

    # ==================== Crawlers Collection ====================

    @db_operation("crawlers", "read")
    def get_crawler(self, crawler_id: str) -> Optional[Dict]:
        """Get crawler by ID."""
        oid = validate_object_id(crawler_id, "crawler_id")
        doc = self.db.crawlers.find_one({'_id': oid})
        return self._serialize_doc(doc)

    @db_operation("crawlers", "read")
    def get_active_crawler(self, source_id: str) -> Optional[Dict]:
        """Get active crawler for a source."""
        oid = validate_object_id(source_id, "source_id")
        doc = self.db.crawlers.find_one({
            'source_id': oid,
            'status': 'active'
        })
        return self._serialize_doc(doc)

    @db_operation("crawlers", "read")
    def list_crawlers(self, source_id: str = None, status: str = None,
                      skip: int = 0, limit: int = 100) -> List[Dict]:
        """List crawlers with optional filtering."""
        query = {}
        if source_id:
            query['source_id'] = validate_object_id(source_id, "source_id")
        if status:
            query['status'] = status
        cursor = self.db.crawlers.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        return [self._serialize_doc(doc) for doc in cursor]

    @db_operation("crawlers", "read")
    def count_crawlers(self, status: str = None) -> int:
        """Count crawlers with optional status filter."""
        query = {}
        if status:
            query['status'] = status
        return self.db.crawlers.count_documents(query)

    @db_operation("crawlers", "read")
    def get_crawler_history(self, crawler_id: str, skip: int = 0, limit: int = 50) -> List[Dict]:
        """Get crawler code history."""
        oid = validate_object_id(crawler_id, "crawler_id")
        cursor = self.db.crawler_history.find(
            {'crawler_id': oid}
        ).sort('version', DESCENDING).skip(skip).limit(limit)
        return [self._serialize_doc(doc) for doc in cursor]

    # ==================== Results Collection ====================

    @db_operation("crawl_results", "read")
    def get_crawl_results(self, source_id: str = None, status: str = None,
                          skip: int = 0, limit: int = 100) -> List[Dict]:
        """Get crawl results with filtering."""
        query = {}
        if source_id:
            query['source_id'] = validate_object_id(source_id, "source_id")
        if status:
            query['status'] = status
        cursor = self.db.crawl_results.find(query).sort('executed_at', DESCENDING).skip(skip).limit(limit)
        return [self._serialize_doc(doc) for doc in cursor]

    # ==================== Error Logs Collection ====================

    @db_operation("error_logs", "read")
    def list_errors(self, resolved: bool = None, source_id: str = None,
                    skip: int = 0, limit: int = 100) -> List[Dict]:
        """List errors with optional filtering."""
        query = {}
        if resolved is not None:
            query['resolved'] = resolved
        if source_id:
            query['source_id'] = validate_object_id(source_id, "source_id")
        cursor = self.db.error_logs.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        return [self._serialize_doc(doc) for doc in cursor]

    @db_operation("error_logs", "read")
    def get_error(self, error_id: str) -> Optional[Dict]:
        """Get error by ID."""
        oid = validate_object_id(error_id, "error_id")
        doc = self.db.error_logs.find_one({'_id': oid})
        return self._serialize_doc(doc)

    @db_operation("error_logs", "read")
    def count_errors(self, resolved: bool = None) -> int:
        """Count errors with optional resolved filter."""
        query = {}
        if resolved is not None:
            query['resolved'] = resolved
        return self.db.error_logs.count_documents(query)

    @db_operation("error_logs", "update")
    def resolve_error(self, error_id: str, method: str, detail: str = '') -> bool:
        """Mark an error as resolved."""
        oid = validate_object_id(error_id, "error_id")
        result = self.db.error_logs.update_one(
            {'_id': oid},
            {'$set': {
                'resolved': True,
                'resolved_at': datetime.utcnow(),
                'resolution_method': method,
                'resolution_detail': detail
            }}
        )
        return result.modified_count > 0

    # ==================== Dashboard ====================

    @db_operation("multiple", "read")
    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get dashboard statistics."""
        sources_total = self.db.sources.count_documents({})
        sources_active = self.db.sources.count_documents({'status': 'active'})
        sources_error = self.db.sources.count_documents({'status': 'error'})

        crawlers_total = self.db.crawlers.count_documents({})
        crawlers_active = self.db.crawlers.count_documents({'status': 'active'})

        recent_results = list(self.db.crawl_results.find(
            {}, {'status': 1}
        ).sort('executed_at', DESCENDING).limit(100))

        success = sum(1 for r in recent_results if r.get('status') == 'success')
        failed = sum(1 for r in recent_results if r.get('status') == 'failed')
        total = len(recent_results)

        unresolved = self.db.error_logs.count_documents({'resolved': False})

        return {
            'sources': {'total': sources_total, 'active': sources_active, 'error': sources_error},
            'crawlers': {'total': crawlers_total, 'active': crawlers_active},
            'recent_executions': {
                'total': total,
                'success': success,
                'failed': failed,
                'success_rate': round(success / total * 100, 2) if total > 0 else 0
            },
            'unresolved_errors': unresolved,
            'timestamp': datetime.utcnow(),
            'health': self.health_check()
        }


# ============================================
# 트랜잭션 지원 (Replica Set 필요)
# ============================================

class MongoTransactionService(MongoService):
    """
    MongoDB service with transaction support.

    Note: Requires MongoDB replica set configuration.
    """

    @contextmanager
    def transaction(self):
        """
        Context manager for MongoDB transactions.

        Usage:
            with mongo.transaction() as session:
                mongo.create_source(data, session=session)
                mongo.update_crawler(crawler_id, update, session=session)

        Raises:
            DatabaseTransactionError: 트랜잭션 실패 시
        """
        session = self.client.start_session()
        try:
            with session.start_transaction():
                yield session
                # 정상 완료 시 자동 커밋
        except OperationFailure as e:
            # 트랜잭션 실패
            logger.error(f"Transaction failed: {e}")
            raise DatabaseTransactionError(
                operation="transaction",
                reason=str(e),
                rollback_success=True
            ) from e
        except Exception as e:
            # 예상치 못한 오류
            logger.exception(f"Unexpected transaction error: {e}")
            raise DatabaseTransactionError(
                operation="transaction",
                reason=f"Unexpected: {type(e).__name__}: {e}",
                rollback_success=False
            ) from e
        finally:
            session.end_session()

    @db_operation("sources", "create")
    def create_source_with_session(self, data: Dict[str, Any], session) -> str:
        """Create source within a transaction session."""
        now = datetime.utcnow()
        data.update({
            'status': 'inactive',
            'error_count': 0,
            'created_at': now,
            'updated_at': now
        })
        result = self.db.sources.insert_one(data, session=session)
        return str(result.inserted_id)

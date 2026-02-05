"""
Change Detection Service - 데이터 변경 감지

기능:
- 콘텐츠 해시 기반 중복/변경 감지
- 증분 크롤링 지원
- 변경 이력 추적
- 트래픽 최소화
"""

import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum

from app.core import get_logger

logger = get_logger(__name__)


class ChangeType(str, Enum):
    """변경 유형"""
    NEW = "new"           # 신규 데이터
    MODIFIED = "modified" # 변경된 데이터
    UNCHANGED = "unchanged"  # 변경 없음
    DELETED = "deleted"   # 삭제된 데이터


@dataclass
class ChangeResult:
    """변경 감지 결과"""
    record_id: str
    content_hash: str
    change_type: ChangeType
    previous_hash: Optional[str] = None
    changed_fields: Optional[List[str]] = None


@dataclass
class BatchChangeResult:
    """배치 변경 감지 결과"""
    source_id: str
    total_records: int
    new_count: int
    modified_count: int
    unchanged_count: int
    deleted_count: int
    new_records: List[Dict]
    modified_records: List[Dict]
    unchanged_ids: List[str]
    processing_time_ms: float


class ChangeDetectionService:
    """
    변경 감지 서비스

    크롤링 데이터의 변경 여부를 해시 비교로 감지하여
    변경된 데이터만 처리하도록 필터링합니다.
    """

    # MongoDB 컬렉션명
    HASH_COLLECTION = "content_hashes"
    CHANGE_LOG_COLLECTION = "change_logs"

    def __init__(self, mongo_service=None):
        self.mongo = mongo_service

    def generate_hash(
        self,
        record: Dict[str, Any],
        hash_fields: List[str] = None,
        include_all: bool = False
    ) -> str:
        """
        레코드의 콘텐츠 해시 생성

        Args:
            record: 데이터 레코드
            hash_fields: 해시에 포함할 필드 목록 (None이면 기본 필드)
            include_all: 모든 필드 포함 여부

        Returns:
            MD5 해시 문자열
        """
        if include_all:
            # 메타데이터 제외하고 모든 필드
            excluded = {'_id', 'created_at', 'updated_at', 'content_hash', 'crawled_at'}
            hash_fields = [k for k in record.keys() if k not in excluded]
        elif hash_fields is None:
            # 기본 필드: title, content, url
            hash_fields = ['title', 'content', 'url', 'body']

        # 필드 값들을 정렬된 순서로 결합
        values = []
        for field in sorted(hash_fields):
            value = record.get(field)
            if value is not None:
                # 리스트/딕셔너리는 JSON 문자열로
                if isinstance(value, (list, dict)):
                    import json
                    value = json.dumps(value, sort_keys=True, ensure_ascii=False)
                values.append(f"{field}:{value}")

        combined = '|'.join(values)
        return hashlib.md5(combined.encode('utf-8')).hexdigest()

    def generate_record_id(
        self,
        record: Dict[str, Any],
        id_fields: List[str] = None
    ) -> str:
        """
        레코드의 고유 ID 생성

        Args:
            record: 데이터 레코드
            id_fields: ID 생성에 사용할 필드 (None이면 url 또는 title)
        """
        if id_fields is None:
            id_fields = ['url', 'title', 'id']

        for field in id_fields:
            if field in record and record[field]:
                value = str(record[field])
                return hashlib.md5(value.encode('utf-8')).hexdigest()[:16]

        # 폴백: 전체 레코드 해시
        return self.generate_hash(record)[:16]

    async def check_single(
        self,
        source_id: str,
        record: Dict[str, Any],
        hash_fields: List[str] = None,
        id_fields: List[str] = None
    ) -> ChangeResult:
        """
        단일 레코드 변경 감지

        Args:
            source_id: 소스 ID
            record: 확인할 레코드
            hash_fields: 해시 생성 필드
            id_fields: ID 생성 필드

        Returns:
            ChangeResult 객체
        """
        record_id = self.generate_record_id(record, id_fields)
        content_hash = self.generate_hash(record, hash_fields)

        if not self.mongo:
            # MongoDB 없으면 모두 새로운 것으로 처리
            return ChangeResult(
                record_id=record_id,
                content_hash=content_hash,
                change_type=ChangeType.NEW
            )

        # 기존 해시 조회
        existing = self.mongo.db[self.HASH_COLLECTION].find_one({
            "source_id": source_id,
            "record_id": record_id
        })

        if not existing:
            return ChangeResult(
                record_id=record_id,
                content_hash=content_hash,
                change_type=ChangeType.NEW
            )

        if existing.get("content_hash") == content_hash:
            return ChangeResult(
                record_id=record_id,
                content_hash=content_hash,
                change_type=ChangeType.UNCHANGED,
                previous_hash=existing.get("content_hash")
            )

        return ChangeResult(
            record_id=record_id,
            content_hash=content_hash,
            change_type=ChangeType.MODIFIED,
            previous_hash=existing.get("content_hash")
        )

    async def check_batch(
        self,
        source_id: str,
        records: List[Dict[str, Any]],
        hash_fields: List[str] = None,
        id_fields: List[str] = None,
        detect_deleted: bool = False
    ) -> BatchChangeResult:
        """
        배치 변경 감지 - 변경된 레코드만 필터링

        Args:
            source_id: 소스 ID
            records: 확인할 레코드 목록
            hash_fields: 해시 생성 필드
            id_fields: ID 생성 필드
            detect_deleted: 삭제된 레코드 감지 여부

        Returns:
            BatchChangeResult 객체 (new_records, modified_records만 처리하면 됨)
        """
        import time
        start_time = time.time()

        new_records = []
        modified_records = []
        unchanged_ids = []

        if not self.mongo:
            # MongoDB 없으면 모두 새로운 것으로 처리
            return BatchChangeResult(
                source_id=source_id,
                total_records=len(records),
                new_count=len(records),
                modified_count=0,
                unchanged_count=0,
                deleted_count=0,
                new_records=records,
                modified_records=[],
                unchanged_ids=[],
                processing_time_ms=(time.time() - start_time) * 1000
            )

        # 레코드별 ID와 해시 계산
        record_data = []
        for record in records:
            record_id = self.generate_record_id(record, id_fields)
            content_hash = self.generate_hash(record, hash_fields)
            record_data.append({
                "record": record,
                "record_id": record_id,
                "content_hash": content_hash
            })

        # 배치로 기존 해시 조회 (단일 쿼리)
        record_ids = [r["record_id"] for r in record_data]
        existing_hashes = {}

        cursor = self.mongo.db[self.HASH_COLLECTION].find({
            "source_id": source_id,
            "record_id": {"$in": record_ids}
        })

        for doc in cursor:
            existing_hashes[doc["record_id"]] = doc.get("content_hash")

        # 변경 분류
        for data in record_data:
            record_id = data["record_id"]
            content_hash = data["content_hash"]
            record = data["record"]

            # 메타데이터 추가
            record["_record_id"] = record_id
            record["_content_hash"] = content_hash

            if record_id not in existing_hashes:
                # 신규
                new_records.append(record)
            elif existing_hashes[record_id] != content_hash:
                # 변경됨
                modified_records.append(record)
            else:
                # 변경 없음
                unchanged_ids.append(record_id)

        # 삭제 감지 (선택적)
        deleted_count = 0
        if detect_deleted:
            current_ids = set(record_ids)
            existing_ids = set(existing_hashes.keys())
            deleted_ids = existing_ids - current_ids
            deleted_count = len(deleted_ids)

        processing_time = (time.time() - start_time) * 1000

        result = BatchChangeResult(
            source_id=source_id,
            total_records=len(records),
            new_count=len(new_records),
            modified_count=len(modified_records),
            unchanged_count=len(unchanged_ids),
            deleted_count=deleted_count,
            new_records=new_records,
            modified_records=modified_records,
            unchanged_ids=unchanged_ids,
            processing_time_ms=processing_time
        )

        logger.info(
            "Change detection completed",
            source_id=source_id,
            total=len(records),
            new=len(new_records),
            modified=len(modified_records),
            unchanged=len(unchanged_ids),
            processing_ms=round(processing_time, 2)
        )

        return result

    async def update_hashes(
        self,
        source_id: str,
        records: List[Dict[str, Any]],
        hash_fields: List[str] = None,
        id_fields: List[str] = None
    ) -> int:
        """
        처리 완료 후 해시 업데이트

        성공적으로 저장된 레코드의 해시를 DB에 저장하여
        다음 크롤링 시 변경 감지에 사용합니다.

        Args:
            source_id: 소스 ID
            records: 저장 완료된 레코드 목록

        Returns:
            업데이트된 레코드 수
        """
        if not self.mongo or not records:
            return 0

        now = datetime.utcnow()
        operations = []

        for record in records:
            # 이미 계산된 해시가 있으면 재사용
            record_id = record.get("_record_id") or self.generate_record_id(record, id_fields)
            content_hash = record.get("_content_hash") or self.generate_hash(record, hash_fields)

            operations.append({
                "filter": {
                    "source_id": source_id,
                    "record_id": record_id
                },
                "update": {
                    "$set": {
                        "content_hash": content_hash,
                        "updated_at": now
                    },
                    "$setOnInsert": {
                        "created_at": now
                    }
                },
                "upsert": True
            })

        # 배치 upsert
        from pymongo import UpdateOne
        bulk_ops = [
            UpdateOne(op["filter"], op["update"], upsert=op["upsert"])
            for op in operations
        ]

        result = self.mongo.db[self.HASH_COLLECTION].bulk_write(bulk_ops)

        return result.upserted_count + result.modified_count

    async def log_changes(
        self,
        source_id: str,
        run_id: str,
        result: BatchChangeResult
    ):
        """변경 이력 로깅"""
        if not self.mongo:
            return

        self.mongo.db[self.CHANGE_LOG_COLLECTION].insert_one({
            "source_id": source_id,
            "run_id": run_id,
            "timestamp": datetime.utcnow(),
            "total_records": result.total_records,
            "new_count": result.new_count,
            "modified_count": result.modified_count,
            "unchanged_count": result.unchanged_count,
            "deleted_count": result.deleted_count,
            "processing_time_ms": result.processing_time_ms,
            "skip_ratio": round(result.unchanged_count / max(result.total_records, 1) * 100, 1)
        })

    async def get_change_stats(
        self,
        source_id: str,
        days: int = 7
    ) -> Dict[str, Any]:
        """변경 감지 통계 조회"""
        if not self.mongo:
            return {}

        since = datetime.utcnow() - timedelta(days=days)

        pipeline = [
            {"$match": {"source_id": source_id, "timestamp": {"$gte": since}}},
            {
                "$group": {
                    "_id": None,
                    "total_runs": {"$sum": 1},
                    "total_records": {"$sum": "$total_records"},
                    "total_new": {"$sum": "$new_count"},
                    "total_modified": {"$sum": "$modified_count"},
                    "total_unchanged": {"$sum": "$unchanged_count"},
                    "avg_skip_ratio": {"$avg": "$skip_ratio"},
                    "total_processing_ms": {"$sum": "$processing_time_ms"}
                }
            }
        ]

        results = list(self.mongo.db[self.CHANGE_LOG_COLLECTION].aggregate(pipeline))

        if not results:
            return {
                "period_days": days,
                "total_runs": 0,
                "total_records": 0,
                "avg_skip_ratio": 0,
                "traffic_saved_estimate": "0%"
            }

        stats = results[0]
        total_records = stats["total_records"]
        unchanged = stats["total_unchanged"]

        return {
            "period_days": days,
            "total_runs": stats["total_runs"],
            "total_records": total_records,
            "new_records": stats["total_new"],
            "modified_records": stats["total_modified"],
            "unchanged_records": unchanged,
            "avg_skip_ratio": round(stats["avg_skip_ratio"], 1),
            "traffic_saved_estimate": f"{round(unchanged / max(total_records, 1) * 100, 1)}%",
            "total_processing_time_ms": stats["total_processing_ms"]
        }

    async def cleanup_old_hashes(
        self,
        source_id: str = None,
        days: int = 90
    ) -> int:
        """오래된 해시 정리"""
        if not self.mongo:
            return 0

        cutoff = datetime.utcnow() - timedelta(days=days)
        query = {"updated_at": {"$lt": cutoff}}

        if source_id:
            query["source_id"] = source_id

        result = self.mongo.db[self.HASH_COLLECTION].delete_many(query)

        logger.info(
            "Old hashes cleaned up",
            source_id=source_id or "all",
            deleted_count=result.deleted_count,
            older_than_days=days
        )

        return result.deleted_count


# 편의 함수
async def filter_changed_only(
    mongo_service,
    source_id: str,
    records: List[Dict[str, Any]],
    hash_fields: List[str] = None
) -> Tuple[List[Dict], List[Dict], int]:
    """
    변경된 레코드만 필터링하는 편의 함수

    Returns:
        (new_records, modified_records, skipped_count)
    """
    service = ChangeDetectionService(mongo_service)
    result = await service.check_batch(source_id, records, hash_fields)

    return (
        result.new_records,
        result.modified_records,
        result.unchanged_count
    )

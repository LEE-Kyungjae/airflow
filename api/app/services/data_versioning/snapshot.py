"""
Snapshot Manager - 데이터 스냅샷 관리

기능:
- 스냅샷 생성 (전체/증분)
- 스냅샷 복원
- 스냅샷 압축 및 최적화
- 스냅샷 만료 관리
"""

import gzip
import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from bson import ObjectId

logger = logging.getLogger(__name__)


class SnapshotStatus(str, Enum):
    """스냅샷 상태"""
    CREATING = "creating"       # 생성 중
    ACTIVE = "active"           # 활성
    RESTORING = "restoring"     # 복원 중
    ARCHIVED = "archived"       # 보관됨
    EXPIRED = "expired"         # 만료됨
    FAILED = "failed"           # 실패
    DELETED = "deleted"         # 삭제됨


class SnapshotType(str, Enum):
    """스냅샷 타입"""
    FULL = "full"               # 전체 스냅샷
    INCREMENTAL = "incremental" # 증분 스냅샷
    DIFFERENTIAL = "differential"  # 차등 스냅샷 (마지막 전체 대비)


class CompressionType(str, Enum):
    """압축 타입"""
    NONE = "none"
    GZIP = "gzip"
    LZ4 = "lz4"


@dataclass
class Snapshot:
    """스냅샷 정보"""
    snapshot_id: str
    source_id: str
    version_id: Optional[str] = None
    snapshot_type: SnapshotType = SnapshotType.FULL
    status: SnapshotStatus = SnapshotStatus.ACTIVE

    # 데이터 정보
    record_count: int = 0
    original_size_bytes: int = 0
    compressed_size_bytes: int = 0
    compression_type: CompressionType = CompressionType.NONE
    data_hash: str = ""

    # 연결 정보
    parent_snapshot_id: Optional[str] = None
    base_snapshot_id: Optional[str] = None  # DIFFERENTIAL용

    # 타임스탬프
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    created_by: str = "system"
    description: str = ""

    # 메타데이터
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "source_id": self.source_id,
            "version_id": self.version_id,
            "snapshot_type": self.snapshot_type.value,
            "status": self.status.value,
            "record_count": self.record_count,
            "original_size_bytes": self.original_size_bytes,
            "compressed_size_bytes": self.compressed_size_bytes,
            "compression_type": self.compression_type.value,
            "data_hash": self.data_hash,
            "parent_snapshot_id": self.parent_snapshot_id,
            "base_snapshot_id": self.base_snapshot_id,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "created_by": self.created_by,
            "description": self.description,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Snapshot":
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.utcnow()

        expires_at = data.get("expires_at")
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)

        return cls(
            snapshot_id=str(data.get("snapshot_id", data.get("_id", ""))),
            source_id=str(data.get("source_id", "")),
            version_id=data.get("version_id"),
            snapshot_type=SnapshotType(data.get("snapshot_type", "full")),
            status=SnapshotStatus(data.get("status", "active")),
            record_count=data.get("record_count", 0),
            original_size_bytes=data.get("original_size_bytes", 0),
            compressed_size_bytes=data.get("compressed_size_bytes", 0),
            compression_type=CompressionType(data.get("compression_type", "none")),
            data_hash=data.get("data_hash", ""),
            parent_snapshot_id=data.get("parent_snapshot_id"),
            base_snapshot_id=data.get("base_snapshot_id"),
            created_at=created_at,
            expires_at=expires_at,
            created_by=data.get("created_by", "system"),
            description=data.get("description", ""),
            metadata=data.get("metadata", {}),
        )

    @property
    def compression_ratio(self) -> float:
        """압축률 (0-1, 낮을수록 좋음)"""
        if self.original_size_bytes == 0:
            return 1.0
        return self.compressed_size_bytes / self.original_size_bytes

    @property
    def is_expired(self) -> bool:
        """만료 여부"""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at


class SnapshotManager:
    """
    스냅샷 관리자

    데이터 스냅샷의 생성, 복원, 압축, 만료 관리를 담당합니다.
    """

    DEFAULT_RETENTION_DAYS = 30
    DEFAULT_COMPRESSION = CompressionType.GZIP

    def __init__(self, mongo_service=None):
        """
        Args:
            mongo_service: MongoDB 서비스 인스턴스
        """
        self.mongo = mongo_service
        self._cache: Dict[str, Snapshot] = {}

    # ==================== 컬렉션 접근 ====================

    def _get_snapshots_collection(self):
        """스냅샷 메타데이터 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_snapshots
        return None

    def _get_snapshot_data_collection(self):
        """스냅샷 데이터 컬렉션"""
        if self.mongo:
            return self.mongo.db.snapshot_data
        return None

    # ==================== 스냅샷 생성 ====================

    def create_snapshot(
        self,
        source_id: str,
        data: List[Dict[str, Any]],
        snapshot_type: SnapshotType = SnapshotType.FULL,
        version_id: str = None,
        compress: bool = True,
        retention_days: int = None,
        created_by: str = "system",
        description: str = "",
        metadata: Dict[str, Any] = None,
    ) -> Snapshot:
        """
        스냅샷 생성

        Args:
            source_id: 소스 ID
            data: 스냅샷할 데이터
            snapshot_type: 스냅샷 타입
            version_id: 연결할 버전 ID
            compress: 압축 여부
            retention_days: 보존 기간 (일)
            created_by: 생성자
            description: 설명
            metadata: 추가 메타데이터

        Returns:
            생성된 Snapshot 객체
        """
        snapshot_id = str(ObjectId())

        # 데이터 직렬화
        data_json = json.dumps(data, default=str, ensure_ascii=False)
        data_bytes = data_json.encode('utf-8')
        original_size = len(data_bytes)

        # 압축
        compression_type = CompressionType.NONE
        compressed_data = data_bytes

        if compress and original_size > 1024:  # 1KB 이상만 압축
            compressed_data = gzip.compress(data_bytes)
            compression_type = CompressionType.GZIP

        compressed_size = len(compressed_data)

        # 해시 계산
        data_hash = hashlib.sha256(data_bytes).hexdigest()[:32]

        # 만료일 계산
        expires_at = None
        if retention_days:
            expires_at = datetime.utcnow() + timedelta(days=retention_days)
        elif retention_days is None:
            expires_at = datetime.utcnow() + timedelta(days=self.DEFAULT_RETENTION_DAYS)

        # 부모 스냅샷 찾기 (INCREMENTAL/DIFFERENTIAL인 경우)
        parent_snapshot_id = None
        base_snapshot_id = None

        if snapshot_type == SnapshotType.INCREMENTAL:
            parent = self._get_latest_snapshot(source_id)
            if parent:
                parent_snapshot_id = parent.snapshot_id

        elif snapshot_type == SnapshotType.DIFFERENTIAL:
            base = self._get_latest_full_snapshot(source_id)
            if base:
                base_snapshot_id = base.snapshot_id

        # 스냅샷 객체 생성
        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            source_id=source_id,
            version_id=version_id,
            snapshot_type=snapshot_type,
            status=SnapshotStatus.CREATING,
            record_count=len(data),
            original_size_bytes=original_size,
            compressed_size_bytes=compressed_size,
            compression_type=compression_type,
            data_hash=data_hash,
            parent_snapshot_id=parent_snapshot_id,
            base_snapshot_id=base_snapshot_id,
            created_at=datetime.utcnow(),
            expires_at=expires_at,
            created_by=created_by,
            description=description,
            metadata=metadata or {},
        )

        # 저장
        self._save_snapshot(snapshot, compressed_data)

        # 상태 업데이트
        snapshot.status = SnapshotStatus.ACTIVE
        self._update_snapshot_status(snapshot_id, SnapshotStatus.ACTIVE)

        logger.info(
            f"Created snapshot: id={snapshot_id}, source={source_id}, "
            f"type={snapshot_type.value}, records={len(data)}, "
            f"compression_ratio={snapshot.compression_ratio:.2f}"
        )

        return snapshot

    def create_incremental_snapshot(
        self,
        source_id: str,
        changes: Dict[str, Any],
        version_id: str = None,
        created_by: str = "system",
        description: str = "",
    ) -> Snapshot:
        """
        증분 스냅샷 생성

        Args:
            source_id: 소스 ID
            changes: 변경 내용 {added: [], modified: [], deleted: []}
            version_id: 연결할 버전 ID
            created_by: 생성자
            description: 설명

        Returns:
            생성된 Snapshot 객체
        """
        # 변경분만 저장
        incremental_data = {
            "type": "incremental",
            "timestamp": datetime.utcnow().isoformat(),
            "added": changes.get("added", []),
            "modified": changes.get("modified", []),
            "deleted": changes.get("deleted", []),
        }

        return self.create_snapshot(
            source_id=source_id,
            data=[incremental_data],
            snapshot_type=SnapshotType.INCREMENTAL,
            version_id=version_id,
            created_by=created_by,
            description=description or "Incremental snapshot",
        )

    # ==================== 스냅샷 조회 ====================

    def get_snapshot(self, snapshot_id: str) -> Optional[Snapshot]:
        """
        스냅샷 정보 조회

        Args:
            snapshot_id: 스냅샷 ID

        Returns:
            Snapshot 또는 None
        """
        if snapshot_id in self._cache:
            return self._cache[snapshot_id]

        collection = self._get_snapshots_collection()
        if collection:
            try:
                doc = collection.find_one({"_id": ObjectId(snapshot_id)})
                if doc:
                    snapshot = Snapshot.from_dict(doc)
                    self._cache[snapshot_id] = snapshot
                    return snapshot
            except Exception as e:
                logger.error(f"Error getting snapshot {snapshot_id}: {e}")

        return None

    def get_snapshot_data(
        self,
        snapshot_id: str,
        decompress: bool = True
    ) -> Optional[List[Dict[str, Any]]]:
        """
        스냅샷 데이터 조회

        Args:
            snapshot_id: 스냅샷 ID
            decompress: 압축 해제 여부

        Returns:
            데이터 목록 또는 None
        """
        snapshot = self.get_snapshot(snapshot_id)
        if not snapshot:
            return None

        data_col = self._get_snapshot_data_collection()
        if not data_col:
            return None

        doc = data_col.find_one({"snapshot_id": snapshot_id})
        if not doc:
            return None

        data_bytes = doc.get("data")
        if isinstance(data_bytes, bytes):
            # 압축 해제
            if decompress and snapshot.compression_type == CompressionType.GZIP:
                data_bytes = gzip.decompress(data_bytes)

            data_json = data_bytes.decode('utf-8')
            return json.loads(data_json)

        return doc.get("data", [])

    def list_snapshots(
        self,
        source_id: str,
        snapshot_type: SnapshotType = None,
        status: SnapshotStatus = None,
        include_expired: bool = False,
        limit: int = 50,
        skip: int = 0,
    ) -> List[Snapshot]:
        """
        스냅샷 목록 조회

        Args:
            source_id: 소스 ID
            snapshot_type: 타입 필터
            status: 상태 필터
            include_expired: 만료된 것 포함 여부
            limit: 최대 개수
            skip: 건너뛸 개수

        Returns:
            스냅샷 목록
        """
        collection = self._get_snapshots_collection()
        if not collection:
            return []

        query = {"source_id": source_id}

        if snapshot_type:
            query["snapshot_type"] = snapshot_type.value

        if status:
            query["status"] = status.value
        elif not include_expired:
            query["status"] = {"$nin": [
                SnapshotStatus.EXPIRED.value,
                SnapshotStatus.DELETED.value
            ]}

        if not include_expired:
            query["$or"] = [
                {"expires_at": None},
                {"expires_at": {"$gt": datetime.utcnow()}}
            ]

        cursor = collection.find(query).sort(
            "created_at", -1
        ).skip(skip).limit(limit)

        return [Snapshot.from_dict(doc) for doc in cursor]

    def _get_latest_snapshot(
        self,
        source_id: str,
        snapshot_type: SnapshotType = None
    ) -> Optional[Snapshot]:
        """최신 스냅샷 조회"""
        collection = self._get_snapshots_collection()
        if collection:
            query = {
                "source_id": source_id,
                "status": SnapshotStatus.ACTIVE.value,
            }
            if snapshot_type:
                query["snapshot_type"] = snapshot_type.value

            doc = collection.find_one(
                query,
                sort=[("created_at", -1)]
            )
            if doc:
                return Snapshot.from_dict(doc)
        return None

    def _get_latest_full_snapshot(self, source_id: str) -> Optional[Snapshot]:
        """최신 전체 스냅샷 조회"""
        return self._get_latest_snapshot(source_id, SnapshotType.FULL)

    # ==================== 스냅샷 복원 ====================

    def restore_snapshot(
        self,
        snapshot_id: str,
        target_collection: str = None,
        created_by: str = "system"
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        스냅샷 복원

        Args:
            snapshot_id: 복원할 스냅샷 ID
            target_collection: 복원 대상 컬렉션 (None이면 데이터만 반환)
            created_by: 실행자

        Returns:
            (성공 여부, 복원된 데이터)
        """
        snapshot = self.get_snapshot(snapshot_id)
        if not snapshot:
            logger.error(f"Snapshot not found: {snapshot_id}")
            return False, []

        if snapshot.status not in (SnapshotStatus.ACTIVE, SnapshotStatus.ARCHIVED):
            logger.error(f"Snapshot not available for restore: {snapshot.status}")
            return False, []

        # 상태 업데이트
        self._update_snapshot_status(snapshot_id, SnapshotStatus.RESTORING)

        try:
            # 스냅샷 타입에 따른 복원
            if snapshot.snapshot_type == SnapshotType.FULL:
                data = self.get_snapshot_data(snapshot_id)
            else:
                # 증분/차등 스냅샷은 체인을 따라 복원
                data = self._restore_snapshot_chain(snapshot)

            if data is None:
                raise ValueError("Could not retrieve snapshot data")

            # 대상 컬렉션에 복원
            if target_collection and self.mongo:
                target = self.mongo.db[target_collection]
                target.delete_many({})  # 기존 데이터 삭제
                if data:
                    target.insert_many(data)
                logger.info(f"Restored {len(data)} records to {target_collection}")

            # 상태 복원
            self._update_snapshot_status(snapshot_id, SnapshotStatus.ACTIVE)

            logger.info(f"Snapshot restored: {snapshot_id}, records={len(data)}")
            return True, data

        except Exception as e:
            logger.error(f"Failed to restore snapshot {snapshot_id}: {e}")
            self._update_snapshot_status(snapshot_id, SnapshotStatus.FAILED)
            return False, []

    def _restore_snapshot_chain(
        self,
        snapshot: Snapshot
    ) -> Optional[List[Dict[str, Any]]]:
        """
        증분/차등 스냅샷 체인 복원
        """
        chain = []
        current = snapshot

        # 기본 스냅샷까지 체인 추적
        while current:
            chain.append(current)

            if current.snapshot_type == SnapshotType.FULL:
                break

            # 차등은 base로, 증분은 parent로
            next_id = (
                current.base_snapshot_id
                if current.snapshot_type == SnapshotType.DIFFERENTIAL
                else current.parent_snapshot_id
            )

            if next_id:
                current = self.get_snapshot(next_id)
            else:
                break

        if not chain or chain[-1].snapshot_type != SnapshotType.FULL:
            logger.error("Could not find base FULL snapshot")
            return None

        # 기본 스냅샷부터 시작하여 변경분 적용
        chain.reverse()
        base_data = self.get_snapshot_data(chain[0].snapshot_id)

        if base_data is None:
            return None

        result = list(base_data)

        # 증분 적용
        for inc_snapshot in chain[1:]:
            inc_data = self.get_snapshot_data(inc_snapshot.snapshot_id)
            if inc_data and len(inc_data) > 0 and "type" in inc_data[0]:
                delta = inc_data[0]
                result = self._apply_incremental(result, delta)

        return result

    def _apply_incremental(
        self,
        base: List[Dict[str, Any]],
        delta: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        증분 변경 적용
        """
        result = list(base)

        # 삭제
        deleted_ids = {str(d.get("_id")) for d in delta.get("deleted", [])}
        result = [r for r in result if str(r.get("_id")) not in deleted_ids]

        # 수정
        modified_map = {str(m.get("_id")): m for m in delta.get("modified", [])}
        result = [
            modified_map.get(str(r.get("_id")), r)
            for r in result
        ]

        # 추가
        result.extend(delta.get("added", []))

        return result

    # ==================== 스냅샷 관리 ====================

    def archive_snapshot(self, snapshot_id: str, reason: str = "") -> bool:
        """스냅샷 아카이브"""
        self._update_snapshot_status(snapshot_id, SnapshotStatus.ARCHIVED)
        logger.info(f"Archived snapshot: {snapshot_id}, reason={reason}")
        return True

    def delete_snapshot(self, snapshot_id: str, hard_delete: bool = False) -> bool:
        """
        스냅샷 삭제

        Args:
            snapshot_id: 스냅샷 ID
            hard_delete: True면 물리 삭제, False면 논리 삭제

        Returns:
            성공 여부
        """
        if hard_delete:
            snapshots_col = self._get_snapshots_collection()
            data_col = self._get_snapshot_data_collection()

            if snapshots_col:
                snapshots_col.delete_one({"_id": ObjectId(snapshot_id)})
            if data_col:
                data_col.delete_one({"snapshot_id": snapshot_id})

            self._cache.pop(snapshot_id, None)
            logger.info(f"Hard deleted snapshot: {snapshot_id}")
        else:
            self._update_snapshot_status(snapshot_id, SnapshotStatus.DELETED)
            logger.info(f"Soft deleted snapshot: {snapshot_id}")

        return True

    def cleanup_expired_snapshots(
        self,
        source_id: str = None,
        dry_run: bool = False
    ) -> int:
        """
        만료된 스냅샷 정리

        Args:
            source_id: 특정 소스만 정리 (None이면 전체)
            dry_run: True면 실제 삭제 없이 대상 수만 반환

        Returns:
            삭제된 스냅샷 수
        """
        collection = self._get_snapshots_collection()
        if not collection:
            return 0

        query = {
            "expires_at": {"$lt": datetime.utcnow()},
            "status": {"$nin": [SnapshotStatus.DELETED.value]}
        }

        if source_id:
            query["source_id"] = source_id

        if dry_run:
            return collection.count_documents(query)

        # 만료된 스냅샷 ID 수집
        expired_ids = [
            str(doc["_id"])
            for doc in collection.find(query, {"_id": 1})
        ]

        # 삭제 처리
        count = 0
        for sid in expired_ids:
            self._update_snapshot_status(sid, SnapshotStatus.EXPIRED)
            count += 1

        logger.info(f"Cleaned up {count} expired snapshots")
        return count

    # ==================== 저장/업데이트 ====================

    def _save_snapshot(self, snapshot: Snapshot, data: bytes):
        """스냅샷 저장"""
        snapshots_col = self._get_snapshots_collection()
        data_col = self._get_snapshot_data_collection()

        if snapshots_col and data_col:
            # 메타데이터 저장
            snapshot_doc = snapshot.to_dict()
            snapshot_doc["_id"] = ObjectId(snapshot.snapshot_id)
            snapshots_col.insert_one(snapshot_doc)

            # 데이터 저장
            data_doc = {
                "snapshot_id": snapshot.snapshot_id,
                "source_id": snapshot.source_id,
                "data": data,
                "created_at": snapshot.created_at,
            }
            data_col.insert_one(data_doc)

    def _update_snapshot_status(self, snapshot_id: str, status: SnapshotStatus):
        """스냅샷 상태 업데이트"""
        collection = self._get_snapshots_collection()
        if collection:
            collection.update_one(
                {"_id": ObjectId(snapshot_id)},
                {"$set": {"status": status.value}}
            )
            self._cache.pop(snapshot_id, None)

    # ==================== 통계 ====================

    def get_snapshot_stats(self, source_id: str) -> Dict[str, Any]:
        """
        스냅샷 통계

        Args:
            source_id: 소스 ID

        Returns:
            통계 정보
        """
        collection = self._get_snapshots_collection()
        if not collection:
            return {}

        # 기본 통계
        pipeline = [
            {"$match": {"source_id": source_id}},
            {"$group": {
                "_id": "$status",
                "count": {"$sum": 1},
                "total_original_size": {"$sum": "$original_size_bytes"},
                "total_compressed_size": {"$sum": "$compressed_size_bytes"},
                "total_records": {"$sum": "$record_count"},
            }}
        ]

        status_stats = {
            doc["_id"]: {
                "count": doc["count"],
                "total_original_size": doc["total_original_size"],
                "total_compressed_size": doc["total_compressed_size"],
                "total_records": doc["total_records"],
            }
            for doc in collection.aggregate(pipeline)
        }

        # 타입별 통계
        type_pipeline = [
            {"$match": {"source_id": source_id, "status": SnapshotStatus.ACTIVE.value}},
            {"$group": {
                "_id": "$snapshot_type",
                "count": {"$sum": 1},
            }}
        ]

        type_stats = {
            doc["_id"]: doc["count"]
            for doc in collection.aggregate(type_pipeline)
        }

        # 최신 스냅샷
        latest = self._get_latest_snapshot(source_id)

        return {
            "source_id": source_id,
            "by_status": status_stats,
            "by_type": type_stats,
            "latest_snapshot": latest.to_dict() if latest else None,
            "total_snapshots": sum(s["count"] for s in status_stats.values()),
        }

    def calculate_storage_savings(self, source_id: str) -> Dict[str, Any]:
        """
        압축으로 인한 스토리지 절감량 계산
        """
        collection = self._get_snapshots_collection()
        if not collection:
            return {}

        pipeline = [
            {"$match": {
                "source_id": source_id,
                "status": {"$in": [SnapshotStatus.ACTIVE.value, SnapshotStatus.ARCHIVED.value]}
            }},
            {"$group": {
                "_id": None,
                "total_original": {"$sum": "$original_size_bytes"},
                "total_compressed": {"$sum": "$compressed_size_bytes"},
                "count": {"$sum": 1},
            }}
        ]

        result = list(collection.aggregate(pipeline))
        if not result:
            return {
                "total_original_bytes": 0,
                "total_compressed_bytes": 0,
                "savings_bytes": 0,
                "savings_percent": 0,
                "snapshot_count": 0,
            }

        stats = result[0]
        original = stats.get("total_original", 0)
        compressed = stats.get("total_compressed", 0)
        savings = original - compressed

        return {
            "total_original_bytes": original,
            "total_compressed_bytes": compressed,
            "savings_bytes": savings,
            "savings_percent": round((savings / original * 100) if original > 0 else 0, 2),
            "snapshot_count": stats.get("count", 0),
        }

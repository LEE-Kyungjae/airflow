"""
Idempotency Manager - 멱등성 키 관리 및 체크포인트

기능:
- 실행 상태 추적
- 체크포인트 기반 재시작
- 중복 실행 방지
- 실패 복구
"""

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class ExecutionState(str, Enum):
    """실행 상태"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class IdempotencyKey:
    """멱등성 키"""
    source_id: str
    operation: str
    params_hash: str
    created_at: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def generate(
        cls,
        source_id: str,
        operation: str,
        params: Dict[str, Any] = None,
        timestamp: datetime = None
    ) -> "IdempotencyKey":
        """
        멱등성 키 생성

        Args:
            source_id: 소스 ID
            operation: 연산 유형 (crawl, transform, load 등)
            params: 연산 파라미터
            timestamp: 타임스탬프 (일 단위 그룹화)

        Returns:
            IdempotencyKey
        """
        # 파라미터 해시
        params = params or {}
        if timestamp:
            params["_date"] = timestamp.strftime("%Y-%m-%d")

        params_str = json.dumps(params, sort_keys=True, default=str)
        params_hash = hashlib.sha256(params_str.encode()).hexdigest()[:16]

        return cls(
            source_id=source_id,
            operation=operation,
            params_hash=params_hash,
        )

    @property
    def key(self) -> str:
        """복합 키 문자열"""
        return f"{self.source_id}:{self.operation}:{self.params_hash}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "operation": self.operation,
            "params_hash": self.params_hash,
            "key": self.key,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class CheckpointData:
    """체크포인트 데이터"""
    idempotency_key: str
    state: ExecutionState
    progress: int = 0  # 0-100
    total_records: int = 0
    processed_records: int = 0
    last_processed_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    started_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "idempotency_key": self.idempotency_key,
            "state": self.state.value,
            "progress": self.progress,
            "total_records": self.total_records,
            "processed_records": self.processed_records,
            "last_processed_id": self.last_processed_id,
            "metadata": self.metadata,
            "started_at": self.started_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CheckpointData":
        return cls(
            idempotency_key=data["idempotency_key"],
            state=ExecutionState(data["state"]),
            progress=data.get("progress", 0),
            total_records=data.get("total_records", 0),
            processed_records=data.get("processed_records", 0),
            last_processed_id=data.get("last_processed_id"),
            metadata=data.get("metadata", {}),
            started_at=datetime.fromisoformat(data["started_at"]) if isinstance(data.get("started_at"), str) else data.get("started_at", datetime.utcnow()),
            updated_at=datetime.fromisoformat(data["updated_at"]) if isinstance(data.get("updated_at"), str) else data.get("updated_at", datetime.utcnow()),
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") and isinstance(data["completed_at"], str) else data.get("completed_at"),
            error=data.get("error"),
        )


class IdempotencyManager:
    """멱등성 관리자"""

    def __init__(
        self,
        mongo_service=None,
        lock_timeout: int = 300,
        checkpoint_interval: int = 100
    ):
        """
        Args:
            mongo_service: MongoDB 서비스
            lock_timeout: 잠금 타임아웃 (초)
            checkpoint_interval: 체크포인트 간격 (레코드 수)
        """
        self.mongo = mongo_service
        self.lock_timeout = lock_timeout
        self.checkpoint_interval = checkpoint_interval
        self._local_locks: Dict[str, datetime] = {}

    def _get_collection(self):
        """MongoDB 컬렉션"""
        if self.mongo:
            return self.mongo.db.idempotency_checkpoints
        return None

    def acquire_lock(self, key: IdempotencyKey) -> bool:
        """
        실행 잠금 획득

        Args:
            key: 멱등성 키

        Returns:
            잠금 성공 여부
        """
        collection = self._get_collection()
        if not collection:
            # DB 없이 로컬 잠금
            if key.key in self._local_locks:
                lock_time = self._local_locks[key.key]
                if datetime.utcnow() - lock_time < timedelta(seconds=self.lock_timeout):
                    return False
            self._local_locks[key.key] = datetime.utcnow()
            return True

        now = datetime.utcnow()
        lock_expiry = now - timedelta(seconds=self.lock_timeout)

        # 원자적 업데이트 시도
        result = collection.update_one(
            {
                "idempotency_key": key.key,
                "$or": [
                    {"state": {"$in": [ExecutionState.COMPLETED.value, ExecutionState.FAILED.value]}},
                    {"updated_at": {"$lt": lock_expiry}},  # 만료된 잠금
                ]
            },
            {
                "$set": {
                    "state": ExecutionState.RUNNING.value,
                    "started_at": now,
                    "updated_at": now,
                }
            },
            upsert=False
        )

        if result.modified_count > 0:
            return True

        # 새 실행 시도
        try:
            collection.insert_one({
                "idempotency_key": key.key,
                **key.to_dict(),
                "state": ExecutionState.RUNNING.value,
                "progress": 0,
                "started_at": now,
                "updated_at": now,
            })
            return True
        except Exception:
            # 중복 키 에러 - 이미 실행 중
            return False

    def release_lock(self, key: IdempotencyKey, state: ExecutionState, error: str = None):
        """
        실행 잠금 해제

        Args:
            key: 멱등성 키
            state: 최종 상태
            error: 에러 메시지 (실패 시)
        """
        now = datetime.utcnow()
        collection = self._get_collection()

        if not collection:
            self._local_locks.pop(key.key, None)
            return

        update = {
            "state": state.value,
            "updated_at": now,
        }

        if state == ExecutionState.COMPLETED:
            update["completed_at"] = now
            update["progress"] = 100

        if error:
            update["error"] = error

        collection.update_one(
            {"idempotency_key": key.key},
            {"$set": update}
        )

    def get_checkpoint(self, key: IdempotencyKey) -> Optional[CheckpointData]:
        """
        체크포인트 조회

        Args:
            key: 멱등성 키

        Returns:
            CheckpointData 또는 None
        """
        collection = self._get_collection()
        if not collection:
            return None

        doc = collection.find_one({"idempotency_key": key.key})
        if doc:
            return CheckpointData.from_dict(doc)
        return None

    def save_checkpoint(
        self,
        key: IdempotencyKey,
        processed_records: int,
        total_records: int = None,
        last_processed_id: str = None,
        metadata: Dict[str, Any] = None
    ):
        """
        체크포인트 저장

        Args:
            key: 멱등성 키
            processed_records: 처리된 레코드 수
            total_records: 전체 레코드 수
            last_processed_id: 마지막 처리된 ID
            metadata: 추가 메타데이터
        """
        collection = self._get_collection()
        if not collection:
            return

        now = datetime.utcnow()
        progress = 0
        if total_records and total_records > 0:
            progress = int((processed_records / total_records) * 100)

        update = {
            "processed_records": processed_records,
            "progress": progress,
            "updated_at": now,
        }

        if total_records is not None:
            update["total_records"] = total_records
        if last_processed_id:
            update["last_processed_id"] = last_processed_id
        if metadata:
            update["metadata"] = metadata

        collection.update_one(
            {"idempotency_key": key.key},
            {"$set": update}
        )

    def is_completed(self, key: IdempotencyKey) -> bool:
        """
        실행 완료 여부

        Args:
            key: 멱등성 키

        Returns:
            완료 여부
        """
        checkpoint = self.get_checkpoint(key)
        return checkpoint is not None and checkpoint.state == ExecutionState.COMPLETED

    def should_skip(self, key: IdempotencyKey) -> bool:
        """
        실행 건너뛰기 여부 (이미 완료됨)

        Args:
            key: 멱등성 키

        Returns:
            건너뛰기 여부
        """
        return self.is_completed(key)

    def get_resume_point(self, key: IdempotencyKey) -> Optional[str]:
        """
        재개 지점 조회 (마지막 처리 ID)

        Args:
            key: 멱등성 키

        Returns:
            마지막 처리된 ID 또는 None
        """
        checkpoint = self.get_checkpoint(key)
        if checkpoint and checkpoint.state == ExecutionState.FAILED:
            return checkpoint.last_processed_id
        return None

    @contextmanager
    def idempotent_execution(
        self,
        key: IdempotencyKey,
        on_duplicate: str = "skip"
    ):
        """
        멱등성 실행 컨텍스트 매니저

        Args:
            key: 멱등성 키
            on_duplicate: 중복 처리 ("skip", "fail", "force")

        Yields:
            체크포인트 저장 함수

        Usage:
            with manager.idempotent_execution(key) as save_progress:
                for i, record in enumerate(records):
                    process(record)
                    if i % 100 == 0:
                        save_progress(i, len(records))
        """
        # 이미 완료된 경우
        if on_duplicate == "skip" and self.is_completed(key):
            logger.info(f"Skipping completed execution: {key.key}")
            yield lambda *args, **kwargs: None
            return

        # 잠금 획득
        if not self.acquire_lock(key):
            if on_duplicate == "fail":
                raise RuntimeError(f"Execution already in progress: {key.key}")
            elif on_duplicate == "force":
                logger.warning(f"Forcing execution despite lock: {key.key}")
            else:
                logger.info(f"Skipping locked execution: {key.key}")
                yield lambda *args, **kwargs: None
                return

        error = None
        try:
            # 체크포인트 저장 함수
            def save_progress(
                processed: int,
                total: int = None,
                last_id: str = None,
                **metadata
            ):
                self.save_checkpoint(
                    key,
                    processed_records=processed,
                    total_records=total,
                    last_processed_id=last_id,
                    metadata=metadata if metadata else None,
                )

            yield save_progress

            # 성공적으로 완료
            self.release_lock(key, ExecutionState.COMPLETED)
            logger.info(f"Execution completed: {key.key}")

        except Exception as e:
            error = str(e)
            self.release_lock(key, ExecutionState.FAILED, error)
            logger.error(f"Execution failed: {key.key} - {error}")
            raise

    def cleanup_old_checkpoints(self, days: int = 30) -> int:
        """
        오래된 체크포인트 정리

        Args:
            days: 보존 기간 (일)

        Returns:
            삭제된 수
        """
        collection = self._get_collection()
        if not collection:
            return 0

        cutoff = datetime.utcnow() - timedelta(days=days)

        result = collection.delete_many({
            "state": ExecutionState.COMPLETED.value,
            "completed_at": {"$lt": cutoff}
        })

        logger.info(f"Cleaned up {result.deleted_count} old checkpoints")
        return result.deleted_count

    def get_running_executions(self, source_id: str = None) -> List[Dict[str, Any]]:
        """
        실행 중인 작업 목록

        Args:
            source_id: 소스 ID 필터

        Returns:
            실행 중인 작업 목록
        """
        collection = self._get_collection()
        if not collection:
            return []

        query = {"state": ExecutionState.RUNNING.value}
        if source_id:
            query["source_id"] = source_id

        docs = list(collection.find(query).sort("started_at", -1))

        return [
            {
                "idempotency_key": d["idempotency_key"],
                "source_id": d.get("source_id"),
                "operation": d.get("operation"),
                "progress": d.get("progress", 0),
                "started_at": d.get("started_at"),
                "updated_at": d.get("updated_at"),
            }
            for d in docs
        ]

    def retry_failed(self, key: IdempotencyKey) -> bool:
        """
        실패한 실행 재시도 (상태 초기화)

        Args:
            key: 멱등성 키

        Returns:
            성공 여부
        """
        collection = self._get_collection()
        if not collection:
            return True

        result = collection.update_one(
            {
                "idempotency_key": key.key,
                "state": ExecutionState.FAILED.value
            },
            {
                "$set": {
                    "state": ExecutionState.PENDING.value,
                    "error": None,
                    "updated_at": datetime.utcnow(),
                }
            }
        )

        return result.modified_count > 0

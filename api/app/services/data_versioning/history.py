"""
History Tracker - 변경 이력 추적

기능:
- 버전 변경 이력 기록
- 이력 조회 및 필터링
- 감사 로그 (audit log)
- 통계 및 분석
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from bson import ObjectId
from collections import defaultdict

logger = logging.getLogger(__name__)


class HistoryAction(str, Enum):
    """이력 액션 타입"""
    CREATE = "create"           # 버전 생성
    UPDATE = "update"           # 버전 업데이트
    DELETE = "delete"           # 버전 삭제
    ARCHIVE = "archive"         # 버전 아카이브
    RESTORE = "restore"         # 버전 복원
    ROLLBACK = "rollback"       # 롤백
    SNAPSHOT = "snapshot"       # 스냅샷 생성
    BRANCH = "branch"           # 브랜치 생성
    TAG = "tag"                 # 태그 추가/삭제
    MERGE = "merge"             # 브랜치 병합
    COMPARE = "compare"         # 버전 비교
    EXPORT = "export"           # 데이터 내보내기
    IMPORT = "import"           # 데이터 가져오기


class HistorySeverity(str, Enum):
    """이력 심각도"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class HistoryEntry:
    """이력 항목"""
    entry_id: str
    source_id: str
    version_id: Optional[str] = None
    action: HistoryAction = HistoryAction.CREATE
    severity: HistorySeverity = HistorySeverity.INFO

    # 실행자 정보
    actor: str = "system"
    actor_type: str = "system"  # system, user, api, scheduler
    actor_ip: Optional[str] = None

    # 상세 정보
    details: Dict[str, Any] = field(default_factory=dict)
    summary: str = ""
    tags: List[str] = field(default_factory=list)

    # 타임스탬프
    timestamp: datetime = field(default_factory=datetime.utcnow)

    # 영향 정보
    affected_records: int = 0
    affected_bytes: int = 0

    # 상태
    success: bool = True
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "source_id": self.source_id,
            "version_id": self.version_id,
            "action": self.action.value,
            "severity": self.severity.value,
            "actor": self.actor,
            "actor_type": self.actor_type,
            "actor_ip": self.actor_ip,
            "details": self.details,
            "summary": self.summary,
            "tags": self.tags,
            "timestamp": self.timestamp,
            "affected_records": self.affected_records,
            "affected_bytes": self.affected_bytes,
            "success": self.success,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HistoryEntry":
        timestamp = data.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        elif timestamp is None:
            timestamp = datetime.utcnow()

        return cls(
            entry_id=str(data.get("entry_id", data.get("_id", ""))),
            source_id=str(data.get("source_id", "")),
            version_id=data.get("version_id"),
            action=HistoryAction(data.get("action", "create")),
            severity=HistorySeverity(data.get("severity", "info")),
            actor=data.get("actor", "system"),
            actor_type=data.get("actor_type", "system"),
            actor_ip=data.get("actor_ip"),
            details=data.get("details", {}),
            summary=data.get("summary", ""),
            tags=data.get("tags", []),
            timestamp=timestamp,
            affected_records=data.get("affected_records", 0),
            affected_bytes=data.get("affected_bytes", 0),
            success=data.get("success", True),
            error_message=data.get("error_message"),
        )


@dataclass
class HistoryFilter:
    """이력 필터"""
    source_id: Optional[str] = None
    version_id: Optional[str] = None
    actions: List[HistoryAction] = field(default_factory=list)
    severities: List[HistorySeverity] = field(default_factory=list)
    actor: Optional[str] = None
    actor_type: Optional[str] = None
    success_only: bool = False
    failure_only: bool = False
    tags: List[str] = field(default_factory=list)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    def to_query(self) -> Dict[str, Any]:
        """MongoDB 쿼리로 변환"""
        query = {}

        if self.source_id:
            query["source_id"] = self.source_id
        if self.version_id:
            query["version_id"] = self.version_id
        if self.actions:
            query["action"] = {"$in": [a.value for a in self.actions]}
        if self.severities:
            query["severity"] = {"$in": [s.value for s in self.severities]}
        if self.actor:
            query["actor"] = self.actor
        if self.actor_type:
            query["actor_type"] = self.actor_type
        if self.success_only:
            query["success"] = True
        if self.failure_only:
            query["success"] = False
        if self.tags:
            query["tags"] = {"$all": self.tags}

        # 날짜 범위
        if self.start_date or self.end_date:
            date_query = {}
            if self.start_date:
                date_query["$gte"] = self.start_date
            if self.end_date:
                date_query["$lte"] = self.end_date
            if date_query:
                query["timestamp"] = date_query

        return query


@dataclass
class HistoryStats:
    """이력 통계"""
    source_id: str
    total_entries: int = 0
    actions_count: Dict[str, int] = field(default_factory=dict)
    actors_count: Dict[str, int] = field(default_factory=dict)
    success_count: int = 0
    failure_count: int = 0
    total_affected_records: int = 0
    total_affected_bytes: int = 0
    first_entry_date: Optional[datetime] = None
    last_entry_date: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "total_entries": self.total_entries,
            "actions_count": self.actions_count,
            "actors_count": self.actors_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "success_rate": round(
                self.success_count / self.total_entries * 100
                if self.total_entries > 0 else 0, 2
            ),
            "total_affected_records": self.total_affected_records,
            "total_affected_bytes": self.total_affected_bytes,
            "first_entry_date": self.first_entry_date.isoformat() if self.first_entry_date else None,
            "last_entry_date": self.last_entry_date.isoformat() if self.last_entry_date else None,
        }


class HistoryTracker:
    """
    이력 추적기

    버전 변경 이력을 기록하고 조회합니다.
    """

    def __init__(self, mongo_service=None):
        """
        Args:
            mongo_service: MongoDB 서비스 인스턴스
        """
        self.mongo = mongo_service

    def _get_collection(self):
        """이력 컬렉션"""
        if self.mongo:
            return self.mongo.db.version_history
        return None

    # ==================== 이력 기록 ====================

    def record(
        self,
        source_id: str,
        action: HistoryAction,
        version_id: str = None,
        actor: str = "system",
        actor_type: str = "system",
        actor_ip: str = None,
        details: Dict[str, Any] = None,
        summary: str = "",
        tags: List[str] = None,
        affected_records: int = 0,
        affected_bytes: int = 0,
        success: bool = True,
        error_message: str = None,
        severity: HistorySeverity = None,
    ) -> HistoryEntry:
        """
        이력 기록

        Args:
            source_id: 소스 ID
            action: 액션 타입
            version_id: 버전 ID
            actor: 실행자
            actor_type: 실행자 타입
            actor_ip: 실행자 IP
            details: 상세 정보
            summary: 요약
            tags: 태그
            affected_records: 영향받은 레코드 수
            affected_bytes: 영향받은 바이트 수
            success: 성공 여부
            error_message: 에러 메시지
            severity: 심각도 (자동 결정됨)

        Returns:
            기록된 HistoryEntry
        """
        entry_id = str(ObjectId())

        # 심각도 자동 결정
        if severity is None:
            severity = self._determine_severity(action, success)

        # 요약 자동 생성
        if not summary:
            summary = self._generate_summary(action, details, success)

        entry = HistoryEntry(
            entry_id=entry_id,
            source_id=source_id,
            version_id=version_id,
            action=action,
            severity=severity,
            actor=actor,
            actor_type=actor_type,
            actor_ip=actor_ip,
            details=details or {},
            summary=summary,
            tags=tags or [],
            timestamp=datetime.utcnow(),
            affected_records=affected_records,
            affected_bytes=affected_bytes,
            success=success,
            error_message=error_message,
        )

        self._save_entry(entry)

        logger.info(
            f"History recorded: source={source_id}, action={action.value}, "
            f"version={version_id}, success={success}"
        )

        return entry

    def record_version_create(
        self,
        source_id: str,
        version_id: str,
        version_number: int,
        record_count: int,
        actor: str = "system",
        details: Dict[str, Any] = None,
    ) -> HistoryEntry:
        """버전 생성 이력"""
        return self.record(
            source_id=source_id,
            action=HistoryAction.CREATE,
            version_id=version_id,
            actor=actor,
            details={
                "version_number": version_number,
                "record_count": record_count,
                **(details or {})
            },
            affected_records=record_count,
        )

    def record_rollback(
        self,
        source_id: str,
        from_version_id: str,
        to_version_id: str,
        to_version_number: int,
        new_version_id: str,
        actor: str = "system",
        reason: str = "",
    ) -> HistoryEntry:
        """롤백 이력"""
        return self.record(
            source_id=source_id,
            action=HistoryAction.ROLLBACK,
            version_id=new_version_id,
            actor=actor,
            severity=HistorySeverity.WARNING,
            details={
                "from_version_id": from_version_id,
                "to_version_id": to_version_id,
                "to_version_number": to_version_number,
                "reason": reason,
            },
            summary=f"Rolled back to version {to_version_number}: {reason}",
        )

    def record_snapshot(
        self,
        source_id: str,
        snapshot_id: str,
        snapshot_type: str,
        record_count: int,
        actor: str = "system",
    ) -> HistoryEntry:
        """스냅샷 생성 이력"""
        return self.record(
            source_id=source_id,
            action=HistoryAction.SNAPSHOT,
            actor=actor,
            details={
                "snapshot_id": snapshot_id,
                "snapshot_type": snapshot_type,
                "record_count": record_count,
            },
            affected_records=record_count,
        )

    def record_error(
        self,
        source_id: str,
        action: HistoryAction,
        error_message: str,
        version_id: str = None,
        actor: str = "system",
        details: Dict[str, Any] = None,
    ) -> HistoryEntry:
        """에러 이력"""
        return self.record(
            source_id=source_id,
            action=action,
            version_id=version_id,
            actor=actor,
            severity=HistorySeverity.ERROR,
            details=details,
            success=False,
            error_message=error_message,
        )

    # ==================== 이력 조회 ====================

    def get_entry(self, entry_id: str) -> Optional[HistoryEntry]:
        """
        이력 항목 조회

        Args:
            entry_id: 이력 ID

        Returns:
            HistoryEntry 또는 None
        """
        collection = self._get_collection()
        if collection:
            try:
                doc = collection.find_one({"_id": ObjectId(entry_id)})
                if doc:
                    return HistoryEntry.from_dict(doc)
            except Exception as e:
                logger.error(f"Error getting history entry {entry_id}: {e}")
        return None

    def list_entries(
        self,
        filter_obj: HistoryFilter = None,
        limit: int = 100,
        skip: int = 0,
        sort_desc: bool = True,
    ) -> List[HistoryEntry]:
        """
        이력 목록 조회

        Args:
            filter_obj: 필터 조건
            limit: 최대 개수
            skip: 건너뛸 개수
            sort_desc: 내림차순 정렬 (최신 먼저)

        Returns:
            이력 목록
        """
        collection = self._get_collection()
        if not collection:
            return []

        query = filter_obj.to_query() if filter_obj else {}
        sort_order = -1 if sort_desc else 1

        cursor = collection.find(query).sort(
            "timestamp", sort_order
        ).skip(skip).limit(limit)

        return [HistoryEntry.from_dict(doc) for doc in cursor]

    def list_by_source(
        self,
        source_id: str,
        limit: int = 100,
        skip: int = 0,
    ) -> List[HistoryEntry]:
        """소스별 이력 조회"""
        filter_obj = HistoryFilter(source_id=source_id)
        return self.list_entries(filter_obj, limit, skip)

    def list_by_version(
        self,
        version_id: str,
        limit: int = 100,
    ) -> List[HistoryEntry]:
        """버전별 이력 조회"""
        filter_obj = HistoryFilter(version_id=version_id)
        return self.list_entries(filter_obj, limit)

    def list_by_actor(
        self,
        actor: str,
        limit: int = 100,
        skip: int = 0,
    ) -> List[HistoryEntry]:
        """실행자별 이력 조회"""
        filter_obj = HistoryFilter(actor=actor)
        return self.list_entries(filter_obj, limit, skip)

    def list_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime = None,
        source_id: str = None,
        limit: int = 100,
    ) -> List[HistoryEntry]:
        """날짜 범위로 이력 조회"""
        filter_obj = HistoryFilter(
            source_id=source_id,
            start_date=start_date,
            end_date=end_date or datetime.utcnow(),
        )
        return self.list_entries(filter_obj, limit)

    def list_failures(
        self,
        source_id: str = None,
        limit: int = 100,
    ) -> List[HistoryEntry]:
        """실패 이력만 조회"""
        filter_obj = HistoryFilter(
            source_id=source_id,
            failure_only=True,
        )
        return self.list_entries(filter_obj, limit)

    def search(
        self,
        keyword: str,
        source_id: str = None,
        limit: int = 100,
    ) -> List[HistoryEntry]:
        """
        키워드 검색

        Args:
            keyword: 검색어
            source_id: 소스 ID 필터
            limit: 최대 개수

        Returns:
            검색된 이력 목록
        """
        collection = self._get_collection()
        if not collection:
            return []

        query = {
            "$or": [
                {"summary": {"$regex": keyword, "$options": "i"}},
                {"error_message": {"$regex": keyword, "$options": "i"}},
                {"actor": {"$regex": keyword, "$options": "i"}},
                {"tags": {"$regex": keyword, "$options": "i"}},
            ]
        }

        if source_id:
            query["source_id"] = source_id

        cursor = collection.find(query).sort(
            "timestamp", -1
        ).limit(limit)

        return [HistoryEntry.from_dict(doc) for doc in cursor]

    # ==================== 통계 ====================

    def get_stats(
        self,
        source_id: str,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> HistoryStats:
        """
        이력 통계

        Args:
            source_id: 소스 ID
            start_date: 시작 날짜
            end_date: 종료 날짜

        Returns:
            HistoryStats
        """
        collection = self._get_collection()
        if not collection:
            return HistoryStats(source_id=source_id)

        query = {"source_id": source_id}
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query["$gte"] = start_date
            if end_date:
                date_query["$lte"] = end_date
            query["timestamp"] = date_query

        # 기본 통계
        total = collection.count_documents(query)

        # 액션별 카운트
        action_pipeline = [
            {"$match": query},
            {"$group": {"_id": "$action", "count": {"$sum": 1}}}
        ]
        actions_count = {
            doc["_id"]: doc["count"]
            for doc in collection.aggregate(action_pipeline)
        }

        # 실행자별 카운트
        actor_pipeline = [
            {"$match": query},
            {"$group": {"_id": "$actor", "count": {"$sum": 1}}}
        ]
        actors_count = {
            doc["_id"]: doc["count"]
            for doc in collection.aggregate(actor_pipeline)
        }

        # 성공/실패 카운트
        success_count = collection.count_documents({**query, "success": True})
        failure_count = collection.count_documents({**query, "success": False})

        # 영향 합계
        impact_pipeline = [
            {"$match": query},
            {"$group": {
                "_id": None,
                "total_records": {"$sum": "$affected_records"},
                "total_bytes": {"$sum": "$affected_bytes"},
            }}
        ]
        impact_result = list(collection.aggregate(impact_pipeline))
        total_records = impact_result[0]["total_records"] if impact_result else 0
        total_bytes = impact_result[0]["total_bytes"] if impact_result else 0

        # 최초/최근 날짜
        first_doc = collection.find_one(query, sort=[("timestamp", 1)])
        last_doc = collection.find_one(query, sort=[("timestamp", -1)])

        return HistoryStats(
            source_id=source_id,
            total_entries=total,
            actions_count=actions_count,
            actors_count=actors_count,
            success_count=success_count,
            failure_count=failure_count,
            total_affected_records=total_records,
            total_affected_bytes=total_bytes,
            first_entry_date=first_doc["timestamp"] if first_doc else None,
            last_entry_date=last_doc["timestamp"] if last_doc else None,
        )

    def get_activity_timeline(
        self,
        source_id: str,
        days: int = 30,
        granularity: str = "day",
    ) -> List[Dict[str, Any]]:
        """
        활동 타임라인

        Args:
            source_id: 소스 ID
            days: 기간 (일)
            granularity: 집계 단위 (day, hour)

        Returns:
            타임라인 데이터
        """
        collection = self._get_collection()
        if not collection:
            return []

        start_date = datetime.utcnow() - timedelta(days=days)

        date_format = "%Y-%m-%d" if granularity == "day" else "%Y-%m-%d %H:00"

        pipeline = [
            {"$match": {
                "source_id": source_id,
                "timestamp": {"$gte": start_date}
            }},
            {"$group": {
                "_id": {
                    "$dateToString": {
                        "format": date_format,
                        "date": "$timestamp"
                    }
                },
                "count": {"$sum": 1},
                "success_count": {
                    "$sum": {"$cond": ["$success", 1, 0]}
                },
                "actions": {"$addToSet": "$action"},
            }},
            {"$sort": {"_id": 1}}
        ]

        return [
            {
                "date": doc["_id"],
                "count": doc["count"],
                "success_count": doc["success_count"],
                "failure_count": doc["count"] - doc["success_count"],
                "actions": doc["actions"],
            }
            for doc in collection.aggregate(pipeline)
        ]

    def get_top_actors(
        self,
        source_id: str = None,
        days: int = 30,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        가장 활발한 실행자 목록
        """
        collection = self._get_collection()
        if not collection:
            return []

        start_date = datetime.utcnow() - timedelta(days=days)
        query = {"timestamp": {"$gte": start_date}}
        if source_id:
            query["source_id"] = source_id

        pipeline = [
            {"$match": query},
            {"$group": {
                "_id": "$actor",
                "count": {"$sum": 1},
                "actions": {"$addToSet": "$action"},
                "last_activity": {"$max": "$timestamp"},
            }},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]

        return [
            {
                "actor": doc["_id"],
                "activity_count": doc["count"],
                "actions": doc["actions"],
                "last_activity": doc["last_activity"].isoformat(),
            }
            for doc in collection.aggregate(pipeline)
        ]

    # ==================== 유틸리티 ====================

    def _save_entry(self, entry: HistoryEntry):
        """이력 저장"""
        collection = self._get_collection()
        if collection:
            entry_doc = entry.to_dict()
            entry_doc["_id"] = ObjectId(entry.entry_id)
            collection.insert_one(entry_doc)

    def _determine_severity(
        self,
        action: HistoryAction,
        success: bool
    ) -> HistorySeverity:
        """심각도 자동 결정"""
        if not success:
            return HistorySeverity.ERROR

        severity_map = {
            HistoryAction.CREATE: HistorySeverity.INFO,
            HistoryAction.UPDATE: HistorySeverity.INFO,
            HistoryAction.DELETE: HistorySeverity.WARNING,
            HistoryAction.ARCHIVE: HistorySeverity.INFO,
            HistoryAction.RESTORE: HistorySeverity.INFO,
            HistoryAction.ROLLBACK: HistorySeverity.WARNING,
            HistoryAction.SNAPSHOT: HistorySeverity.INFO,
            HistoryAction.BRANCH: HistorySeverity.INFO,
            HistoryAction.TAG: HistorySeverity.INFO,
            HistoryAction.MERGE: HistorySeverity.WARNING,
            HistoryAction.COMPARE: HistorySeverity.INFO,
            HistoryAction.EXPORT: HistorySeverity.INFO,
            HistoryAction.IMPORT: HistorySeverity.WARNING,
        }

        return severity_map.get(action, HistorySeverity.INFO)

    def _generate_summary(
        self,
        action: HistoryAction,
        details: Dict[str, Any],
        success: bool
    ) -> str:
        """요약 자동 생성"""
        details = details or {}

        if not success:
            return f"Failed to {action.value}"

        summary_templates = {
            HistoryAction.CREATE: "Created version {version_number}",
            HistoryAction.UPDATE: "Updated version",
            HistoryAction.DELETE: "Deleted version",
            HistoryAction.ARCHIVE: "Archived version",
            HistoryAction.RESTORE: "Restored version",
            HistoryAction.ROLLBACK: "Rolled back to version {to_version_number}",
            HistoryAction.SNAPSHOT: "Created {snapshot_type} snapshot",
            HistoryAction.BRANCH: "Created branch {branch_name}",
            HistoryAction.TAG: "Added tag {tag}",
            HistoryAction.MERGE: "Merged branches",
            HistoryAction.COMPARE: "Compared versions",
            HistoryAction.EXPORT: "Exported data",
            HistoryAction.IMPORT: "Imported data",
        }

        template = summary_templates.get(action, f"Performed {action.value}")

        try:
            return template.format(**details)
        except KeyError:
            return template

    def cleanup_old_entries(
        self,
        days: int = 90,
        source_id: str = None,
        dry_run: bool = False,
    ) -> int:
        """
        오래된 이력 정리

        Args:
            days: 보존 기간 (일)
            source_id: 특정 소스만 정리
            dry_run: True면 삭제 없이 대상 수만 반환

        Returns:
            삭제된 항목 수
        """
        collection = self._get_collection()
        if not collection:
            return 0

        cutoff_date = datetime.utcnow() - timedelta(days=days)
        query = {"timestamp": {"$lt": cutoff_date}}

        if source_id:
            query["source_id"] = source_id

        if dry_run:
            return collection.count_documents(query)

        result = collection.delete_many(query)
        logger.info(f"Cleaned up {result.deleted_count} old history entries")
        return result.deleted_count

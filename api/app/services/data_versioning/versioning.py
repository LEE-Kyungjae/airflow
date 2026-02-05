"""
Data Version Manager - 데이터 버전 관리 핵심 클래스

기능:
- 버전 생성 및 관리
- 델타(변경분) 저장 또는 전체 스냅샷 저장
- 버전 간 비교
- 롤백 지원
- 브랜치/태그 지원
"""

import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from bson import ObjectId

logger = logging.getLogger(__name__)


class VersionStatus(str, Enum):
    """버전 상태"""
    ACTIVE = "active"           # 현재 활성 버전
    ARCHIVED = "archived"       # 보관된 이전 버전
    PENDING = "pending"         # 검토 대기 중
    ROLLED_BACK = "rolled_back" # 롤백된 버전
    DELETED = "deleted"         # 논리 삭제


class VersionType(str, Enum):
    """버전 타입"""
    FULL = "full"               # 전체 데이터 스냅샷
    DELTA = "delta"             # 변경분만 저장
    INCREMENTAL = "incremental" # 증분 변경


@dataclass
class VersionInfo:
    """버전 정보"""
    version_id: str
    source_id: str
    version_number: int
    parent_version_id: Optional[str] = None
    version_type: VersionType = VersionType.FULL
    status: VersionStatus = VersionStatus.ACTIVE

    # 메타데이터
    record_count: int = 0
    data_hash: str = ""
    size_bytes: int = 0

    # 변경 요약
    changes_summary: Dict[str, int] = field(default_factory=dict)

    # 브랜치/태그
    branch: str = "main"
    tags: List[str] = field(default_factory=list)

    # 타임스탬프
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = "system"
    description: str = ""

    # 연결 정보
    snapshot_id: Optional[str] = None
    lineage_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version_id": self.version_id,
            "source_id": self.source_id,
            "version_number": self.version_number,
            "parent_version_id": self.parent_version_id,
            "version_type": self.version_type.value,
            "status": self.status.value,
            "record_count": self.record_count,
            "data_hash": self.data_hash,
            "size_bytes": self.size_bytes,
            "changes_summary": self.changes_summary,
            "branch": self.branch,
            "tags": self.tags,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "description": self.description,
            "snapshot_id": self.snapshot_id,
            "lineage_id": self.lineage_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VersionInfo":
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.utcnow()

        return cls(
            version_id=str(data.get("version_id", data.get("_id", ""))),
            source_id=str(data.get("source_id", "")),
            version_number=data.get("version_number", 1),
            parent_version_id=data.get("parent_version_id"),
            version_type=VersionType(data.get("version_type", "full")),
            status=VersionStatus(data.get("status", "active")),
            record_count=data.get("record_count", 0),
            data_hash=data.get("data_hash", ""),
            size_bytes=data.get("size_bytes", 0),
            changes_summary=data.get("changes_summary", {}),
            branch=data.get("branch", "main"),
            tags=data.get("tags", []),
            created_at=created_at,
            created_by=data.get("created_by", "system"),
            description=data.get("description", ""),
            snapshot_id=data.get("snapshot_id"),
            lineage_id=data.get("lineage_id"),
        )


class DataVersionManager:
    """
    데이터 버전 관리자

    데이터의 버전을 관리하고 변경 이력을 추적합니다.
    스냅샷, diff, 롤백 기능을 제공합니다.
    """

    def __init__(self, mongo_service=None):
        """
        Args:
            mongo_service: MongoDB 서비스 인스턴스
        """
        self.mongo = mongo_service
        self._cache: Dict[str, VersionInfo] = {}

    # ==================== 컬렉션 접근 ====================

    def _get_versions_collection(self):
        """버전 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_versions
        return None

    def _get_snapshots_collection(self):
        """스냅샷 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_snapshots
        return None

    def _get_history_collection(self):
        """히스토리 컬렉션"""
        if self.mongo:
            return self.mongo.db.version_history
        return None

    def _get_lineage_collection(self):
        """리니지 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_lineage
        return None

    # ==================== 버전 생성 ====================

    def create_version(
        self,
        source_id: str,
        data: List[Dict[str, Any]],
        version_type: VersionType = VersionType.FULL,
        branch: str = "main",
        created_by: str = "system",
        description: str = "",
        tags: List[str] = None,
        parent_version_id: str = None,
    ) -> VersionInfo:
        """
        새 버전 생성

        Args:
            source_id: 소스 ID
            data: 버전 데이터
            version_type: 버전 타입 (full/delta/incremental)
            branch: 브랜치 이름
            created_by: 생성자
            description: 버전 설명
            tags: 태그 목록
            parent_version_id: 부모 버전 ID (지정하지 않으면 자동 탐색)

        Returns:
            VersionInfo: 생성된 버전 정보
        """
        # 현재 버전 번호 조회
        latest = self.get_latest_version(source_id, branch)
        next_version = (latest.version_number + 1) if latest else 1

        # 부모 버전 결정
        if parent_version_id is None and latest:
            parent_version_id = latest.version_id

        # 데이터 해시 계산
        data_hash = self._compute_hash(data)
        data_json = json.dumps(data, default=str, ensure_ascii=False)
        size_bytes = len(data_json.encode('utf-8'))

        # 변경 요약 계산
        changes_summary = {}
        if latest and version_type in (VersionType.DELTA, VersionType.INCREMENTAL):
            from .diff import DiffEngine
            diff_engine = DiffEngine()
            previous_data = self.get_version_data(latest.version_id)
            if previous_data:
                diff_result = diff_engine.compute_diff(previous_data, data)
                changes_summary = {
                    "added": diff_result.added_count,
                    "modified": diff_result.modified_count,
                    "deleted": diff_result.deleted_count,
                    "unchanged": diff_result.unchanged_count,
                }

        # 버전 ID 생성
        version_id = str(ObjectId())

        # 버전 정보 생성
        version_info = VersionInfo(
            version_id=version_id,
            source_id=source_id,
            version_number=next_version,
            parent_version_id=parent_version_id,
            version_type=version_type,
            status=VersionStatus.ACTIVE,
            record_count=len(data),
            data_hash=data_hash,
            size_bytes=size_bytes,
            changes_summary=changes_summary,
            branch=branch,
            tags=tags or [],
            created_at=datetime.utcnow(),
            created_by=created_by,
            description=description,
        )

        # 이전 버전을 ARCHIVED로 변경
        if latest:
            self._archive_version(latest.version_id)

        # 버전 저장
        self._save_version(version_info, data)

        # 히스토리 기록
        self._record_history(
            source_id=source_id,
            version_id=version_id,
            action="create",
            actor=created_by,
            details={
                "version_number": next_version,
                "record_count": len(data),
                "version_type": version_type.value,
            }
        )

        # 리니지 연결
        self._link_lineage(source_id, version_id)

        logger.info(
            f"Created version: source={source_id}, version={next_version}, "
            f"type={version_type.value}, records={len(data)}"
        )

        return version_info

    def create_delta_version(
        self,
        source_id: str,
        changes: Dict[str, Any],
        created_by: str = "system",
        description: str = "",
    ) -> VersionInfo:
        """
        델타(변경분) 버전 생성

        Args:
            source_id: 소스 ID
            changes: 변경 내용 {added: [], modified: [], deleted: []}
            created_by: 생성자
            description: 설명

        Returns:
            VersionInfo: 생성된 버전 정보
        """
        latest = self.get_latest_version(source_id)
        if not latest:
            raise ValueError(f"No existing version for source {source_id}")

        # 델타 데이터 구조
        delta_data = {
            "parent_version_id": latest.version_id,
            "added": changes.get("added", []),
            "modified": changes.get("modified", []),
            "deleted": changes.get("deleted", []),
            "timestamp": datetime.utcnow().isoformat(),
        }

        return self.create_version(
            source_id=source_id,
            data=[delta_data],  # 델타는 단일 문서로 저장
            version_type=VersionType.DELTA,
            created_by=created_by,
            description=description,
            parent_version_id=latest.version_id,
        )

    # ==================== 버전 조회 ====================

    def get_version(self, version_id: str) -> Optional[VersionInfo]:
        """
        버전 정보 조회

        Args:
            version_id: 버전 ID

        Returns:
            VersionInfo 또는 None
        """
        # 캐시 확인
        if version_id in self._cache:
            return self._cache[version_id]

        collection = self._get_versions_collection()
        if collection:
            try:
                doc = collection.find_one({"_id": ObjectId(version_id)})
                if doc:
                    version = VersionInfo.from_dict(doc)
                    self._cache[version_id] = version
                    return version
            except Exception as e:
                logger.error(f"Error getting version {version_id}: {e}")

        return None

    def get_version_by_number(
        self,
        source_id: str,
        version_number: int,
        branch: str = "main"
    ) -> Optional[VersionInfo]:
        """
        버전 번호로 조회

        Args:
            source_id: 소스 ID
            version_number: 버전 번호
            branch: 브랜치

        Returns:
            VersionInfo 또는 None
        """
        collection = self._get_versions_collection()
        if collection:
            doc = collection.find_one({
                "source_id": source_id,
                "version_number": version_number,
                "branch": branch,
            })
            if doc:
                return VersionInfo.from_dict(doc)
        return None

    def get_latest_version(
        self,
        source_id: str,
        branch: str = "main",
        include_archived: bool = False
    ) -> Optional[VersionInfo]:
        """
        최신 버전 조회

        Args:
            source_id: 소스 ID
            branch: 브랜치
            include_archived: 아카이브된 버전 포함 여부

        Returns:
            VersionInfo 또는 None
        """
        collection = self._get_versions_collection()
        if collection:
            query = {
                "source_id": source_id,
                "branch": branch,
            }
            if not include_archived:
                query["status"] = {"$ne": VersionStatus.DELETED.value}

            doc = collection.find_one(
                query,
                sort=[("version_number", -1)]
            )
            if doc:
                return VersionInfo.from_dict(doc)
        return None

    def get_active_version(self, source_id: str, branch: str = "main") -> Optional[VersionInfo]:
        """
        현재 활성 버전 조회

        Args:
            source_id: 소스 ID
            branch: 브랜치

        Returns:
            활성 VersionInfo 또는 None
        """
        collection = self._get_versions_collection()
        if collection:
            doc = collection.find_one({
                "source_id": source_id,
                "branch": branch,
                "status": VersionStatus.ACTIVE.value,
            })
            if doc:
                return VersionInfo.from_dict(doc)
        return None

    def list_versions(
        self,
        source_id: str,
        branch: str = None,
        status: VersionStatus = None,
        limit: int = 50,
        skip: int = 0,
    ) -> List[VersionInfo]:
        """
        버전 목록 조회

        Args:
            source_id: 소스 ID
            branch: 브랜치 (None이면 전체)
            status: 상태 필터
            limit: 최대 개수
            skip: 건너뛸 개수

        Returns:
            버전 목록
        """
        collection = self._get_versions_collection()
        if not collection:
            return []

        query = {"source_id": source_id}
        if branch:
            query["branch"] = branch
        if status:
            query["status"] = status.value

        cursor = collection.find(query).sort(
            "version_number", -1
        ).skip(skip).limit(limit)

        return [VersionInfo.from_dict(doc) for doc in cursor]

    def get_version_data(self, version_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        버전의 실제 데이터 조회

        Args:
            version_id: 버전 ID

        Returns:
            데이터 목록 또는 None
        """
        collection = self._get_snapshots_collection()
        if collection:
            doc = collection.find_one({"version_id": version_id})
            if doc:
                return doc.get("data", [])
        return None

    def get_version_data_materialized(
        self,
        version_id: str
    ) -> Optional[List[Dict[str, Any]]]:
        """
        델타 버전의 경우 전체 데이터로 복원하여 반환

        Args:
            version_id: 버전 ID

        Returns:
            복원된 전체 데이터
        """
        version = self.get_version(version_id)
        if not version:
            return None

        if version.version_type == VersionType.FULL:
            return self.get_version_data(version_id)

        # 델타 버전인 경우 체인을 따라가며 복원
        return self._materialize_delta_chain(version_id)

    # ==================== 버전 비교 ====================

    def compare_versions(
        self,
        version_id_1: str,
        version_id_2: str,
        key_field: str = "_id"
    ) -> "DiffResult":
        """
        두 버전 비교

        Args:
            version_id_1: 첫 번째 버전 ID
            version_id_2: 두 번째 버전 ID
            key_field: 레코드 식별 필드

        Returns:
            DiffResult: 비교 결과
        """
        from .diff import DiffEngine

        data1 = self.get_version_data_materialized(version_id_1)
        data2 = self.get_version_data_materialized(version_id_2)

        if data1 is None or data2 is None:
            raise ValueError("Could not retrieve version data")

        diff_engine = DiffEngine(key_field=key_field)
        return diff_engine.compute_diff(data1, data2)

    # ==================== 롤백 ====================

    def rollback_to_version(
        self,
        source_id: str,
        target_version_id: str,
        created_by: str = "system",
        reason: str = ""
    ) -> VersionInfo:
        """
        특정 버전으로 롤백

        Args:
            source_id: 소스 ID
            target_version_id: 롤백 대상 버전 ID
            created_by: 실행자
            reason: 롤백 사유

        Returns:
            새로 생성된 버전 (롤백 결과)
        """
        target_version = self.get_version(target_version_id)
        if not target_version:
            raise ValueError(f"Version not found: {target_version_id}")

        if target_version.source_id != source_id:
            raise ValueError("Version does not belong to this source")

        # 롤백 대상 버전의 데이터 가져오기
        target_data = self.get_version_data_materialized(target_version_id)
        if target_data is None:
            raise ValueError("Could not retrieve version data for rollback")

        # 현재 활성 버전을 ROLLED_BACK으로 변경
        current = self.get_active_version(source_id, target_version.branch)
        if current:
            self._update_version_status(
                current.version_id,
                VersionStatus.ROLLED_BACK
            )

        # 롤백 버전 생성
        new_version = self.create_version(
            source_id=source_id,
            data=target_data,
            version_type=VersionType.FULL,
            branch=target_version.branch,
            created_by=created_by,
            description=f"Rollback to v{target_version.version_number}: {reason}",
            tags=["rollback"],
            parent_version_id=current.version_id if current else None,
        )

        # 히스토리 기록
        self._record_history(
            source_id=source_id,
            version_id=new_version.version_id,
            action="rollback",
            actor=created_by,
            details={
                "target_version": target_version_id,
                "target_version_number": target_version.version_number,
                "reason": reason,
                "rolled_back_from": current.version_id if current else None,
            }
        )

        logger.info(
            f"Rollback completed: source={source_id}, "
            f"target=v{target_version.version_number}, new=v{new_version.version_number}"
        )

        return new_version

    # ==================== 태그/브랜치 ====================

    def add_tag(self, version_id: str, tag: str, created_by: str = "system") -> bool:
        """
        버전에 태그 추가

        Args:
            version_id: 버전 ID
            tag: 태그 이름
            created_by: 실행자

        Returns:
            성공 여부
        """
        collection = self._get_versions_collection()
        if collection:
            result = collection.update_one(
                {"_id": ObjectId(version_id)},
                {"$addToSet": {"tags": tag}}
            )

            if result.modified_count > 0:
                self._invalidate_cache(version_id)

                version = self.get_version(version_id)
                if version:
                    self._record_history(
                        source_id=version.source_id,
                        version_id=version_id,
                        action="add_tag",
                        actor=created_by,
                        details={"tag": tag}
                    )
                return True
        return False

    def remove_tag(self, version_id: str, tag: str, created_by: str = "system") -> bool:
        """
        버전에서 태그 제거
        """
        collection = self._get_versions_collection()
        if collection:
            result = collection.update_one(
                {"_id": ObjectId(version_id)},
                {"$pull": {"tags": tag}}
            )

            if result.modified_count > 0:
                self._invalidate_cache(version_id)
                return True
        return False

    def get_version_by_tag(self, source_id: str, tag: str) -> Optional[VersionInfo]:
        """
        태그로 버전 조회
        """
        collection = self._get_versions_collection()
        if collection:
            doc = collection.find_one({
                "source_id": source_id,
                "tags": tag,
            })
            if doc:
                return VersionInfo.from_dict(doc)
        return None

    def create_branch(
        self,
        source_id: str,
        from_version_id: str,
        branch_name: str,
        created_by: str = "system"
    ) -> VersionInfo:
        """
        새 브랜치 생성

        Args:
            source_id: 소스 ID
            from_version_id: 분기 시작 버전
            branch_name: 새 브랜치 이름
            created_by: 생성자

        Returns:
            새 브랜치의 첫 버전
        """
        from_version = self.get_version(from_version_id)
        if not from_version:
            raise ValueError(f"Version not found: {from_version_id}")

        # 브랜치 이름 중복 확인
        existing = self.get_latest_version(source_id, branch_name)
        if existing:
            raise ValueError(f"Branch already exists: {branch_name}")

        # 원본 데이터 가져오기
        data = self.get_version_data_materialized(from_version_id)
        if data is None:
            raise ValueError("Could not retrieve version data")

        # 새 브랜치로 버전 생성
        new_version = self.create_version(
            source_id=source_id,
            data=data,
            version_type=VersionType.FULL,
            branch=branch_name,
            created_by=created_by,
            description=f"Branch from v{from_version.version_number} ({from_version.branch})",
            parent_version_id=from_version_id,
        )

        logger.info(
            f"Created branch: source={source_id}, branch={branch_name}, "
            f"from=v{from_version.version_number}"
        )

        return new_version

    def list_branches(self, source_id: str) -> List[str]:
        """
        소스의 모든 브랜치 목록
        """
        collection = self._get_versions_collection()
        if collection:
            branches = collection.distinct("branch", {"source_id": source_id})
            return sorted(branches)
        return []

    # ==================== 유틸리티 ====================

    def _compute_hash(self, data: List[Dict[str, Any]]) -> str:
        """데이터 해시 계산"""
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()[:32]

    def _save_version(self, version: VersionInfo, data: List[Dict[str, Any]]):
        """버전 및 데이터 저장"""
        versions_col = self._get_versions_collection()
        snapshots_col = self._get_snapshots_collection()

        if versions_col and snapshots_col:
            # 버전 메타데이터 저장
            version_doc = version.to_dict()
            version_doc["_id"] = ObjectId(version.version_id)
            versions_col.insert_one(version_doc)

            # 스냅샷 데이터 저장
            snapshot_doc = {
                "version_id": version.version_id,
                "source_id": version.source_id,
                "data": data,
                "created_at": version.created_at,
            }
            result = snapshots_col.insert_one(snapshot_doc)

            # 스냅샷 ID 업데이트
            versions_col.update_one(
                {"_id": ObjectId(version.version_id)},
                {"$set": {"snapshot_id": str(result.inserted_id)}}
            )

    def _archive_version(self, version_id: str):
        """버전 아카이브"""
        self._update_version_status(version_id, VersionStatus.ARCHIVED)

    def _update_version_status(self, version_id: str, status: VersionStatus):
        """버전 상태 업데이트"""
        collection = self._get_versions_collection()
        if collection:
            collection.update_one(
                {"_id": ObjectId(version_id)},
                {"$set": {"status": status.value}}
            )
            self._invalidate_cache(version_id)

    def _record_history(
        self,
        source_id: str,
        version_id: str,
        action: str,
        actor: str,
        details: Dict[str, Any] = None
    ):
        """히스토리 기록"""
        collection = self._get_history_collection()
        if collection:
            collection.insert_one({
                "source_id": source_id,
                "version_id": version_id,
                "action": action,
                "actor": actor,
                "details": details or {},
                "timestamp": datetime.utcnow(),
            })

    def _link_lineage(self, source_id: str, version_id: str):
        """리니지 컬렉션과 연결"""
        lineage_col = self._get_lineage_collection()
        versions_col = self._get_versions_collection()

        if lineage_col and versions_col:
            # 최신 리니지 레코드 찾기
            lineage = lineage_col.find_one(
                {"source_id": source_id},
                sort=[("created_at", -1)]
            )

            if lineage:
                lineage_id = str(lineage["_id"])
                versions_col.update_one(
                    {"_id": ObjectId(version_id)},
                    {"$set": {"lineage_id": lineage_id}}
                )

                # 리니지에도 버전 ID 추가
                lineage_col.update_one(
                    {"_id": lineage["_id"]},
                    {"$addToSet": {"version_ids": version_id}}
                )

    def _materialize_delta_chain(
        self,
        version_id: str
    ) -> Optional[List[Dict[str, Any]]]:
        """
        델타 체인을 따라가며 전체 데이터 복원
        """
        version = self.get_version(version_id)
        if not version:
            return None

        # FULL 버전을 찾을 때까지 체인 추적
        chain = []
        current = version

        while current:
            chain.append(current)

            if current.version_type == VersionType.FULL:
                break

            if current.parent_version_id:
                current = self.get_version(current.parent_version_id)
            else:
                break

        if not chain or chain[-1].version_type != VersionType.FULL:
            logger.error(f"Could not find base FULL version for {version_id}")
            return None

        # 가장 오래된 FULL 버전부터 시작하여 델타 적용
        chain.reverse()
        base_data = self.get_version_data(chain[0].version_id)

        if base_data is None:
            return None

        # 각 델타 적용
        result = list(base_data)
        for delta_version in chain[1:]:
            if delta_version.version_type == VersionType.DELTA:
                delta_data = self.get_version_data(delta_version.version_id)
                if delta_data and len(delta_data) > 0:
                    delta = delta_data[0]
                    result = self._apply_delta(result, delta)

        return result

    def _apply_delta(
        self,
        base: List[Dict[str, Any]],
        delta: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        델타를 기본 데이터에 적용
        """
        result = list(base)

        # 삭제 적용
        deleted_ids = {str(d.get("_id")) for d in delta.get("deleted", [])}
        result = [r for r in result if str(r.get("_id")) not in deleted_ids]

        # 수정 적용
        modified_map = {str(m.get("_id")): m for m in delta.get("modified", [])}
        result = [
            modified_map.get(str(r.get("_id")), r)
            for r in result
        ]

        # 추가 적용
        result.extend(delta.get("added", []))

        return result

    def _invalidate_cache(self, version_id: str = None):
        """캐시 무효화"""
        if version_id:
            self._cache.pop(version_id, None)
        else:
            self._cache.clear()

    # ==================== 통계 ====================

    def get_version_stats(self, source_id: str) -> Dict[str, Any]:
        """
        소스의 버전 통계

        Args:
            source_id: 소스 ID

        Returns:
            통계 정보
        """
        collection = self._get_versions_collection()
        if not collection:
            return {}

        # 버전 수
        total_versions = collection.count_documents({"source_id": source_id})
        active_versions = collection.count_documents({
            "source_id": source_id,
            "status": VersionStatus.ACTIVE.value
        })

        # 브랜치 수
        branches = collection.distinct("branch", {"source_id": source_id})

        # 최신 버전
        latest = self.get_latest_version(source_id)

        # 총 데이터 크기
        pipeline = [
            {"$match": {"source_id": source_id}},
            {"$group": {
                "_id": None,
                "total_size": {"$sum": "$size_bytes"},
                "total_records": {"$sum": "$record_count"},
            }}
        ]
        agg_result = list(collection.aggregate(pipeline))

        size_info = agg_result[0] if agg_result else {"total_size": 0, "total_records": 0}

        return {
            "source_id": source_id,
            "total_versions": total_versions,
            "active_versions": active_versions,
            "branches": branches,
            "branch_count": len(branches),
            "latest_version": latest.to_dict() if latest else None,
            "total_size_bytes": size_info.get("total_size", 0),
            "total_records_across_versions": size_info.get("total_records", 0),
        }

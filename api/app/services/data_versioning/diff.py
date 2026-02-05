"""
Diff Engine - 데이터 변경 비교 엔진

기능:
- 두 데이터셋 간 차이점 계산
- 필드 레벨 변경 추적
- 레코드 레벨 변경 추적
- 변경 요약 생성
"""

import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger(__name__)


class ChangeType(str, Enum):
    """변경 타입"""
    ADDED = "added"           # 추가됨
    DELETED = "deleted"       # 삭제됨
    MODIFIED = "modified"     # 수정됨
    UNCHANGED = "unchanged"   # 변경 없음
    TYPE_CHANGED = "type_changed"  # 타입 변경


@dataclass
class FieldChange:
    """필드 변경 정보"""
    field_name: str
    change_type: ChangeType
    old_value: Any = None
    new_value: Any = None
    old_type: str = ""
    new_type: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field_name": self.field_name,
            "change_type": self.change_type.value,
            "old_value": self._serialize_value(self.old_value),
            "new_value": self._serialize_value(self.new_value),
            "old_type": self.old_type,
            "new_type": self.new_type,
        }

    def _serialize_value(self, value: Any) -> Any:
        """값 직렬화 (너무 긴 값은 잘라냄)"""
        if value is None:
            return None
        str_val = str(value)
        if len(str_val) > 200:
            return str_val[:200] + "..."
        return value

    @property
    def is_significant(self) -> bool:
        """의미 있는 변경인지"""
        return self.change_type != ChangeType.UNCHANGED


@dataclass
class RecordChange:
    """레코드 변경 정보"""
    record_id: str
    change_type: ChangeType
    field_changes: List[FieldChange] = field(default_factory=list)
    old_record: Dict[str, Any] = field(default_factory=dict)
    new_record: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_id": self.record_id,
            "change_type": self.change_type.value,
            "field_changes": [fc.to_dict() for fc in self.field_changes],
            "changed_fields_count": len([f for f in self.field_changes if f.is_significant]),
        }

    @property
    def modified_fields(self) -> List[str]:
        """수정된 필드 이름 목록"""
        return [
            fc.field_name
            for fc in self.field_changes
            if fc.change_type == ChangeType.MODIFIED
        ]

    @property
    def added_fields(self) -> List[str]:
        """추가된 필드 이름 목록"""
        return [
            fc.field_name
            for fc in self.field_changes
            if fc.change_type == ChangeType.ADDED
        ]

    @property
    def deleted_fields(self) -> List[str]:
        """삭제된 필드 이름 목록"""
        return [
            fc.field_name
            for fc in self.field_changes
            if fc.change_type == ChangeType.DELETED
        ]


@dataclass
class DiffResult:
    """비교 결과"""
    source_version_id: Optional[str] = None
    target_version_id: Optional[str] = None
    computed_at: datetime = field(default_factory=datetime.utcnow)

    # 레코드 변경
    added_records: List[RecordChange] = field(default_factory=list)
    deleted_records: List[RecordChange] = field(default_factory=list)
    modified_records: List[RecordChange] = field(default_factory=list)
    unchanged_records: List[RecordChange] = field(default_factory=list)

    # 통계
    source_record_count: int = 0
    target_record_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_version_id": self.source_version_id,
            "target_version_id": self.target_version_id,
            "computed_at": self.computed_at.isoformat(),
            "summary": self.summary,
            "added_records": [r.to_dict() for r in self.added_records[:100]],  # 최대 100개
            "deleted_records": [r.to_dict() for r in self.deleted_records[:100]],
            "modified_records": [r.to_dict() for r in self.modified_records[:100]],
            "source_record_count": self.source_record_count,
            "target_record_count": self.target_record_count,
        }

    @property
    def added_count(self) -> int:
        return len(self.added_records)

    @property
    def deleted_count(self) -> int:
        return len(self.deleted_records)

    @property
    def modified_count(self) -> int:
        return len(self.modified_records)

    @property
    def unchanged_count(self) -> int:
        return len(self.unchanged_records)

    @property
    def total_changes(self) -> int:
        return self.added_count + self.deleted_count + self.modified_count

    @property
    def has_changes(self) -> bool:
        return self.total_changes > 0

    @property
    def change_rate(self) -> float:
        """변경률 (0-1)"""
        total = max(self.source_record_count, self.target_record_count)
        if total == 0:
            return 0.0
        return self.total_changes / total

    @property
    def summary(self) -> Dict[str, Any]:
        """변경 요약"""
        return {
            "added": self.added_count,
            "deleted": self.deleted_count,
            "modified": self.modified_count,
            "unchanged": self.unchanged_count,
            "total_changes": self.total_changes,
            "change_rate": round(self.change_rate * 100, 2),
            "source_count": self.source_record_count,
            "target_count": self.target_record_count,
        }

    def get_modified_fields_summary(self) -> Dict[str, int]:
        """
        수정된 필드별 변경 횟수 요약
        """
        field_counts = defaultdict(int)
        for record in self.modified_records:
            for fc in record.field_changes:
                if fc.is_significant:
                    field_counts[fc.field_name] += 1
        return dict(field_counts)

    def get_field_change_types(self) -> Dict[str, Dict[str, int]]:
        """
        필드별 변경 타입 통계
        """
        stats = defaultdict(lambda: defaultdict(int))
        for record in self.modified_records:
            for fc in record.field_changes:
                stats[fc.field_name][fc.change_type.value] += 1
        return {k: dict(v) for k, v in stats.items()}


class DiffEngine:
    """
    데이터 비교 엔진

    두 데이터셋 간의 차이점을 계산하고 상세한 변경 정보를 제공합니다.
    """

    def __init__(
        self,
        key_field: str = "_id",
        ignore_fields: List[str] = None,
        case_sensitive: bool = True,
        deep_compare: bool = True,
    ):
        """
        Args:
            key_field: 레코드 식별에 사용할 필드
            ignore_fields: 비교에서 제외할 필드
            case_sensitive: 문자열 비교 시 대소문자 구분
            deep_compare: 중첩 객체 심층 비교 여부
        """
        self.key_field = key_field
        self.ignore_fields = set(ignore_fields or ["_id", "created_at", "updated_at"])
        self.case_sensitive = case_sensitive
        self.deep_compare = deep_compare

    def compute_diff(
        self,
        source_data: List[Dict[str, Any]],
        target_data: List[Dict[str, Any]],
        source_version_id: str = None,
        target_version_id: str = None,
    ) -> DiffResult:
        """
        두 데이터셋 비교

        Args:
            source_data: 원본 데이터 (이전 버전)
            target_data: 대상 데이터 (새 버전)
            source_version_id: 원본 버전 ID
            target_version_id: 대상 버전 ID

        Returns:
            DiffResult: 비교 결과
        """
        result = DiffResult(
            source_version_id=source_version_id,
            target_version_id=target_version_id,
            source_record_count=len(source_data),
            target_record_count=len(target_data),
        )

        # 키로 인덱싱
        source_map = self._index_by_key(source_data)
        target_map = self._index_by_key(target_data)

        source_keys = set(source_map.keys())
        target_keys = set(target_map.keys())

        # 추가된 레코드
        added_keys = target_keys - source_keys
        for key in added_keys:
            result.added_records.append(RecordChange(
                record_id=key,
                change_type=ChangeType.ADDED,
                new_record=target_map[key],
            ))

        # 삭제된 레코드
        deleted_keys = source_keys - target_keys
        for key in deleted_keys:
            result.deleted_records.append(RecordChange(
                record_id=key,
                change_type=ChangeType.DELETED,
                old_record=source_map[key],
            ))

        # 공통 레코드 비교
        common_keys = source_keys & target_keys
        for key in common_keys:
            old_record = source_map[key]
            new_record = target_map[key]

            field_changes = self._compare_records(old_record, new_record)
            has_changes = any(fc.is_significant for fc in field_changes)

            record_change = RecordChange(
                record_id=key,
                change_type=ChangeType.MODIFIED if has_changes else ChangeType.UNCHANGED,
                field_changes=field_changes,
                old_record=old_record if has_changes else {},
                new_record=new_record if has_changes else {},
            )

            if has_changes:
                result.modified_records.append(record_change)
            else:
                result.unchanged_records.append(record_change)

        logger.debug(
            f"Diff computed: added={result.added_count}, "
            f"deleted={result.deleted_count}, modified={result.modified_count}"
        )

        return result

    def compute_field_diff(
        self,
        old_value: Any,
        new_value: Any,
        field_name: str
    ) -> FieldChange:
        """
        단일 필드 비교

        Args:
            old_value: 이전 값
            new_value: 새 값
            field_name: 필드 이름

        Returns:
            FieldChange: 필드 변경 정보
        """
        old_type = type(old_value).__name__ if old_value is not None else "null"
        new_type = type(new_value).__name__ if new_value is not None else "null"

        # 타입 변경
        if old_type != new_type and old_value is not None and new_value is not None:
            return FieldChange(
                field_name=field_name,
                change_type=ChangeType.TYPE_CHANGED,
                old_value=old_value,
                new_value=new_value,
                old_type=old_type,
                new_type=new_type,
            )

        # 추가됨 (이전에 없음)
        if old_value is None and new_value is not None:
            return FieldChange(
                field_name=field_name,
                change_type=ChangeType.ADDED,
                new_value=new_value,
                new_type=new_type,
            )

        # 삭제됨 (새로 없음)
        if old_value is not None and new_value is None:
            return FieldChange(
                field_name=field_name,
                change_type=ChangeType.DELETED,
                old_value=old_value,
                old_type=old_type,
            )

        # 값 비교
        if self._values_equal(old_value, new_value):
            return FieldChange(
                field_name=field_name,
                change_type=ChangeType.UNCHANGED,
                old_value=old_value,
                new_value=new_value,
            )

        return FieldChange(
            field_name=field_name,
            change_type=ChangeType.MODIFIED,
            old_value=old_value,
            new_value=new_value,
            old_type=old_type,
            new_type=new_type,
        )

    def compute_hash(self, record: Dict[str, Any]) -> str:
        """
        레코드 해시 계산 (비교용)
        """
        filtered = {
            k: v for k, v in record.items()
            if k not in self.ignore_fields
        }
        data_str = json.dumps(filtered, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()

    def get_changed_fields(
        self,
        old_record: Dict[str, Any],
        new_record: Dict[str, Any]
    ) -> List[str]:
        """
        변경된 필드 이름 목록 반환 (빠른 확인용)
        """
        changes = self._compare_records(old_record, new_record)
        return [fc.field_name for fc in changes if fc.is_significant]

    def create_patch(
        self,
        source_data: List[Dict[str, Any]],
        target_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        패치 문서 생성 (source를 target으로 변환하는 변경분)

        Args:
            source_data: 원본 데이터
            target_data: 대상 데이터

        Returns:
            패치 문서 {added: [], modified: [], deleted: []}
        """
        diff = self.compute_diff(source_data, target_data)

        return {
            "added": [r.new_record for r in diff.added_records],
            "modified": [
                {
                    "_id": r.record_id,
                    "old": r.old_record,
                    "new": r.new_record,
                    "changes": [fc.to_dict() for fc in r.field_changes if fc.is_significant]
                }
                for r in diff.modified_records
            ],
            "deleted": [
                {"_id": r.record_id, "record": r.old_record}
                for r in diff.deleted_records
            ],
            "summary": diff.summary,
            "computed_at": datetime.utcnow().isoformat(),
        }

    def apply_patch(
        self,
        data: List[Dict[str, Any]],
        patch: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        패치 적용

        Args:
            data: 원본 데이터
            patch: 패치 문서

        Returns:
            패치 적용된 데이터
        """
        result = list(data)
        data_map = self._index_by_key(result)

        # 삭제 적용
        for deleted in patch.get("deleted", []):
            key = str(deleted.get("_id"))
            if key in data_map:
                result = [r for r in result if str(r.get(self.key_field)) != key]

        # 수정 적용
        for modified in patch.get("modified", []):
            key = str(modified.get("_id"))
            new_record = modified.get("new", {})
            if key in data_map:
                for i, r in enumerate(result):
                    if str(r.get(self.key_field)) == key:
                        result[i] = new_record
                        break

        # 추가 적용
        result.extend(patch.get("added", []))

        return result

    # ==================== 내부 메서드 ====================

    def _index_by_key(
        self,
        data: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """데이터를 키로 인덱싱"""
        return {
            str(record.get(self.key_field, hash(json.dumps(record, sort_keys=True, default=str)))): record
            for record in data
        }

    def _compare_records(
        self,
        old_record: Dict[str, Any],
        new_record: Dict[str, Any]
    ) -> List[FieldChange]:
        """
        두 레코드의 모든 필드 비교
        """
        changes = []

        all_fields = set(old_record.keys()) | set(new_record.keys())
        compare_fields = all_fields - self.ignore_fields

        for field_name in compare_fields:
            old_value = old_record.get(field_name)
            new_value = new_record.get(field_name)

            change = self.compute_field_diff(old_value, new_value, field_name)
            changes.append(change)

        return changes

    def _values_equal(self, v1: Any, v2: Any) -> bool:
        """
        두 값이 같은지 비교
        """
        if v1 is None and v2 is None:
            return True

        if type(v1) != type(v2):
            return False

        # 문자열 비교
        if isinstance(v1, str):
            if self.case_sensitive:
                return v1 == v2
            return v1.lower() == v2.lower()

        # 딕셔너리 심층 비교
        if isinstance(v1, dict) and self.deep_compare:
            return self._dicts_equal(v1, v2)

        # 리스트 심층 비교
        if isinstance(v1, list) and self.deep_compare:
            return self._lists_equal(v1, v2)

        # 기본 비교
        return v1 == v2

    def _dicts_equal(self, d1: Dict, d2: Dict) -> bool:
        """딕셔너리 심층 비교"""
        if set(d1.keys()) != set(d2.keys()):
            return False

        for key in d1:
            if not self._values_equal(d1[key], d2[key]):
                return False
        return True

    def _lists_equal(self, l1: List, l2: List) -> bool:
        """리스트 심층 비교"""
        if len(l1) != len(l2):
            return False

        for i in range(len(l1)):
            if not self._values_equal(l1[i], l2[i]):
                return False
        return True


class DiffAnalyzer:
    """
    Diff 분석 도우미

    DiffResult를 분석하여 인사이트를 제공합니다.
    """

    @staticmethod
    def analyze_trends(
        diff_results: List[DiffResult]
    ) -> Dict[str, Any]:
        """
        여러 diff 결과에서 트렌드 분석

        Args:
            diff_results: diff 결과 목록 (시간순)

        Returns:
            트렌드 분석 결과
        """
        if not diff_results:
            return {}

        total_added = sum(d.added_count for d in diff_results)
        total_deleted = sum(d.deleted_count for d in diff_results)
        total_modified = sum(d.modified_count for d in diff_results)
        avg_change_rate = sum(d.change_rate for d in diff_results) / len(diff_results)

        # 변경률 추이
        change_rates = [
            {
                "date": d.computed_at.isoformat(),
                "rate": d.change_rate,
                "total_changes": d.total_changes,
            }
            for d in diff_results
        ]

        # 가장 자주 변경되는 필드
        field_frequency = defaultdict(int)
        for diff in diff_results:
            for field, count in diff.get_modified_fields_summary().items():
                field_frequency[field] += count

        top_changed_fields = sorted(
            field_frequency.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]

        return {
            "period_count": len(diff_results),
            "total_added": total_added,
            "total_deleted": total_deleted,
            "total_modified": total_modified,
            "avg_change_rate": round(avg_change_rate * 100, 2),
            "change_rate_trend": change_rates,
            "top_changed_fields": dict(top_changed_fields),
            "data_growth": total_added - total_deleted,
        }

    @staticmethod
    def detect_anomalies(
        diff: DiffResult,
        thresholds: Dict[str, float] = None
    ) -> List[Dict[str, Any]]:
        """
        이상 탐지

        Args:
            diff: diff 결과
            thresholds: 임계값 설정

        Returns:
            이상 항목 목록
        """
        thresholds = thresholds or {
            "change_rate": 0.5,        # 50% 이상 변경
            "deleted_rate": 0.3,       # 30% 이상 삭제
            "single_field_changes": 0.8  # 한 필드가 80% 이상 변경
        }

        anomalies = []

        # 높은 변경률
        if diff.change_rate > thresholds.get("change_rate", 0.5):
            anomalies.append({
                "type": "high_change_rate",
                "severity": "warning",
                "value": diff.change_rate,
                "threshold": thresholds["change_rate"],
                "message": f"High change rate: {diff.change_rate * 100:.1f}%",
            })

        # 많은 삭제
        if diff.source_record_count > 0:
            deleted_rate = diff.deleted_count / diff.source_record_count
            if deleted_rate > thresholds.get("deleted_rate", 0.3):
                anomalies.append({
                    "type": "high_deletion_rate",
                    "severity": "warning",
                    "value": deleted_rate,
                    "threshold": thresholds["deleted_rate"],
                    "message": f"High deletion rate: {deleted_rate * 100:.1f}%",
                })

        # 특정 필드에 집중된 변경
        field_changes = diff.get_modified_fields_summary()
        total_field_changes = sum(field_changes.values())
        if total_field_changes > 0:
            for field, count in field_changes.items():
                ratio = count / total_field_changes
                if ratio > thresholds.get("single_field_changes", 0.8):
                    anomalies.append({
                        "type": "concentrated_field_changes",
                        "severity": "info",
                        "field": field,
                        "value": ratio,
                        "threshold": thresholds["single_field_changes"],
                        "message": f"Field '{field}' accounts for {ratio * 100:.1f}% of changes",
                    })

        return anomalies

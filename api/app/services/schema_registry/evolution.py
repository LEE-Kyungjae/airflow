"""
Schema Evolution - 스키마 진화 및 마이그레이션

기능:
- 스키마 변경 계획 생성
- 마이그레이션 액션 정의
- 데이터 변환 로직 생성
- 롤백 지원
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

from .models import Schema, FieldSchema, FieldType, SchemaVersion

logger = logging.getLogger(__name__)


class EvolutionAction(str, Enum):
    """스키마 진화 액션 타입"""
    ADD_FIELD = "add_field"
    REMOVE_FIELD = "remove_field"
    RENAME_FIELD = "rename_field"
    CHANGE_TYPE = "change_type"
    ADD_CONSTRAINT = "add_constraint"
    REMOVE_CONSTRAINT = "remove_constraint"
    SET_DEFAULT = "set_default"
    SET_NULLABLE = "set_nullable"
    SET_REQUIRED = "set_required"
    MERGE_FIELDS = "merge_fields"
    SPLIT_FIELD = "split_field"


@dataclass
class MigrationStep:
    """마이그레이션 단계"""
    action: EvolutionAction
    field_name: str
    params: Dict[str, Any] = field(default_factory=dict)
    reversible: bool = True
    reverse_action: Optional["MigrationStep"] = None
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action.value,
            "field_name": self.field_name,
            "params": self.params,
            "reversible": self.reversible,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationStep":
        return cls(
            action=EvolutionAction(data["action"]),
            field_name=data["field_name"],
            params=data.get("params", {}),
            reversible=data.get("reversible", True),
            description=data.get("description", ""),
        )


@dataclass
class MigrationPlan:
    """마이그레이션 계획"""
    source_id: str
    from_version: int
    to_version: int
    steps: List[MigrationStep] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    estimated_records: int = 0
    requires_backfill: bool = False
    breaking_changes: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "steps": [s.to_dict() for s in self.steps],
            "created_at": self.created_at.isoformat(),
            "estimated_records": self.estimated_records,
            "requires_backfill": self.requires_backfill,
            "breaking_changes": self.breaking_changes,
            "step_count": len(self.steps),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationPlan":
        return cls(
            source_id=data["source_id"],
            from_version=data["from_version"],
            to_version=data["to_version"],
            steps=[MigrationStep.from_dict(s) for s in data.get("steps", [])],
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data.get("created_at"), str) else data.get("created_at", datetime.utcnow()),
            estimated_records=data.get("estimated_records", 0),
            requires_backfill=data.get("requires_backfill", False),
            breaking_changes=data.get("breaking_changes", False),
        )

    def add_step(self, step: MigrationStep):
        """마이그레이션 스텝 추가"""
        self.steps.append(step)
        if step.action in (EvolutionAction.ADD_FIELD, EvolutionAction.CHANGE_TYPE):
            self.requires_backfill = True
        if step.action in (EvolutionAction.REMOVE_FIELD, EvolutionAction.CHANGE_TYPE):
            self.breaking_changes = True

    def summary(self) -> Dict[str, int]:
        """액션별 요약"""
        summary = {}
        for step in self.steps:
            action = step.action.value
            summary[action] = summary.get(action, 0) + 1
        return summary


@dataclass
class MigrationResult:
    """마이그레이션 실행 결과"""
    success: bool
    total_records: int
    migrated_count: int
    failed_count: int
    skipped_count: int
    errors: List[Dict[str, Any]] = field(default_factory=list)
    duration_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "total_records": self.total_records,
            "migrated_count": self.migrated_count,
            "failed_count": self.failed_count,
            "skipped_count": self.skipped_count,
            "success_rate": round(self.migrated_count / self.total_records * 100, 2) if self.total_records > 0 else 0,
            "error_count": len(self.errors),
            "sample_errors": self.errors[:10],
            "duration_ms": self.duration_ms,
        }


class SchemaEvolution:
    """스키마 진화 관리"""

    # 타입 변환 함수
    TYPE_CONVERTERS: Dict[str, Dict[str, Callable]] = {
        "integer": {
            "string": lambda x: str(x) if x is not None else None,
            "float": lambda x: float(x) if x is not None else None,
            "boolean": lambda x: bool(x) if x is not None else None,
        },
        "float": {
            "string": lambda x: str(x) if x is not None else None,
            "integer": lambda x: int(x) if x is not None else None,
        },
        "string": {
            "integer": lambda x: int(x) if x and str(x).strip().lstrip('-').isdigit() else None,
            "float": lambda x: float(x) if x else None,
            "boolean": lambda x: str(x).lower() in ('true', 'yes', '1') if x else None,
        },
        "boolean": {
            "string": lambda x: str(x).lower() if x is not None else None,
            "integer": lambda x: 1 if x else 0,
        },
        "date": {
            "string": lambda x: x.isoformat() if hasattr(x, 'isoformat') else str(x),
            "datetime": lambda x: x,
        },
        "datetime": {
            "string": lambda x: x.isoformat() if hasattr(x, 'isoformat') else str(x),
            "date": lambda x: x.date() if hasattr(x, 'date') else x,
        },
    }

    def __init__(self, mongo_service=None):
        self.mongo = mongo_service

    def create_migration_plan(
        self,
        source_id: str,
        from_schema: Schema,
        to_schema: Schema,
        from_version: int,
        to_version: int
    ) -> MigrationPlan:
        """
        두 스키마 간의 마이그레이션 계획 생성

        Args:
            source_id: 소스 ID
            from_schema: 이전 스키마
            to_schema: 새 스키마
            from_version: 이전 버전
            to_version: 새 버전

        Returns:
            MigrationPlan
        """
        plan = MigrationPlan(
            source_id=source_id,
            from_version=from_version,
            to_version=to_version,
        )

        old_fields = {f.name: f for f in from_schema.fields}
        new_fields = {f.name: f for f in to_schema.fields}

        old_names = set(old_fields.keys())
        new_names = set(new_fields.keys())

        # 1. 필드 추가
        for name in sorted(new_names - old_names):
            field = new_fields[name]
            step = MigrationStep(
                action=EvolutionAction.ADD_FIELD,
                field_name=name,
                params={
                    "type": field.field_type.value,
                    "required": field.required,
                    "default": field.default,
                    "nullable": field.nullable,
                },
                description=f"Add new field '{name}' with type {field.field_type.value}",
                reverse_action=MigrationStep(
                    action=EvolutionAction.REMOVE_FIELD,
                    field_name=name,
                    description=f"Remove field '{name}'",
                )
            )
            plan.add_step(step)

        # 2. 필드 제거
        for name in sorted(old_names - new_names):
            field = old_fields[name]
            step = MigrationStep(
                action=EvolutionAction.REMOVE_FIELD,
                field_name=name,
                params={"original_field": field.to_dict()},
                description=f"Remove field '{name}'",
                reverse_action=MigrationStep(
                    action=EvolutionAction.ADD_FIELD,
                    field_name=name,
                    params=field.to_dict(),
                    description=f"Restore field '{name}'",
                )
            )
            plan.add_step(step)

        # 3. 필드 변경
        for name in sorted(old_names & new_names):
            old_field = old_fields[name]
            new_field = new_fields[name]

            # 타입 변경
            if old_field.field_type != new_field.field_type:
                step = MigrationStep(
                    action=EvolutionAction.CHANGE_TYPE,
                    field_name=name,
                    params={
                        "from_type": old_field.field_type.value,
                        "to_type": new_field.field_type.value,
                    },
                    description=f"Change type of '{name}' from {old_field.field_type.value} to {new_field.field_type.value}",
                    reverse_action=MigrationStep(
                        action=EvolutionAction.CHANGE_TYPE,
                        field_name=name,
                        params={
                            "from_type": new_field.field_type.value,
                            "to_type": old_field.field_type.value,
                        },
                        description=f"Revert type of '{name}' to {old_field.field_type.value}",
                    )
                )
                plan.add_step(step)

            # 필수 여부 변경
            if old_field.required != new_field.required:
                action = EvolutionAction.SET_REQUIRED if new_field.required else EvolutionAction.SET_NULLABLE
                step = MigrationStep(
                    action=action,
                    field_name=name,
                    params={"default": new_field.default},
                    description=f"Change '{name}' to {'required' if new_field.required else 'optional'}",
                )
                plan.add_step(step)

            # 기본값 변경
            if old_field.default != new_field.default:
                step = MigrationStep(
                    action=EvolutionAction.SET_DEFAULT,
                    field_name=name,
                    params={
                        "old_default": old_field.default,
                        "new_default": new_field.default,
                    },
                    description=f"Change default of '{name}' from {old_field.default} to {new_field.default}",
                )
                plan.add_step(step)

        return plan

    def apply_migration(
        self,
        plan: MigrationPlan,
        record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        단일 레코드에 마이그레이션 적용

        Args:
            plan: 마이그레이션 계획
            record: 원본 레코드

        Returns:
            변환된 레코드
        """
        result = record.copy()

        for step in plan.steps:
            result = self._apply_step(step, result)

        return result

    def _apply_step(
        self,
        step: MigrationStep,
        record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """단일 스텝 적용"""
        result = record.copy()

        if step.action == EvolutionAction.ADD_FIELD:
            if step.field_name not in result:
                result[step.field_name] = step.params.get("default")

        elif step.action == EvolutionAction.REMOVE_FIELD:
            result.pop(step.field_name, None)

        elif step.action == EvolutionAction.CHANGE_TYPE:
            if step.field_name in result:
                from_type = step.params["from_type"]
                to_type = step.params["to_type"]
                converter = self.TYPE_CONVERTERS.get(from_type, {}).get(to_type)

                if converter:
                    try:
                        result[step.field_name] = converter(result[step.field_name])
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"Type conversion failed: {step.field_name} "
                            f"{from_type}->{to_type}: {e}"
                        )
                        result[step.field_name] = None

        elif step.action == EvolutionAction.SET_DEFAULT:
            if step.field_name not in result or result[step.field_name] is None:
                result[step.field_name] = step.params.get("new_default")

        elif step.action == EvolutionAction.SET_REQUIRED:
            if step.field_name not in result or result[step.field_name] is None:
                result[step.field_name] = step.params.get("default")

        elif step.action == EvolutionAction.SET_NULLABLE:
            pass  # Nothing to do

        elif step.action == EvolutionAction.RENAME_FIELD:
            old_name = step.params.get("old_name", step.field_name)
            new_name = step.params.get("new_name")
            if old_name in result and new_name:
                result[new_name] = result.pop(old_name)

        elif step.action == EvolutionAction.MERGE_FIELDS:
            # 여러 필드를 하나로 합치기
            source_fields = step.params.get("source_fields", [])
            separator = step.params.get("separator", " ")
            values = [str(result.get(f, "")) for f in source_fields if result.get(f)]
            result[step.field_name] = separator.join(values) if values else None
            if step.params.get("remove_sources", False):
                for f in source_fields:
                    result.pop(f, None)

        elif step.action == EvolutionAction.SPLIT_FIELD:
            # 필드를 여러 개로 분리
            source_value = result.get(step.field_name, "")
            separator = step.params.get("separator", " ")
            target_fields = step.params.get("target_fields", [])
            if source_value:
                parts = str(source_value).split(separator)
                for i, target in enumerate(target_fields):
                    result[target] = parts[i] if i < len(parts) else None
            if step.params.get("remove_source", False):
                result.pop(step.field_name, None)

        return result

    def batch_migrate(
        self,
        plan: MigrationPlan,
        records: List[Dict[str, Any]],
        on_error: str = "skip"
    ) -> MigrationResult:
        """
        배치 마이그레이션

        Args:
            plan: 마이그레이션 계획
            records: 레코드 목록
            on_error: 에러 처리 방식 ("skip", "fail", "null")

        Returns:
            MigrationResult
        """
        import time
        start_time = time.time()

        migrated = []
        errors = []
        skipped = 0

        for idx, record in enumerate(records):
            try:
                migrated.append(self.apply_migration(plan, record))
            except Exception as e:
                if on_error == "fail":
                    raise
                elif on_error == "skip":
                    errors.append({"index": idx, "error": str(e)})
                    skipped += 1
                    continue
                else:  # null
                    errors.append({"index": idx, "error": str(e)})
                    migrated.append(record)

        duration_ms = int((time.time() - start_time) * 1000)

        return MigrationResult(
            success=len(errors) == 0,
            total_records=len(records),
            migrated_count=len(migrated),
            failed_count=len(errors),
            skipped_count=skipped,
            errors=errors,
            duration_ms=duration_ms,
        )

    def generate_rollback_plan(self, plan: MigrationPlan) -> MigrationPlan:
        """
        롤백 계획 생성

        Args:
            plan: 원본 마이그레이션 계획

        Returns:
            롤백 계획
        """
        rollback = MigrationPlan(
            source_id=plan.source_id,
            from_version=plan.to_version,
            to_version=plan.from_version,
        )

        # 역순으로 reverse_action 적용
        for step in reversed(plan.steps):
            if step.reverse_action:
                rollback.add_step(step.reverse_action)
            elif step.reversible:
                reverse = self._create_reverse_step(step)
                if reverse:
                    rollback.add_step(reverse)

        return rollback

    def _create_reverse_step(self, step: MigrationStep) -> Optional[MigrationStep]:
        """역방향 스텝 생성"""
        if step.action == EvolutionAction.ADD_FIELD:
            return MigrationStep(
                action=EvolutionAction.REMOVE_FIELD,
                field_name=step.field_name,
                description=f"Rollback: Remove added field '{step.field_name}'",
            )
        elif step.action == EvolutionAction.REMOVE_FIELD:
            return MigrationStep(
                action=EvolutionAction.ADD_FIELD,
                field_name=step.field_name,
                params=step.params.get("original_field", {}),
                description=f"Rollback: Restore removed field '{step.field_name}'",
            )
        elif step.action == EvolutionAction.CHANGE_TYPE:
            return MigrationStep(
                action=EvolutionAction.CHANGE_TYPE,
                field_name=step.field_name,
                params={
                    "from_type": step.params["to_type"],
                    "to_type": step.params["from_type"],
                },
                description=f"Rollback: Revert type change of '{step.field_name}'",
            )
        elif step.action == EvolutionAction.SET_DEFAULT:
            return MigrationStep(
                action=EvolutionAction.SET_DEFAULT,
                field_name=step.field_name,
                params={
                    "old_default": step.params.get("new_default"),
                    "new_default": step.params.get("old_default"),
                },
                description=f"Rollback: Revert default of '{step.field_name}'",
            )
        elif step.action == EvolutionAction.RENAME_FIELD:
            return MigrationStep(
                action=EvolutionAction.RENAME_FIELD,
                field_name=step.params.get("new_name", step.field_name),
                params={
                    "old_name": step.params.get("new_name"),
                    "new_name": step.params.get("old_name", step.field_name),
                },
                description=f"Rollback: Rename '{step.params.get('new_name')}' back to '{step.field_name}'",
            )

        return None

    def estimate_migration_impact(
        self,
        plan: MigrationPlan,
        sample_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        마이그레이션 영향도 분석

        Args:
            plan: 마이그레이션 계획
            sample_data: 샘플 데이터

        Returns:
            영향도 분석 결과
        """
        if not sample_data:
            return {"error": "No sample data provided"}

        affected_fields = set()
        null_introductions = {}
        type_conversions = {}
        data_loss_risk = []

        for step in plan.steps:
            affected_fields.add(step.field_name)

            if step.action == EvolutionAction.CHANGE_TYPE:
                field_name = step.field_name
                from_type = step.params["from_type"]
                to_type = step.params["to_type"]

                # 변환 실패율 추정
                failures = 0
                for record in sample_data:
                    if field_name in record and record[field_name] is not None:
                        converter = self.TYPE_CONVERTERS.get(from_type, {}).get(to_type)
                        if converter:
                            try:
                                converter(record[field_name])
                            except Exception:
                                failures += 1
                        else:
                            failures += 1

                type_conversions[field_name] = {
                    "from": from_type,
                    "to": to_type,
                    "estimated_failure_rate": round(failures / len(sample_data), 4) if sample_data else 0,
                    "sample_failures": failures,
                }

                if failures > 0:
                    data_loss_risk.append({
                        "field": field_name,
                        "risk_type": "type_conversion_failure",
                        "affected_records": failures,
                    })

            if step.action == EvolutionAction.REMOVE_FIELD:
                field_name = step.field_name
                non_null = sum(
                    1 for r in sample_data
                    if field_name in r and r[field_name] is not None
                )
                null_introductions[field_name] = {
                    "action": "removed",
                    "data_loss_rate": round(non_null / len(sample_data), 4) if sample_data else 0,
                    "non_null_count": non_null,
                }

                if non_null > 0:
                    data_loss_risk.append({
                        "field": field_name,
                        "risk_type": "field_removal",
                        "affected_records": non_null,
                    })

        return {
            "affected_fields": list(affected_fields),
            "step_count": len(plan.steps),
            "requires_backfill": plan.requires_backfill,
            "breaking_changes": plan.breaking_changes,
            "type_conversions": type_conversions,
            "potential_data_loss": null_introductions,
            "data_loss_risks": data_loss_risk,
            "risk_level": "high" if data_loss_risk else ("medium" if plan.breaking_changes else "low"),
            "sample_size": len(sample_data),
            "action_summary": plan.summary(),
        }

    def validate_plan(self, plan: MigrationPlan, from_schema: Schema, to_schema: Schema) -> List[str]:
        """
        마이그레이션 계획 유효성 검사

        Args:
            plan: 검사할 마이그레이션 계획
            from_schema: 시작 스키마
            to_schema: 목표 스키마

        Returns:
            발견된 문제점 목록
        """
        issues = []

        # 스키마 필드 맵
        from_fields = {f.name: f for f in from_schema.fields}
        to_fields = {f.name: f for f in to_schema.fields}

        # 계획의 각 스텝 검증
        for step in plan.steps:
            if step.action == EvolutionAction.ADD_FIELD:
                if step.field_name not in to_fields:
                    issues.append(f"ADD_FIELD '{step.field_name}' not in target schema")

            elif step.action == EvolutionAction.REMOVE_FIELD:
                if step.field_name not in from_fields:
                    issues.append(f"REMOVE_FIELD '{step.field_name}' not in source schema")

            elif step.action == EvolutionAction.CHANGE_TYPE:
                from_type = step.params.get("from_type")
                to_type = step.params.get("to_type")

                if step.field_name in from_fields:
                    if from_fields[step.field_name].field_type.value != from_type:
                        issues.append(
                            f"CHANGE_TYPE '{step.field_name}': from_type mismatch "
                            f"(plan: {from_type}, actual: {from_fields[step.field_name].field_type.value})"
                        )

                if step.field_name in to_fields:
                    if to_fields[step.field_name].field_type.value != to_type:
                        issues.append(
                            f"CHANGE_TYPE '{step.field_name}': to_type mismatch "
                            f"(plan: {to_type}, actual: {to_fields[step.field_name].field_type.value})"
                        )

                # 변환기 존재 확인
                converter = self.TYPE_CONVERTERS.get(from_type, {}).get(to_type)
                if not converter:
                    issues.append(
                        f"CHANGE_TYPE '{step.field_name}': No converter for {from_type} -> {to_type}"
                    )

        return issues

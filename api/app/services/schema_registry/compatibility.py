"""
Schema Compatibility Checker - 스키마 호환성 검증 로직

BACKWARD, FORWARD, FULL 호환성 검사 구현
"""

import logging
from typing import List, Set, Dict, Any, Optional
from dataclasses import dataclass

from .models import (
    Schema,
    FieldSchema,
    FieldType,
    CompatibilityMode,
    CompatibilityIssue,
    CompatibilityResult,
)

logger = logging.getLogger(__name__)


# 타입 확장(Widening) 규칙: 데이터 손실 없이 변환 가능한 타입 전환
TYPE_WIDENING_RULES: Dict[FieldType, Set[FieldType]] = {
    FieldType.INTEGER: {FieldType.FLOAT, FieldType.STRING, FieldType.ANY},
    FieldType.FLOAT: {FieldType.STRING, FieldType.ANY},
    FieldType.BOOLEAN: {FieldType.STRING, FieldType.INTEGER, FieldType.ANY},
    FieldType.DATE: {FieldType.DATETIME, FieldType.STRING, FieldType.ANY},
    FieldType.DATETIME: {FieldType.STRING, FieldType.ANY},
    FieldType.STRING: {FieldType.ANY},
    FieldType.ARRAY: {FieldType.ANY},
    FieldType.OBJECT: {FieldType.ANY},
}

# 타입 축소(Narrowing) 규칙: 데이터 손실 가능성이 있는 타입 전환
TYPE_NARROWING_RULES: Dict[FieldType, Set[FieldType]] = {
    FieldType.ANY: {FieldType.STRING, FieldType.INTEGER, FieldType.FLOAT, FieldType.BOOLEAN,
                    FieldType.DATE, FieldType.DATETIME, FieldType.ARRAY, FieldType.OBJECT},
    FieldType.STRING: {FieldType.INTEGER, FieldType.FLOAT, FieldType.BOOLEAN,
                       FieldType.DATE, FieldType.DATETIME},
    FieldType.FLOAT: {FieldType.INTEGER},
    FieldType.DATETIME: {FieldType.DATE},
}


class CompatibilityChecker:
    """
    스키마 호환성 검사기

    BACKWARD: 새 스키마가 이전 데이터를 처리할 수 있는지
              - 필수 필드 추가 금지 (기본값 없이)
              - 기존 필드의 타입 축소 금지

    FORWARD: 이전 스키마가 새 데이터를 처리할 수 있는지
             - 필드 삭제 금지
             - 기존 필드의 타입 확장 금지

    FULL: BACKWARD + FORWARD 모두 충족
    """

    def __init__(self, strict_mode: bool = False):
        """
        Args:
            strict_mode: True이면 경고도 에러로 처리
        """
        self.strict_mode = strict_mode

    def check_compatibility(
        self,
        old_schema: Schema,
        new_schema: Schema,
        mode: CompatibilityMode
    ) -> CompatibilityResult:
        """
        두 스키마 간의 호환성 검사

        Args:
            old_schema: 이전(기존) 스키마
            new_schema: 새로운 스키마
            mode: 호환성 검사 모드

        Returns:
            CompatibilityResult: 호환성 검사 결과
        """
        if mode == CompatibilityMode.NONE:
            return CompatibilityResult(is_compatible=True, issues=[], mode=mode)

        issues: List[CompatibilityIssue] = []

        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}

        old_names = set(old_fields.keys())
        new_names = set(new_fields.keys())

        # 1. 추가된 필드 검사
        added_fields = new_names - old_names
        issues.extend(self._check_added_fields(added_fields, new_fields, mode))

        # 2. 제거된 필드 검사
        removed_fields = old_names - new_names
        issues.extend(self._check_removed_fields(removed_fields, old_fields, mode))

        # 3. 공통 필드의 변경 검사
        common_fields = old_names & new_names
        issues.extend(self._check_modified_fields(common_fields, old_fields, new_fields, mode))

        # 호환성 판정
        has_errors = any(issue.is_error() for issue in issues)
        if self.strict_mode:
            has_errors = has_errors or any(issue.is_warning() for issue in issues)

        return CompatibilityResult(
            is_compatible=not has_errors,
            issues=issues,
            mode=mode,
        )

    def _check_added_fields(
        self,
        added_fields: Set[str],
        new_fields: Dict[str, FieldSchema],
        mode: CompatibilityMode
    ) -> List[CompatibilityIssue]:
        """추가된 필드에 대한 호환성 검사"""
        issues = []

        for name in added_fields:
            field = new_fields[name]

            # BACKWARD 호환성: 필수 필드 추가 시 기본값 필요
            if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL,
                       CompatibilityMode.BACKWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                if field.required and field.default is None:
                    issues.append(CompatibilityIssue(
                        field_name=name,
                        issue_type="added_required_field",
                        severity="error",
                        message=f"필수 필드 '{name}' 추가됨 (기본값 없음) - BACKWARD 호환성 위반",
                        new_value=field.to_dict(),
                    ))
                elif field.required and field.default is not None:
                    issues.append(CompatibilityIssue(
                        field_name=name,
                        issue_type="added_required_field_with_default",
                        severity="warning",
                        message=f"필수 필드 '{name}' 추가됨 (기본값: {field.default})",
                        new_value=field.to_dict(),
                    ))
                else:
                    issues.append(CompatibilityIssue(
                        field_name=name,
                        issue_type="added_optional_field",
                        severity="info",
                        message=f"선택 필드 '{name}' 추가됨",
                        new_value=field.to_dict(),
                    ))
            else:
                # FORWARD-only mode
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="added_field",
                    severity="info",
                    message=f"필드 '{name}' 추가됨",
                    new_value=field.to_dict(),
                ))

        return issues

    def _check_removed_fields(
        self,
        removed_fields: Set[str],
        old_fields: Dict[str, FieldSchema],
        mode: CompatibilityMode
    ) -> List[CompatibilityIssue]:
        """제거된 필드에 대한 호환성 검사"""
        issues = []

        for name in removed_fields:
            field = old_fields[name]

            # FORWARD 호환성: 필드 삭제 금지
            if mode in (CompatibilityMode.FORWARD, CompatibilityMode.FULL,
                       CompatibilityMode.FORWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="removed_field",
                    severity="error",
                    message=f"필드 '{name}' 제거됨 - FORWARD 호환성 위반",
                    old_value=field.to_dict(),
                ))
            else:
                # BACKWARD-only mode
                severity = "warning" if field.required else "info"
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="removed_field",
                    severity=severity,
                    message=f"필드 '{name}' 제거됨",
                    old_value=field.to_dict(),
                ))

        return issues

    def _check_modified_fields(
        self,
        common_fields: Set[str],
        old_fields: Dict[str, FieldSchema],
        new_fields: Dict[str, FieldSchema],
        mode: CompatibilityMode
    ) -> List[CompatibilityIssue]:
        """공통 필드의 변경 사항 검사"""
        issues = []

        for name in common_fields:
            old_field = old_fields[name]
            new_field = new_fields[name]

            # 타입 변경 검사
            issues.extend(self._check_type_change(old_field, new_field, mode))

            # 필수/선택 변경 검사
            issues.extend(self._check_required_change(old_field, new_field, mode))

            # 제약 조건 변경 검사
            issues.extend(self._check_constraint_change(old_field, new_field, mode))

            # nullable 변경 검사
            issues.extend(self._check_nullable_change(old_field, new_field, mode))

        return issues

    def _check_type_change(
        self,
        old_field: FieldSchema,
        new_field: FieldSchema,
        mode: CompatibilityMode
    ) -> List[CompatibilityIssue]:
        """필드 타입 변경 검사"""
        issues = []

        if old_field.field_type == new_field.field_type:
            return issues

        name = old_field.name
        old_type = old_field.field_type
        new_type = new_field.field_type

        # 타입 확장 (widening) 여부 확인
        is_widening = new_type in TYPE_WIDENING_RULES.get(old_type, set())
        # 타입 축소 (narrowing) 여부 확인
        is_narrowing = new_type in TYPE_NARROWING_RULES.get(old_type, set())

        if is_widening:
            # 타입 확장: BACKWARD 호환 OK, FORWARD 호환 위반
            if mode in (CompatibilityMode.FORWARD, CompatibilityMode.FULL,
                       CompatibilityMode.FORWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="type_widened",
                    severity="error",
                    message=f"타입 확장 '{old_type.value}' -> '{new_type.value}' - FORWARD 호환성 위반",
                    old_value=old_type.value,
                    new_value=new_type.value,
                ))
            else:
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="type_widened",
                    severity="info",
                    message=f"타입 확장 '{old_type.value}' -> '{new_type.value}'",
                    old_value=old_type.value,
                    new_value=new_type.value,
                ))
        elif is_narrowing:
            # 타입 축소: BACKWARD 호환 위반, FORWARD 호환 OK
            if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL,
                       CompatibilityMode.BACKWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="type_narrowed",
                    severity="error",
                    message=f"타입 축소 '{old_type.value}' -> '{new_type.value}' - BACKWARD 호환성 위반",
                    old_value=old_type.value,
                    new_value=new_type.value,
                ))
            else:
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="type_narrowed",
                    severity="warning",
                    message=f"타입 축소 '{old_type.value}' -> '{new_type.value}'",
                    old_value=old_type.value,
                    new_value=new_type.value,
                ))
        else:
            # 호환되지 않는 타입 변경
            issues.append(CompatibilityIssue(
                field_name=name,
                issue_type="type_incompatible",
                severity="error",
                message=f"호환되지 않는 타입 변경 '{old_type.value}' -> '{new_type.value}'",
                old_value=old_type.value,
                new_value=new_type.value,
            ))

        return issues

    def _check_required_change(
        self,
        old_field: FieldSchema,
        new_field: FieldSchema,
        mode: CompatibilityMode
    ) -> List[CompatibilityIssue]:
        """필수/선택 변경 검사"""
        issues = []
        name = old_field.name

        if old_field.required == new_field.required:
            return issues

        if not old_field.required and new_field.required:
            # 선택 -> 필수: BACKWARD 호환성 위반 가능
            if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL,
                       CompatibilityMode.BACKWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                if new_field.default is None:
                    issues.append(CompatibilityIssue(
                        field_name=name,
                        issue_type="optional_to_required",
                        severity="error",
                        message=f"선택 -> 필수 변경 (기본값 없음) - BACKWARD 호환성 위반",
                    ))
                else:
                    issues.append(CompatibilityIssue(
                        field_name=name,
                        issue_type="optional_to_required",
                        severity="warning",
                        message=f"선택 -> 필수 변경 (기본값: {new_field.default})",
                    ))
            else:
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="optional_to_required",
                    severity="info",
                    message="선택 -> 필수 변경",
                ))

        elif old_field.required and not new_field.required:
            # 필수 -> 선택: FORWARD 호환성 위반
            if mode in (CompatibilityMode.FORWARD, CompatibilityMode.FULL,
                       CompatibilityMode.FORWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="required_to_optional",
                    severity="error",
                    message="필수 -> 선택 변경 - FORWARD 호환성 위반",
                ))
            else:
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="required_to_optional",
                    severity="info",
                    message="필수 -> 선택 변경",
                ))

        return issues

    def _check_nullable_change(
        self,
        old_field: FieldSchema,
        new_field: FieldSchema,
        mode: CompatibilityMode
    ) -> List[CompatibilityIssue]:
        """nullable 변경 검사"""
        issues = []
        name = old_field.name

        if old_field.nullable == new_field.nullable:
            return issues

        if old_field.nullable and not new_field.nullable:
            # nullable -> not nullable: BACKWARD 호환성 위반
            if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL,
                       CompatibilityMode.BACKWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="nullable_removed",
                    severity="error",
                    message="nullable 제거 - BACKWARD 호환성 위반",
                ))
            else:
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="nullable_removed",
                    severity="warning",
                    message="nullable 제거",
                ))
        else:
            # not nullable -> nullable: 일반적으로 안전
            issues.append(CompatibilityIssue(
                field_name=name,
                issue_type="nullable_added",
                severity="info",
                message="nullable 추가",
            ))

        return issues

    def _check_constraint_change(
        self,
        old_field: FieldSchema,
        new_field: FieldSchema,
        mode: CompatibilityMode
    ) -> List[CompatibilityIssue]:
        """제약 조건 변경 검사"""
        issues = []
        name = old_field.name

        # min_value 제약 강화
        if old_field.min_value is not None and new_field.min_value is not None:
            if new_field.min_value > old_field.min_value:
                severity = "warning"
                if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL,
                           CompatibilityMode.BACKWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                    severity = "error"
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="min_value_increased",
                    severity=severity,
                    message=f"최소값 강화: {old_field.min_value} -> {new_field.min_value}",
                    old_value=old_field.min_value,
                    new_value=new_field.min_value,
                ))

        # max_value 제약 강화
        if old_field.max_value is not None and new_field.max_value is not None:
            if new_field.max_value < old_field.max_value:
                severity = "warning"
                if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL,
                           CompatibilityMode.BACKWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                    severity = "error"
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="max_value_decreased",
                    severity=severity,
                    message=f"최대값 강화: {old_field.max_value} -> {new_field.max_value}",
                    old_value=old_field.max_value,
                    new_value=new_field.max_value,
                ))

        # min_length 제약 강화
        if old_field.min_length is not None and new_field.min_length is not None:
            if new_field.min_length > old_field.min_length:
                severity = "warning"
                if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL,
                           CompatibilityMode.BACKWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                    severity = "error"
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="min_length_increased",
                    severity=severity,
                    message=f"최소 길이 강화: {old_field.min_length} -> {new_field.min_length}",
                    old_value=old_field.min_length,
                    new_value=new_field.min_length,
                ))

        # max_length 제약 강화
        if old_field.max_length is not None and new_field.max_length is not None:
            if new_field.max_length < old_field.max_length:
                severity = "warning"
                if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL,
                           CompatibilityMode.BACKWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                    severity = "error"
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="max_length_decreased",
                    severity=severity,
                    message=f"최대 길이 강화: {old_field.max_length} -> {new_field.max_length}",
                    old_value=old_field.max_length,
                    new_value=new_field.max_length,
                ))

        # enum 값 변경
        if old_field.enum_values and new_field.enum_values:
            old_enums = set(old_field.enum_values)
            new_enums = set(new_field.enum_values)

            removed_enums = old_enums - new_enums
            added_enums = new_enums - old_enums

            if removed_enums:
                severity = "warning"
                if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL,
                           CompatibilityMode.BACKWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                    severity = "error"
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="enum_values_removed",
                    severity=severity,
                    message=f"Enum 값 제거: {removed_enums}",
                    old_value=list(old_enums),
                    new_value=list(new_enums),
                ))

            if added_enums:
                severity = "info"
                if mode in (CompatibilityMode.FORWARD, CompatibilityMode.FULL,
                           CompatibilityMode.FORWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                    severity = "warning"
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="enum_values_added",
                    severity=severity,
                    message=f"Enum 값 추가: {added_enums}",
                    old_value=list(old_enums),
                    new_value=list(new_enums),
                ))

        # pattern 변경
        if old_field.pattern != new_field.pattern:
            if old_field.pattern and new_field.pattern:
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="pattern_changed",
                    severity="warning",
                    message=f"패턴 변경: '{old_field.pattern}' -> '{new_field.pattern}'",
                    old_value=old_field.pattern,
                    new_value=new_field.pattern,
                ))
            elif new_field.pattern and not old_field.pattern:
                severity = "warning"
                if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL,
                           CompatibilityMode.BACKWARD_TRANSITIVE, CompatibilityMode.FULL_TRANSITIVE):
                    severity = "error"
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="pattern_added",
                    severity=severity,
                    message=f"패턴 추가: '{new_field.pattern}'",
                    new_value=new_field.pattern,
                ))
            elif old_field.pattern and not new_field.pattern:
                issues.append(CompatibilityIssue(
                    field_name=name,
                    issue_type="pattern_removed",
                    severity="info",
                    message=f"패턴 제거: '{old_field.pattern}'",
                    old_value=old_field.pattern,
                ))

        return issues

    def is_type_compatible(
        self,
        from_type: FieldType,
        to_type: FieldType,
        mode: CompatibilityMode
    ) -> bool:
        """
        두 타입 간의 호환성 확인

        Args:
            from_type: 원본 타입
            to_type: 대상 타입
            mode: 호환성 모드

        Returns:
            bool: 호환 여부
        """
        if from_type == to_type:
            return True

        if mode == CompatibilityMode.NONE:
            return True

        is_widening = to_type in TYPE_WIDENING_RULES.get(from_type, set())
        is_narrowing = to_type in TYPE_NARROWING_RULES.get(from_type, set())

        if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.BACKWARD_TRANSITIVE):
            # BACKWARD: 확장만 허용
            return is_widening

        if mode in (CompatibilityMode.FORWARD, CompatibilityMode.FORWARD_TRANSITIVE):
            # FORWARD: 축소만 허용
            return is_narrowing

        # FULL: 동일 타입만 허용
        return False


def check_compatibility(
    old_schema: Schema,
    new_schema: Schema,
    mode: CompatibilityMode = CompatibilityMode.BACKWARD
) -> CompatibilityResult:
    """
    스키마 호환성 검사 편의 함수

    Args:
        old_schema: 이전 스키마
        new_schema: 새 스키마
        mode: 호환성 모드 (기본: BACKWARD)

    Returns:
        CompatibilityResult
    """
    checker = CompatibilityChecker()
    return checker.check_compatibility(old_schema, new_schema, mode)

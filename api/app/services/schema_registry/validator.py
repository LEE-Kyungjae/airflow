"""
Schema Validator - 데이터 스키마 검증

데이터가 정의된 스키마를 준수하는지 검증
"""

import re
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Set, Union
from dataclasses import dataclass, field

from .models import (
    Schema,
    FieldSchema,
    FieldType,
    DataCategory,
)

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """검증 오류"""
    field_name: str
    error_type: str
    message: str
    expected: Any = None
    actual: Any = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "field": self.field_name,
            "error_type": self.error_type,
            "message": self.message,
        }
        if self.expected is not None:
            result["expected"] = str(self.expected)
        if self.actual is not None:
            result["actual"] = str(self.actual)[:100]
        return result


@dataclass
class ValidationResult:
    """검증 결과"""
    is_valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    validated_at: datetime = field(default_factory=datetime.utcnow)
    # 검증 통계
    total_fields: int = 0
    validated_fields: int = 0
    missing_fields: int = 0
    extra_fields: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "errors": [e.to_dict() for e in self.errors],
            "warnings": self.warnings,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "validated_at": self.validated_at.isoformat(),
            "statistics": {
                "total_fields": self.total_fields,
                "validated_fields": self.validated_fields,
                "missing_fields": self.missing_fields,
                "extra_fields": self.extra_fields,
            }
        }


@dataclass
class BatchValidationResult:
    """배치 검증 결과"""
    total_records: int
    valid_records: int
    invalid_records: int
    results: List[ValidationResult] = field(default_factory=list)
    validation_errors_by_field: Dict[str, int] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.valid_records / self.total_records) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "success_rate": round(self.success_rate, 2),
            "errors_by_field": self.validation_errors_by_field,
            "sample_errors": [r.to_dict() for r in self.results[:10] if not r.is_valid],
        }


class SchemaValidator:
    """
    스키마 기반 데이터 검증기

    데이터가 정의된 스키마를 준수하는지 검증하고,
    타입 변환, 제약 조건 확인 등을 수행
    """

    def __init__(
        self,
        strict_mode: bool = False,
        allow_extra_fields: bool = True,
        coerce_types: bool = True
    ):
        """
        Args:
            strict_mode: True이면 모든 경고를 에러로 처리
            allow_extra_fields: False이면 스키마에 없는 필드가 있으면 에러
            coerce_types: True이면 타입 변환 시도
        """
        self.strict_mode = strict_mode
        self.allow_extra_fields = allow_extra_fields
        self.coerce_types = coerce_types

    def validate(
        self,
        data: Dict[str, Any],
        schema: Schema,
        partial: bool = False
    ) -> ValidationResult:
        """
        단일 데이터 레코드를 스키마에 대해 검증

        Args:
            data: 검증할 데이터
            schema: 검증 기준 스키마
            partial: True이면 필수 필드 검사 스킵 (부분 업데이트용)

        Returns:
            ValidationResult
        """
        errors: List[ValidationError] = []
        warnings: List[str] = []

        schema_fields = {f.name: f for f in schema.fields}
        data_fields = set(data.keys())
        schema_field_names = set(schema_fields.keys())

        # 통계 초기화
        total_fields = len(schema_fields)
        validated_fields = 0
        missing_fields = 0
        extra_fields = 0

        # 1. 필수 필드 존재 확인
        if not partial:
            for field_name, field_schema in schema_fields.items():
                if field_schema.required:
                    if field_name not in data or data[field_name] is None:
                        if field_schema.default is None:
                            errors.append(ValidationError(
                                field_name=field_name,
                                error_type="missing_required",
                                message=f"필수 필드 '{field_name}'이(가) 없습니다",
                                expected="non-null value",
                                actual=data.get(field_name),
                            ))
                            missing_fields += 1

        # 2. 스키마에 없는 필드 확인
        unknown_fields = data_fields - schema_field_names - {'_id', '_created_at', '_updated_at'}
        # 메타 필드 (_로 시작하는 필드) 제외
        unknown_fields = {f for f in unknown_fields if not f.startswith('_')}

        if unknown_fields:
            extra_fields = len(unknown_fields)
            if not self.allow_extra_fields:
                for field_name in unknown_fields:
                    errors.append(ValidationError(
                        field_name=field_name,
                        error_type="unknown_field",
                        message=f"스키마에 정의되지 않은 필드 '{field_name}'",
                    ))
            else:
                warnings.append(f"스키마에 없는 필드: {', '.join(sorted(unknown_fields))}")

        # 3. 각 필드 검증
        for field_name, value in data.items():
            if field_name not in schema_fields:
                continue

            field_schema = schema_fields[field_name]
            field_errors = self._validate_field(field_name, value, field_schema)
            errors.extend(field_errors)

            if not field_errors:
                validated_fields += 1

        # 4. 결과 생성
        is_valid = len(errors) == 0
        if self.strict_mode and warnings:
            is_valid = False

        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            total_fields=total_fields,
            validated_fields=validated_fields,
            missing_fields=missing_fields,
            extra_fields=extra_fields,
        )

    def validate_batch(
        self,
        records: List[Dict[str, Any]],
        schema: Schema,
        fail_fast: bool = False
    ) -> BatchValidationResult:
        """
        다수의 레코드를 검증

        Args:
            records: 검증할 레코드 목록
            schema: 검증 기준 스키마
            fail_fast: True이면 첫 에러에서 중단

        Returns:
            BatchValidationResult
        """
        results: List[ValidationResult] = []
        valid_count = 0
        invalid_count = 0
        errors_by_field: Dict[str, int] = {}

        for idx, record in enumerate(records):
            result = self.validate(record, schema)
            results.append(result)

            if result.is_valid:
                valid_count += 1
            else:
                invalid_count += 1
                for error in result.errors:
                    errors_by_field[error.field_name] = errors_by_field.get(error.field_name, 0) + 1

                if fail_fast:
                    break

        return BatchValidationResult(
            total_records=len(records),
            valid_records=valid_count,
            invalid_records=invalid_count,
            results=results,
            validation_errors_by_field=errors_by_field,
        )

    def _validate_field(
        self,
        field_name: str,
        value: Any,
        field_schema: FieldSchema
    ) -> List[ValidationError]:
        """개별 필드 검증"""
        errors: List[ValidationError] = []

        # null 값 처리
        if value is None:
            if not field_schema.nullable and field_schema.required:
                errors.append(ValidationError(
                    field_name=field_name,
                    error_type="null_not_allowed",
                    message=f"필드 '{field_name}'은(는) null일 수 없습니다",
                ))
            return errors

        # 타입 검증
        type_error = self._validate_type(field_name, value, field_schema)
        if type_error:
            errors.append(type_error)
            return errors  # 타입 에러 시 추가 검증 스킵

        # 제약 조건 검증
        constraint_errors = self._validate_constraints(field_name, value, field_schema)
        errors.extend(constraint_errors)

        return errors

    def _validate_type(
        self,
        field_name: str,
        value: Any,
        field_schema: FieldSchema
    ) -> Optional[ValidationError]:
        """타입 검증"""
        expected_type = field_schema.field_type
        actual_type = self._get_value_type(value)

        if expected_type == FieldType.ANY:
            return None

        # 타입 일치 확인
        if actual_type == expected_type:
            return None

        # 타입 변환 시도 (coerce_types가 True인 경우)
        if self.coerce_types:
            if self._can_coerce(value, expected_type):
                return None

        return ValidationError(
            field_name=field_name,
            error_type="type_mismatch",
            message=f"타입 불일치: '{expected_type.value}' 예상, '{actual_type.value if actual_type else type(value).__name__}' 실제",
            expected=expected_type.value,
            actual=type(value).__name__,
        )

    def _get_value_type(self, value: Any) -> Optional[FieldType]:
        """값의 FieldType 추론"""
        if value is None:
            return None
        if isinstance(value, bool):
            return FieldType.BOOLEAN
        if isinstance(value, int):
            return FieldType.INTEGER
        if isinstance(value, float):
            return FieldType.FLOAT
        if isinstance(value, str):
            return FieldType.STRING
        if isinstance(value, list):
            return FieldType.ARRAY
        if isinstance(value, dict):
            return FieldType.OBJECT
        if isinstance(value, datetime):
            return FieldType.DATETIME
        if isinstance(value, date):
            return FieldType.DATE
        return None

    def _can_coerce(self, value: Any, target_type: FieldType) -> bool:
        """타입 변환 가능 여부 확인"""
        try:
            if target_type == FieldType.STRING:
                str(value)
                return True
            elif target_type == FieldType.INTEGER:
                if isinstance(value, str):
                    int(value)
                    return True
                elif isinstance(value, float):
                    return value == int(value)
            elif target_type == FieldType.FLOAT:
                if isinstance(value, (str, int)):
                    float(value)
                    return True
            elif target_type == FieldType.BOOLEAN:
                if isinstance(value, str):
                    return value.lower() in ('true', 'false', 'yes', 'no', '1', '0')
                elif isinstance(value, int):
                    return value in (0, 1)
            elif target_type == FieldType.DATETIME:
                if isinstance(value, str):
                    datetime.fromisoformat(value.replace('Z', '+00:00'))
                    return True
                elif isinstance(value, date):
                    return True
            elif target_type == FieldType.DATE:
                if isinstance(value, str):
                    datetime.fromisoformat(value.split('T')[0])
                    return True
                elif isinstance(value, datetime):
                    return True
        except (ValueError, TypeError):
            pass
        return False

    def _validate_constraints(
        self,
        field_name: str,
        value: Any,
        field_schema: FieldSchema
    ) -> List[ValidationError]:
        """제약 조건 검증"""
        errors: List[ValidationError] = []

        # 숫자 범위 검증
        if isinstance(value, (int, float)):
            if field_schema.min_value is not None and value < field_schema.min_value:
                errors.append(ValidationError(
                    field_name=field_name,
                    error_type="min_value_violation",
                    message=f"값이 최소값({field_schema.min_value})보다 작습니다",
                    expected=f">= {field_schema.min_value}",
                    actual=value,
                ))

            if field_schema.max_value is not None and value > field_schema.max_value:
                errors.append(ValidationError(
                    field_name=field_name,
                    error_type="max_value_violation",
                    message=f"값이 최대값({field_schema.max_value})보다 큽니다",
                    expected=f"<= {field_schema.max_value}",
                    actual=value,
                ))

        # 문자열 길이 검증
        if isinstance(value, str):
            if field_schema.min_length is not None and len(value) < field_schema.min_length:
                errors.append(ValidationError(
                    field_name=field_name,
                    error_type="min_length_violation",
                    message=f"문자열 길이가 최소({field_schema.min_length})보다 짧습니다",
                    expected=f">= {field_schema.min_length} chars",
                    actual=len(value),
                ))

            if field_schema.max_length is not None and len(value) > field_schema.max_length:
                errors.append(ValidationError(
                    field_name=field_name,
                    error_type="max_length_violation",
                    message=f"문자열 길이가 최대({field_schema.max_length})를 초과합니다",
                    expected=f"<= {field_schema.max_length} chars",
                    actual=len(value),
                ))

            # 패턴 검증
            if field_schema.pattern:
                if not re.match(field_schema.pattern, value):
                    errors.append(ValidationError(
                        field_name=field_name,
                        error_type="pattern_violation",
                        message=f"값이 패턴 '{field_schema.pattern}'과 일치하지 않습니다",
                        expected=field_schema.pattern,
                        actual=value[:50],
                    ))

        # Enum 검증
        if field_schema.enum_values:
            if value not in field_schema.enum_values:
                errors.append(ValidationError(
                    field_name=field_name,
                    error_type="enum_violation",
                    message=f"값이 허용된 목록에 없습니다",
                    expected=field_schema.enum_values,
                    actual=value,
                ))

        # 배열 검증
        if isinstance(value, list) and field_schema.nested_schema:
            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    nested_validator = SchemaValidator(
                        strict_mode=self.strict_mode,
                        allow_extra_fields=self.allow_extra_fields,
                        coerce_types=self.coerce_types,
                    )
                    nested_result = nested_validator.validate(item, field_schema.nested_schema)
                    for error in nested_result.errors:
                        errors.append(ValidationError(
                            field_name=f"{field_name}[{idx}].{error.field_name}",
                            error_type=error.error_type,
                            message=error.message,
                            expected=error.expected,
                            actual=error.actual,
                        ))

        # 객체 검증
        if isinstance(value, dict) and field_schema.nested_schema:
            nested_validator = SchemaValidator(
                strict_mode=self.strict_mode,
                allow_extra_fields=self.allow_extra_fields,
                coerce_types=self.coerce_types,
            )
            nested_result = nested_validator.validate(value, field_schema.nested_schema)
            for error in nested_result.errors:
                errors.append(ValidationError(
                    field_name=f"{field_name}.{error.field_name}",
                    error_type=error.error_type,
                    message=error.message,
                    expected=error.expected,
                    actual=error.actual,
                ))

        return errors


def validate_data(
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    schema: Schema,
    strict: bool = False
) -> Union[ValidationResult, BatchValidationResult]:
    """
    데이터 검증 편의 함수

    Args:
        data: 검증할 데이터 (단일 또는 목록)
        schema: 검증 기준 스키마
        strict: 엄격 모드 여부

    Returns:
        ValidationResult 또는 BatchValidationResult
    """
    validator = SchemaValidator(strict_mode=strict)

    if isinstance(data, list):
        return validator.validate_batch(data, schema)
    else:
        return validator.validate(data, schema)


def validate_by_category(
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    category: DataCategory,
    strict: bool = False
) -> Union[ValidationResult, BatchValidationResult]:
    """
    데이터 카테고리 기반 검증 편의 함수

    Args:
        data: 검증할 데이터
        category: 데이터 카테고리
        strict: 엄격 모드 여부

    Returns:
        ValidationResult 또는 BatchValidationResult
    """
    from .models import get_default_schema

    schema = get_default_schema(category)
    return validate_data(data, schema, strict)

"""
Data Expectations - Great Expectations 스타일의 기대치 정의

각 Expectation은 데이터에 대한 기대치를 정의하고 검증합니다:
- expect_column_not_null: NULL 값 불허
- expect_column_unique: 고유값 검증
- expect_column_values_in_range: 숫자 범위 검증
- expect_column_values_to_match_regex: 정규식 패턴 검증
- expect_table_row_count_between: 행 개수 범위 검증
- expect_column_values_to_be_of_type: 데이터 타입 검증
"""

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union, Type
from dataclasses import dataclass, field
from enum import Enum


class ExpectationResult(str, Enum):
    """기대치 검증 결과"""
    SUCCESS = "success"
    FAILURE = "failure"
    WARNING = "warning"
    SKIPPED = "skipped"


class ExpectationSeverity(str, Enum):
    """기대치 실패 심각도"""
    INFO = "info"           # 정보 (무시 가능)
    WARNING = "warning"     # 경고 (데이터 사용 가능)
    ERROR = "error"         # 오류 (검토 필요)
    CRITICAL = "critical"   # 심각 (데이터 사용 불가)


@dataclass
class ExpectationValidationResult:
    """개별 기대치 검증 결과"""
    expectation_type: str
    success: bool
    result: ExpectationResult
    severity: ExpectationSeverity
    column: Optional[str]
    details: Dict[str, Any] = field(default_factory=dict)
    exception_info: Optional[str] = None

    # 통계 정보
    element_count: int = 0
    unexpected_count: int = 0
    unexpected_percent: float = 0.0
    unexpected_values: List[Any] = field(default_factory=list)
    unexpected_index_list: List[int] = field(default_factory=list)

    @property
    def unexpected_percent_display(self) -> str:
        return f"{self.unexpected_percent:.2f}%"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "expectation_type": self.expectation_type,
            "success": self.success,
            "result": self.result.value,
            "severity": self.severity.value,
            "column": self.column,
            "details": self.details,
            "exception_info": self.exception_info,
            "statistics": {
                "element_count": self.element_count,
                "unexpected_count": self.unexpected_count,
                "unexpected_percent": self.unexpected_percent,
            },
            "unexpected_values": self.unexpected_values[:20],  # 최대 20개
            "unexpected_index_list": self.unexpected_index_list[:100],  # 최대 100개
        }


class Expectation(ABC):
    """기대치 기본 클래스"""

    expectation_type: str = "base_expectation"

    def __init__(
        self,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR,
        result_format: str = "COMPLETE",
        include_config: bool = True,
        catch_exceptions: bool = True,
        meta: Optional[Dict[str, Any]] = None
    ):
        self.severity = severity
        self.result_format = result_format
        self.include_config = include_config
        self.catch_exceptions = catch_exceptions
        self.meta = meta or {}

    @abstractmethod
    def validate(
        self,
        data: List[Dict[str, Any]],
        column: Optional[str] = None
    ) -> ExpectationValidationResult:
        """
        데이터에 대해 기대치 검증 수행

        Args:
            data: 검증할 데이터 목록
            column: 검증할 컬럼명 (테이블 레벨 기대치의 경우 None)

        Returns:
            ExpectationValidationResult
        """
        pass

    def _create_result(
        self,
        success: bool,
        column: Optional[str],
        element_count: int = 0,
        unexpected_count: int = 0,
        unexpected_values: List[Any] = None,
        unexpected_index_list: List[int] = None,
        details: Dict[str, Any] = None,
        exception_info: str = None
    ) -> ExpectationValidationResult:
        """결과 객체 생성 헬퍼"""
        unexpected_values = unexpected_values or []
        unexpected_index_list = unexpected_index_list or []

        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        result = ExpectationResult.SUCCESS if success else ExpectationResult.FAILURE
        if exception_info:
            result = ExpectationResult.SKIPPED

        return ExpectationValidationResult(
            expectation_type=self.expectation_type,
            success=success,
            result=result,
            severity=self.severity if not success else ExpectationSeverity.INFO,
            column=column,
            details=details or {},
            exception_info=exception_info,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_percent=unexpected_percent,
            unexpected_values=unexpected_values,
            unexpected_index_list=unexpected_index_list,
        )


class ExpectColumnNotNull(Expectation):
    """
    컬럼 값이 NULL이 아님을 기대

    Example:
        expect_column_not_null("user_id")
    """

    expectation_type = "expect_column_not_null"

    def __init__(
        self,
        column: str,
        mostly: float = 1.0,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR,
        **kwargs
    ):
        super().__init__(severity=severity, **kwargs)
        self.column = column
        self.mostly = mostly  # 허용 비율 (0.0 ~ 1.0)

    def validate(
        self,
        data: List[Dict[str, Any]],
        column: Optional[str] = None
    ) -> ExpectationValidationResult:
        target_column = column or self.column

        if not data:
            return self._create_result(True, target_column, element_count=0)

        unexpected_values = []
        unexpected_indices = []

        for idx, record in enumerate(data):
            value = record.get(target_column)
            if value is None:
                unexpected_values.append(None)
                unexpected_indices.append(idx)

        element_count = len(data)
        unexpected_count = len(unexpected_values)
        success_ratio = 1 - (unexpected_count / element_count) if element_count > 0 else 1.0
        success = success_ratio >= self.mostly

        return self._create_result(
            success=success,
            column=target_column,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_values=unexpected_values,
            unexpected_index_list=unexpected_indices,
            details={
                "mostly": self.mostly,
                "success_ratio": round(success_ratio, 4),
            }
        )


class ExpectColumnUnique(Expectation):
    """
    컬럼 값이 고유함을 기대

    Example:
        expect_column_unique("email")
    """

    expectation_type = "expect_column_unique"

    def __init__(
        self,
        column: str,
        mostly: float = 1.0,
        severity: ExpectationSeverity = ExpectationSeverity.WARNING,
        **kwargs
    ):
        super().__init__(severity=severity, **kwargs)
        self.column = column
        self.mostly = mostly

    def validate(
        self,
        data: List[Dict[str, Any]],
        column: Optional[str] = None
    ) -> ExpectationValidationResult:
        target_column = column or self.column

        if not data:
            return self._create_result(True, target_column, element_count=0)

        seen_values: Dict[Any, List[int]] = {}
        unexpected_values = []
        unexpected_indices = []

        for idx, record in enumerate(data):
            value = record.get(target_column)
            if value is None:
                continue

            # 값의 해시 가능 여부 확인
            try:
                hashable_value = str(value) if isinstance(value, (dict, list)) else value
            except Exception:
                hashable_value = str(value)

            if hashable_value in seen_values:
                # 중복 발견
                if len(seen_values[hashable_value]) == 1:
                    # 첫 번째 중복은 원본도 unexpected에 추가
                    first_idx = seen_values[hashable_value][0]
                    unexpected_values.append(value)
                    unexpected_indices.append(first_idx)

                unexpected_values.append(value)
                unexpected_indices.append(idx)
                seen_values[hashable_value].append(idx)
            else:
                seen_values[hashable_value] = [idx]

        element_count = len(data)
        unique_count = len(seen_values)
        unexpected_count = element_count - unique_count
        success_ratio = unique_count / element_count if element_count > 0 else 1.0
        success = success_ratio >= self.mostly

        return self._create_result(
            success=success,
            column=target_column,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_values=unexpected_values,
            unexpected_index_list=unexpected_indices,
            details={
                "mostly": self.mostly,
                "unique_count": unique_count,
                "duplicate_count": unexpected_count,
            }
        )


class ExpectColumnValuesInRange(Expectation):
    """
    컬럼 값이 지정된 범위 내에 있음을 기대

    Example:
        expect_column_values_in_range("price", min_value=0, max_value=1000000)
    """

    expectation_type = "expect_column_values_in_range"

    def __init__(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        strict_min: bool = False,
        strict_max: bool = False,
        mostly: float = 1.0,
        allow_null: bool = True,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR,
        **kwargs
    ):
        super().__init__(severity=severity, **kwargs)
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.strict_min = strict_min  # True: > min_value, False: >= min_value
        self.strict_max = strict_max  # True: < max_value, False: <= max_value
        self.mostly = mostly
        self.allow_null = allow_null

    def _parse_number(self, value: Any) -> Optional[float]:
        """숫자로 변환 시도"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # 쉼표, 통화 기호 제거
            cleaned = re.sub(r'[,\s$%원\u20a9]', '', value)
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    def _check_in_range(self, value: float) -> bool:
        """범위 내에 있는지 확인"""
        if self.min_value is not None:
            if self.strict_min:
                if value <= self.min_value:
                    return False
            else:
                if value < self.min_value:
                    return False

        if self.max_value is not None:
            if self.strict_max:
                if value >= self.max_value:
                    return False
            else:
                if value > self.max_value:
                    return False

        return True

    def validate(
        self,
        data: List[Dict[str, Any]],
        column: Optional[str] = None
    ) -> ExpectationValidationResult:
        target_column = column or self.column

        if not data:
            return self._create_result(True, target_column, element_count=0)

        unexpected_values = []
        unexpected_indices = []
        non_null_count = 0

        for idx, record in enumerate(data):
            raw_value = record.get(target_column)

            if raw_value is None:
                if not self.allow_null:
                    unexpected_values.append(raw_value)
                    unexpected_indices.append(idx)
                continue

            non_null_count += 1
            value = self._parse_number(raw_value)

            if value is None:
                # 숫자로 변환 불가
                unexpected_values.append(raw_value)
                unexpected_indices.append(idx)
                continue

            if not self._check_in_range(value):
                unexpected_values.append(raw_value)
                unexpected_indices.append(idx)

        element_count = non_null_count if self.allow_null else len(data)
        unexpected_count = len(unexpected_values)
        success_ratio = 1 - (unexpected_count / element_count) if element_count > 0 else 1.0
        success = success_ratio >= self.mostly

        return self._create_result(
            success=success,
            column=target_column,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_values=unexpected_values,
            unexpected_index_list=unexpected_indices,
            details={
                "min_value": self.min_value,
                "max_value": self.max_value,
                "strict_min": self.strict_min,
                "strict_max": self.strict_max,
                "mostly": self.mostly,
            }
        )


class ExpectColumnValuesToMatchRegex(Expectation):
    """
    컬럼 값이 정규식 패턴과 일치함을 기대

    Example:
        expect_column_values_to_match_regex("email", r"^[\w.-]+@[\w.-]+\.\w+$")
    """

    expectation_type = "expect_column_values_to_match_regex"

    # 자주 사용되는 프리셋 패턴
    PRESET_PATTERNS = {
        "email": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        "phone_kr": r'^(01[016789]|02|0[3-9][0-9])-?[0-9]{3,4}-?[0-9]{4}$',
        "url": r'^https?://[^\s]+$',
        "uuid": r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        "date_iso": r'^\d{4}-\d{2}-\d{2}$',
        "datetime_iso": r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',
        "stock_code_kr": r'^\d{6}$',
        "business_number_kr": r'^\d{3}-\d{2}-\d{5}$',
    }

    def __init__(
        self,
        column: str,
        regex: Optional[str] = None,
        preset: Optional[str] = None,
        mostly: float = 1.0,
        allow_null: bool = True,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR,
        **kwargs
    ):
        super().__init__(severity=severity, **kwargs)
        self.column = column
        self.mostly = mostly
        self.allow_null = allow_null

        # 패턴 설정
        if preset and preset in self.PRESET_PATTERNS:
            self.regex = self.PRESET_PATTERNS[preset]
            self.preset = preset
        elif regex:
            self.regex = regex
            self.preset = None
        else:
            raise ValueError("regex 또는 preset 중 하나가 필요합니다")

        self._compiled_pattern = re.compile(self.regex)

    def validate(
        self,
        data: List[Dict[str, Any]],
        column: Optional[str] = None
    ) -> ExpectationValidationResult:
        target_column = column or self.column

        if not data:
            return self._create_result(True, target_column, element_count=0)

        unexpected_values = []
        unexpected_indices = []
        non_null_count = 0

        for idx, record in enumerate(data):
            value = record.get(target_column)

            if value is None:
                if not self.allow_null:
                    unexpected_values.append(value)
                    unexpected_indices.append(idx)
                continue

            non_null_count += 1
            str_value = str(value)

            if not self._compiled_pattern.match(str_value):
                unexpected_values.append(value)
                unexpected_indices.append(idx)

        element_count = non_null_count if self.allow_null else len(data)
        unexpected_count = len(unexpected_values)
        success_ratio = 1 - (unexpected_count / element_count) if element_count > 0 else 1.0
        success = success_ratio >= self.mostly

        return self._create_result(
            success=success,
            column=target_column,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_values=unexpected_values,
            unexpected_index_list=unexpected_indices,
            details={
                "regex": self.regex,
                "preset": self.preset,
                "mostly": self.mostly,
            }
        )


class ExpectTableRowCountBetween(Expectation):
    """
    테이블 행 개수가 지정된 범위 내에 있음을 기대

    Example:
        expect_table_row_count_between(min_value=1, max_value=10000)
    """

    expectation_type = "expect_table_row_count_between"

    def __init__(
        self,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR,
        **kwargs
    ):
        super().__init__(severity=severity, **kwargs)
        self.min_value = min_value
        self.max_value = max_value

    def validate(
        self,
        data: List[Dict[str, Any]],
        column: Optional[str] = None
    ) -> ExpectationValidationResult:
        row_count = len(data)
        success = True

        if self.min_value is not None and row_count < self.min_value:
            success = False
        if self.max_value is not None and row_count > self.max_value:
            success = False

        return self._create_result(
            success=success,
            column=None,
            element_count=row_count,
            unexpected_count=0 if success else 1,
            details={
                "min_value": self.min_value,
                "max_value": self.max_value,
                "observed_value": row_count,
            }
        )


class ExpectColumnValuesToBeOfType(Expectation):
    """
    컬럼 값이 지정된 데이터 타입임을 기대

    Example:
        expect_column_values_to_be_of_type("price", "number")
    """

    expectation_type = "expect_column_values_to_be_of_type"

    # 지원하는 타입과 검증 로직
    TYPE_VALIDATORS = {
        "string": lambda v: isinstance(v, str),
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "float": lambda v: isinstance(v, float),
        "boolean": lambda v: isinstance(v, bool),
        "list": lambda v: isinstance(v, list),
        "dict": lambda v: isinstance(v, dict),
        "datetime": lambda v: isinstance(v, datetime),
        "date_string": lambda v: isinstance(v, str) and bool(re.match(r'^\d{4}-\d{2}-\d{2}', v)),
    }

    def __init__(
        self,
        column: str,
        type_: str,
        mostly: float = 1.0,
        allow_null: bool = True,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR,
        **kwargs
    ):
        super().__init__(severity=severity, **kwargs)
        self.column = column
        self.type_ = type_.lower()
        self.mostly = mostly
        self.allow_null = allow_null

        if self.type_ not in self.TYPE_VALIDATORS:
            raise ValueError(f"지원하지 않는 타입: {type_}. 지원 타입: {list(self.TYPE_VALIDATORS.keys())}")

        self._validator = self.TYPE_VALIDATORS[self.type_]

    def validate(
        self,
        data: List[Dict[str, Any]],
        column: Optional[str] = None
    ) -> ExpectationValidationResult:
        target_column = column or self.column

        if not data:
            return self._create_result(True, target_column, element_count=0)

        unexpected_values = []
        unexpected_indices = []
        non_null_count = 0

        for idx, record in enumerate(data):
            value = record.get(target_column)

            if value is None:
                if not self.allow_null:
                    unexpected_values.append(value)
                    unexpected_indices.append(idx)
                continue

            non_null_count += 1

            if not self._validator(value):
                unexpected_values.append(value)
                unexpected_indices.append(idx)

        element_count = non_null_count if self.allow_null else len(data)
        unexpected_count = len(unexpected_values)
        success_ratio = 1 - (unexpected_count / element_count) if element_count > 0 else 1.0
        success = success_ratio >= self.mostly

        return self._create_result(
            success=success,
            column=target_column,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_values=unexpected_values,
            unexpected_index_list=unexpected_indices,
            details={
                "expected_type": self.type_,
                "mostly": self.mostly,
            }
        )


class ExpectColumnValuesToBeInSet(Expectation):
    """
    컬럼 값이 지정된 집합에 포함됨을 기대

    Example:
        expect_column_values_to_be_in_set("status", ["active", "inactive", "pending"])
    """

    expectation_type = "expect_column_values_to_be_in_set"

    def __init__(
        self,
        column: str,
        value_set: Union[List[Any], Set[Any]],
        mostly: float = 1.0,
        allow_null: bool = True,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR,
        **kwargs
    ):
        super().__init__(severity=severity, **kwargs)
        self.column = column
        self.value_set = set(value_set)
        self.mostly = mostly
        self.allow_null = allow_null

    def validate(
        self,
        data: List[Dict[str, Any]],
        column: Optional[str] = None
    ) -> ExpectationValidationResult:
        target_column = column or self.column

        if not data:
            return self._create_result(True, target_column, element_count=0)

        unexpected_values = []
        unexpected_indices = []
        non_null_count = 0

        for idx, record in enumerate(data):
            value = record.get(target_column)

            if value is None:
                if not self.allow_null:
                    unexpected_values.append(value)
                    unexpected_indices.append(idx)
                continue

            non_null_count += 1

            if value not in self.value_set:
                unexpected_values.append(value)
                unexpected_indices.append(idx)

        element_count = non_null_count if self.allow_null else len(data)
        unexpected_count = len(unexpected_values)
        success_ratio = 1 - (unexpected_count / element_count) if element_count > 0 else 1.0
        success = success_ratio >= self.mostly

        return self._create_result(
            success=success,
            column=target_column,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_values=unexpected_values,
            unexpected_index_list=unexpected_indices,
            details={
                "value_set": list(self.value_set)[:20],  # 최대 20개
                "mostly": self.mostly,
            }
        )


class ExpectColumnValueLengthToBeBetween(Expectation):
    """
    컬럼 값의 길이가 지정된 범위 내에 있음을 기대

    Example:
        expect_column_value_length_to_be_between("title", min_value=1, max_value=200)
    """

    expectation_type = "expect_column_value_length_to_be_between"

    def __init__(
        self,
        column: str,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        mostly: float = 1.0,
        allow_null: bool = True,
        severity: ExpectationSeverity = ExpectationSeverity.WARNING,
        **kwargs
    ):
        super().__init__(severity=severity, **kwargs)
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.mostly = mostly
        self.allow_null = allow_null

    def validate(
        self,
        data: List[Dict[str, Any]],
        column: Optional[str] = None
    ) -> ExpectationValidationResult:
        target_column = column or self.column

        if not data:
            return self._create_result(True, target_column, element_count=0)

        unexpected_values = []
        unexpected_indices = []
        non_null_count = 0

        for idx, record in enumerate(data):
            value = record.get(target_column)

            if value is None:
                if not self.allow_null:
                    unexpected_values.append(value)
                    unexpected_indices.append(idx)
                continue

            non_null_count += 1

            try:
                length = len(value) if hasattr(value, '__len__') else len(str(value))
            except Exception:
                length = len(str(value))

            in_range = True
            if self.min_value is not None and length < self.min_value:
                in_range = False
            if self.max_value is not None and length > self.max_value:
                in_range = False

            if not in_range:
                unexpected_values.append(value)
                unexpected_indices.append(idx)

        element_count = non_null_count if self.allow_null else len(data)
        unexpected_count = len(unexpected_values)
        success_ratio = 1 - (unexpected_count / element_count) if element_count > 0 else 1.0
        success = success_ratio >= self.mostly

        return self._create_result(
            success=success,
            column=target_column,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_values=unexpected_values,
            unexpected_index_list=unexpected_indices,
            details={
                "min_value": self.min_value,
                "max_value": self.max_value,
                "mostly": self.mostly,
            }
        )


class ExpectColumnPairValuesToBeEqual(Expectation):
    """
    두 컬럼의 값이 동일함을 기대

    Example:
        expect_column_pair_values_to_be_equal("email", "confirm_email")
    """

    expectation_type = "expect_column_pair_values_to_be_equal"

    def __init__(
        self,
        column_A: str,
        column_B: str,
        mostly: float = 1.0,
        ignore_row_if: str = "both_values_are_missing",
        severity: ExpectationSeverity = ExpectationSeverity.ERROR,
        **kwargs
    ):
        super().__init__(severity=severity, **kwargs)
        self.column_A = column_A
        self.column_B = column_B
        self.mostly = mostly
        self.ignore_row_if = ignore_row_if  # both_values_are_missing, either_value_is_missing, never

    def validate(
        self,
        data: List[Dict[str, Any]],
        column: Optional[str] = None
    ) -> ExpectationValidationResult:
        if not data:
            return self._create_result(True, f"{self.column_A}={self.column_B}", element_count=0)

        unexpected_values = []
        unexpected_indices = []
        compared_count = 0

        for idx, record in enumerate(data):
            value_A = record.get(self.column_A)
            value_B = record.get(self.column_B)

            # NULL 처리
            if self.ignore_row_if == "both_values_are_missing" and value_A is None and value_B is None:
                continue
            elif self.ignore_row_if == "either_value_is_missing" and (value_A is None or value_B is None):
                continue

            compared_count += 1

            if value_A != value_B:
                unexpected_values.append({self.column_A: value_A, self.column_B: value_B})
                unexpected_indices.append(idx)

        element_count = compared_count
        unexpected_count = len(unexpected_values)
        success_ratio = 1 - (unexpected_count / element_count) if element_count > 0 else 1.0
        success = success_ratio >= self.mostly

        return self._create_result(
            success=success,
            column=f"{self.column_A}={self.column_B}",
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_values=unexpected_values,
            unexpected_index_list=unexpected_indices,
            details={
                "column_A": self.column_A,
                "column_B": self.column_B,
                "mostly": self.mostly,
            }
        )


# 편의 함수들 (Great Expectations 스타일 인터페이스)
def expect_column_not_null(column: str, **kwargs) -> ExpectColumnNotNull:
    """컬럼이 NULL이 아님을 기대"""
    return ExpectColumnNotNull(column=column, **kwargs)


def expect_column_unique(column: str, **kwargs) -> ExpectColumnUnique:
    """컬럼이 고유값임을 기대"""
    return ExpectColumnUnique(column=column, **kwargs)


def expect_column_values_in_range(
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    **kwargs
) -> ExpectColumnValuesInRange:
    """컬럼 값이 범위 내에 있음을 기대"""
    return ExpectColumnValuesInRange(
        column=column,
        min_value=min_value,
        max_value=max_value,
        **kwargs
    )


def expect_column_values_to_match_regex(
    column: str,
    regex: Optional[str] = None,
    preset: Optional[str] = None,
    **kwargs
) -> ExpectColumnValuesToMatchRegex:
    """컬럼 값이 정규식과 일치함을 기대"""
    return ExpectColumnValuesToMatchRegex(
        column=column,
        regex=regex,
        preset=preset,
        **kwargs
    )


def expect_table_row_count_between(
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
    **kwargs
) -> ExpectTableRowCountBetween:
    """테이블 행 개수가 범위 내에 있음을 기대"""
    return ExpectTableRowCountBetween(
        min_value=min_value,
        max_value=max_value,
        **kwargs
    )


def expect_column_values_to_be_of_type(
    column: str,
    type_: str,
    **kwargs
) -> ExpectColumnValuesToBeOfType:
    """컬럼 값이 지정된 타입임을 기대"""
    return ExpectColumnValuesToBeOfType(
        column=column,
        type_=type_,
        **kwargs
    )


def expect_column_values_to_be_in_set(
    column: str,
    value_set: Union[List[Any], Set[Any]],
    **kwargs
) -> ExpectColumnValuesToBeInSet:
    """컬럼 값이 지정된 집합에 포함됨을 기대"""
    return ExpectColumnValuesToBeInSet(
        column=column,
        value_set=value_set,
        **kwargs
    )


def expect_column_value_length_to_be_between(
    column: str,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
    **kwargs
) -> ExpectColumnValueLengthToBeBetween:
    """컬럼 값 길이가 범위 내에 있음을 기대"""
    return ExpectColumnValueLengthToBeBetween(
        column=column,
        min_value=min_value,
        max_value=max_value,
        **kwargs
    )


def expect_column_pair_values_to_be_equal(
    column_A: str,
    column_B: str,
    **kwargs
) -> ExpectColumnPairValuesToBeEqual:
    """두 컬럼 값이 동일함을 기대"""
    return ExpectColumnPairValuesToBeEqual(
        column_A=column_A,
        column_B=column_B,
        **kwargs
    )

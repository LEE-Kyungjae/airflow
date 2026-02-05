"""
Validation Rules - 데이터 검증 규칙 정의

각 규칙은 특정 데이터 품질 문제를 감지합니다:
- EncodingRule: 인코딩 깨짐 감지
- DateRule: 날짜 유효성 (미래 날짜, 범위 등)
- RequiredFieldRule: 필수 필드 누락
- RangeRule: 숫자 범위 검증
- FormatRule: 포맷 패턴 검증
- UniqueRule: 중복 검사
- ReferenceRule: 참조 무결성
- CustomRule: 사용자 정의 규칙
"""

import re
import unicodedata
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable, Set
from dataclasses import dataclass
from enum import Enum


class ValidationSeverity(str, Enum):
    """검증 실패 심각도"""
    INFO = "info"           # 참고 정보
    WARNING = "warning"     # 경고 (데이터 사용 가능)
    ERROR = "error"         # 오류 (데이터 검토 필요)
    CRITICAL = "critical"   # 심각 (데이터 사용 불가)


@dataclass
class ValidationIssue:
    """검증 문제 상세"""
    rule_name: str
    field_name: str
    severity: ValidationSeverity
    message: str
    actual_value: Any
    expected: Optional[str] = None
    row_index: Optional[int] = None
    suggestion: Optional[str] = None


class ValidationRule(ABC):
    """검증 규칙 기본 클래스"""

    def __init__(
        self,
        name: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
        enabled: bool = True
    ):
        self.name = name
        self.severity = severity
        self.enabled = enabled

    @abstractmethod
    def validate(self, value: Any, field_name: str, row_index: int = None, context: Dict = None) -> Optional[ValidationIssue]:
        """
        값 검증 수행

        Returns:
            ValidationIssue if validation fails, None if passes
        """
        pass

    def validate_batch(self, values: List[Any], field_name: str, context: Dict = None) -> List[ValidationIssue]:
        """배치 검증"""
        issues = []
        for idx, value in enumerate(values):
            issue = self.validate(value, field_name, row_index=idx, context=context)
            if issue:
                issues.append(issue)
        return issues


class EncodingRule(ValidationRule):
    """
    인코딩 검증 규칙

    감지 항목:
    - 깨진 문자 (replacement character)
    - 비정상 유니코드 시퀀스
    - NULL 바이트
    - 제어 문자
    """

    # 깨진 인코딩 패턴
    BROKEN_PATTERNS = [
        r'\ufffd',              # Unicode replacement character
        r'[\x00-\x08\x0b\x0c\x0e-\x1f]',  # Control characters (except newline, tab)
        r'\x00',                # NULL byte
        r'â€™|â€œ|â€|Ã©|Ã¨|Ã ',  # Common UTF-8 decoded as Latin-1
        r'ï»¿',                 # BOM as text
        r'\\x[0-9a-fA-F]{2}',   # Escaped hex bytes in string
    ]

    # 의심스러운 문자 조합 (EUC-KR -> UTF-8 깨짐)
    SUSPICIOUS_KOREAN = [
        r'[가-힣]+[\x80-\xff]+[가-힣]*',  # Mixed encoding
        r'占쏙옙',              # Common broken Korean
    ]

    def __init__(
        self,
        name: str = "encoding_check",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
        check_korean: bool = True
    ):
        super().__init__(name, severity)
        self.check_korean = check_korean
        self._compile_patterns()

    def _compile_patterns(self):
        patterns = self.BROKEN_PATTERNS.copy()
        if self.check_korean:
            patterns.extend(self.SUSPICIOUS_KOREAN)
        self.compiled_patterns = [re.compile(p) for p in patterns]

    def validate(self, value: Any, field_name: str, row_index: int = None, context: Dict = None) -> Optional[ValidationIssue]:
        if value is None:
            return None

        str_value = str(value)

        # Check for broken patterns
        for pattern in self.compiled_patterns:
            match = pattern.search(str_value)
            if match:
                return ValidationIssue(
                    rule_name=self.name,
                    field_name=field_name,
                    severity=self.severity,
                    message=f"인코딩 깨짐 감지: '{match.group()}'",
                    actual_value=str_value[:100],
                    row_index=row_index,
                    suggestion="원본 데이터의 인코딩 확인 필요 (EUC-KR, CP949 등)"
                )

        # Check for abnormal Unicode categories
        for char in str_value:
            category = unicodedata.category(char)
            if category in ('Cn', 'Co', 'Cs'):  # Not assigned, Private use, Surrogate
                if char != '\ufffd':  # Already checked above
                    return ValidationIssue(
                        rule_name=self.name,
                        field_name=field_name,
                        severity=ValidationSeverity.WARNING,
                        message=f"비정상 유니코드 문자 감지: U+{ord(char):04X}",
                        actual_value=str_value[:100],
                        row_index=row_index,
                        suggestion="문자 인코딩 검토 필요"
                    )

        return None


class DateRule(ValidationRule):
    """
    날짜 검증 규칙

    검증 항목:
    - 미래 날짜 여부
    - 과거 한계 날짜
    - 날짜 범위
    - 논리적 순서 (시작일 < 종료일)
    """

    COMMON_DATE_FORMATS = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
    ]

    def __init__(
        self,
        name: str = "date_check",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
        allow_future: bool = False,
        max_future_days: int = 0,
        min_date: datetime = None,
        max_date: datetime = None,
        date_format: str = None
    ):
        super().__init__(name, severity)
        self.allow_future = allow_future
        self.max_future_days = max_future_days
        self.min_date = min_date or datetime(1900, 1, 1)
        self.max_date = max_date
        self.date_format = date_format

    def _parse_date(self, value: Any) -> Optional[datetime]:
        """다양한 형식의 날짜 파싱"""
        if isinstance(value, datetime):
            return value

        if not isinstance(value, str):
            return None

        # Try specified format first
        if self.date_format:
            try:
                return datetime.strptime(value, self.date_format)
            except ValueError:
                pass

        # Try common formats
        for fmt in self.COMMON_DATE_FORMATS:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

        return None

    def validate(self, value: Any, field_name: str, row_index: int = None, context: Dict = None) -> Optional[ValidationIssue]:
        if value is None:
            return None

        parsed_date = self._parse_date(value)

        if parsed_date is None:
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message=f"날짜 형식 파싱 실패",
                actual_value=value,
                expected="YYYY-MM-DD 형식",
                row_index=row_index,
                suggestion="날짜 형식 확인 필요"
            )

        now = datetime.now()

        # Future date check
        if not self.allow_future and parsed_date > now:
            future_days = (parsed_date - now).days
            if future_days > self.max_future_days:
                return ValidationIssue(
                    rule_name=self.name,
                    field_name=field_name,
                    severity=self.severity,
                    message=f"미래 날짜 감지: {future_days}일 후",
                    actual_value=str(parsed_date.date()),
                    expected=f"현재 날짜({now.date()}) 이전",
                    row_index=row_index,
                    suggestion="날짜 데이터 오류 확인 필요"
                )

        # Min date check
        if parsed_date < self.min_date:
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message=f"날짜가 허용 범위 이전: {self.min_date.date()} 이전",
                actual_value=str(parsed_date.date()),
                expected=f"{self.min_date.date()} 이후",
                row_index=row_index,
                suggestion="날짜 데이터 검토 필요"
            )

        # Max date check
        if self.max_date and parsed_date > self.max_date:
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message=f"날짜가 허용 범위 이후",
                actual_value=str(parsed_date.date()),
                expected=f"{self.max_date.date()} 이전",
                row_index=row_index,
                suggestion="날짜 데이터 검토 필요"
            )

        return None


class RequiredFieldRule(ValidationRule):
    """필수 필드 검증"""

    def __init__(
        self,
        name: str = "required_check",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
        allow_empty_string: bool = False,
        allow_whitespace_only: bool = False
    ):
        super().__init__(name, severity)
        self.allow_empty_string = allow_empty_string
        self.allow_whitespace_only = allow_whitespace_only

    def validate(self, value: Any, field_name: str, row_index: int = None, context: Dict = None) -> Optional[ValidationIssue]:
        # None check
        if value is None:
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message="필수 필드 누락 (NULL)",
                actual_value=None,
                expected="값이 존재해야 함",
                row_index=row_index,
                suggestion="데이터 소스 확인 필요"
            )

        # Empty string check
        if isinstance(value, str):
            if not self.allow_empty_string and value == "":
                return ValidationIssue(
                    rule_name=self.name,
                    field_name=field_name,
                    severity=self.severity,
                    message="필수 필드가 빈 문자열",
                    actual_value="(empty)",
                    expected="값이 존재해야 함",
                    row_index=row_index,
                    suggestion="데이터 소스 확인 필요"
                )

            if not self.allow_whitespace_only and value.strip() == "":
                return ValidationIssue(
                    rule_name=self.name,
                    field_name=field_name,
                    severity=self.severity,
                    message="필수 필드가 공백만 포함",
                    actual_value="(whitespace only)",
                    expected="유효한 값이 존재해야 함",
                    row_index=row_index,
                    suggestion="데이터 소스 확인 필요"
                )

        return None


class RangeRule(ValidationRule):
    """숫자 범위 검증"""

    def __init__(
        self,
        name: str = "range_check",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
        min_value: float = None,
        max_value: float = None,
        allow_negative: bool = True,
        allow_zero: bool = True
    ):
        super().__init__(name, severity)
        self.min_value = min_value
        self.max_value = max_value
        self.allow_negative = allow_negative
        self.allow_zero = allow_zero

    def validate(self, value: Any, field_name: str, row_index: int = None, context: Dict = None) -> Optional[ValidationIssue]:
        if value is None:
            return None

        # Try to convert to number
        try:
            num_value = float(value)
        except (ValueError, TypeError):
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message="숫자로 변환 불가",
                actual_value=value,
                expected="숫자 값",
                row_index=row_index,
                suggestion="데이터 형식 확인 필요"
            )

        # Negative check
        if not self.allow_negative and num_value < 0:
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message="음수 값 감지",
                actual_value=num_value,
                expected="0 이상",
                row_index=row_index,
                suggestion="음수 데이터 검토 필요"
            )

        # Zero check
        if not self.allow_zero and num_value == 0:
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message="0 값 감지",
                actual_value=num_value,
                expected="0이 아닌 값",
                row_index=row_index,
                suggestion="0 데이터 검토 필요"
            )

        # Min value check
        if self.min_value is not None and num_value < self.min_value:
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message=f"최소값 미달: {self.min_value} 미만",
                actual_value=num_value,
                expected=f">= {self.min_value}",
                row_index=row_index,
                suggestion="값 범위 검토 필요"
            )

        # Max value check
        if self.max_value is not None and num_value > self.max_value:
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message=f"최대값 초과: {self.max_value} 초과",
                actual_value=num_value,
                expected=f"<= {self.max_value}",
                row_index=row_index,
                suggestion="값 범위 검토 필요"
            )

        return None


class FormatRule(ValidationRule):
    """포맷/패턴 검증"""

    # 자주 사용되는 패턴
    PRESET_PATTERNS = {
        "email": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        "phone_kr": r'^(01[016789]|02|0[3-9][0-9])-?[0-9]{3,4}-?[0-9]{4}$',
        "url": r'^https?://[^\s]+$',
        "korean_name": r'^[가-힣]{2,10}$',
        "business_number": r'^\d{3}-\d{2}-\d{5}$',
        "postal_code_kr": r'^\d{5}$',
    }

    def __init__(
        self,
        name: str = "format_check",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
        pattern: str = None,
        preset: str = None,
        case_sensitive: bool = True
    ):
        super().__init__(name, severity)

        if preset and preset in self.PRESET_PATTERNS:
            pattern = self.PRESET_PATTERNS[preset]

        if not pattern:
            raise ValueError("pattern 또는 preset 필수")

        flags = 0 if case_sensitive else re.IGNORECASE
        self.pattern = re.compile(pattern, flags)
        self.pattern_str = pattern

    def validate(self, value: Any, field_name: str, row_index: int = None, context: Dict = None) -> Optional[ValidationIssue]:
        if value is None:
            return None

        str_value = str(value)

        if not self.pattern.match(str_value):
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message="포맷 패턴 불일치",
                actual_value=str_value[:100],
                expected=f"패턴: {self.pattern_str}",
                row_index=row_index,
                suggestion="데이터 형식 확인 필요"
            )

        return None


class UniqueRule(ValidationRule):
    """중복 검사"""

    def __init__(
        self,
        name: str = "unique_check",
        severity: ValidationSeverity = ValidationSeverity.WARNING,
        case_sensitive: bool = True
    ):
        super().__init__(name, severity)
        self.case_sensitive = case_sensitive
        self._seen_values: Set[Any] = set()

    def reset(self):
        """상태 초기화"""
        self._seen_values.clear()

    def validate(self, value: Any, field_name: str, row_index: int = None, context: Dict = None) -> Optional[ValidationIssue]:
        if value is None:
            return None

        check_value = value
        if isinstance(value, str) and not self.case_sensitive:
            check_value = value.lower()

        if check_value in self._seen_values:
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message="중복 값 감지",
                actual_value=value,
                row_index=row_index,
                suggestion="중복 데이터 검토 필요"
            )

        self._seen_values.add(check_value)
        return None


class ReferenceRule(ValidationRule):
    """참조 무결성 검사"""

    def __init__(
        self,
        name: str = "reference_check",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
        valid_values: Set[Any] = None,
        lookup_func: Callable[[Any], bool] = None
    ):
        super().__init__(name, severity)
        self.valid_values = valid_values or set()
        self.lookup_func = lookup_func

    def validate(self, value: Any, field_name: str, row_index: int = None, context: Dict = None) -> Optional[ValidationIssue]:
        if value is None:
            return None

        # Check against valid values set
        if self.valid_values and value not in self.valid_values:
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message="참조 값 없음 (허용 목록에 없음)",
                actual_value=value,
                row_index=row_index,
                suggestion="참조 데이터 확인 필요"
            )

        # Check using lookup function
        if self.lookup_func and not self.lookup_func(value):
            return ValidationIssue(
                rule_name=self.name,
                field_name=field_name,
                severity=self.severity,
                message="참조 값 조회 실패",
                actual_value=value,
                row_index=row_index,
                suggestion="참조 데이터 확인 필요"
            )

        return None


class CustomRule(ValidationRule):
    """사용자 정의 검증 규칙"""

    def __init__(
        self,
        name: str,
        validate_func: Callable[[Any, str, int, Dict], Optional[ValidationIssue]],
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        super().__init__(name, severity)
        self.validate_func = validate_func

    def validate(self, value: Any, field_name: str, row_index: int = None, context: Dict = None) -> Optional[ValidationIssue]:
        return self.validate_func(value, field_name, row_index, context)

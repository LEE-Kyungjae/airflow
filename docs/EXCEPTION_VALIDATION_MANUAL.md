# 예외처리 및 Validation 개선 매뉴얼

> **목적**: 완전자동화 시스템의 안정성 확보를 위한 예외처리 및 검증 강화 가이드

---

## 목차

1. [현재 시스템 진단](#1-현재-시스템-진단)
2. [커스텀 예외 클래스 체계](#2-커스텀-예외-클래스-체계)
3. [입력 Validation 강화](#3-입력-validation-강화)
4. [API 레이어 예외처리](#4-api-레이어-예외처리)
5. [크롤러 예외처리](#5-크롤러-예외처리)
6. [GPT 서비스 예외처리](#6-gpt-서비스-예외처리)
7. [데이터베이스 예외처리](#7-데이터베이스-예외처리)
8. [Self-Healing 시스템 강화](#8-self-healing-시스템-강화)
9. [데이터 품질 검증](#9-데이터-품질-검증)
10. [보안 검증](#10-보안-검증)
11. [구현 체크리스트](#11-구현-체크리스트)

---

## 1. 현재 시스템 진단

### 1.1 강점 ✅

| 구성요소 | 현재 상태 |
|---------|----------|
| `error_handler.py` | E001~E010 에러 코드 체계, 패턴 매칭 분류 |
| `code_validator.py` | AST 검증, 보안 패턴 탐지, import 화이트리스트 |
| `self_healing.py` | GPT 진단, wellknown case 매칭, 학습 시스템 |
| Pydantic 스키마 | 기본 입력 검증 (SourceCreate, FieldDefinition) |

### 1.2 주요 갭 ❌

| 영역 | 문제점 | 심각도 |
|-----|--------|--------|
| 입력 검증 | URL 검증 미흡, Rate limiting 없음 | 🔴 Critical |
| 예외 체계 | 커스텀 예외 클래스 없음 | 🔴 Critical |
| GPT API | 타임아웃/재시도 없음, 비용 추적 없음 | 🔴 Critical |
| DB 연산 | 트랜잭션 없음, 부분 실패 처리 없음 | 🟡 High |
| 보안 | CORS 와일드카드, 인증 없음 | 🟡 High |
| 데이터 품질 | Silent 변환 실패, 비결정적 스코어링 | 🟢 Medium |

---

## 2. 커스텀 예외 클래스 체계

### 2.1 예외 계층 구조

`api/app/exceptions/__init__.py` 생성:

```python
"""
커스텀 예외 클래스 체계
모든 시스템 예외는 이 계층을 따름
"""
from typing import Optional, Dict, Any
from datetime import datetime


class CrawlerSystemException(Exception):
    """시스템 최상위 예외 클래스"""

    def __init__(
        self,
        message: str,
        error_code: str = "E000",
        details: Optional[Dict[str, Any]] = None,
        recoverable: bool = False
    ):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.recoverable = recoverable
        self.timestamp = datetime.utcnow()
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "recoverable": self.recoverable,
            "timestamp": self.timestamp.isoformat()
        }


# ============================================
# Validation 예외 (V001-V099)
# ============================================

class ValidationException(CrawlerSystemException):
    """검증 관련 예외"""
    pass


class URLValidationError(ValidationException):
    """URL 검증 실패"""
    def __init__(self, url: str, reason: str):
        super().__init__(
            message=f"URL 검증 실패: {reason}",
            error_code="V001",
            details={"url": url, "reason": reason},
            recoverable=False
        )


class SchemaValidationError(ValidationException):
    """스키마 검증 실패"""
    def __init__(self, field: str, expected: str, received: Any):
        super().__init__(
            message=f"필드 '{field}' 검증 실패: {expected} 예상, {type(received).__name__} 수신",
            error_code="V002",
            details={"field": field, "expected": expected, "received": str(received)},
            recoverable=False
        )


class SelectorValidationError(ValidationException):
    """CSS 선택자 검증 실패"""
    def __init__(self, selector: str, reason: str):
        super().__init__(
            message=f"선택자 검증 실패: {reason}",
            error_code="V003",
            details={"selector": selector, "reason": reason},
            recoverable=False
        )


class CronValidationError(ValidationException):
    """Cron 표현식 검증 실패"""
    def __init__(self, expression: str, reason: str):
        super().__init__(
            message=f"Cron 표현식 검증 실패: {reason}",
            error_code="V004",
            details={"expression": expression, "reason": reason},
            recoverable=False
        )


class DataTypeValidationError(ValidationException):
    """데이터 타입 검증 실패"""
    def __init__(self, field: str, value: Any, expected_type: str):
        super().__init__(
            message=f"데이터 타입 검증 실패: '{field}'는 {expected_type} 타입이어야 함",
            error_code="V005",
            details={"field": field, "value": str(value)[:100], "expected_type": expected_type},
            recoverable=False
        )


# ============================================
# 크롤러 예외 (C001-C099)
# ============================================

class CrawlerException(CrawlerSystemException):
    """크롤러 관련 예외"""
    pass


class RequestTimeoutError(CrawlerException):
    """요청 타임아웃"""
    def __init__(self, url: str, timeout: int):
        super().__init__(
            message=f"요청 타임아웃: {timeout}초 초과",
            error_code="E001",
            details={"url": url, "timeout": timeout},
            recoverable=True
        )


class SelectorNotFoundError(CrawlerException):
    """CSS 선택자를 찾을 수 없음"""
    def __init__(self, selector: str, url: str):
        super().__init__(
            message=f"선택자를 찾을 수 없음: {selector}",
            error_code="E002",
            details={"selector": selector, "url": url},
            recoverable=True  # GPT로 선택자 수정 가능
        )


class AuthenticationRequiredError(CrawlerException):
    """인증 필요"""
    def __init__(self, url: str, status_code: int):
        super().__init__(
            message=f"인증 필요: HTTP {status_code}",
            error_code="E003",
            details={"url": url, "status_code": status_code},
            recoverable=False
        )


class SiteStructureChangedError(CrawlerException):
    """사이트 구조 변경"""
    def __init__(self, url: str, expected_elements: list, found_elements: list):
        super().__init__(
            message="사이트 구조가 변경됨",
            error_code="E004",
            details={
                "url": url,
                "expected": expected_elements,
                "found": found_elements
            },
            recoverable=True  # GPT로 코드 재생성 가능
        )


class RateLimitError(CrawlerException):
    """IP 차단/속도 제한"""
    def __init__(self, url: str, retry_after: Optional[int] = None):
        super().__init__(
            message="속도 제한 감지",
            error_code="E005",
            details={"url": url, "retry_after": retry_after},
            recoverable=True
        )


class DataParsingError(CrawlerException):
    """데이터 파싱 에러"""
    def __init__(self, field: str, raw_value: str, reason: str):
        super().__init__(
            message=f"데이터 파싱 실패: {reason}",
            error_code="E006",
            details={"field": field, "raw_value": raw_value[:100], "reason": reason},
            recoverable=True
        )


class ConnectionError(CrawlerException):
    """연결 에러"""
    def __init__(self, url: str, reason: str):
        super().__init__(
            message=f"연결 실패: {reason}",
            error_code="E007",
            details={"url": url, "reason": reason},
            recoverable=True
        )


class InvalidHTTPResponseError(CrawlerException):
    """유효하지 않은 HTTP 응답"""
    def __init__(self, url: str, status_code: int, reason: str):
        super().__init__(
            message=f"HTTP 오류: {status_code} {reason}",
            error_code="E008",
            details={"url": url, "status_code": status_code, "reason": reason},
            recoverable=True
        )


class FileProcessingError(CrawlerException):
    """파일 처리 에러 (PDF, Excel 등)"""
    def __init__(self, file_type: str, reason: str):
        super().__init__(
            message=f"{file_type} 파일 처리 실패: {reason}",
            error_code="E009",
            details={"file_type": file_type, "reason": reason},
            recoverable=False
        )


# ============================================
# 외부 서비스 예외 (S001-S099)
# ============================================

class ExternalServiceException(CrawlerSystemException):
    """외부 서비스 관련 예외"""
    pass


class GPTServiceError(ExternalServiceException):
    """GPT API 오류"""
    def __init__(self, operation: str, reason: str, retryable: bool = True):
        super().__init__(
            message=f"GPT 서비스 오류: {reason}",
            error_code="S001",
            details={"operation": operation, "reason": reason},
            recoverable=retryable
        )


class GPTTimeoutError(GPTServiceError):
    """GPT API 타임아웃"""
    def __init__(self, operation: str, timeout: int):
        super().__init__(
            operation=operation,
            reason=f"타임아웃 ({timeout}초)",
            retryable=True
        )
        self.error_code = "S002"


class GPTRateLimitError(GPTServiceError):
    """GPT API 속도 제한"""
    def __init__(self, retry_after: Optional[int] = None):
        super().__init__(
            operation="api_call",
            reason="속도 제한 도달",
            retryable=True
        )
        self.error_code = "S003"
        self.details["retry_after"] = retry_after


class GPTTokenLimitError(GPTServiceError):
    """GPT 토큰 한도 초과"""
    def __init__(self, requested: int, limit: int):
        super().__init__(
            operation="api_call",
            reason=f"토큰 한도 초과: {requested}/{limit}",
            retryable=False
        )
        self.error_code = "S004"
        self.details["requested_tokens"] = requested
        self.details["limit"] = limit


# ============================================
# 데이터베이스 예외 (D001-D099)
# ============================================

class DatabaseException(CrawlerSystemException):
    """데이터베이스 관련 예외"""
    pass


class DatabaseConnectionError(DatabaseException):
    """DB 연결 실패"""
    def __init__(self, reason: str):
        super().__init__(
            message=f"데이터베이스 연결 실패: {reason}",
            error_code="D001",
            details={"reason": reason},
            recoverable=True
        )


class DatabaseOperationError(DatabaseException):
    """DB 연산 실패"""
    def __init__(self, operation: str, collection: str, reason: str):
        super().__init__(
            message=f"DB 연산 실패 ({operation}): {reason}",
            error_code="D002",
            details={
                "operation": operation,
                "collection": collection,
                "reason": reason
            },
            recoverable=True
        )


class DuplicateKeyError(DatabaseException):
    """중복 키 에러"""
    def __init__(self, collection: str, key: str, value: Any):
        super().__init__(
            message=f"중복 키: {key}={value}",
            error_code="D003",
            details={
                "collection": collection,
                "key": key,
                "value": str(value)[:100]
            },
            recoverable=False
        )


class DocumentNotFoundError(DatabaseException):
    """문서를 찾을 수 없음"""
    def __init__(self, collection: str, query: Dict[str, Any]):
        super().__init__(
            message="문서를 찾을 수 없음",
            error_code="D004",
            details={
                "collection": collection,
                "query": str(query)[:200]
            },
            recoverable=False
        )


# ============================================
# 자가 치유 예외 (H001-H099)
# ============================================

class HealingException(CrawlerSystemException):
    """자가 치유 관련 예외"""
    pass


class HealingMaxRetriesError(HealingException):
    """최대 재시도 횟수 초과"""
    def __init__(self, source_id: str, attempts: int):
        super().__init__(
            message=f"자가 치유 실패: {attempts}회 시도 후 포기",
            error_code="H001",
            details={"source_id": source_id, "attempts": attempts},
            recoverable=False
        )


class HealingTimeoutError(HealingException):
    """자가 치유 타임아웃"""
    def __init__(self, source_id: str, elapsed_time: int):
        super().__init__(
            message=f"자가 치유 타임아웃: {elapsed_time}초 경과",
            error_code="H002",
            details={"source_id": source_id, "elapsed_time": elapsed_time},
            recoverable=False
        )


class HealingDiagnosisError(HealingException):
    """진단 실패"""
    def __init__(self, source_id: str, reason: str):
        super().__init__(
            message=f"진단 실패: {reason}",
            error_code="H003",
            details={"source_id": source_id, "reason": reason},
            recoverable=True
        )


# ============================================
# 예외 매핑 헬퍼
# ============================================

ERROR_CODE_MAPPING = {
    "E001": RequestTimeoutError,
    "E002": SelectorNotFoundError,
    "E003": AuthenticationRequiredError,
    "E004": SiteStructureChangedError,
    "E005": RateLimitError,
    "E006": DataParsingError,
    "E007": ConnectionError,
    "E008": InvalidHTTPResponseError,
    "E009": FileProcessingError,
    "V001": URLValidationError,
    "V002": SchemaValidationError,
    "V003": SelectorValidationError,
    "V004": CronValidationError,
    "V005": DataTypeValidationError,
    "S001": GPTServiceError,
    "S002": GPTTimeoutError,
    "S003": GPTRateLimitError,
    "S004": GPTTokenLimitError,
    "D001": DatabaseConnectionError,
    "D002": DatabaseOperationError,
    "D003": DuplicateKeyError,
    "D004": DocumentNotFoundError,
    "H001": HealingMaxRetriesError,
    "H002": HealingTimeoutError,
    "H003": HealingDiagnosisError,
}


def is_recoverable(error_code: str) -> bool:
    """에러 코드로 복구 가능 여부 확인"""
    exception_class = ERROR_CODE_MAPPING.get(error_code)
    if exception_class:
        # 임시 인스턴스 생성하여 recoverable 확인
        try:
            return exception_class.__init__.__defaults__[-1]  # recoverable 기본값
        except (TypeError, IndexError):
            pass
    return False
```

### 2.2 예외 클래스 사용 예시

```python
# 기존 코드 (❌)
try:
    response = requests.get(url, timeout=30)
except requests.Timeout:
    logger.error("Timeout occurred")
    raise

# 개선된 코드 (✅)
from api.app.exceptions import RequestTimeoutError

try:
    response = requests.get(url, timeout=30)
except requests.Timeout:
    raise RequestTimeoutError(url=url, timeout=30)
```

---

## 3. 입력 Validation 강화

### 3.1 URL 검증 강화

`api/app/validators/url_validator.py` 생성:

```python
"""
URL 검증 모듈
DNS 해석, 프로토콜 검증, 차단 목록 확인
"""
import re
import socket
from urllib.parse import urlparse
from typing import Optional, Tuple
import ipaddress

from api.app.exceptions import URLValidationError


class URLValidator:
    """URL 종합 검증기"""

    # 허용 프로토콜
    ALLOWED_PROTOCOLS = {"http", "https"}

    # 차단된 호스트 패턴 (내부망, 로컬호스트 등)
    BLOCKED_HOST_PATTERNS = [
        r"^localhost$",
        r"^127\.",
        r"^10\.",
        r"^172\.(1[6-9]|2[0-9]|3[01])\.",
        r"^192\.168\.",
        r"^0\.",
        r"\.local$",
        r"\.internal$",
    ]

    # 차단된 도메인 (크롤링 금지 사이트)
    BLOCKED_DOMAINS = [
        # 필요시 추가
    ]

    # URL 최대 길이
    MAX_URL_LENGTH = 2048

    @classmethod
    def validate(cls, url: str, check_dns: bool = True) -> Tuple[bool, Optional[str]]:
        """
        URL 종합 검증

        Returns:
            (is_valid, error_message)
        """
        # 1. 기본 형식 검증
        if not url or not isinstance(url, str):
            return False, "URL이 비어있거나 문자열이 아님"

        url = url.strip()

        # 2. 길이 검증
        if len(url) > cls.MAX_URL_LENGTH:
            return False, f"URL 길이 초과 (최대 {cls.MAX_URL_LENGTH}자)"

        # 3. URL 파싱
        try:
            parsed = urlparse(url)
        except Exception as e:
            return False, f"URL 파싱 실패: {str(e)}"

        # 4. 프로토콜 검증
        if parsed.scheme.lower() not in cls.ALLOWED_PROTOCOLS:
            return False, f"허용되지 않은 프로토콜: {parsed.scheme}"

        # 5. 호스트 검증
        if not parsed.netloc:
            return False, "호스트가 없음"

        host = parsed.hostname or ""

        # 6. 차단 패턴 검증
        for pattern in cls.BLOCKED_HOST_PATTERNS:
            if re.match(pattern, host, re.IGNORECASE):
                return False, f"차단된 호스트 패턴: {host}"

        # 7. 차단 도메인 검증
        for blocked in cls.BLOCKED_DOMAINS:
            if host.lower() == blocked or host.lower().endswith(f".{blocked}"):
                return False, f"차단된 도메인: {host}"

        # 8. IP 주소 검증 (사설 IP 차단)
        try:
            ip = ipaddress.ip_address(host)
            if ip.is_private or ip.is_loopback or ip.is_reserved:
                return False, f"사설/예약 IP 주소: {host}"
        except ValueError:
            pass  # 도메인 이름인 경우 무시

        # 9. DNS 해석 검증 (선택)
        if check_dns:
            try:
                socket.gethostbyname(host)
            except socket.gaierror:
                return False, f"DNS 해석 실패: {host}"

        return True, None

    @classmethod
    def validate_or_raise(cls, url: str, check_dns: bool = True) -> str:
        """검증 실패 시 예외 발생"""
        is_valid, error_message = cls.validate(url, check_dns)
        if not is_valid:
            raise URLValidationError(url=url, reason=error_message)
        return url
```

### 3.2 Pydantic 스키마 강화

`api/app/schemas/source.py` 수정:

```python
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Literal
from croniter import croniter
from datetime import datetime
import re

from api.app.validators.url_validator import URLValidator
from api.app.exceptions import (
    CronValidationError,
    SelectorValidationError,
    SchemaValidationError
)


class FieldDefinition(BaseModel):
    """필드 정의 스키마"""
    name: str = Field(..., min_length=1, max_length=100)
    selector: Optional[str] = None
    data_type: Literal["string", "number", "date"]  # Enum 대신 Literal 사용
    is_list: bool = False
    attribute: Optional[str] = None
    pattern: Optional[str] = None

    @field_validator("selector")
    @classmethod
    def validate_selector(cls, v: Optional[str]) -> Optional[str]:
        """CSS 선택자 구문 검증"""
        if v is None:
            return v

        # 기본 CSS 선택자 패턴 검증
        invalid_patterns = [
            r"<script",    # XSS 방지
            r"javascript:",
            r"data:",
            r"vbscript:",
        ]

        for pattern in invalid_patterns:
            if re.search(pattern, v, re.IGNORECASE):
                raise SelectorValidationError(
                    selector=v,
                    reason=f"위험한 패턴 감지: {pattern}"
                )

        # 선택자 길이 제한
        if len(v) > 500:
            raise SelectorValidationError(
                selector=v[:50] + "...",
                reason="선택자 길이 초과 (최대 500자)"
            )

        return v

    @field_validator("pattern")
    @classmethod
    def validate_regex_pattern(cls, v: Optional[str]) -> Optional[str]:
        """정규표현식 패턴 검증"""
        if v is None:
            return v

        try:
            re.compile(v)
        except re.error as e:
            raise SchemaValidationError(
                field="pattern",
                expected="유효한 정규표현식",
                received=v
            )

        # 위험한 정규식 패턴 방지 (ReDoS)
        dangerous_patterns = [
            r"\(\.\*\)\+",      # (.*)+
            r"\(\.\+\)\+",      # (.+)+
            r"\([^\)]*\)\{.*,\}",  # 과도한 반복
        ]

        for dp in dangerous_patterns:
            if re.search(dp, v):
                raise SchemaValidationError(
                    field="pattern",
                    expected="안전한 정규표현식",
                    received=f"ReDoS 취약 패턴 감지"
                )

        return v


class SourceCreate(BaseModel):
    """소스 생성 스키마"""
    name: str = Field(..., min_length=1, max_length=100)
    url: str = Field(..., min_length=1, max_length=2048)
    type: Literal["html", "pdf", "excel", "csv"]
    fields: List[FieldDefinition] = Field(..., min_length=1, max_length=50)
    schedule: str = Field(..., min_length=9, max_length=100)  # "* * * * *" 최소 9자

    # 선택 필드
    description: Optional[str] = Field(None, max_length=500)
    timeout: int = Field(default=30, ge=5, le=300)
    retry_count: int = Field(default=3, ge=0, le=10)
    headers: Optional[dict] = None

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """URL 종합 검증"""
        return URLValidator.validate_or_raise(v, check_dns=False)

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v: str) -> str:
        """Cron 표현식 검증"""
        try:
            # croniter로 유효성 검증
            croniter(v)
        except (ValueError, KeyError) as e:
            raise CronValidationError(
                expression=v,
                reason=str(e)
            )

        # 너무 빈번한 스케줄 방지 (1분 미만)
        try:
            cron = croniter(v)
            first = cron.get_next(datetime)
            second = cron.get_next(datetime)
            interval = (second - first).total_seconds()

            if interval < 60:
                raise CronValidationError(
                    expression=v,
                    reason="최소 간격은 1분입니다"
                )
        except Exception:
            pass  # 검증 실패해도 기본 검증은 통과

        return v

    @field_validator("headers")
    @classmethod
    def validate_headers(cls, v: Optional[dict]) -> Optional[dict]:
        """헤더 검증"""
        if v is None:
            return v

        # 위험한 헤더 방지
        forbidden_headers = [
            "host",  # 호스트 스푸핑 방지
            "content-length",  # 자동 계산되어야 함
        ]

        for key in v.keys():
            if key.lower() in forbidden_headers:
                raise SchemaValidationError(
                    field="headers",
                    expected=f"'{key}' 헤더 제외",
                    received=key
                )

        return v

    @model_validator(mode="after")
    def validate_fields_for_type(self):
        """타입별 필드 검증"""
        if self.type in ("pdf", "excel", "csv"):
            # 파일 타입은 선택자 대신 컬럼명 사용
            for field in self.fields:
                if field.selector and not field.selector.isidentifier():
                    # 파일 타입은 컬럼명이어야 함
                    pass  # 또는 경고 추가

        return self


class SourceUpdate(BaseModel):
    """소스 수정 스키마"""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    url: Optional[str] = Field(None, min_length=1, max_length=2048)
    type: Optional[Literal["html", "pdf", "excel", "csv"]] = None
    fields: Optional[List[FieldDefinition]] = None
    schedule: Optional[str] = None
    description: Optional[str] = Field(None, max_length=500)
    timeout: Optional[int] = Field(None, ge=5, le=300)
    retry_count: Optional[int] = Field(None, ge=0, le=10)
    is_active: Optional[bool] = None

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        """URL 검증 (수정 시에도 적용)"""
        if v is not None:
            return URLValidator.validate_or_raise(v, check_dns=False)
        return v

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v: Optional[str]) -> Optional[str]:
        """Cron 표현식 검증"""
        if v is not None:
            try:
                croniter(v)
            except (ValueError, KeyError) as e:
                raise CronValidationError(expression=v, reason=str(e))
        return v
```

### 3.3 요청 크기 제한

`api/app/middleware/request_validator.py` 생성:

```python
"""
요청 검증 미들웨어
크기 제한, Rate Limiting 등
"""
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import time
from collections import defaultdict
from typing import Dict, Tuple
import asyncio


class RequestSizeLimitMiddleware(BaseHTTPMiddleware):
    """요청 크기 제한 미들웨어"""

    def __init__(self, app, max_size: int = 10 * 1024 * 1024):  # 기본 10MB
        super().__init__(app)
        self.max_size = max_size

    async def dispatch(self, request: Request, call_next):
        # Content-Length 헤더 확인
        content_length = request.headers.get("content-length")
        if content_length:
            if int(content_length) > self.max_size:
                raise HTTPException(
                    status_code=413,
                    detail=f"요청 크기 초과: 최대 {self.max_size // (1024*1024)}MB"
                )

        return await call_next(request)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate Limiting 미들웨어"""

    def __init__(
        self,
        app,
        requests_per_minute: int = 60,
        requests_per_hour: int = 1000
    ):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour

        # IP별 요청 추적
        self.minute_requests: Dict[str, list] = defaultdict(list)
        self.hour_requests: Dict[str, list] = defaultdict(list)

        # 클린업 태스크
        self._cleanup_task = None

    def _get_client_ip(self, request: Request) -> str:
        """클라이언트 IP 추출"""
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def _clean_old_requests(self, requests: list, max_age: int) -> list:
        """오래된 요청 기록 제거"""
        now = time.time()
        return [t for t in requests if now - t < max_age]

    async def dispatch(self, request: Request, call_next):
        client_ip = self._get_client_ip(request)
        now = time.time()

        # 분당 요청 확인
        self.minute_requests[client_ip] = self._clean_old_requests(
            self.minute_requests[client_ip], 60
        )

        if len(self.minute_requests[client_ip]) >= self.requests_per_minute:
            raise HTTPException(
                status_code=429,
                detail="요청 한도 초과: 분당 요청 제한",
                headers={"Retry-After": "60"}
            )

        # 시간당 요청 확인
        self.hour_requests[client_ip] = self._clean_old_requests(
            self.hour_requests[client_ip], 3600
        )

        if len(self.hour_requests[client_ip]) >= self.requests_per_hour:
            raise HTTPException(
                status_code=429,
                detail="요청 한도 초과: 시간당 요청 제한",
                headers={"Retry-After": "3600"}
            )

        # 요청 기록
        self.minute_requests[client_ip].append(now)
        self.hour_requests[client_ip].append(now)

        return await call_next(request)
```

`api/app/main.py`에 미들웨어 추가:

```python
from api.app.middleware.request_validator import (
    RequestSizeLimitMiddleware,
    RateLimitMiddleware
)

# 미들웨어 등록 (순서 중요: 먼저 등록된 것이 바깥쪽)
app.add_middleware(RequestSizeLimitMiddleware, max_size=10 * 1024 * 1024)
app.add_middleware(RateLimitMiddleware, requests_per_minute=60, requests_per_hour=1000)
```

---

## 4. API 레이어 예외처리

### 4.1 전역 예외 핸들러 강화

`api/app/handlers/exception_handlers.py` 생성:

```python
"""
전역 예외 핸들러
모든 예외를 일관된 형식으로 처리
"""
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
import logging
import traceback
from datetime import datetime

from api.app.exceptions import (
    CrawlerSystemException,
    ValidationException,
    CrawlerException,
    ExternalServiceException,
    DatabaseException,
    HealingException
)

logger = logging.getLogger(__name__)


def setup_exception_handlers(app: FastAPI):
    """예외 핸들러 등록"""

    @app.exception_handler(CrawlerSystemException)
    async def crawler_system_exception_handler(
        request: Request,
        exc: CrawlerSystemException
    ):
        """커스텀 시스템 예외 처리"""
        log_level = logging.WARNING if exc.recoverable else logging.ERROR
        logger.log(
            log_level,
            f"[{exc.error_code}] {exc.message}",
            extra={
                "error_code": exc.error_code,
                "details": exc.details,
                "path": request.url.path
            }
        )

        # 에러 코드별 HTTP 상태 코드 매핑
        status_code_map = {
            "V": 400,  # Validation → 400 Bad Request
            "C": 502,  # Crawler → 502 Bad Gateway (외부 사이트 문제)
            "S": 503,  # Service → 503 Service Unavailable
            "D": 500,  # Database → 500 Internal Server Error
            "H": 500,  # Healing → 500 Internal Server Error
        }

        prefix = exc.error_code[0] if exc.error_code else "E"
        status_code = status_code_map.get(prefix, 500)

        return JSONResponse(
            status_code=status_code,
            content={
                "success": False,
                "error": {
                    "code": exc.error_code,
                    "message": exc.message,
                    "details": exc.details,
                    "recoverable": exc.recoverable
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request,
        exc: RequestValidationError
    ):
        """Pydantic 검증 오류 처리"""
        errors = []
        for error in exc.errors():
            field = ".".join(str(loc) for loc in error["loc"])
            errors.append({
                "field": field,
                "message": error["msg"],
                "type": error["type"]
            })

        logger.warning(
            f"Validation error: {errors}",
            extra={"path": request.url.path}
        )

        return JSONResponse(
            status_code=422,
            content={
                "success": False,
                "error": {
                    "code": "V000",
                    "message": "입력 데이터 검증 실패",
                    "details": {"validation_errors": errors},
                    "recoverable": False
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        """HTTP 예외 처리"""
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "success": False,
                "error": {
                    "code": f"HTTP{exc.status_code}",
                    "message": exc.detail,
                    "details": {},
                    "recoverable": exc.status_code < 500
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        )

    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception):
        """예상치 못한 예외 처리"""
        # 스택 트레이스 로깅 (민감 정보 필터링)
        tb = traceback.format_exc()

        # 민감 정보 마스킹
        sensitive_patterns = [
            (r"password['\"]?\s*[:=]\s*['\"]?[^'\"]+", "password=***"),
            (r"api[_-]?key['\"]?\s*[:=]\s*['\"]?[^'\"]+", "api_key=***"),
            (r"token['\"]?\s*[:=]\s*['\"]?[^'\"]+", "token=***"),
        ]

        import re
        for pattern, replacement in sensitive_patterns:
            tb = re.sub(pattern, replacement, tb, flags=re.IGNORECASE)

        logger.error(
            f"Unhandled exception: {str(exc)}",
            extra={
                "path": request.url.path,
                "traceback": tb[:2000]  # 트레이스 길이 제한
            }
        )

        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": {
                    "code": "E999",
                    "message": "내부 서버 오류가 발생했습니다",
                    "details": {},  # 상세 정보 숨김
                    "recoverable": False
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        )
```

### 4.2 라우터별 예외 처리 패턴

```python
# api/app/routers/sources.py 수정 예시

from fastapi import APIRouter, HTTPException, Depends
from api.app.exceptions import (
    DocumentNotFoundError,
    DuplicateKeyError,
    ValidationException
)

router = APIRouter()


@router.post("/", response_model=SourceResponse)
async def create_source(source: SourceCreate):
    """소스 생성"""
    try:
        # 중복 체크
        existing = await db.sources.find_one({"url": source.url})
        if existing:
            raise DuplicateKeyError(
                collection="sources",
                key="url",
                value=source.url
            )

        # 생성 로직
        result = await db.sources.insert_one(source.model_dump())

        return SourceResponse(
            success=True,
            data={"id": str(result.inserted_id)}
        )

    except DuplicateKeyError:
        raise  # 커스텀 예외는 그대로 전파
    except Exception as e:
        logger.exception("소스 생성 실패")
        raise DatabaseOperationError(
            operation="insert",
            collection="sources",
            reason=str(e)
        )


@router.get("/{source_id}", response_model=SourceResponse)
async def get_source(source_id: str):
    """소스 조회"""
    try:
        # ObjectId 검증
        if not ObjectId.is_valid(source_id):
            raise ValidationException(
                message="유효하지 않은 소스 ID 형식",
                error_code="V006",
                details={"source_id": source_id}
            )

        source = await db.sources.find_one({"_id": ObjectId(source_id)})

        if not source:
            raise DocumentNotFoundError(
                collection="sources",
                query={"_id": source_id}
            )

        return SourceResponse(success=True, data=source)

    except (ValidationException, DocumentNotFoundError):
        raise
    except Exception as e:
        logger.exception(f"소스 조회 실패: {source_id}")
        raise DatabaseOperationError(
            operation="find",
            collection="sources",
            reason=str(e)
        )
```

---

## 5. 크롤러 예외처리

### 5.1 기본 크롤러 예외 처리 강화

`airflow/dags/utils/base_crawler.py` 수정:

```python
"""
향상된 기본 크롤러 클래스
체계적인 예외 처리 포함
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

# 커스텀 예외 임포트
from api.app.exceptions import (
    RequestTimeoutError,
    ConnectionError as CrawlerConnectionError,
    AuthenticationRequiredError,
    RateLimitError,
    InvalidHTTPResponseError,
    SelectorNotFoundError,
    DataParsingError,
    CrawlerException
)

logger = logging.getLogger(__name__)


@dataclass
class CrawlResult:
    """크롤링 결과"""
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    error: Optional[CrawlerException] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    html_snapshot: Optional[str] = None
    execution_time: float = 0.0
    records_count: int = 0
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseCrawler:
    """향상된 기본 크롤러"""

    # HTTP 상태 코드 → 예외 클래스 매핑
    HTTP_STATUS_EXCEPTIONS = {
        401: (AuthenticationRequiredError, {"reason": "Unauthorized"}),
        403: (AuthenticationRequiredError, {"reason": "Forbidden"}),
        404: (InvalidHTTPResponseError, {"reason": "Not Found"}),
        429: (RateLimitError, {}),
        500: (InvalidHTTPResponseError, {"reason": "Internal Server Error"}),
        502: (InvalidHTTPResponseError, {"reason": "Bad Gateway"}),
        503: (InvalidHTTPResponseError, {"reason": "Service Unavailable"}),
        504: (InvalidHTTPResponseError, {"reason": "Gateway Timeout"}),
    }

    def __init__(
        self,
        source_id: str,
        url: str,
        timeout: int = 30,
        retry_count: int = 3,
        headers: Optional[Dict[str, str]] = None
    ):
        self.source_id = source_id
        self.url = url
        self.timeout = timeout
        self.retry_count = retry_count
        self.headers = headers or {}

        # 세션 설정 (재시도 포함)
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """재시도 로직이 포함된 세션 생성"""
        session = requests.Session()

        retry_strategy = Retry(
            total=self.retry_count,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # 기본 헤더
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; CrawlerBot/1.0)",
            **self.headers
        })

        return session

    def execute(self) -> CrawlResult:
        """크롤링 실행 (예외 처리 포함)"""
        start_time = datetime.now()
        html_snapshot = None

        try:
            # 1. HTTP 요청
            response = self._make_request()
            html_snapshot = response.text[:5000]  # 스냅샷 저장

            # 2. HTTP 상태 확인
            self._check_response_status(response)

            # 3. 데이터 추출 (서브클래스에서 구현)
            data, warnings = self._extract_data(response)

            # 4. 데이터 검증
            validated_data = self._validate_data(data)

            execution_time = (datetime.now() - start_time).total_seconds()

            return CrawlResult(
                success=True,
                data=validated_data,
                records_count=len(validated_data),
                execution_time=execution_time,
                warnings=warnings,
                html_snapshot=html_snapshot
            )

        except CrawlerException as e:
            # 커스텀 예외는 그대로 전달
            execution_time = (datetime.now() - start_time).total_seconds()
            return CrawlResult(
                success=False,
                error=e,
                error_code=e.error_code,
                error_message=e.message,
                html_snapshot=html_snapshot,
                execution_time=execution_time
            )

        except requests.Timeout as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            error = RequestTimeoutError(url=self.url, timeout=self.timeout)
            return CrawlResult(
                success=False,
                error=error,
                error_code=error.error_code,
                error_message=error.message,
                execution_time=execution_time
            )

        except requests.ConnectionError as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            error = CrawlerConnectionError(url=self.url, reason=str(e))
            return CrawlResult(
                success=False,
                error=error,
                error_code=error.error_code,
                error_message=error.message,
                execution_time=execution_time
            )

        except Exception as e:
            # 예상치 못한 예외
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.exception(f"크롤링 중 예상치 못한 오류: {self.source_id}")

            error = CrawlerException(
                message=f"크롤링 실패: {str(e)}",
                error_code="E010",
                details={"exception_type": type(e).__name__},
                recoverable=False
            )
            return CrawlResult(
                success=False,
                error=error,
                error_code="E010",
                error_message=str(e),
                html_snapshot=html_snapshot,
                execution_time=execution_time
            )

        finally:
            self._cleanup()

    def _make_request(self) -> requests.Response:
        """HTTP 요청 수행"""
        return self.session.get(self.url, timeout=self.timeout)

    def _check_response_status(self, response: requests.Response):
        """HTTP 응답 상태 확인"""
        if response.status_code >= 400:
            exception_info = self.HTTP_STATUS_EXCEPTIONS.get(
                response.status_code,
                (InvalidHTTPResponseError, {"reason": response.reason})
            )

            exception_class, kwargs = exception_info

            if exception_class == AuthenticationRequiredError:
                raise exception_class(
                    url=self.url,
                    status_code=response.status_code
                )
            elif exception_class == RateLimitError:
                retry_after = response.headers.get("Retry-After")
                raise exception_class(
                    url=self.url,
                    retry_after=int(retry_after) if retry_after else None
                )
            else:
                raise exception_class(
                    url=self.url,
                    status_code=response.status_code,
                    **kwargs
                )

    def _extract_data(self, response: requests.Response) -> tuple:
        """데이터 추출 (서브클래스에서 구현)"""
        raise NotImplementedError("서브클래스에서 구현 필요")

    def _validate_data(self, data: List[Dict]) -> List[Dict]:
        """추출된 데이터 검증"""
        validated = []

        for idx, record in enumerate(data):
            # 빈 레코드 제거
            if not any(v is not None and v != "" for v in record.values()):
                logger.debug(f"빈 레코드 스킵: index={idx}")
                continue

            validated.append(record)

        if not validated:
            raise DataParsingError(
                field="all",
                raw_value="",
                reason="유효한 데이터 레코드 없음"
            )

        return validated

    def _cleanup(self):
        """리소스 정리"""
        try:
            self.session.close()
        except Exception:
            pass  # 정리 중 오류는 무시
```

### 5.2 HTML 크롤러 선택자 예외 처리

```python
# airflow/dags/utils/html_crawler.py 수정

from bs4 import BeautifulSoup
from api.app.exceptions import SelectorNotFoundError, DataParsingError


class HTMLCrawler(BaseCrawler):
    """HTML 크롤러"""

    def __init__(self, source_id: str, url: str, fields: List[dict], **kwargs):
        super().__init__(source_id, url, **kwargs)
        self.fields = fields

    def _extract_data(self, response: requests.Response) -> tuple:
        """HTML에서 데이터 추출"""
        warnings = []
        soup = BeautifulSoup(response.text, "html.parser")

        data = []

        # 각 필드별 선택자로 데이터 추출
        for field in self.fields:
            selector = field.get("selector")
            if not selector:
                continue

            elements = soup.select(selector)

            # 선택자를 찾지 못한 경우
            if not elements:
                if field.get("required", False):
                    raise SelectorNotFoundError(
                        selector=selector,
                        url=self.url
                    )
                else:
                    warnings.append(f"선택자 '{selector}' 결과 없음 (필드: {field['name']})")
                    continue

            # 데이터 추출 및 파싱
            for elem in elements:
                try:
                    value = self._extract_value(elem, field)
                    data.append({field["name"]: value})
                except Exception as e:
                    raise DataParsingError(
                        field=field["name"],
                        raw_value=str(elem)[:100],
                        reason=str(e)
                    )

        return data, warnings

    def _extract_value(self, element, field: dict) -> Any:
        """요소에서 값 추출"""
        attr = field.get("attribute")

        if attr:
            value = element.get(attr)
        else:
            value = element.get_text(strip=True)

        # 타입 변환
        data_type = field.get("data_type", "string")

        if data_type == "number":
            return self._parse_number(value, field["name"])
        elif data_type == "date":
            return self._parse_date(value, field["name"])

        return value

    def _parse_number(self, value: str, field_name: str) -> Optional[float]:
        """숫자 파싱 (실패 시 예외)"""
        if not value:
            return None

        # 숫자 정규화
        cleaned = re.sub(r"[^\d.\-,]", "", str(value))
        cleaned = cleaned.replace(",", "")

        try:
            return float(cleaned)
        except ValueError:
            raise DataParsingError(
                field=field_name,
                raw_value=value,
                reason="숫자로 변환 불가"
            )

    def _parse_date(self, value: str, field_name: str) -> Optional[str]:
        """날짜 파싱 (실패 시 예외)"""
        if not value:
            return None

        date_patterns = [
            r"\d{4}-\d{2}-\d{2}",
            r"\d{4}/\d{2}/\d{2}",
            r"\d{4}\.\d{2}\.\d{2}",
        ]

        for pattern in date_patterns:
            match = re.search(pattern, value)
            if match:
                return match.group()

        raise DataParsingError(
            field=field_name,
            raw_value=value,
            reason="날짜 형식 인식 불가"
        )
```

---

## 6. GPT 서비스 예외처리

### 6.1 GPT 서비스 강화

`api/app/services/gpt_service.py` 수정:

```python
"""
향상된 GPT 서비스
타임아웃, 재시도, 비용 추적 포함
"""
import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime
import tiktoken
from openai import OpenAI, APIError, APITimeoutError, RateLimitError as OpenAIRateLimitError

from api.app.exceptions import (
    GPTServiceError,
    GPTTimeoutError,
    GPTRateLimitError,
    GPTTokenLimitError
)

logger = logging.getLogger(__name__)


class GPTService:
    """향상된 GPT 서비스"""

    # 모델별 토큰 제한
    MODEL_TOKEN_LIMITS = {
        "gpt-4o-mini": 128000,
        "gpt-4o": 128000,
        "gpt-4-turbo": 128000,
        "gpt-3.5-turbo": 16385,
    }

    # 모델별 가격 (1K 토큰당 USD)
    MODEL_PRICING = {
        "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},
        "gpt-4o": {"input": 0.005, "output": 0.015},
    }

    # 재시도 설정
    MAX_RETRIES = 3
    RETRY_DELAYS = [1, 2, 4]  # 지수 백오프

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        timeout: int = 60,
        max_tokens: int = 4096
    ):
        self.client = OpenAI(api_key=api_key, timeout=timeout)
        self.model = model
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.encoding = tiktoken.encoding_for_model(model)

        # 비용 추적
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_cost = 0.0

    def count_tokens(self, text: str) -> int:
        """토큰 수 계산"""
        return len(self.encoding.encode(text))

    def estimate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """비용 추정"""
        pricing = self.MODEL_PRICING.get(self.model, {"input": 0, "output": 0})
        return (
            (input_tokens / 1000) * pricing["input"] +
            (output_tokens / 1000) * pricing["output"]
        )

    async def call(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.7,
        operation: str = "unknown"
    ) -> Dict[str, Any]:
        """
        GPT API 호출 (재시도, 타임아웃, 비용 추적 포함)

        Returns:
            {
                "content": str,
                "input_tokens": int,
                "output_tokens": int,
                "cost": float,
                "model": str
            }
        """
        # 1. 토큰 제한 검증
        input_tokens = self.count_tokens(prompt)
        if system_prompt:
            input_tokens += self.count_tokens(system_prompt)

        token_limit = self.MODEL_TOKEN_LIMITS.get(self.model, 16000)

        if input_tokens + self.max_tokens > token_limit:
            raise GPTTokenLimitError(
                requested=input_tokens + self.max_tokens,
                limit=token_limit
            )

        # 2. 메시지 구성
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        # 3. 재시도 루프
        last_error = None

        for attempt in range(self.MAX_RETRIES):
            try:
                response = await asyncio.wait_for(
                    self._async_create(messages, temperature),
                    timeout=self.timeout
                )

                # 비용 계산 및 추적
                usage = response.usage
                cost = self.estimate_cost(
                    usage.prompt_tokens,
                    usage.completion_tokens
                )

                self.total_input_tokens += usage.prompt_tokens
                self.total_output_tokens += usage.completion_tokens
                self.total_cost += cost

                logger.info(
                    f"GPT 호출 성공: operation={operation}, "
                    f"tokens={usage.prompt_tokens}+{usage.completion_tokens}, "
                    f"cost=${cost:.4f}"
                )

                return {
                    "content": response.choices[0].message.content,
                    "input_tokens": usage.prompt_tokens,
                    "output_tokens": usage.completion_tokens,
                    "cost": cost,
                    "model": self.model
                }

            except asyncio.TimeoutError:
                last_error = GPTTimeoutError(
                    operation=operation,
                    timeout=self.timeout
                )

            except OpenAIRateLimitError as e:
                # Rate limit - 대기 후 재시도
                retry_after = getattr(e, "retry_after", None)
                last_error = GPTRateLimitError(retry_after=retry_after)

                if attempt < self.MAX_RETRIES - 1:
                    wait_time = retry_after or self.RETRY_DELAYS[attempt]
                    logger.warning(
                        f"GPT Rate limit, {wait_time}초 후 재시도 "
                        f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                    )
                    await asyncio.sleep(wait_time)
                    continue

            except APITimeoutError:
                last_error = GPTTimeoutError(
                    operation=operation,
                    timeout=self.timeout
                )

            except APIError as e:
                last_error = GPTServiceError(
                    operation=operation,
                    reason=str(e),
                    retryable=e.status_code >= 500
                )

                # 5xx 에러는 재시도
                if e.status_code >= 500 and attempt < self.MAX_RETRIES - 1:
                    wait_time = self.RETRY_DELAYS[attempt]
                    logger.warning(
                        f"GPT API 오류 ({e.status_code}), {wait_time}초 후 재시도"
                    )
                    await asyncio.sleep(wait_time)
                    continue

            except Exception as e:
                last_error = GPTServiceError(
                    operation=operation,
                    reason=str(e),
                    retryable=False
                )

            # 재시도 대기
            if attempt < self.MAX_RETRIES - 1:
                wait_time = self.RETRY_DELAYS[attempt]
                logger.warning(
                    f"GPT 호출 실패, {wait_time}초 후 재시도 "
                    f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                )
                await asyncio.sleep(wait_time)

        # 모든 재시도 실패
        logger.error(f"GPT 호출 최종 실패: operation={operation}")
        raise last_error

    async def _async_create(self, messages: list, temperature: float):
        """비동기 API 호출 래퍼"""
        return await asyncio.to_thread(
            self.client.chat.completions.create,
            model=self.model,
            messages=messages,
            temperature=temperature,
            max_tokens=self.max_tokens
        )

    def get_usage_stats(self) -> Dict[str, Any]:
        """사용량 통계 반환"""
        return {
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_tokens": self.total_input_tokens + self.total_output_tokens,
            "total_cost_usd": round(self.total_cost, 4),
            "model": self.model
        }
```

---

## 7. 데이터베이스 예외처리

### 7.1 MongoDB 연산 래퍼

`api/app/services/database.py` 수정:

```python
"""
향상된 MongoDB 서비스
트랜잭션, 재시도, 예외 처리 포함
"""
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import (
    ConnectionFailure,
    ServerSelectionTimeoutError,
    DuplicateKeyError as PyMongoDuplicateKeyError,
    OperationFailure,
    WriteError
)
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
import logging

from api.app.exceptions import (
    DatabaseConnectionError,
    DatabaseOperationError,
    DuplicateKeyError,
    DocumentNotFoundError
)

logger = logging.getLogger(__name__)


class MongoDBService:
    """향상된 MongoDB 서비스"""

    MAX_RETRIES = 3
    RETRY_DELAY = 1

    def __init__(self, uri: str, database: str):
        self.uri = uri
        self.database_name = database
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None

    async def connect(self):
        """데이터베이스 연결"""
        try:
            self.client = AsyncIOMotorClient(
                self.uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )
            # 연결 테스트
            await self.client.admin.command("ping")
            self.db = self.client[self.database_name]
            logger.info(f"MongoDB 연결 성공: {self.database_name}")

        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            raise DatabaseConnectionError(reason=str(e))

    async def close(self):
        """연결 종료"""
        if self.client:
            self.client.close()

    @asynccontextmanager
    async def transaction(self):
        """트랜잭션 컨텍스트 매니저"""
        async with await self.client.start_session() as session:
            async with session.start_transaction():
                try:
                    yield session
                except Exception as e:
                    # 트랜잭션 자동 롤백
                    logger.error(f"트랜잭션 롤백: {e}")
                    raise

    async def find_one(
        self,
        collection: str,
        query: Dict[str, Any],
        raise_not_found: bool = False
    ) -> Optional[Dict[str, Any]]:
        """단일 문서 조회"""
        try:
            result = await self.db[collection].find_one(query)

            if result is None and raise_not_found:
                raise DocumentNotFoundError(
                    collection=collection,
                    query=query
                )

            return result

        except DocumentNotFoundError:
            raise
        except Exception as e:
            raise DatabaseOperationError(
                operation="find_one",
                collection=collection,
                reason=str(e)
            )

    async def find_many(
        self,
        collection: str,
        query: Dict[str, Any],
        skip: int = 0,
        limit: int = 100,
        sort: Optional[List[tuple]] = None
    ) -> List[Dict[str, Any]]:
        """다중 문서 조회"""
        try:
            cursor = self.db[collection].find(query)

            if sort:
                cursor = cursor.sort(sort)

            cursor = cursor.skip(skip).limit(limit)

            return await cursor.to_list(length=limit)

        except Exception as e:
            raise DatabaseOperationError(
                operation="find",
                collection=collection,
                reason=str(e)
            )

    async def insert_one(
        self,
        collection: str,
        document: Dict[str, Any]
    ) -> str:
        """단일 문서 삽입"""
        try:
            result = await self.db[collection].insert_one(document)
            return str(result.inserted_id)

        except PyMongoDuplicateKeyError as e:
            # 중복 키 에러 파싱
            key_pattern = e.details.get("keyPattern", {})
            key_value = e.details.get("keyValue", {})

            key = list(key_pattern.keys())[0] if key_pattern else "unknown"
            value = key_value.get(key) if key_value else "unknown"

            raise DuplicateKeyError(
                collection=collection,
                key=key,
                value=value
            )

        except Exception as e:
            raise DatabaseOperationError(
                operation="insert_one",
                collection=collection,
                reason=str(e)
            )

    async def insert_many(
        self,
        collection: str,
        documents: List[Dict[str, Any]],
        ordered: bool = False
    ) -> Dict[str, Any]:
        """
        다중 문서 삽입 (부분 실패 처리)

        ordered=False: 실패해도 나머지 계속 삽입
        """
        try:
            result = await self.db[collection].insert_many(
                documents,
                ordered=ordered
            )

            return {
                "inserted_count": len(result.inserted_ids),
                "inserted_ids": [str(id) for id in result.inserted_ids]
            }

        except PyMongoDuplicateKeyError as e:
            # 부분 성공 처리 (ordered=False인 경우)
            inserted_count = e.details.get("nInserted", 0)
            write_errors = e.details.get("writeErrors", [])

            logger.warning(
                f"부분 삽입 완료: {inserted_count}/{len(documents)} "
                f"(중복: {len(write_errors)}개)"
            )

            return {
                "inserted_count": inserted_count,
                "duplicate_count": len(write_errors),
                "partial_success": True
            }

        except Exception as e:
            raise DatabaseOperationError(
                operation="insert_many",
                collection=collection,
                reason=str(e)
            )

    async def update_one(
        self,
        collection: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False
    ) -> Dict[str, Any]:
        """단일 문서 업데이트"""
        try:
            result = await self.db[collection].update_one(
                query,
                {"$set": update},
                upsert=upsert
            )

            return {
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }

        except Exception as e:
            raise DatabaseOperationError(
                operation="update_one",
                collection=collection,
                reason=str(e)
            )

    async def upsert_many(
        self,
        collection: str,
        documents: List[Dict[str, Any]],
        upsert_keys: List[str]
    ) -> Dict[str, Any]:
        """
        다중 문서 Upsert (키 기반)

        upsert_keys: 중복 체크에 사용할 필드명 목록
        """
        if not upsert_keys:
            raise DatabaseOperationError(
                operation="upsert_many",
                collection=collection,
                reason="upsert_keys가 비어있음"
            )

        upserted = 0
        updated = 0
        errors = []

        for doc in documents:
            try:
                # upsert_keys로 필터 생성
                filter_query = {key: doc.get(key) for key in upsert_keys}

                # 모든 키가 존재하는지 확인
                if None in filter_query.values():
                    missing = [k for k, v in filter_query.items() if v is None]
                    errors.append({
                        "document": str(doc)[:100],
                        "error": f"upsert_keys 누락: {missing}"
                    })
                    continue

                result = await self.db[collection].update_one(
                    filter_query,
                    {"$set": doc},
                    upsert=True
                )

                if result.upserted_id:
                    upserted += 1
                else:
                    updated += 1

            except Exception as e:
                errors.append({
                    "document": str(doc)[:100],
                    "error": str(e)
                })

        return {
            "upserted_count": upserted,
            "updated_count": updated,
            "error_count": len(errors),
            "errors": errors if errors else None
        }

    async def delete_one(
        self,
        collection: str,
        query: Dict[str, Any]
    ) -> bool:
        """단일 문서 삭제"""
        try:
            result = await self.db[collection].delete_one(query)
            return result.deleted_count > 0

        except Exception as e:
            raise DatabaseOperationError(
                operation="delete_one",
                collection=collection,
                reason=str(e)
            )
```

---

## 8. Self-Healing 시스템 강화

### 8.1 치유 프로세스 예외 처리

`airflow/dags/utils/self_healing.py` 수정:

```python
"""
향상된 Self-Healing 시스템
무한 루프 방지, 타임아웃, 상태 추적 강화
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging

from api.app.exceptions import (
    HealingException,
    HealingMaxRetriesError,
    HealingTimeoutError,
    HealingDiagnosisError,
    GPTServiceError
)

logger = logging.getLogger(__name__)


class HealingStatus(str, Enum):
    """치유 상태"""
    PENDING = "pending"
    DIAGNOSING = "diagnosing"
    FINDING_SOLUTION = "finding_solution"
    APPLYING_FIX = "applying_fix"
    TESTING = "testing"
    RESOLVED = "resolved"
    FAILED = "failed"
    WAITING_ADMIN = "waiting_admin"
    TIMEOUT = "timeout"


@dataclass
class HealingSession:
    """치유 세션"""
    session_id: str
    source_id: str
    error_code: str
    error_message: str
    status: HealingStatus = HealingStatus.PENDING
    attempts: int = 0
    max_attempts: int = 5
    started_at: datetime = field(default_factory=datetime.utcnow)
    timeout_at: Optional[datetime] = None
    last_attempt_at: Optional[datetime] = None
    resolution: Optional[str] = None
    attempt_history: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self):
        # 기본 타임아웃: 1시간
        if self.timeout_at is None:
            self.timeout_at = self.started_at + timedelta(hours=1)

    def is_expired(self) -> bool:
        """타임아웃 여부"""
        return datetime.utcnow() > self.timeout_at

    def can_retry(self) -> bool:
        """재시도 가능 여부"""
        return (
            self.attempts < self.max_attempts and
            not self.is_expired() and
            self.status not in (
                HealingStatus.RESOLVED,
                HealingStatus.FAILED,
                HealingStatus.TIMEOUT
            )
        )

    def record_attempt(self, action: str, result: str, success: bool):
        """시도 기록"""
        self.attempts += 1
        self.last_attempt_at = datetime.utcnow()
        self.attempt_history.append({
            "attempt": self.attempts,
            "action": action,
            "result": result,
            "success": success,
            "timestamp": self.last_attempt_at.isoformat()
        })


class SelfHealingEngine:
    """향상된 Self-Healing 엔진"""

    # 에러 코드별 최대 재시도 횟수
    ERROR_MAX_ATTEMPTS = {
        "E001": 5,   # 타임아웃 - 많이 재시도
        "E002": 3,   # 선택자 없음 - GPT 수정 3회
        "E003": 1,   # 인증 필요 - 관리자 개입 필요
        "E004": 3,   # 구조 변경 - GPT 재생성 3회
        "E005": 5,   # Rate limit - 대기 후 재시도
        "E006": 3,   # 파싱 에러 - GPT 수정
        "E007": 5,   # 연결 에러 - 재시도
        "E008": 3,   # HTTP 에러 - 재시도
        "E009": 1,   # 파일 에러 - 관리자 개입
        "E010": 2,   # 알 수 없음 - 제한적 재시도
    }

    # 에러 코드별 타임아웃 (분)
    ERROR_TIMEOUTS = {
        "E001": 30,
        "E002": 60,
        "E003": 1440,  # 24시간 (관리자 대기)
        "E004": 60,
        "E005": 120,   # Rate limit 대기
        "E006": 60,
        "E007": 30,
        "E008": 30,
        "E009": 1440,
        "E010": 60,
    }

    def __init__(self, db_service, gpt_service):
        self.db = db_service
        self.gpt = gpt_service
        self.active_sessions: Dict[str, HealingSession] = {}

    async def start_healing(
        self,
        source_id: str,
        error_code: str,
        error_message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> HealingSession:
        """치유 세션 시작"""
        # 기존 활성 세션 확인
        existing = self.active_sessions.get(source_id)
        if existing and existing.status not in (
            HealingStatus.RESOLVED,
            HealingStatus.FAILED,
            HealingStatus.TIMEOUT
        ):
            logger.warning(f"기존 치유 세션 존재: {source_id}")
            return existing

        # 새 세션 생성
        session = HealingSession(
            session_id=f"heal_{source_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            source_id=source_id,
            error_code=error_code,
            error_message=error_message,
            max_attempts=self.ERROR_MAX_ATTEMPTS.get(error_code, 3),
            timeout_at=datetime.utcnow() + timedelta(
                minutes=self.ERROR_TIMEOUTS.get(error_code, 60)
            )
        )

        self.active_sessions[source_id] = session

        # 치유 시작
        await self._execute_healing(session, context)

        return session

    async def _execute_healing(
        self,
        session: HealingSession,
        context: Optional[Dict[str, Any]] = None
    ):
        """치유 프로세스 실행"""
        try:
            while session.can_retry():
                # 타���아웃 체크
                if session.is_expired():
                    session.status = HealingStatus.TIMEOUT
                    raise HealingTimeoutError(
                        source_id=session.source_id,
                        elapsed_time=int(
                            (datetime.utcnow() - session.started_at).total_seconds()
                        )
                    )

                # 1. 진단
                session.status = HealingStatus.DIAGNOSING
                diagnosis = await self._diagnose(session, context)

                if not diagnosis:
                    session.record_attempt("diagnose", "진단 실패", False)
                    continue

                # 2. 해결책 찾기
                session.status = HealingStatus.FINDING_SOLUTION
                solution = await self._find_solution(session, diagnosis)

                if not solution:
                    session.record_attempt("find_solution", "해결책 없음", False)
                    continue

                # 3. 수정 적용
                session.status = HealingStatus.APPLYING_FIX
                fix_result = await self._apply_fix(session, solution)

                if not fix_result["success"]:
                    session.record_attempt(
                        "apply_fix",
                        fix_result.get("error", "적용 실패"),
                        False
                    )
                    continue

                # 4. 테스트
                session.status = HealingStatus.TESTING
                test_result = await self._test_fix(session)

                if test_result["success"]:
                    session.status = HealingStatus.RESOLVED
                    session.resolution = solution.get("description", "자동 수정")
                    session.record_attempt("test", "성공", True)

                    # 성공 패턴 학습
                    await self._learn_success(session, diagnosis, solution)

                    logger.info(f"치유 성공: {session.source_id}")
                    return
                else:
                    session.record_attempt(
                        "test",
                        test_result.get("error", "테스트 실패"),
                        False
                    )

            # 재시도 한도 초과
            if session.attempts >= session.max_attempts:
                session.status = HealingStatus.WAITING_ADMIN
                raise HealingMaxRetriesError(
                    source_id=session.source_id,
                    attempts=session.attempts
                )

        except HealingException:
            raise
        except GPTServiceError as e:
            session.status = HealingStatus.FAILED
            raise HealingDiagnosisError(
                source_id=session.source_id,
                reason=f"GPT 서비스 오류: {e.message}"
            )
        except Exception as e:
            session.status = HealingStatus.FAILED
            logger.exception(f"치유 중 예상치 못한 오류: {session.source_id}")
            raise HealingDiagnosisError(
                source_id=session.source_id,
                reason=str(e)
            )

    async def _diagnose(
        self,
        session: HealingSession,
        context: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """에러 진단"""
        # Wellknown case 확인
        wellknown = await self._check_wellknown_case(session)
        if wellknown:
            return {
                "type": "wellknown",
                "case": wellknown,
                "confidence": wellknown.get("success_rate", 0.5)
            }

        # GPT 진단
        try:
            result = await self.gpt.call(
                prompt=self._build_diagnosis_prompt(session, context),
                system_prompt="당신은 웹 크롤러 오류 진단 전문가입니다.",
                operation="diagnosis"
            )

            # 응답 파싱
            return self._parse_diagnosis(result["content"])

        except GPTServiceError:
            # GPT 실패 시 기본 진단
            return self._basic_diagnosis(session)

    async def _find_solution(
        self,
        session: HealingSession,
        diagnosis: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """해결책 찾기"""
        if diagnosis.get("type") == "wellknown":
            return diagnosis["case"].get("solution")

        # GPT로 해결책 생성
        try:
            result = await self.gpt.call(
                prompt=self._build_solution_prompt(session, diagnosis),
                system_prompt="당신은 웹 크롤러 코드 수정 전문가입니다.",
                operation="find_solution"
            )

            return self._parse_solution(result["content"])

        except GPTServiceError:
            return None

    async def _apply_fix(
        self,
        session: HealingSession,
        solution: Dict[str, Any]
    ) -> Dict[str, Any]:
        """수정 적용"""
        # 구현 세부사항...
        pass

    async def _test_fix(self, session: HealingSession) -> Dict[str, Any]:
        """수정 테스트"""
        # 구현 세부사항...
        pass

    async def _check_wellknown_case(
        self,
        session: HealingSession
    ) -> Optional[Dict[str, Any]]:
        """Wellknown case 확인"""
        # 에러 패턴 해시 생성
        pattern_hash = self._generate_pattern_hash(
            session.error_code,
            session.error_message
        )

        # DB에서 검색
        case = await self.db.find_one(
            "wellknown_cases",
            {
                "pattern_hash": pattern_hash,
                "success_rate": {"$gte": 0.6}
            }
        )

        return case

    async def _learn_success(
        self,
        session: HealingSession,
        diagnosis: Dict[str, Any],
        solution: Dict[str, Any]
    ):
        """성공 패턴 학습"""
        pattern_hash = self._generate_pattern_hash(
            session.error_code,
            session.error_message
        )

        # Wellknown case 업데이트/생성
        await self.db.update_one(
            "wellknown_cases",
            {"pattern_hash": pattern_hash},
            {
                "pattern_hash": pattern_hash,
                "error_code": session.error_code,
                "diagnosis": diagnosis,
                "solution": solution,
                "success_count": {"$inc": 1},
                "last_success": datetime.utcnow(),
                "success_rate": {"$avg": 1.0}  # 실제로는 계산 로직 필요
            },
            upsert=True
        )

    def _generate_pattern_hash(self, error_code: str, message: str) -> str:
        """에러 패턴 해시 생성"""
        import hashlib
        import re

        # 메시지 정규화 (숫자, URL 등 제거)
        normalized = re.sub(r'\d+', 'N', message)
        normalized = re.sub(r'https?://\S+', 'URL', normalized)
        normalized = normalized.lower().strip()

        content = f"{error_code}:{normalized}"
        return hashlib.md5(content.encode()).hexdigest()

    def _basic_diagnosis(self, session: HealingSession) -> Dict[str, Any]:
        """기본 진단 (GPT 없이)"""
        basic_diagnoses = {
            "E001": {"category": "network", "action": "increase_timeout"},
            "E002": {"category": "selector", "action": "update_selector"},
            "E003": {"category": "auth", "action": "require_admin"},
            "E004": {"category": "structure", "action": "regenerate_code"},
            "E005": {"category": "rate_limit", "action": "wait_and_retry"},
            "E006": {"category": "parsing", "action": "update_parser"},
            "E007": {"category": "network", "action": "retry"},
            "E008": {"category": "http", "action": "retry"},
            "E009": {"category": "file", "action": "require_admin"},
            "E010": {"category": "unknown", "action": "retry"},
        }

        return {
            "type": "basic",
            **basic_diagnoses.get(session.error_code, {"category": "unknown", "action": "retry"}),
            "confidence": 0.5
        }

    def _build_diagnosis_prompt(
        self,
        session: HealingSession,
        context: Optional[Dict[str, Any]]
    ) -> str:
        """진단 프롬프트 생성"""
        prompt = f"""
웹 크롤러 오류를 진단해주세요.

## 오류 정보
- 에러 코드: {session.error_code}
- 에러 메시지: {session.error_message}
- 이전 시도 횟수: {session.attempts}

## 이전 시도 기록
{self._format_attempts(session.attempt_history)}

## 컨텍스트
{context if context else "없음"}

## 요청
1. 오류의 근본 원인을 분석하세요
2. 가능한 해결 방법을 제안하세요
3. JSON 형식으로 응답하세요

응답 형식:
{{
    "category": "network|selector|auth|structure|rate_limit|parsing|file|unknown",
    "root_cause": "근본 원인 설명",
    "suggested_action": "제안 조치",
    "confidence": 0.0-1.0
}}
"""
        return prompt

    def _format_attempts(self, attempts: List[Dict]) -> str:
        """시도 기록 포맷팅"""
        if not attempts:
            return "없음"

        lines = []
        for a in attempts[-5:]:  # 최근 5개만
            lines.append(f"- [{a['attempt']}] {a['action']}: {a['result']}")

        return "\n".join(lines)
```

---

## 9. 데이터 품질 검증

### 9.1 품질 검증 강화

`api/app/services/data_validator.py` 생성:

```python
"""
데이터 품질 검증 모듈
결정적이고 투명한 품질 스코어링
"""
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import re
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """검증 결과"""
    is_valid: bool
    score: float  # 0.0 - 1.0
    level: str  # HIGH, MEDIUM, LOW, INVALID
    issues: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    field_scores: Dict[str, float] = field(default_factory=dict)

    @classmethod
    def from_score(cls, score: float, issues: List[Dict] = None, warnings: List[str] = None):
        """점수로부터 결과 생성"""
        levels = [
            (0.8, "HIGH"),
            (0.6, "MEDIUM"),
            (0.4, "LOW"),
            (0.0, "INVALID"),
        ]

        level = "INVALID"
        for threshold, lvl in levels:
            if score >= threshold:
                level = lvl
                break

        return cls(
            is_valid=score >= 0.4,
            score=round(score, 3),
            level=level,
            issues=issues or [],
            warnings=warnings or []
        )


class DataQualityValidator:
    """데이터 품질 검증기"""

    # 점수 가중치 (명시적, 결정적)
    SCORING_WEIGHTS = {
        # 필수 필드 존재 여부
        "required_field_missing": -0.20,
        "required_field_empty": -0.15,

        # 타입 검증
        "type_mismatch": -0.15,
        "type_conversion_failed": -0.20,

        # 값 범위/형식
        "date_future": -0.10,
        "date_too_old": -0.05,  # 1년 이상 오래된 날짜
        "number_negative_unexpected": -0.10,
        "string_too_short": -0.05,
        "string_too_long": -0.05,

        # 데이터 품질
        "html_tags_in_text": -0.10,
        "special_chars_excessive": -0.05,
        "whitespace_excessive": -0.05,

        # 중복/일관성
        "duplicate_detected": -0.25,
        "inconsistent_format": -0.10,
    }

    # 카테고리별 필수 필드
    CATEGORY_REQUIRED_FIELDS = {
        "news": ["title", "published_at"],
        "stock": ["name", "price"],
        "product": ["name", "price"],
        "announcement": ["title", "date"],
        "default": ["title"]
    }

    def __init__(self, category: str = "default"):
        self.category = category
        self.required_fields = self.CATEGORY_REQUIRED_FIELDS.get(
            category,
            self.CATEGORY_REQUIRED_FIELDS["default"]
        )

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        """단일 레코드 검증"""
        score = 1.0
        issues = []
        warnings = []
        field_scores = {}

        # 1. 필수 필드 검증
        for req_field in self.required_fields:
            field_score, field_issues = self._validate_required_field(
                record, req_field
            )
            field_scores[req_field] = field_score

            if field_score < 1.0:
                score += self.SCORING_WEIGHTS.get(
                    "required_field_missing" if req_field not in record
                    else "required_field_empty",
                    -0.15
                )
                issues.extend(field_issues)

        # 2. 필드별 타입/값 검증
        for field_name, value in record.items():
            field_result = self._validate_field_value(field_name, value)

            if field_result["score"] < 1.0:
                score += (field_result["score"] - 1.0) * 0.5  # 필드별 가중치
                issues.extend(field_result.get("issues", []))
                warnings.extend(field_result.get("warnings", []))

            field_scores[field_name] = field_result["score"]

        # 3. 전체 레코드 검증
        record_issues = self._validate_record_level(record)
        for issue in record_issues:
            weight = self.SCORING_WEIGHTS.get(issue["type"], -0.05)
            score += weight
            issues.append(issue)

        # 점수 범위 보정
        score = max(0.0, min(1.0, score))

        result = ValidationResult.from_score(score, issues, warnings)
        result.field_scores = field_scores

        return result

    def validate_batch(
        self,
        records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict], List[Dict], Dict[str, Any]]:
        """
        배치 검증

        Returns:
            (valid_records, invalid_records, stats)
        """
        valid = []
        invalid = []

        stats = {
            "total": len(records),
            "valid_count": 0,
            "invalid_count": 0,
            "avg_score": 0.0,
            "score_distribution": {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "INVALID": 0},
            "common_issues": {}
        }

        total_score = 0.0

        for record in records:
            result = self.validate(record)

            record_with_meta = {
                **record,
                "_quality": {
                    "score": result.score,
                    "level": result.level,
                    "issues": result.issues
                }
            }

            if result.is_valid:
                valid.append(record_with_meta)
                stats["valid_count"] += 1
            else:
                invalid.append(record_with_meta)
                stats["invalid_count"] += 1

            total_score += result.score
            stats["score_distribution"][result.level] += 1

            # 이슈 통계
            for issue in result.issues:
                issue_type = issue.get("type", "unknown")
                stats["common_issues"][issue_type] = \
                    stats["common_issues"].get(issue_type, 0) + 1

        stats["avg_score"] = round(total_score / len(records), 3) if records else 0

        return valid, invalid, stats

    def _validate_required_field(
        self,
        record: Dict[str, Any],
        field_name: str
    ) -> Tuple[float, List[Dict]]:
        """필수 필드 검증"""
        issues = []

        if field_name not in record:
            issues.append({
                "type": "required_field_missing",
                "field": field_name,
                "message": f"필수 필드 '{field_name}' 누락"
            })
            return 0.0, issues

        value = record[field_name]

        if value is None or value == "":
            issues.append({
                "type": "required_field_empty",
                "field": field_name,
                "message": f"필수 필드 '{field_name}' 값이 비어있음"
            })
            return 0.5, issues

        return 1.0, issues

    def _validate_field_value(
        self,
        field_name: str,
        value: Any
    ) -> Dict[str, Any]:
        """필드 값 검증"""
        result = {"score": 1.0, "issues": [], "warnings": []}

        if value is None:
            return result

        # 문자열 검증
        if isinstance(value, str):
            # HTML 태그 확인
            if re.search(r'<[^>]+>', value):
                result["score"] -= 0.1
                result["warnings"].append(f"'{field_name}'에 HTML 태그 포함")

            # 과도한 공백
            if len(value) > 0 and len(value.strip()) / len(value) < 0.5:
                result["score"] -= 0.05
                result["warnings"].append(f"'{field_name}'에 과도한 공백")

            # 길이 검증
            if len(value) > 10000:
                result["score"] -= 0.05
                result["issues"].append({
                    "type": "string_too_long",
                    "field": field_name,
                    "message": f"문자열 길이 초과: {len(value)}"
                })

        # 날짜 검증 (field_name에 'date' 포함 시)
        if 'date' in field_name.lower() or 'at' in field_name.lower():
            date_result = self._validate_date(value)
            result["score"] = min(result["score"], date_result["score"])
            result["issues"].extend(date_result.get("issues", []))

        # 숫자 검증 (field_name에 'price', 'amount' 등 포함 시)
        if any(kw in field_name.lower() for kw in ['price', 'amount', 'count', 'quantity']):
            num_result = self._validate_number(value, field_name)
            result["score"] = min(result["score"], num_result["score"])
            result["issues"].extend(num_result.get("issues", []))

        return result

    def _validate_date(self, value: Any) -> Dict[str, Any]:
        """날짜 검증"""
        result = {"score": 1.0, "issues": []}

        if not isinstance(value, (str, datetime)):
            return result

        try:
            if isinstance(value, str):
                # 간단한 날짜 파싱
                date_patterns = [
                    (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),
                    (r'(\d{4})/(\d{2})/(\d{2})', '%Y/%m/%d'),
                ]

                parsed_date = None
                for pattern, fmt in date_patterns:
                    match = re.search(pattern, value)
                    if match:
                        parsed_date = datetime.strptime(match.group(), fmt)
                        break

                if not parsed_date:
                    return result  # 파싱 실패 시 검증 생략
            else:
                parsed_date = value

            # 미래 날짜 검증
            if parsed_date > datetime.now() + timedelta(days=1):
                result["score"] = 0.9
                result["issues"].append({
                    "type": "date_future",
                    "message": f"미래 날짜: {parsed_date}"
                })

            # 너무 오래된 날짜
            if parsed_date < datetime.now() - timedelta(days=365):
                result["score"] = min(result["score"], 0.95)
                result["issues"].append({
                    "type": "date_too_old",
                    "message": f"1년 이상 오래된 날짜: {parsed_date}"
                })

        except Exception:
            pass  # 파싱 실패는 무시

        return result

    def _validate_number(self, value: Any, field_name: str) -> Dict[str, Any]:
        """숫자 검증"""
        result = {"score": 1.0, "issues": []}

        # 숫자 추출
        if isinstance(value, str):
            cleaned = re.sub(r'[^\d.\-]', '', value)
            try:
                num_value = float(cleaned) if cleaned else None
            except ValueError:
                return result
        elif isinstance(value, (int, float)):
            num_value = value
        else:
            return result

        if num_value is None:
            return result

        # 가격/금액이 음수인 경우
        if 'price' in field_name.lower() and num_value < 0:
            result["score"] = 0.9
            result["issues"].append({
                "type": "number_negative_unexpected",
                "field": field_name,
                "message": f"예상치 못한 음수 값: {num_value}"
            })

        return result

    def _validate_record_level(self, record: Dict[str, Any]) -> List[Dict]:
        """레코드 전체 수준 검증"""
        issues = []

        # 모든 값이 비어있는지 확인
        non_empty_count = sum(
            1 for v in record.values()
            if v is not None and v != ""
        )

        if non_empty_count == 0:
            issues.append({
                "type": "empty_record",
                "message": "모든 필드가 비어있음"
            })

        return issues
```

---

## 10. 보안 검증

### 10.1 CORS 및 보안 설정

`api/app/main.py` 보안 강화:

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import os

app = FastAPI(
    title="Crawler System API",
    docs_url="/docs" if os.getenv("ENV") != "production" else None,
    redoc_url="/redoc" if os.getenv("ENV") != "production" else None,
)

# CORS 설정 (환경별)
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "").split(",")
if not ALLOWED_ORIGINS or ALLOWED_ORIGINS == [""]:
    # 개발 환경
    ALLOWED_ORIGINS = ["http://localhost:3000", "http://localhost:8000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
    max_age=86400,
)

# Trusted Host (프로덕션)
if os.getenv("ENV") == "production":
    ALLOWED_HOSTS = os.getenv("ALLOWED_HOSTS", "").split(",")
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=ALLOWED_HOSTS
    )
```

### 10.2 민감 정보 마스킹

`api/app/utils/security.py` 생성:

```python
"""
보안 유틸리티
민감 정보 마스킹, 로깅 안전화
"""
import re
from typing import Any, Dict


class SensitiveDataMasker:
    """민감 정보 마스킹"""

    PATTERNS = [
        # API 키
        (r'(api[_-]?key)["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', r'\1=***MASKED***'),
        # 패스워드
        (r'(password|passwd|pwd)["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', r'\1=***MASKED***'),
        # 토큰
        (r'(token|bearer)["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', r'\1=***MASKED***'),
        # 시크릿
        (r'(secret)["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', r'\1=***MASKED***'),
        # 이메일
        (r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', r'***EMAIL***'),
        # 신용카드 (간단 패턴)
        (r'\b(\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4})\b', r'***CARD***'),
    ]

    @classmethod
    def mask(cls, text: str) -> str:
        """텍스트에서 민감 정보 마스킹"""
        if not text:
            return text

        result = text
        for pattern, replacement in cls.PATTERNS:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

        return result

    @classmethod
    def mask_dict(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """딕셔너리에서 민감 정보 마스킹"""
        sensitive_keys = {
            'password', 'passwd', 'pwd', 'secret', 'token',
            'api_key', 'apikey', 'api-key', 'authorization',
            'access_token', 'refresh_token', 'private_key'
        }

        result = {}
        for key, value in data.items():
            if key.lower() in sensitive_keys:
                result[key] = "***MASKED***"
            elif isinstance(value, str):
                result[key] = cls.mask(value)
            elif isinstance(value, dict):
                result[key] = cls.mask_dict(value)
            else:
                result[key] = value

        return result
```

---

## 11. 구현 체크리스트

### 11.1 Priority 1 (Critical) - 즉시 구현

- [ ] **커스텀 예외 클래스 체계**
  - [ ] `api/app/exceptions/__init__.py` 생성
  - [ ] 기존 코드에서 generic Exception을 커스텀 예외로 교체

- [ ] **URL 검증 강화**
  - [ ] `api/app/validators/url_validator.py` 생성
  - [ ] `SourceUpdate.url` 검증 추가

- [ ] **Rate Limiting 구현**
  - [ ] `api/app/middleware/request_validator.py` 생성
  - [ ] 미들웨어 등록

- [ ] **GPT 서비스 타임아웃/재시도**
  - [ ] `GPTService` 클래스 리팩토링
  - [ ] 비용 추적 추가

- [ ] **CORS 보안 설정**
  - [ ] 환경별 CORS 설정
  - [ ] Trusted Host 미들웨어 추가

### 11.2 Priority 2 (High) - 다음 스프린트

- [ ] **전역 예외 핸들러 강화**
  - [ ] `api/app/handlers/exception_handlers.py` 생성
  - [ ] 민감 정보 마스킹 적용

- [ ] **데이터베이스 트랜잭션 지원**
  - [ ] `MongoDBService` 트랜잭션 컨텍스트 매니저
  - [ ] 부분 실패 처리

- [ ] **크롤러 예외 처리 개선**
  - [ ] `BaseCrawler` 리팩토링
  - [ ] HTTP 상태 코드 매핑 완성

- [ ] **데이터 품질 검증 강화**
  - [ ] `DataQualityValidator` 클래스 생성
  - [ ] 배치 검증 통계 추가

### 11.3 Priority 3 (Enhancement) - 점진적 개선

- [ ] **Self-Healing 강화**
  - [ ] 무한 루프 방지 로직
  - [ ] 타임아웃 처리
  - [ ] 성공 패턴 학습

- [ ] **로깅 강화**
  - [ ] 구조화된 로깅
  - [ ] 민감 정보 자동 마스킹
  - [ ] 에러 추적 ID 부여

- [ ] **모니터링 통합**
  - [ ] 에러 메트릭 수집
  - [ ] 알림 임계값 설정
  - [ ] 대시보드 연동

---

## 부록: 에러 코드 전체 목록

| 코드 | 분류 | 설명 | 복구 가능 |
|-----|-----|-----|----------|
| V001 | Validation | URL 검증 실패 | ❌ |
| V002 | Validation | 스키마 검증 실패 | ❌ |
| V003 | Validation | 선택자 검증 실패 | ❌ |
| V004 | Validation | Cron 표현식 검증 실패 | ❌ |
| V005 | Validation | 데이터 타입 검증 실패 | ❌ |
| E001 | Crawler | 요청 타임아웃 | ✅ |
| E002 | Crawler | 선택자를 찾을 수 없음 | ✅ |
| E003 | Crawler | 인증 필요 | ❌ |
| E004 | Crawler | 사이트 구조 변경 | ✅ |
| E005 | Crawler | IP 차단/속도 제한 | ✅ |
| E006 | Crawler | 데이터 파싱 에러 | ✅ |
| E007 | Crawler | 연결 에러 | ✅ |
| E008 | Crawler | 유효하지 않은 HTTP 응답 | ✅ |
| E009 | Crawler | 파일 처리 에러 | ❌ |
| E010 | Crawler | 알 수 없는 에러 | ❌ |
| S001 | Service | GPT 서비스 오류 | ✅ |
| S002 | Service | GPT 타임아웃 | ✅ |
| S003 | Service | GPT 속도 제한 | ✅ |
| S004 | Service | GPT 토큰 한도 초과 | ❌ |
| D001 | Database | DB 연결 실패 | ✅ |
| D002 | Database | DB 연산 실패 | ✅ |
| D003 | Database | 중복 키 에러 | ❌ |
| D004 | Database | 문서를 찾을 수 없음 | ❌ |
| H001 | Healing | 최대 재시도 횟수 초과 | ❌ |
| H002 | Healing | 자가 치유 타임아웃 | ❌ |
| H003 | Healing | 진단 실패 | ✅ |

---

**문서 버전**: 1.0
**최종 업데이트**: 2025-02-03
**작성**: Claude Code

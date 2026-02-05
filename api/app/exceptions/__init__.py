"""
커스텀 예외 클래스 체계
모든 시스템 예외는 이 계층을 따름
"""
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


class ErrorSeverity(str, Enum):
    """에러 심각도"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RecoveryAction(str, Enum):
    """복구 액션 타입"""
    RETRY = "retry"
    RETRY_WITH_BACKOFF = "retry_with_backoff"
    RETRY_WITH_LONGER_TIMEOUT = "retry_with_longer_timeout"
    GPT_FIX_SELECTORS = "gpt_fix_selectors"
    GPT_REGENERATE_CODE = "gpt_regenerate_code"
    SWITCH_PROXY = "switch_proxy"
    WAIT_AND_RETRY = "wait_and_retry"
    NOTIFY_ADMIN = "notify_admin"
    SKIP = "skip"
    FAIL = "fail"


class CrawlerSystemException(Exception):
    """시스템 최상위 예외 클래스"""

    def __init__(
        self,
        message: str,
        error_code: str = "E000",
        details: Optional[Dict[str, Any]] = None,
        recoverable: bool = False,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        recovery_actions: Optional[List[RecoveryAction]] = None,
        retry_after: Optional[int] = None,
        cause: Optional[Exception] = None
    ):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.recoverable = recoverable
        self.severity = severity
        self.recovery_actions = recovery_actions or []
        self.retry_after = retry_after
        self.cause = cause
        self.timestamp = datetime.utcnow()
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """예외를 딕셔너리로 변환"""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "recoverable": self.recoverable,
            "severity": self.severity.value,
            "recovery_actions": [a.value for a in self.recovery_actions],
            "retry_after": self.retry_after,
            "timestamp": self.timestamp.isoformat(),
            "cause": str(self.cause) if self.cause else None
        }

    def __str__(self):
        return f"[{self.error_code}] {self.message}"

    def __repr__(self):
        return f"{self.__class__.__name__}(code={self.error_code}, message={self.message})"


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
            recoverable=False,
            severity=ErrorSeverity.MEDIUM
        )


class SchemaValidationError(ValidationException):
    """스키마 검증 실패"""
    def __init__(self, field: str, expected: str, received: Any):
        super().__init__(
            message=f"필드 '{field}' 검증 실패: {expected} 예상, {type(received).__name__} 수신",
            error_code="V002",
            details={"field": field, "expected": expected, "received": str(received)[:100]},
            recoverable=False,
            severity=ErrorSeverity.MEDIUM
        )


class SelectorValidationError(ValidationException):
    """CSS 선택자 검증 실패"""
    def __init__(self, selector: str, reason: str):
        super().__init__(
            message=f"선택자 검증 실패: {reason}",
            error_code="V003",
            details={"selector": selector, "reason": reason},
            recoverable=False,
            severity=ErrorSeverity.MEDIUM
        )


class CronValidationError(ValidationException):
    """Cron 표현식 검증 실패"""
    def __init__(self, expression: str, reason: str):
        super().__init__(
            message=f"Cron 표현식 검증 실패: {reason}",
            error_code="V004",
            details={"expression": expression, "reason": reason},
            recoverable=False,
            severity=ErrorSeverity.MEDIUM
        )


class DataTypeValidationError(ValidationException):
    """데이터 타입 검증 실패"""
    def __init__(self, field: str, value: Any, expected_type: str):
        super().__init__(
            message=f"데이터 타입 검증 실패: '{field}'는 {expected_type} 타입이어야 함",
            error_code="V005",
            details={"field": field, "value": str(value)[:100], "expected_type": expected_type},
            recoverable=False,
            severity=ErrorSeverity.MEDIUM
        )


class ObjectIdValidationError(ValidationException):
    """ObjectId 검증 실패"""
    def __init__(self, value: str, context: str = ""):
        super().__init__(
            message=f"유효하지 않은 ObjectId: {value}",
            error_code="V006",
            details={"value": value, "context": context},
            recoverable=False,
            severity=ErrorSeverity.LOW
        )


# ============================================
# 크롤러 예외 (E001-E099)
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
            recoverable=True,
            severity=ErrorSeverity.MEDIUM,
            recovery_actions=[
                RecoveryAction.RETRY_WITH_LONGER_TIMEOUT,
                RecoveryAction.RETRY_WITH_BACKOFF
            ]
        )


class SelectorNotFoundError(CrawlerException):
    """CSS 선택자를 찾을 수 없음"""
    def __init__(self, selector: str, url: str, html_snippet: str = ""):
        super().__init__(
            message=f"선택자를 찾을 수 없음: {selector}",
            error_code="E002",
            details={
                "selector": selector,
                "url": url,
                "html_snippet": html_snippet[:500] if html_snippet else ""
            },
            recoverable=True,
            severity=ErrorSeverity.HIGH,
            recovery_actions=[
                RecoveryAction.GPT_FIX_SELECTORS,
                RecoveryAction.GPT_REGENERATE_CODE
            ]
        )


class AuthenticationRequiredError(CrawlerException):
    """인증 필요"""
    def __init__(self, url: str, status_code: int):
        super().__init__(
            message=f"인증 필요: HTTP {status_code}",
            error_code="E003",
            details={"url": url, "status_code": status_code},
            recoverable=False,
            severity=ErrorSeverity.HIGH,
            recovery_actions=[RecoveryAction.NOTIFY_ADMIN]
        )


class SiteStructureChangedError(CrawlerException):
    """사이트 구조 변경"""
    def __init__(self, url: str, expected_elements: list = None, found_elements: list = None):
        super().__init__(
            message="사이트 구조가 변경됨",
            error_code="E004",
            details={
                "url": url,
                "expected": expected_elements or [],
                "found": found_elements or []
            },
            recoverable=True,
            severity=ErrorSeverity.HIGH,
            recovery_actions=[
                RecoveryAction.GPT_REGENERATE_CODE,
                RecoveryAction.GPT_FIX_SELECTORS
            ]
        )


class RateLimitError(CrawlerException):
    """IP 차단/속도 제한"""
    def __init__(self, url: str, retry_after: Optional[int] = None):
        super().__init__(
            message="속도 제한 감지",
            error_code="E005",
            details={"url": url},
            recoverable=True,
            severity=ErrorSeverity.MEDIUM,
            recovery_actions=[
                RecoveryAction.WAIT_AND_RETRY,
                RecoveryAction.SWITCH_PROXY
            ],
            retry_after=retry_after or 60
        )


class DataParsingError(CrawlerException):
    """데이터 파싱 에러"""
    def __init__(self, field: str, raw_value: str, reason: str):
        super().__init__(
            message=f"데이터 파싱 실패: {reason}",
            error_code="E006",
            details={"field": field, "raw_value": raw_value[:200], "reason": reason},
            recoverable=True,
            severity=ErrorSeverity.MEDIUM,
            recovery_actions=[RecoveryAction.GPT_FIX_SELECTORS]
        )


class CrawlerConnectionError(CrawlerException):
    """연결 에러"""
    def __init__(self, url: str, reason: str):
        super().__init__(
            message=f"연결 실패: {reason}",
            error_code="E007",
            details={"url": url, "reason": reason},
            recoverable=True,
            severity=ErrorSeverity.MEDIUM,
            recovery_actions=[RecoveryAction.RETRY_WITH_BACKOFF]
        )


class InvalidHTTPResponseError(CrawlerException):
    """유효하지 않은 HTTP 응답"""
    def __init__(self, url: str, status_code: int, reason: str = ""):
        super().__init__(
            message=f"HTTP 오류: {status_code} {reason}",
            error_code="E008",
            details={"url": url, "status_code": status_code, "reason": reason},
            recoverable=status_code >= 500,
            severity=ErrorSeverity.MEDIUM if status_code >= 500 else ErrorSeverity.HIGH,
            recovery_actions=[RecoveryAction.RETRY_WITH_BACKOFF] if status_code >= 500 else []
        )


class FileProcessingError(CrawlerException):
    """파일 처리 에러 (PDF, Excel 등)"""
    def __init__(self, file_type: str, reason: str, file_path: str = ""):
        super().__init__(
            message=f"{file_type} 파일 처리 실패: {reason}",
            error_code="E009",
            details={"file_type": file_type, "reason": reason, "file_path": file_path},
            recoverable=False,
            severity=ErrorSeverity.HIGH,
            recovery_actions=[RecoveryAction.NOTIFY_ADMIN]
        )


class UnknownCrawlerError(CrawlerException):
    """알 수 없는 크롤러 에러"""
    def __init__(self, message: str, cause: Exception = None):
        super().__init__(
            message=message,
            error_code="E010",
            details={"original_error": str(cause) if cause else ""},
            recoverable=False,
            severity=ErrorSeverity.HIGH,
            recovery_actions=[RecoveryAction.NOTIFY_ADMIN],
            cause=cause
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
            recoverable=retryable,
            severity=ErrorSeverity.HIGH,
            recovery_actions=[RecoveryAction.RETRY_WITH_BACKOFF] if retryable else []
        )


class GPTTimeoutError(ExternalServiceException):
    """GPT API 타임아웃"""
    def __init__(self, operation: str, timeout: int):
        super().__init__(
            message=f"GPT 타임아웃: {timeout}초 초과",
            error_code="S002",
            details={"operation": operation, "timeout": timeout},
            recoverable=True,
            severity=ErrorSeverity.MEDIUM,
            recovery_actions=[RecoveryAction.RETRY_WITH_LONGER_TIMEOUT]
        )


class GPTRateLimitError(ExternalServiceException):
    """GPT API 속도 제한"""
    def __init__(self, retry_after: Optional[int] = None):
        super().__init__(
            message="GPT API 속도 제한 도달",
            error_code="S003",
            details={},
            recoverable=True,
            severity=ErrorSeverity.MEDIUM,
            recovery_actions=[RecoveryAction.WAIT_AND_RETRY],
            retry_after=retry_after or 60
        )


class GPTTokenLimitError(ExternalServiceException):
    """GPT 토큰 한도 초과"""
    def __init__(self, requested: int, limit: int):
        super().__init__(
            message=f"토큰 한도 초과: {requested}/{limit}",
            error_code="S004",
            details={"requested_tokens": requested, "limit": limit},
            recoverable=False,
            severity=ErrorSeverity.HIGH,
            recovery_actions=[RecoveryAction.NOTIFY_ADMIN]
        )


class GPTInvalidResponseError(ExternalServiceException):
    """GPT 응답 파싱 실패"""
    def __init__(self, expected_format: str, raw_response: str):
        super().__init__(
            message=f"GPT 응답 형식 오류: {expected_format} 예상",
            error_code="S005",
            details={
                "expected_format": expected_format,
                "raw_response": raw_response[:500]
            },
            recoverable=True,
            severity=ErrorSeverity.MEDIUM,
            recovery_actions=[RecoveryAction.RETRY]
        )


# ============================================
# 데이터베이스 예외 (D001-D099)
# ============================================

class DatabaseException(CrawlerSystemException):
    """데이터베이스 관련 예외"""
    pass


class DatabaseConnectionError(DatabaseException):
    """DB 연결 실패"""
    def __init__(self, reason: str, host: str = ""):
        super().__init__(
            message=f"데이터베이스 연결 실패: {reason}",
            error_code="D001",
            details={"reason": reason, "host": host},
            recoverable=True,
            severity=ErrorSeverity.CRITICAL,
            recovery_actions=[RecoveryAction.RETRY_WITH_BACKOFF]
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
            recoverable=True,
            severity=ErrorSeverity.HIGH,
            recovery_actions=[RecoveryAction.RETRY_WITH_BACKOFF]
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
            recoverable=False,
            severity=ErrorSeverity.MEDIUM
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
            recoverable=False,
            severity=ErrorSeverity.LOW
        )


class DatabaseTransactionError(DatabaseException):
    """트랜잭션 실패"""
    def __init__(self, operation: str, reason: str, rollback_success: bool = False):
        super().__init__(
            message=f"트랜잭션 실패: {reason}",
            error_code="D005",
            details={
                "operation": operation,
                "reason": reason,
                "rollback_success": rollback_success
            },
            recoverable=False,
            severity=ErrorSeverity.CRITICAL
        )


# ============================================
# 자가 치유 예외 (H001-H099)
# ============================================

class HealingException(CrawlerSystemException):
    """자가 치유 관련 예외"""
    pass


class HealingMaxRetriesError(HealingException):
    """최대 재시도 횟수 초과"""
    def __init__(self, source_id: str, attempts: int, last_error: str = ""):
        super().__init__(
            message=f"자가 치유 실패: {attempts}회 시도 후 포기",
            error_code="H001",
            details={
                "source_id": source_id,
                "attempts": attempts,
                "last_error": last_error
            },
            recoverable=False,
            severity=ErrorSeverity.CRITICAL,
            recovery_actions=[RecoveryAction.NOTIFY_ADMIN]
        )


class HealingTimeoutError(HealingException):
    """자가 치유 타임아웃"""
    def __init__(self, source_id: str, elapsed_time: int, max_time: int = 3600):
        super().__init__(
            message=f"자가 치유 타임아웃: {elapsed_time}초 경과",
            error_code="H002",
            details={
                "source_id": source_id,
                "elapsed_time": elapsed_time,
                "max_time": max_time
            },
            recoverable=False,
            severity=ErrorSeverity.HIGH,
            recovery_actions=[RecoveryAction.NOTIFY_ADMIN]
        )


class HealingDiagnosisError(HealingException):
    """진단 실패"""
    def __init__(self, source_id: str, reason: str, error_code_analyzed: str = ""):
        super().__init__(
            message=f"진단 실패: {reason}",
            error_code="H003",
            details={
                "source_id": source_id,
                "reason": reason,
                "error_code_analyzed": error_code_analyzed
            },
            recoverable=True,
            severity=ErrorSeverity.MEDIUM,
            recovery_actions=[RecoveryAction.RETRY]
        )


class HealingCodeGenerationError(HealingException):
    """코드 생성 실패"""
    def __init__(self, source_id: str, reason: str):
        super().__init__(
            message=f"코드 생성 실패: {reason}",
            error_code="H004",
            details={"source_id": source_id, "reason": reason},
            recoverable=True,
            severity=ErrorSeverity.HIGH,
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.NOTIFY_ADMIN]
        )


# ============================================
# Circuit Breaker 예외 (B001-B099)
# ============================================

class CircuitBreakerException(CrawlerSystemException):
    """Circuit Breaker 관련 예외"""
    pass


class CircuitOpenError(CircuitBreakerException):
    """Circuit이 열린 상태"""
    def __init__(self, service_name: str, failure_count: int, reset_time: int):
        super().__init__(
            message=f"서비스 '{service_name}' Circuit이 열림 (실패: {failure_count}회)",
            error_code="B001",
            details={
                "service_name": service_name,
                "failure_count": failure_count,
                "reset_time": reset_time
            },
            recoverable=True,
            severity=ErrorSeverity.HIGH,
            retry_after=reset_time,
            recovery_actions=[RecoveryAction.WAIT_AND_RETRY]
        )


# ============================================
# 예외 매핑 및 유틸리티
# ============================================

ERROR_CODE_MAPPING = {
    # Validation
    "V001": URLValidationError,
    "V002": SchemaValidationError,
    "V003": SelectorValidationError,
    "V004": CronValidationError,
    "V005": DataTypeValidationError,
    "V006": ObjectIdValidationError,
    # Crawler
    "E001": RequestTimeoutError,
    "E002": SelectorNotFoundError,
    "E003": AuthenticationRequiredError,
    "E004": SiteStructureChangedError,
    "E005": RateLimitError,
    "E006": DataParsingError,
    "E007": CrawlerConnectionError,
    "E008": InvalidHTTPResponseError,
    "E009": FileProcessingError,
    "E010": UnknownCrawlerError,
    # External Service
    "S001": GPTServiceError,
    "S002": GPTTimeoutError,
    "S003": GPTRateLimitError,
    "S004": GPTTokenLimitError,
    "S005": GPTInvalidResponseError,
    # Database
    "D001": DatabaseConnectionError,
    "D002": DatabaseOperationError,
    "D003": DuplicateKeyError,
    "D004": DocumentNotFoundError,
    "D005": DatabaseTransactionError,
    # Healing
    "H001": HealingMaxRetriesError,
    "H002": HealingTimeoutError,
    "H003": HealingDiagnosisError,
    "H004": HealingCodeGenerationError,
    # Circuit Breaker
    "B001": CircuitOpenError,
}


def get_exception_class(error_code: str) -> type:
    """에러 코드로 예외 클래스 조회"""
    return ERROR_CODE_MAPPING.get(error_code, CrawlerSystemException)


def is_recoverable(error_code: str) -> bool:
    """에러 코드로 복구 가능 여부 확인"""
    exception_class = ERROR_CODE_MAPPING.get(error_code)
    if not exception_class:
        return False

    # 기본 인스턴스 생성하여 확인 (서브클래스마다 다름)
    recoverable_codes = {
        "E001", "E002", "E004", "E005", "E006", "E007", "E008",  # Crawler
        "S001", "S002", "S003", "S005",  # Service
        "D001", "D002",  # Database
        "H003", "H004",  # Healing
        "B001",  # Circuit Breaker
    }
    return error_code in recoverable_codes


def get_recovery_actions(error_code: str) -> List[RecoveryAction]:
    """에러 코드별 복구 액션 조회"""
    recovery_map = {
        "E001": [RecoveryAction.RETRY_WITH_LONGER_TIMEOUT, RecoveryAction.RETRY_WITH_BACKOFF],
        "E002": [RecoveryAction.GPT_FIX_SELECTORS, RecoveryAction.GPT_REGENERATE_CODE],
        "E003": [RecoveryAction.NOTIFY_ADMIN],
        "E004": [RecoveryAction.GPT_REGENERATE_CODE],
        "E005": [RecoveryAction.WAIT_AND_RETRY, RecoveryAction.SWITCH_PROXY],
        "E006": [RecoveryAction.GPT_FIX_SELECTORS],
        "E007": [RecoveryAction.RETRY_WITH_BACKOFF],
        "E008": [RecoveryAction.RETRY_WITH_BACKOFF],
        "E009": [RecoveryAction.NOTIFY_ADMIN],
        "E010": [RecoveryAction.NOTIFY_ADMIN],
        "S001": [RecoveryAction.RETRY_WITH_BACKOFF],
        "S002": [RecoveryAction.RETRY_WITH_LONGER_TIMEOUT],
        "S003": [RecoveryAction.WAIT_AND_RETRY],
        "S004": [RecoveryAction.NOTIFY_ADMIN],
        "D001": [RecoveryAction.RETRY_WITH_BACKOFF],
        "D002": [RecoveryAction.RETRY_WITH_BACKOFF],
        "H001": [RecoveryAction.NOTIFY_ADMIN],
        "H002": [RecoveryAction.NOTIFY_ADMIN],
        "H003": [RecoveryAction.RETRY],
        "B001": [RecoveryAction.WAIT_AND_RETRY],
    }
    return recovery_map.get(error_code, [])


def create_exception_from_code(
    error_code: str,
    message: str = "",
    **kwargs
) -> CrawlerSystemException:
    """에러 코드로 예외 인스턴스 생성"""
    exception_class = ERROR_CODE_MAPPING.get(error_code)

    if not exception_class:
        return CrawlerSystemException(
            message=message or f"Unknown error: {error_code}",
            error_code=error_code,
            **kwargs
        )

    # 각 예외 클래스의 필수 파라미터에 맞게 생성
    # 기본값으로 생성 시도
    try:
        return exception_class(**kwargs)
    except TypeError:
        # 파라미터 불일치 시 기본 예외 반환
        return CrawlerSystemException(
            message=message or f"Error: {error_code}",
            error_code=error_code,
            details=kwargs
        )


__all__ = [
    # Base
    "CrawlerSystemException",
    "ErrorSeverity",
    "RecoveryAction",
    # Validation
    "ValidationException",
    "URLValidationError",
    "SchemaValidationError",
    "SelectorValidationError",
    "CronValidationError",
    "DataTypeValidationError",
    "ObjectIdValidationError",
    # Crawler
    "CrawlerException",
    "RequestTimeoutError",
    "SelectorNotFoundError",
    "AuthenticationRequiredError",
    "SiteStructureChangedError",
    "RateLimitError",
    "DataParsingError",
    "CrawlerConnectionError",
    "InvalidHTTPResponseError",
    "FileProcessingError",
    "UnknownCrawlerError",
    # External Service
    "ExternalServiceException",
    "GPTServiceError",
    "GPTTimeoutError",
    "GPTRateLimitError",
    "GPTTokenLimitError",
    "GPTInvalidResponseError",
    # Database
    "DatabaseException",
    "DatabaseConnectionError",
    "DatabaseOperationError",
    "DuplicateKeyError",
    "DocumentNotFoundError",
    "DatabaseTransactionError",
    # Healing
    "HealingException",
    "HealingMaxRetriesError",
    "HealingTimeoutError",
    "HealingDiagnosisError",
    "HealingCodeGenerationError",
    # Circuit Breaker
    "CircuitBreakerException",
    "CircuitOpenError",
    # Utilities
    "ERROR_CODE_MAPPING",
    "get_exception_class",
    "is_recoverable",
    "get_recovery_actions",
    "create_exception_from_code",
]

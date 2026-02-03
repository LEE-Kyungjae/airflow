"""
Error Handler for crawler error classification and recovery.

This module classifies crawler errors and determines the appropriate
recovery action based on error type.
"""

import re
import logging
import traceback
from enum import Enum
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class ErrorCode(Enum):
    """Error codes for crawler failures."""

    E001_TIMEOUT = "E001"           # Request timeout
    E002_SELECTOR_FAIL = "E002"     # CSS selector not found
    E003_AUTH_REQUIRED = "E003"     # Authentication required
    E004_SITE_CHANGED = "E004"      # Site structure changed significantly
    E005_BLOCKED = "E005"           # IP blocked / rate limited
    E006_PARSE_ERROR = "E006"       # Data parsing error
    E007_CONNECTION_ERROR = "E007"  # Network connection error
    E008_INVALID_RESPONSE = "E008"  # Invalid HTTP response
    E009_FILE_ERROR = "E009"        # File download/processing error
    E010_UNKNOWN = "E010"           # Unknown error


@dataclass
class ErrorClassification:
    """Error classification result."""
    code: ErrorCode
    error_type: str
    message: str
    stack_trace: str
    auto_recoverable: bool
    recovery_action: str
    max_retries: int


class ErrorHandler:
    """Handler for classifying and managing crawler errors."""

    # Error patterns for classification
    ERROR_PATTERNS = {
        ErrorCode.E001_TIMEOUT: [
            r'timeout',
            r'timed out',
            r'TimeoutError',
            r'ReadTimeout',
            r'ConnectTimeout'
        ],
        ErrorCode.E002_SELECTOR_FAIL: [
            r'no such element',
            r'element not found',
            r'selector.*not found',
            r'NoneType.*has no attribute',
            r'ResultSet.*is not found',
            r'find.*returned None',
            r'IndexError.*list index out of range'
        ],
        ErrorCode.E003_AUTH_REQUIRED: [
            r'401',
            r'403',
            r'unauthorized',
            r'forbidden',
            r'login required',
            r'authentication required',
            r'access denied'
        ],
        ErrorCode.E004_SITE_CHANGED: [
            r'unexpected.*structure',
            r'schema.*changed',
            r'layout.*different',
            r'page.*redesigned'
        ],
        ErrorCode.E005_BLOCKED: [
            r'429',
            r'too many requests',
            r'rate limit',
            r'blocked',
            r'captcha',
            r'bot.*detected',
            r'access.*denied.*temporarily'
        ],
        ErrorCode.E006_PARSE_ERROR: [
            r'JSONDecodeError',
            r'ValueError.*could not convert',
            r'parsing.*error',
            r'invalid.*format',
            r'decode.*error',
            r'UnicodeDecodeError'
        ],
        ErrorCode.E007_CONNECTION_ERROR: [
            r'ConnectionError',
            r'ConnectionRefused',
            r'DNS.*failed',
            r'network.*unreachable',
            r'host.*not found',
            r'NameResolutionError'
        ],
        ErrorCode.E008_INVALID_RESPONSE: [
            r'500',
            r'502',
            r'503',
            r'504',
            r'internal server error',
            r'bad gateway',
            r'service unavailable'
        ],
        ErrorCode.E009_FILE_ERROR: [
            r'file.*not found',
            r'permission denied',
            r'invalid.*pdf',
            r'corrupt.*file',
            r'unsupported.*format'
        ]
    }

    # Recovery configurations
    RECOVERY_CONFIG = {
        ErrorCode.E001_TIMEOUT: {
            'auto_recoverable': True,
            'action': 'retry_with_longer_timeout',
            'max_retries': 3
        },
        ErrorCode.E002_SELECTOR_FAIL: {
            'auto_recoverable': True,
            'action': 'gpt_fix_selectors',
            'max_retries': 2
        },
        ErrorCode.E003_AUTH_REQUIRED: {
            'auto_recoverable': False,
            'action': 'notify_manual_intervention',
            'max_retries': 0
        },
        ErrorCode.E004_SITE_CHANGED: {
            'auto_recoverable': True,
            'action': 'gpt_regenerate_code',
            'max_retries': 1
        },
        ErrorCode.E005_BLOCKED: {
            'auto_recoverable': True,
            'action': 'switch_proxy_and_retry',
            'max_retries': 3
        },
        ErrorCode.E006_PARSE_ERROR: {
            'auto_recoverable': True,
            'action': 'gpt_fix_parsing',
            'max_retries': 2
        },
        ErrorCode.E007_CONNECTION_ERROR: {
            'auto_recoverable': True,
            'action': 'retry_with_backoff',
            'max_retries': 5
        },
        ErrorCode.E008_INVALID_RESPONSE: {
            'auto_recoverable': True,
            'action': 'retry_with_backoff',
            'max_retries': 3
        },
        ErrorCode.E009_FILE_ERROR: {
            'auto_recoverable': True,
            'action': 'gpt_fix_file_handling',
            'max_retries': 2
        },
        ErrorCode.E010_UNKNOWN: {
            'auto_recoverable': False,
            'action': 'notify_and_log',
            'max_retries': 1
        }
    }

    @classmethod
    def classify_error(
        cls,
        exception: Exception,
        html_snapshot: str = ""
    ) -> ErrorClassification:
        """
        Classify an exception into an error code.

        Args:
            exception: The exception that occurred
            html_snapshot: Optional HTML snapshot for context

        Returns:
            ErrorClassification with details
        """
        error_message = str(exception)
        stack_trace = traceback.format_exc()

        # Combined text for pattern matching
        search_text = f"{error_message} {stack_trace}".lower()

        # Try to match against known patterns
        matched_code = ErrorCode.E010_UNKNOWN

        for code, patterns in cls.ERROR_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, search_text, re.IGNORECASE):
                    matched_code = code
                    break
            if matched_code != ErrorCode.E010_UNKNOWN:
                break

        # Additional context-based classification
        if matched_code == ErrorCode.E010_UNKNOWN:
            matched_code = cls._classify_by_context(exception, html_snapshot)

        # Get recovery config
        config = cls.RECOVERY_CONFIG.get(matched_code, cls.RECOVERY_CONFIG[ErrorCode.E010_UNKNOWN])

        return ErrorClassification(
            code=matched_code,
            error_type=matched_code.name,
            message=error_message[:500],  # Truncate long messages
            stack_trace=stack_trace[:2000],  # Truncate long traces
            auto_recoverable=config['auto_recoverable'],
            recovery_action=config['action'],
            max_retries=config['max_retries']
        )

    @classmethod
    def _classify_by_context(
        cls,
        exception: Exception,
        html_snapshot: str
    ) -> ErrorCode:
        """
        Additional classification based on context.

        Args:
            exception: The exception
            html_snapshot: HTML content for analysis

        Returns:
            ErrorCode based on context analysis
        """
        # Check HTML for login forms or captcha
        html_lower = html_snapshot.lower() if html_snapshot else ""

        if any(term in html_lower for term in ['login', 'sign in', 'password']):
            return ErrorCode.E003_AUTH_REQUIRED

        if any(term in html_lower for term in ['captcha', 'verify', 'robot']):
            return ErrorCode.E005_BLOCKED

        # Check exception type
        exception_type = type(exception).__name__

        if 'Timeout' in exception_type:
            return ErrorCode.E001_TIMEOUT

        if 'Connection' in exception_type:
            return ErrorCode.E007_CONNECTION_ERROR

        return ErrorCode.E010_UNKNOWN

    @classmethod
    def get_recovery_action(cls, error_code: ErrorCode) -> Dict[str, Any]:
        """
        Get the recommended recovery action for an error code.

        Args:
            error_code: The error code

        Returns:
            Recovery configuration dict
        """
        return cls.RECOVERY_CONFIG.get(error_code, cls.RECOVERY_CONFIG[ErrorCode.E010_UNKNOWN])

    @classmethod
    def should_retry(cls, error_code: ErrorCode, attempt: int) -> bool:
        """
        Check if a retry should be attempted.

        Args:
            error_code: The error code
            attempt: Current attempt number (1-based)

        Returns:
            True if should retry, False otherwise
        """
        config = cls.RECOVERY_CONFIG.get(error_code, cls.RECOVERY_CONFIG[ErrorCode.E010_UNKNOWN])
        return attempt <= config['max_retries']

    @classmethod
    def requires_gpt_fix(cls, error_code: ErrorCode) -> bool:
        """
        Check if the error requires GPT-based code fix.

        Args:
            error_code: The error code

        Returns:
            True if GPT fix is needed
        """
        config = cls.RECOVERY_CONFIG.get(error_code, {})
        action = config.get('action', '')
        return 'gpt' in action.lower()

    @classmethod
    def format_error_for_alert(
        cls,
        classification: ErrorClassification,
        source_name: str,
        url: str
    ) -> str:
        """
        Format error for email/notification alert.

        Args:
            classification: The error classification
            source_name: Name of the source
            url: URL being crawled

        Returns:
            Formatted alert message
        """
        recovery_status = "자동 복구 시도 중" if classification.auto_recoverable else "수동 처리 필요"

        return f"""
크롤링 오류 알림

소스: {source_name}
URL: {url}

오류 코드: {classification.code.value}
오류 유형: {classification.error_type}
메시지: {classification.message}

복구 상태: {recovery_status}
권장 조치: {classification.recovery_action}

스택 트레이스:
{classification.stack_trace[:1000]}
"""

    @classmethod
    def create_error_log_data(
        cls,
        classification: ErrorClassification,
        source_id: str,
        crawler_id: Optional[str] = None,
        run_id: Optional[str] = None,
        html_snapshot: str = ""
    ) -> Dict[str, Any]:
        """
        Create error log data for MongoDB.

        Args:
            classification: Error classification
            source_id: Source ID
            crawler_id: Crawler ID (optional)
            run_id: Airflow run ID (optional)
            html_snapshot: HTML snapshot (optional)

        Returns:
            Dict ready for MongoDB insertion
        """
        return {
            'source_id': source_id,
            'crawler_id': crawler_id,
            'run_id': run_id,
            'error_code': classification.code.value,
            'error_type': classification.error_type,
            'message': classification.message,
            'stack_trace': classification.stack_trace,
            'html_snapshot': html_snapshot[:10000] if html_snapshot else None,
            'auto_recoverable': classification.auto_recoverable
        }

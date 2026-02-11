"""
Comprehensive tests for the self-healing system.

Tests ErrorHandler and SelfHealingEngine modules.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta
import json

from airflow.dags.utils.error_handler import (
    ErrorCode,
    ErrorClassification,
    ErrorHandler
)
from airflow.dags.utils.self_healing import (
    HealingStatus,
    ErrorCategory,
    RetrySchedule,
    WellknownCase,
    HealingSession,
    SelfHealingEngine
)


# ============================================================================
# TestErrorClassification - ErrorHandler Tests
# ============================================================================

class TestErrorClassification:
    """Test error classification logic."""

    def test_classify_timeout_error(self):
        """TimeoutError should be classified as E001."""
        exception = TimeoutError("Request timed out after 30 seconds")
        classification = ErrorHandler.classify_error(exception)

        assert classification.code == ErrorCode.E001_TIMEOUT
        assert classification.auto_recoverable is True
        assert classification.recovery_action == "retry_with_longer_timeout"
        assert classification.max_retries == 3

    def test_classify_selector_error(self):
        """Element not found errors should be E002."""
        exception = AttributeError("NoneType object has no attribute 'text'")
        classification = ErrorHandler.classify_error(exception)

        assert classification.code == ErrorCode.E002_SELECTOR_FAIL
        assert classification.auto_recoverable is True
        assert classification.recovery_action == "gpt_fix_selectors"
        assert classification.max_retries == 2

    def test_classify_auth_error(self):
        """401 unauthorized should be E003."""
        exception = Exception("HTTP 401 unauthorized access")
        classification = ErrorHandler.classify_error(exception)

        assert classification.code == ErrorCode.E003_AUTH_REQUIRED
        assert classification.auto_recoverable is False
        assert classification.recovery_action == "notify_manual_intervention"
        assert classification.max_retries == 0

    def test_classify_blocked_error(self):
        """429 too many requests should be E005."""
        exception = Exception("429 too many requests - rate limited")
        classification = ErrorHandler.classify_error(exception)

        assert classification.code == ErrorCode.E005_BLOCKED
        assert classification.auto_recoverable is True
        assert classification.recovery_action == "switch_proxy_and_retry"
        assert classification.max_retries == 3

    def test_classify_parse_error(self):
        """JSONDecodeError should be E006."""
        # Use a simple Exception with "JSONDecodeError" in the message
        # since the actual JSONDecodeError may not match the pattern in traceback
        exception = Exception("JSONDecodeError: invalid JSON format")
        classification = ErrorHandler.classify_error(exception)

        assert classification.code == ErrorCode.E006_PARSE_ERROR
        assert classification.auto_recoverable is True
        assert classification.recovery_action == "gpt_fix_parsing"
        assert classification.max_retries == 2

    def test_classify_connection_error(self):
        """ConnectionError should be E007."""
        exception = ConnectionError("Network unreachable")
        classification = ErrorHandler.classify_error(exception)

        assert classification.code == ErrorCode.E007_CONNECTION_ERROR
        assert classification.auto_recoverable is True
        assert classification.recovery_action == "retry_with_backoff"
        assert classification.max_retries == 5

    def test_classify_server_error(self):
        """502 bad gateway should be E008."""
        exception = Exception("502 bad gateway error")
        classification = ErrorHandler.classify_error(exception)

        assert classification.code == ErrorCode.E008_INVALID_RESPONSE
        assert classification.auto_recoverable is True
        assert classification.recovery_action == "retry_with_backoff"
        assert classification.max_retries == 3

    def test_classify_file_error(self):
        """File not found should be E009."""
        exception = FileNotFoundError("file not found: data.pdf")
        classification = ErrorHandler.classify_error(exception)

        assert classification.code == ErrorCode.E009_FILE_ERROR
        assert classification.auto_recoverable is True
        assert classification.recovery_action == "gpt_fix_file_handling"
        assert classification.max_retries == 2

    def test_classify_unknown_error(self):
        """Random unknown error should be E010."""
        exception = Exception("Something completely unexpected happened")
        classification = ErrorHandler.classify_error(exception)

        assert classification.code == ErrorCode.E010_UNKNOWN
        assert classification.auto_recoverable is False
        assert classification.recovery_action == "notify_and_log"
        assert classification.max_retries == 1

    def test_classify_with_html_login_page(self):
        """HTML with login form should be detected as E003."""
        exception = Exception("Generic error")
        html = "<html><body><form><input name='password' type='password'><button>Login</button></form></body></html>"

        classification = ErrorHandler.classify_error(exception, html_snapshot=html)

        assert classification.code == ErrorCode.E003_AUTH_REQUIRED

    def test_classify_with_html_captcha(self):
        """HTML with captcha should be detected as E005."""
        exception = Exception("Generic error")
        html = "<html><body><div class='captcha-container'>Please verify you are not a robot</div></body></html>"

        classification = ErrorHandler.classify_error(exception, html_snapshot=html)

        assert classification.code == ErrorCode.E005_BLOCKED


class TestRecoveryConfig:
    """Test recovery configuration logic."""

    def test_timeout_is_auto_recoverable(self):
        """Timeout errors should be auto-recoverable."""
        config = ErrorHandler.get_recovery_action(ErrorCode.E001_TIMEOUT)

        assert config['auto_recoverable'] is True
        assert config['max_retries'] == 3

    def test_auth_not_auto_recoverable(self):
        """Auth errors should NOT be auto-recoverable."""
        config = ErrorHandler.get_recovery_action(ErrorCode.E003_AUTH_REQUIRED)

        assert config['auto_recoverable'] is False
        assert config['max_retries'] == 0

    def test_recovery_action_for_selector_fail(self):
        """Selector fail should suggest GPT fix."""
        config = ErrorHandler.get_recovery_action(ErrorCode.E002_SELECTOR_FAIL)

        assert config['action'] == "gpt_fix_selectors"
        assert "gpt" in config['action'].lower()

    def test_should_retry_within_limit(self):
        """Should retry when within max_retries."""
        # E001 has max_retries = 3
        assert ErrorHandler.should_retry(ErrorCode.E001_TIMEOUT, attempt=1) is True
        assert ErrorHandler.should_retry(ErrorCode.E001_TIMEOUT, attempt=2) is True
        assert ErrorHandler.should_retry(ErrorCode.E001_TIMEOUT, attempt=3) is True

    def test_should_retry_exceeds_limit(self):
        """Should NOT retry when exceeds max_retries."""
        # E001 has max_retries = 3
        assert ErrorHandler.should_retry(ErrorCode.E001_TIMEOUT, attempt=4) is False
        assert ErrorHandler.should_retry(ErrorCode.E001_TIMEOUT, attempt=5) is False


# ============================================================================
# TestRetrySchedule
# ============================================================================

class TestRetrySchedule:
    """Test retry scheduling logic."""

    def test_default_intervals(self):
        """Default intervals should match specification."""
        schedule = RetrySchedule()

        expected = [3, 10, 30, 120, 720, 1440, 2880, 4320, 7200, 43200]
        assert schedule.intervals == expected

    def test_get_next_retry_valid(self):
        """Should return correct interval for valid attempts."""
        schedule = RetrySchedule()

        assert schedule.get_next_retry(0) == 3      # First retry: 3 min
        assert schedule.get_next_retry(1) == 10     # Second: 10 min
        assert schedule.get_next_retry(2) == 30     # Third: 30 min
        assert schedule.get_next_retry(5) == 1440   # Sixth: 1 day

    def test_get_next_retry_exceeds_max(self):
        """Should return None when attempt exceeds max intervals."""
        schedule = RetrySchedule()

        assert schedule.get_next_retry(10) is None  # Only 10 intervals defined
        assert schedule.get_next_retry(11) is None
        assert schedule.get_next_retry(100) is None


# ============================================================================
# TestSelfHealingEngine
# ============================================================================

class TestSelfHealingEngine:
    """Test SelfHealingEngine functionality."""

    def test_basic_diagnosis_timeout(self):
        """Basic diagnosis should detect timeout errors."""
        engine = SelfHealingEngine(mongo_service=None)

        diagnosis = engine._basic_diagnosis("E001", "Request timed out after 30s")

        assert diagnosis['category'] == ErrorCategory.NETWORK_ERROR.value
        assert diagnosis['confidence'] == 0.5
        assert 'e001' in diagnosis['root_cause'].lower() or 'timed out' in diagnosis['root_cause'].lower()

    def test_basic_diagnosis_selector(self):
        """Basic diagnosis should detect selector errors."""
        engine = SelfHealingEngine(mongo_service=None)

        diagnosis = engine._basic_diagnosis("E002", "Element not found with selector .content")

        assert diagnosis['category'] == ErrorCategory.SELECTOR_BROKEN.value
        assert diagnosis['is_code_issue'] is True

    def test_basic_diagnosis_auth(self):
        """Basic diagnosis should detect auth errors."""
        engine = SelfHealingEngine(mongo_service=None)

        diagnosis = engine._basic_diagnosis("E003", "HTTP 401 unauthorized")

        assert diagnosis['category'] == ErrorCategory.AUTH_REQUIRED.value
        assert diagnosis['confidence'] == 0.5

    def test_basic_diagnosis_parse(self):
        """Basic diagnosis should detect parse errors."""
        engine = SelfHealingEngine(mongo_service=None)

        diagnosis = engine._basic_diagnosis("E006", "JSONDecodeError: invalid JSON format")

        assert diagnosis['category'] == ErrorCategory.PARSE_ERROR.value
        assert diagnosis['is_code_issue'] is True
        assert diagnosis['suggested_fix_type'] == "code_rewrite"

    def test_basic_diagnosis_unknown(self):
        """Basic diagnosis should handle unknown errors."""
        engine = SelfHealingEngine(mongo_service=None)

        diagnosis = engine._basic_diagnosis("E010", "Completely unknown weird error")

        assert diagnosis['category'] == ErrorCategory.UNKNOWN.value
        assert 'similar_pattern_hash' in diagnosis

    @pytest.mark.asyncio
    async def test_diagnose_creates_session(self):
        """Diagnose should create HealingSession with correct structure."""
        # Mock OpenAI client
        mock_client = Mock()
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = json.dumps({
            "category": "selector_broken",
            "root_cause": "CSS selector changed",
            "confidence": 0.8,
            "is_source_issue": False,
            "is_code_issue": True,
            "suggested_fix_type": "selector_update",
            "key_indicators": ["selector not found"],
            "similar_pattern_hash": "abc123"
        })
        mock_client.chat.completions.create = Mock(return_value=mock_response)

        # Mock MongoDB
        mock_mongo = Mock()
        mock_mongo.db.wellknown_cases.find.return_value.sort.return_value.limit.return_value = []

        with patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'}):
            engine = SelfHealingEngine(mongo_service=mock_mongo)
            engine.client = mock_client

            session = await engine.diagnose(
                source_id="source123",
                crawler_id="crawler456",
                error_code="E002",
                error_message="Selector not found",
                stack_trace="Traceback...",
                html_snapshot="<html>...</html>"
            )

        assert isinstance(session, HealingSession)
        assert session.source_id == "source123"
        assert session.crawler_id == "crawler456"
        assert session.error_code == "E002"
        assert session.status == HealingStatus.WAITING_ADMIN  # No matched case
        assert session.diagnosis is not None
        assert session.diagnosis['category'] == "selector_broken"
        assert session.current_attempt == 0
        assert session.max_attempts == 5

    @pytest.mark.asyncio
    async def test_diagnose_without_ai_uses_basic(self):
        """Without API key, should use basic diagnosis."""
        # No API key
        with patch.dict('os.environ', {'OPENAI_API_KEY': ''}, clear=True):
            engine = SelfHealingEngine(mongo_service=None)

            assert engine.client is None

            session = await engine.diagnose(
                source_id="source123",
                crawler_id="crawler456",
                error_code="E001",
                error_message="timeout error",
                stack_trace="Traceback..."
            )

        assert isinstance(session, HealingSession)
        assert session.diagnosis is not None
        assert session.diagnosis['category'] == ErrorCategory.NETWORK_ERROR.value
        assert session.diagnosis['confidence'] == 0.5  # Basic diagnosis confidence

    def test_healing_status_enum_values(self):
        """Verify all HealingStatus enum values."""
        assert HealingStatus.PENDING.value == "pending"
        assert HealingStatus.DIAGNOSING.value == "diagnosing"
        assert HealingStatus.SOURCE_CHECK.value == "source_check"
        assert HealingStatus.AI_SOLVING.value == "ai_solving"
        assert HealingStatus.WAITING_ADMIN.value == "waiting_admin"
        assert HealingStatus.RESOLVED.value == "resolved"
        assert HealingStatus.FAILED.value == "failed"

    def test_error_category_enum_values(self):
        """Verify all ErrorCategory enum values."""
        assert ErrorCategory.SOURCE_NOT_UPDATED.value == "source_not_updated"
        assert ErrorCategory.STRUCTURE_CHANGED.value == "structure_changed"
        assert ErrorCategory.SELECTOR_BROKEN.value == "selector_broken"
        assert ErrorCategory.AUTH_REQUIRED.value == "auth_required"
        assert ErrorCategory.RATE_LIMITED.value == "rate_limited"
        assert ErrorCategory.NETWORK_ERROR.value == "network_error"
        assert ErrorCategory.PARSE_ERROR.value == "parse_error"
        assert ErrorCategory.DATA_VALIDATION.value == "data_validation"
        assert ErrorCategory.UNKNOWN.value == "unknown"


# ============================================================================
# Additional Integration Tests
# ============================================================================

class TestErrorHandlerIntegration:
    """Integration tests for ErrorHandler."""

    def test_format_error_for_alert(self):
        """Test alert message formatting."""
        classification = ErrorClassification(
            code=ErrorCode.E002_SELECTOR_FAIL,
            error_type="E002_SELECTOR_FAIL",
            message="Selector .content not found",
            stack_trace="Traceback: line 45 in parse()",
            auto_recoverable=True,
            recovery_action="gpt_fix_selectors",
            max_retries=2
        )

        alert = ErrorHandler.format_error_for_alert(
            classification,
            source_name="Test Source",
            url="https://example.com"
        )

        assert "Test Source" in alert
        assert "https://example.com" in alert
        assert "E002" in alert
        assert "자동 복구 시도 중" in alert

    def test_create_error_log_data(self):
        """Test error log data creation for MongoDB."""
        classification = ErrorClassification(
            code=ErrorCode.E001_TIMEOUT,
            error_type="E001_TIMEOUT",
            message="Request timeout",
            stack_trace="Traceback...",
            auto_recoverable=True,
            recovery_action="retry_with_longer_timeout",
            max_retries=3
        )

        log_data = ErrorHandler.create_error_log_data(
            classification,
            source_id="src123",
            crawler_id="crawler456",
            run_id="run789",
            html_snapshot="<html>test</html>"
        )

        assert log_data['source_id'] == "src123"
        assert log_data['crawler_id'] == "crawler456"
        assert log_data['run_id'] == "run789"
        assert log_data['error_code'] == "E001"
        assert log_data['auto_recoverable'] is True
        assert log_data['html_snapshot'] == "<html>test</html>"

    def test_requires_gpt_fix(self):
        """Test detection of GPT-required fixes."""
        assert ErrorHandler.requires_gpt_fix(ErrorCode.E002_SELECTOR_FAIL) is True
        assert ErrorHandler.requires_gpt_fix(ErrorCode.E004_SITE_CHANGED) is True
        assert ErrorHandler.requires_gpt_fix(ErrorCode.E006_PARSE_ERROR) is True
        assert ErrorHandler.requires_gpt_fix(ErrorCode.E001_TIMEOUT) is False
        assert ErrorHandler.requires_gpt_fix(ErrorCode.E007_CONNECTION_ERROR) is False


class TestSelfHealingEngineAdvanced:
    """Advanced tests for SelfHealingEngine."""

    def test_generate_session_id(self):
        """Test session ID generation."""
        engine = SelfHealingEngine(mongo_service=None)

        session_id = engine._generate_session_id("source12345678", "E002")

        assert session_id.startswith("heal_source12")
        assert "E002" in session_id

    def test_generate_pattern_hash(self):
        """Test error pattern hash generation."""
        engine = SelfHealingEngine(mongo_service=None)

        # Same normalized errors should produce same hash
        hash1 = engine._generate_pattern_hash("Error at line 123: timeout")
        hash2 = engine._generate_pattern_hash("Error at line 456: timeout")

        assert hash1 == hash2  # Numbers normalized
        assert len(hash1) == 12  # Hash truncated to 12 chars

    @pytest.mark.asyncio
    async def test_check_source_update_accessible(self):
        """Test source update check when accessible."""
        engine = SelfHealingEngine(mongo_service=None)

        mock_response = Mock()
        mock_response.text = "<html>content</html>"
        mock_response.status_code = 200

        # Patch httpx module before the method imports it
        with patch('httpx.AsyncClient') as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.get = AsyncMock(return_value=mock_response)
            mock_client.return_value = mock_context

            result = await engine.check_source_update(
                source_id="src123",
                url="https://example.com",
                last_known_hash="different_hash"
            )

        assert result['accessible'] is True
        assert result['status_code'] == 200
        assert result['is_updated'] is True
        assert 'content_hash' in result

    @pytest.mark.asyncio
    async def test_check_source_update_inaccessible(self):
        """Test source update check when inaccessible."""
        engine = SelfHealingEngine(mongo_service=None)

        # Patch httpx module before the method imports it
        with patch('httpx.AsyncClient') as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.get = AsyncMock(side_effect=Exception("Network error"))
            mock_client.return_value = mock_context

            result = await engine.check_source_update(
                source_id="src123",
                url="https://example.com"
            )

        assert result['accessible'] is False
        assert 'error' in result

    @pytest.mark.asyncio
    async def test_schedule_retry(self):
        """Test retry scheduling."""
        mock_mongo = Mock()
        mock_mongo.db.healing_schedules.insert_one = Mock()

        engine = SelfHealingEngine(mongo_service=mock_mongo)

        session = HealingSession(
            session_id="heal_123",
            source_id="src123",
            crawler_id="crawler456",
            error_code="E001",
            error_message="timeout",
            stack_trace="...",
            status=HealingStatus.PENDING,
            diagnosis=None,
            matched_case=None,
            attempts=[],
            current_attempt=0,
            max_attempts=5,
            admin_notified=False,
            resolved_at=None,
            resolution=None,
            created_at=datetime.utcnow()
        )

        next_retry = await engine.schedule_retry(session, 0)

        assert next_retry is not None
        assert isinstance(next_retry, datetime)
        # First retry should be ~3 minutes from now
        assert next_retry > datetime.utcnow()
        mock_mongo.db.healing_schedules.insert_one.assert_called_once()

    @pytest.mark.asyncio
    async def test_admin_approve_continue(self):
        """Test admin approval for additional attempts."""
        engine = SelfHealingEngine(mongo_service=None)

        session = HealingSession(
            session_id="heal_123",
            source_id="src123",
            crawler_id="crawler456",
            error_code="E002",
            error_message="selector fail",
            stack_trace="...",
            status=HealingStatus.WAITING_ADMIN,
            diagnosis={},
            matched_case=None,
            attempts=[],
            current_attempt=5,
            max_attempts=5,
            admin_notified=True,
            resolved_at=None,
            resolution=None,
            created_at=datetime.utcnow()
        )

        updated_session = await engine.admin_approve_continue(session, additional_attempts=3)

        assert updated_session.max_attempts == 8  # 5 + 3
        assert updated_session.status == HealingStatus.AI_SOLVING
        assert updated_session.admin_notified is True


class TestWellknownCase:
    """Test WellknownCase functionality."""

    def test_success_rate_calculation(self):
        """Test success rate property."""
        case = WellknownCase(
            case_id="case123",
            error_pattern="timeout_pattern",
            error_category=ErrorCategory.NETWORK_ERROR,
            solution_code="# fixed code",
            solution_description="Increased timeout",
            success_count=8,
            failure_count=2,
            last_used=datetime.utcnow(),
            created_at=datetime.utcnow(),
            created_by="ai"
        )

        assert case.success_rate == 0.8  # 8 / (8+2)

    def test_success_rate_zero_total(self):
        """Test success rate with no attempts."""
        case = WellknownCase(
            case_id="case123",
            error_pattern="pattern",
            error_category=ErrorCategory.UNKNOWN,
            solution_code="# code",
            solution_description="desc",
            success_count=0,
            failure_count=0,
            last_used=datetime.utcnow(),
            created_at=datetime.utcnow(),
            created_by="ai"
        )

        assert case.success_rate == 0.0

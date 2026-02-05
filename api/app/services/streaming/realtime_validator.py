"""
Real-time Data Validator for Streaming Pipeline.

This module provides real-time validation of incoming data events,
integrating with the existing data_quality validation framework.

Features:
- Stream-based validation (per-event)
- Async validation for high throughput
- Integration with existing ValidationProfile
- Validation result events emission
- Caching for validation rules
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from functools import lru_cache
from collections import defaultdict
import hashlib

from .event_types import (
    BaseEvent,
    DataEvent,
    ValidationEvent,
    EventType,
    EventPriority,
    EventSource,
    EventMetadata
)
from .change_stream import EventHandler
from ..data_quality.validator import (
    DataValidator,
    ValidationProfile,
    ValidationResult
)
from ..data_quality.rules import (
    ValidationRule,
    ValidationIssue,
    ValidationSeverity,
    EncodingRule,
    DateRule,
    RequiredFieldRule,
    RangeRule,
    FormatRule,
    UniqueRule
)

logger = logging.getLogger(__name__)


@dataclass
class ValidationStats:
    """Real-time validation statistics."""
    total_validated: int = 0
    passed: int = 0
    failed: int = 0
    warnings: int = 0
    validation_time_total_ms: float = 0
    last_validated_at: Optional[datetime] = None

    # Per-source stats
    by_source: Dict[str, Dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: {
        "total": 0, "passed": 0, "failed": 0
    }))

    # Per-rule stats
    by_rule: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    @property
    def pass_rate(self) -> float:
        if self.total_validated == 0:
            return 100.0
        return round(self.passed / self.total_validated * 100, 2)

    @property
    def avg_validation_time_ms(self) -> float:
        if self.total_validated == 0:
            return 0
        return round(self.validation_time_total_ms / self.total_validated, 2)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_validated": self.total_validated,
            "passed": self.passed,
            "failed": self.failed,
            "warnings": self.warnings,
            "pass_rate": self.pass_rate,
            "avg_validation_time_ms": self.avg_validation_time_ms,
            "last_validated_at": self.last_validated_at.isoformat() if self.last_validated_at else None,
            "by_source": dict(self.by_source),
            "by_rule": dict(self.by_rule)
        }


@dataclass
class RealtimeValidationResult:
    """Result of real-time validation for a single event."""
    event_id: str
    source_id: str
    document_id: str
    collection: str
    passed: bool
    quality_score: float
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    validation_time_ms: float = 0
    validated_at: datetime = field(default_factory=datetime.utcnow)
    rules_applied: List[str] = field(default_factory=list)

    def to_event(self) -> ValidationEvent:
        """Convert to ValidationEvent for emission."""
        return ValidationEvent(
            event_type=EventType.DATA_VALIDATED if self.passed else EventType.DATA_VALIDATION_FAILED,
            priority=EventPriority.NORMAL if self.passed else EventPriority.HIGH,
            metadata=EventMetadata(
                source=EventSource.API_ENDPOINT,
                causation_id=self.event_id
            ),
            document_id=self.document_id,
            collection=self.collection,
            source_id=self.source_id,
            validation_passed=self.passed,
            validation_rules=self.rules_applied,
            errors=self.errors,
            warnings=self.warnings,
            quality_score=self.quality_score
        )


class ValidatorCache:
    """
    Cache for validation profiles and rules.

    Reduces overhead of creating validators for each event.
    """

    def __init__(self, ttl_seconds: int = 300):
        self._validators: Dict[str, Tuple[DataValidator, datetime]] = {}
        self._profiles: Dict[str, Tuple[ValidationProfile, datetime]] = {}
        self._ttl = timedelta(seconds=ttl_seconds)
        self._lock = asyncio.Lock()

    async def get_validator(
        self,
        source_id: str,
        source_config: Dict[str, Any] = None
    ) -> DataValidator:
        """Get or create validator for source."""
        async with self._lock:
            now = datetime.utcnow()

            # Check cache
            if source_id in self._validators:
                validator, cached_at = self._validators[source_id]
                if now - cached_at < self._ttl:
                    return validator

            # Create new validator
            if source_config:
                validator = DataValidator.create_for_source(source_config)
            else:
                validator = DataValidator(ValidationProfile.create_default("strict"))

            self._validators[source_id] = (validator, now)
            return validator

    async def invalidate(self, source_id: str = None) -> None:
        """Invalidate cache for source or all."""
        async with self._lock:
            if source_id:
                self._validators.pop(source_id, None)
                self._profiles.pop(source_id, None)
            else:
                self._validators.clear()
                self._profiles.clear()

    async def cleanup_expired(self) -> int:
        """Remove expired cache entries."""
        async with self._lock:
            now = datetime.utcnow()
            expired_validators = [
                k for k, (_, cached_at) in self._validators.items()
                if now - cached_at >= self._ttl
            ]
            for k in expired_validators:
                del self._validators[k]

            expired_profiles = [
                k for k, (_, cached_at) in self._profiles.items()
                if now - cached_at >= self._ttl
            ]
            for k in expired_profiles:
                del self._profiles[k]

            return len(expired_validators) + len(expired_profiles)


class RealtimeValidator(EventHandler):
    """
    Real-time validator that processes streaming events.

    Integrates with the event processor to:
    1. Validate incoming data events
    2. Emit validation result events
    3. Block invalid data from progression (optional)

    Example:
        validator = RealtimeValidator(mongo_service)
        processor.register_handler(EventType.DATA_CREATED, validator.validate)

        # Or use as EventHandler
        change_stream.add_handler(validator)
    """

    def __init__(
        self,
        mongo_service=None,
        emit_validation_events: bool = True,
        block_on_failure: bool = False,
        cache_ttl_seconds: int = 300
    ):
        """
        Initialize the real-time validator.

        Args:
            mongo_service: MongoDB service for source config lookup
            emit_validation_events: Whether to emit ValidationEvent after validation
            block_on_failure: Whether to return False (block) on validation failure
            cache_ttl_seconds: TTL for validator cache
        """
        self.mongo = mongo_service
        self.emit_validation_events = emit_validation_events
        self.block_on_failure = block_on_failure
        self._cache = ValidatorCache(ttl_seconds=cache_ttl_seconds)
        self._stats = ValidationStats()
        self._event_emitter: Optional[Callable[[BaseEvent], None]] = None

        # Collections that should be validated
        self._validate_collections = {
            "staging_news",
            "staging_financial",
            "staging_data",
            "crawl_results"
        }

        # Quick validation rules for high throughput
        self._quick_rules = self._build_quick_rules()

    def _build_quick_rules(self) -> List[ValidationRule]:
        """Build fast validation rules for real-time processing."""
        return [
            EncodingRule(name="realtime_encoding", severity=ValidationSeverity.WARNING),
            # Add more lightweight rules as needed
        ]

    def set_event_emitter(self, emitter: Callable[[BaseEvent], None]) -> None:
        """Set the function to emit validation events."""
        self._event_emitter = emitter

    def should_validate(self, event: DataEvent) -> bool:
        """Determine if event should be validated."""
        # Only validate data events
        if event.event_type not in (EventType.DATA_CREATED, EventType.DATA_UPDATED):
            return False

        # Only validate specific collections
        if event.collection not in self._validate_collections:
            return False

        # Skip if no data
        if not event.data:
            return False

        return True

    async def _get_source_config(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get source configuration from MongoDB."""
        if not self.mongo or not source_id:
            return None

        try:
            source = self.mongo.get_source(source_id)
            return source
        except Exception as e:
            logger.warning(f"Failed to get source config for {source_id}: {e}")
            return None

    async def validate_event(self, event: DataEvent) -> RealtimeValidationResult:
        """
        Validate a single data event.

        Args:
            event: DataEvent to validate

        Returns:
            RealtimeValidationResult with validation details
        """
        start_time = datetime.utcnow()

        # Get validator (cached)
        source_config = await self._get_source_config(event.source_id)
        validator = await self._cache.get_validator(event.source_id, source_config)

        # Perform validation
        issues = validator.validate_record(event.data, row_index=0)

        # Categorize issues
        errors = []
        warnings = []
        rules_applied = set()

        for issue in issues:
            rules_applied.add(issue.rule_name)
            issue_dict = {
                "rule_name": issue.rule_name,
                "field_name": issue.field_name,
                "message": issue.message,
                "severity": issue.severity.value,
                "suggestion": issue.suggestion
            }

            if issue.severity in (ValidationSeverity.ERROR, ValidationSeverity.CRITICAL):
                errors.append(issue_dict)
            else:
                warnings.append(issue_dict)

        # Calculate quality score
        quality_score = self._calculate_quality_score(errors, warnings)

        # Calculate validation time
        validation_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000

        result = RealtimeValidationResult(
            event_id=event.event_id,
            source_id=event.source_id,
            document_id=event.document_id,
            collection=event.collection,
            passed=len(errors) == 0,
            quality_score=quality_score,
            errors=errors,
            warnings=warnings,
            validation_time_ms=validation_time_ms,
            rules_applied=list(rules_applied)
        )

        # Update stats
        self._update_stats(result)

        return result

    def _calculate_quality_score(
        self,
        errors: List[Dict],
        warnings: List[Dict]
    ) -> float:
        """Calculate quality score (0-100) based on issues."""
        if not errors and not warnings:
            return 100.0

        # Weighted penalty
        error_penalty = len(errors) * 10
        warning_penalty = len(warnings) * 2

        score = max(0, 100 - error_penalty - warning_penalty)
        return round(score, 2)

    def _update_stats(self, result: RealtimeValidationResult) -> None:
        """Update validation statistics."""
        self._stats.total_validated += 1
        self._stats.validation_time_total_ms += result.validation_time_ms
        self._stats.last_validated_at = result.validated_at

        if result.passed:
            self._stats.passed += 1
        else:
            self._stats.failed += 1

        if result.warnings:
            self._stats.warnings += len(result.warnings)

        # Per-source stats
        source_stats = self._stats.by_source[result.source_id]
        source_stats["total"] += 1
        if result.passed:
            source_stats["passed"] += 1
        else:
            source_stats["failed"] += 1

        # Per-rule stats
        for rule in result.rules_applied:
            self._stats.by_rule[rule] += 1

    async def handle(self, event: BaseEvent) -> bool:
        """
        EventHandler interface implementation.

        Called by ChangeStreamListener for each change event.
        """
        if not isinstance(event, DataEvent):
            return True

        if not self.should_validate(event):
            return True

        try:
            result = await self.validate_event(event)

            # Emit validation event if configured
            if self.emit_validation_events and self._event_emitter:
                validation_event = result.to_event()
                await self._emit_event(validation_event)

            # Block on failure if configured
            if self.block_on_failure and not result.passed:
                logger.warning(
                    f"Validation failed for {event.document_id}: {len(result.errors)} errors"
                )
                return False

            return True

        except Exception as e:
            logger.error(f"Validation error for event {event.event_id}: {e}", exc_info=True)
            return not self.block_on_failure

    async def on_error(self, event: BaseEvent, error: Exception) -> None:
        """Handle validation errors."""
        logger.error(f"Realtime validator error for {event.event_id}: {error}")

    async def _emit_event(self, event: BaseEvent) -> None:
        """Emit event using configured emitter."""
        if self._event_emitter:
            result = self._event_emitter(event)
            if asyncio.iscoroutine(result):
                await result

    def get_stats(self) -> Dict[str, Any]:
        """Get validation statistics."""
        return self._stats.to_dict()

    def reset_stats(self) -> None:
        """Reset validation statistics."""
        self._stats = ValidationStats()

    async def invalidate_cache(self, source_id: str = None) -> None:
        """Invalidate validator cache."""
        await self._cache.invalidate(source_id)


class QuickValidator:
    """
    Lightweight validator for high-throughput scenarios.

    Performs fast validation without full DataValidator overhead.
    Useful for real-time streams where speed is critical.
    """

    def __init__(self):
        self._rules: Dict[str, List[Callable[[Any], Optional[str]]]] = defaultdict(list)
        self._setup_default_rules()

    def _setup_default_rules(self) -> None:
        """Setup default quick validation rules."""
        # Encoding check (simplified)
        def check_encoding(value: Any) -> Optional[str]:
            if isinstance(value, str):
                try:
                    value.encode('utf-8')
                except UnicodeEncodeError:
                    return "Invalid UTF-8 encoding"
            return None

        # Empty string check
        def check_not_empty(value: Any) -> Optional[str]:
            if isinstance(value, str) and value.strip() == "":
                return "Empty string value"
            return None

        # Add rules
        self.add_rule("*", check_encoding)

    def add_rule(
        self,
        field_pattern: str,
        rule: Callable[[Any], Optional[str]]
    ) -> None:
        """
        Add validation rule.

        Args:
            field_pattern: Field name or "*" for all fields
            rule: Function that returns error message or None
        """
        self._rules[field_pattern].append(rule)

    def validate(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Quickly validate data.

        Returns:
            Tuple of (passed, list of error messages)
        """
        errors = []

        for field_name, value in data.items():
            # Apply field-specific rules
            for rule in self._rules.get(field_name, []):
                error = rule(value)
                if error:
                    errors.append(f"{field_name}: {error}")

            # Apply global rules
            for rule in self._rules.get("*", []):
                error = rule(value)
                if error:
                    errors.append(f"{field_name}: {error}")

        return len(errors) == 0, errors


class ValidationPipeline:
    """
    Configurable validation pipeline for streaming data.

    Allows chaining multiple validators with different configurations.
    """

    def __init__(self):
        self._stages: List[Tuple[str, Callable[[DataEvent], RealtimeValidationResult]]] = []
        self._continue_on_failure = True

    def add_stage(
        self,
        name: str,
        validator: Callable[[DataEvent], RealtimeValidationResult]
    ) -> "ValidationPipeline":
        """Add validation stage to pipeline."""
        self._stages.append((name, validator))
        return self

    def set_continue_on_failure(self, continue_on_failure: bool) -> "ValidationPipeline":
        """Set whether to continue validation after stage failure."""
        self._continue_on_failure = continue_on_failure
        return self

    async def validate(self, event: DataEvent) -> Dict[str, RealtimeValidationResult]:
        """
        Run validation pipeline.

        Returns:
            Dictionary of stage name -> validation result
        """
        results = {}

        for name, validator in self._stages:
            try:
                result = validator(event)
                if asyncio.iscoroutine(result):
                    result = await result
                results[name] = result

                if not result.passed and not self._continue_on_failure:
                    break

            except Exception as e:
                logger.error(f"Pipeline stage '{name}' failed: {e}")
                # Create failure result
                results[name] = RealtimeValidationResult(
                    event_id=event.event_id,
                    source_id=event.source_id,
                    document_id=event.document_id,
                    collection=event.collection,
                    passed=False,
                    quality_score=0,
                    errors=[{"message": f"Stage error: {str(e)}"}]
                )

                if not self._continue_on_failure:
                    break

        return results

    @property
    def all_passed(self) -> Callable[[Dict[str, RealtimeValidationResult]], bool]:
        """Predicate to check if all stages passed."""
        def check(results: Dict[str, RealtimeValidationResult]) -> bool:
            return all(r.passed for r in results.values())
        return check


# Factory functions

def create_realtime_validator(
    mongo_service=None,
    strict: bool = True,
    emit_events: bool = True
) -> RealtimeValidator:
    """
    Create a configured real-time validator.

    Args:
        mongo_service: MongoDB service for source lookups
        strict: Whether to use strict validation
        emit_events: Whether to emit validation events

    Returns:
        Configured RealtimeValidator instance
    """
    return RealtimeValidator(
        mongo_service=mongo_service,
        emit_validation_events=emit_events,
        block_on_failure=strict,
        cache_ttl_seconds=300
    )


def create_validation_pipeline(
    mongo_service=None
) -> ValidationPipeline:
    """
    Create a default validation pipeline.

    Returns:
        Configured ValidationPipeline instance
    """
    quick_validator = QuickValidator()
    realtime_validator = RealtimeValidator(mongo_service)

    async def quick_stage(event: DataEvent) -> RealtimeValidationResult:
        passed, errors = quick_validator.validate(event.data)
        return RealtimeValidationResult(
            event_id=event.event_id,
            source_id=event.source_id,
            document_id=event.document_id,
            collection=event.collection,
            passed=passed,
            quality_score=100 if passed else 50,
            errors=[{"message": e} for e in errors],
            rules_applied=["quick_validation"]
        )

    async def full_stage(event: DataEvent) -> RealtimeValidationResult:
        return await realtime_validator.validate_event(event)

    pipeline = ValidationPipeline()
    pipeline.add_stage("quick", quick_stage)
    pipeline.add_stage("full", full_stage)

    return pipeline

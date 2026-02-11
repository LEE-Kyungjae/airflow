"""
Tests for Data Quality Validation System.

Tests cover:
- Individual validation rules (encoding, date, required, range, format, unique)
- DataValidator batch validation
- Quality score calculation
- ValidationProfile configuration
- DataValidator.create_for_source factory
"""

from datetime import datetime, timedelta

import pytest

from api.app.services.data_quality.rules import (
    DateRule,
    EncodingRule,
    FormatRule,
    RangeRule,
    RequiredFieldRule,
    UniqueRule,
    ValidationIssue,
    ValidationSeverity,
)
from api.app.services.data_quality.validator import (
    DataValidator,
    ValidationProfile,
    ValidationResult,
)


# ============================================
# Encoding Rule Tests
# ============================================


class TestEncodingRule:
    def test_clean_text_passes(self):
        rule = EncodingRule()
        assert rule.validate("Hello World", "title") is None

    def test_clean_korean_passes(self):
        rule = EncodingRule()
        assert rule.validate("서울시 강남구", "address") is None

    def test_replacement_character_fails(self):
        rule = EncodingRule()
        result = rule.validate("Hello \ufffd World", "title")
        assert result is not None
        assert result.severity == ValidationSeverity.ERROR

    def test_null_byte_fails(self):
        rule = EncodingRule()
        result = rule.validate("Hello\x00World", "title")
        assert result is not None

    def test_control_character_fails(self):
        rule = EncodingRule()
        result = rule.validate("Hello\x07World", "title")
        assert result is not None

    def test_none_value_passes(self):
        rule = EncodingRule()
        assert rule.validate(None, "title") is None

    def test_latin1_mojibake_fails(self):
        rule = EncodingRule()
        result = rule.validate("CafÃ©", "title")
        assert result is not None


# ============================================
# Date Rule Tests
# ============================================


class TestDateRule:
    def test_valid_date_passes(self):
        rule = DateRule()
        assert rule.validate("2024-01-15", "date") is None

    def test_valid_datetime_passes(self):
        rule = DateRule()
        assert rule.validate("2024-01-15 10:30:00", "date") is None

    def test_future_date_fails(self):
        rule = DateRule(allow_future=False)
        future = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        result = rule.validate(future, "date")
        assert result is not None
        assert "미래" in result.message

    def test_future_date_allowed(self):
        rule = DateRule(allow_future=True)
        future = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        assert rule.validate(future, "date") is None

    def test_invalid_format_fails(self):
        rule = DateRule()
        result = rule.validate("not-a-date", "date")
        assert result is not None
        assert "파싱 실패" in result.message

    def test_very_old_date_fails(self):
        rule = DateRule()
        result = rule.validate("1800-01-01", "date")
        assert result is not None

    def test_none_value_passes(self):
        rule = DateRule()
        assert rule.validate(None, "date") is None

    def test_datetime_object_passes(self):
        rule = DateRule()
        assert rule.validate(datetime(2024, 6, 15), "date") is None


# ============================================
# Required Field Rule Tests
# ============================================


class TestRequiredFieldRule:
    def test_present_value_passes(self):
        rule = RequiredFieldRule(name="required_title")
        assert rule.validate("Some title", "title") is None

    def test_none_value_fails(self):
        rule = RequiredFieldRule(name="required_title")
        result = rule.validate(None, "title")
        assert result is not None

    def test_empty_string_fails(self):
        rule = RequiredFieldRule(name="required_title")
        result = rule.validate("", "title")
        assert result is not None

    def test_whitespace_only_fails(self):
        rule = RequiredFieldRule(name="required_title")
        result = rule.validate("   ", "title")
        assert result is not None

    def test_zero_value_passes(self):
        rule = RequiredFieldRule(name="required_count")
        assert rule.validate(0, "count") is None


# ============================================
# Range Rule Tests
# ============================================


class TestRangeRule:
    def test_in_range_passes(self):
        rule = RangeRule(name="range_price", min_value=0, max_value=1000)
        assert rule.validate(500, "price") is None

    def test_below_min_fails(self):
        rule = RangeRule(name="range_price", min_value=0, max_value=1000)
        result = rule.validate(-1, "price")
        assert result is not None

    def test_above_max_fails(self):
        rule = RangeRule(name="range_price", min_value=0, max_value=1000)
        result = rule.validate(1001, "price")
        assert result is not None

    def test_none_value_passes(self):
        rule = RangeRule(name="range_price", min_value=0, max_value=100)
        assert rule.validate(None, "price") is None

    def test_min_only(self):
        rule = RangeRule(name="range_price", min_value=0)
        assert rule.validate(999999, "price") is None

    def test_max_only(self):
        rule = RangeRule(name="range_price", max_value=100)
        assert rule.validate(-50, "price") is None

    def test_string_number_conversion(self):
        rule = RangeRule(name="range_price", min_value=0, max_value=100)
        assert rule.validate("50", "price") is None


# ============================================
# Format Rule Tests
# ============================================


class TestFormatRule:
    def test_matching_pattern_passes(self):
        rule = FormatRule(name="format_email", pattern=r"^[\w.+-]+@[\w-]+\.[\w.]+$")
        assert rule.validate("test@example.com", "email") is None

    def test_non_matching_fails(self):
        rule = FormatRule(name="format_email", pattern=r"^[\w.+-]+@[\w-]+\.[\w.]+$")
        result = rule.validate("not-an-email", "email")
        assert result is not None

    def test_none_value_passes(self):
        rule = FormatRule(name="format_url", pattern=r"^https?://")
        assert rule.validate(None, "url") is None

    def test_url_pattern(self):
        rule = FormatRule(name="format_url", pattern=r"^https?://")
        assert rule.validate("https://example.com", "url") is None
        result = rule.validate("ftp://example.com", "url")
        assert result is not None


# ============================================
# Unique Rule Tests
# ============================================


class TestUniqueRule:
    def test_unique_values_pass(self):
        rule = UniqueRule(name="unique_id")
        assert rule.validate("id1", "id") is None
        assert rule.validate("id2", "id") is None
        assert rule.validate("id3", "id") is None

    def test_duplicate_value_fails(self):
        rule = UniqueRule(name="unique_id")
        assert rule.validate("id1", "id") is None
        result = rule.validate("id1", "id")
        assert result is not None

    def test_reset_clears_state(self):
        rule = UniqueRule(name="unique_id")
        rule.validate("id1", "id")
        rule.reset()
        assert rule.validate("id1", "id") is None

    def test_none_values_not_tracked(self):
        rule = UniqueRule(name="unique_id")
        assert rule.validate(None, "id") is None
        assert rule.validate(None, "id") is None


# ============================================
# DataValidator Tests
# ============================================


class TestDataValidator:
    def test_validate_record_clean_data(self):
        validator = DataValidator()
        record = {"title": "Test Article", "content": "Some content"}
        issues = validator.validate_record(record)
        assert len(issues) == 0

    def test_validate_record_with_encoding_issue(self):
        validator = DataValidator()
        record = {"title": "Broken \ufffd text"}
        issues = validator.validate_record(record)
        assert len(issues) > 0

    def test_validate_batch_returns_result(self):
        records = [
            {"title": "Article 1", "url": "https://example.com/1"},
            {"title": "Article 2", "url": "https://example.com/2"},
            {"title": "Article 3", "url": "https://example.com/3"},
        ]
        validator = DataValidator()
        result = validator.validate_batch(records, "source_1", "run_1")

        assert isinstance(result, ValidationResult)
        assert result.total_records == 3
        assert result.source_id == "source_1"
        assert result.run_id == "run_1"

    def test_validate_batch_quality_score_perfect(self):
        records = [
            {"title": "Clean Article", "content": "Good content"},
            {"title": "Another Clean", "content": "More content"},
        ]
        validator = DataValidator(ValidationProfile.create_default("lenient"))
        result = validator.validate_batch(records, "s1", "r1")
        assert result.quality_score == 100.0
        assert result.is_valid is True

    def test_validate_batch_quality_score_with_issues(self):
        records = [
            {"title": "Clean"},
            {"title": "Broken \ufffd text"},
            {"title": "Also \ufffd broken"},
        ]
        validator = DataValidator()
        result = validator.validate_batch(records, "s1", "r1")
        assert result.quality_score < 100.0
        assert len(result.issues) >= 2

    def test_validate_batch_field_stats(self):
        records = [
            {"title": "A", "count": 1},
            {"title": "B", "count": None},
            {"title": None, "count": 3},
        ]
        validator = DataValidator(ValidationProfile.create_default("lenient"))
        result = validator.validate_batch(records, "s1", "r1")

        assert "title" in result.field_stats
        assert result.field_stats["title"]["null_count"] == 1
        assert result.field_stats["count"]["null_count"] == 1

    def test_validate_batch_to_dict(self):
        records = [{"title": "Test"}]
        validator = DataValidator()
        result = validator.validate_batch(records, "s1", "r1")
        d = result.to_dict()

        assert "source_id" in d
        assert "quality_score" in d
        assert "issue_summary" in d
        assert "field_stats" in d


# ============================================
# ValidationProfile Tests
# ============================================


class TestValidationProfile:
    def test_default_strict_profile(self):
        profile = ValidationProfile.create_default("strict")
        assert profile.name == "strict"
        assert len(profile.global_rules) > 0

    def test_default_lenient_profile(self):
        profile = ValidationProfile.create_default("lenient")
        assert profile.name == "lenient"

    def test_add_field_rule(self):
        profile = ValidationProfile("custom")
        profile.add_field_rule("price", RangeRule(name="range_price", min_value=0))
        assert "price" in profile.field_rules
        assert len(profile.field_rules["price"]) == 1

    def test_add_global_rule(self):
        profile = ValidationProfile("custom")
        profile.add_global_rule(EncodingRule())
        assert len(profile.global_rules) == 1


# ============================================
# DataValidator.create_for_source Tests
# ============================================


class TestCreateForSource:
    def test_creates_validator_with_required_fields(self):
        config = {
            "fields": [
                {"name": "title", "required": True},
                {"name": "url", "required": True},
                {"name": "content", "required": False},
            ]
        }
        validator = DataValidator.create_for_source(config)
        assert validator is not None

        # Test that required fields are enforced
        record = {"title": None, "url": "https://example.com", "content": None}
        issues = validator.validate_record(record)
        has_required_issue = any("title" in i.field_name for i in issues)
        assert has_required_issue

    def test_creates_validator_with_date_fields(self):
        config = {
            "fields": [
                {"name": "published_at", "data_type": "date"},
            ]
        }
        validator = DataValidator.create_for_source(config)
        record = {"published_at": "not-a-date"}
        issues = validator.validate_record(record)
        assert len(issues) > 0

    def test_creates_validator_with_number_range(self):
        config = {
            "fields": [
                {"name": "price", "data_type": "number", "min_value": 0, "max_value": 10000},
            ]
        }
        validator = DataValidator.create_for_source(config)
        issues = validator.validate_record({"price": -5})
        assert len(issues) > 0

    def test_creates_validator_with_unique_fields(self):
        config = {
            "fields": [
                {"name": "id", "unique": True},
            ]
        }
        validator = DataValidator.create_for_source(config)
        records = [{"id": "1"}, {"id": "2"}, {"id": "1"}]
        result = validator.validate_batch(records, "s1", "r1")
        assert len(result.issues) > 0

    def test_creates_validator_with_format_pattern(self):
        config = {
            "fields": [
                {"name": "email", "pattern": r"^[\w.+-]+@[\w-]+\.[\w.]+$"},
            ]
        }
        validator = DataValidator.create_for_source(config)
        issues = validator.validate_record({"email": "not-email"})
        assert len(issues) > 0

    def test_empty_fields_config(self):
        config = {"fields": []}
        validator = DataValidator.create_for_source(config)
        result = validator.validate_batch([{"a": "b"}], "s1", "r1")
        assert result.total_records == 1


# ============================================
# ValidationResult Tests
# ============================================


class TestValidationResult:
    def test_issue_count_by_severity(self):
        result = ValidationResult(
            source_id="s1",
            run_id="r1",
            total_records=10,
            validated_at=datetime.utcnow(),
            issues=[
                ValidationIssue("r", "f", ValidationSeverity.ERROR, "msg", None),
                ValidationIssue("r", "f", ValidationSeverity.WARNING, "msg", None),
                ValidationIssue("r", "f", ValidationSeverity.ERROR, "msg", None),
            ],
        )
        counts = result.issue_count_by_severity
        assert counts["error"] == 2
        assert counts["warning"] == 1

    def test_is_valid_with_no_errors(self):
        result = ValidationResult(
            source_id="s1",
            run_id="r1",
            total_records=5,
            validated_at=datetime.utcnow(),
            issues=[
                ValidationIssue("r", "f", ValidationSeverity.WARNING, "msg", None),
                ValidationIssue("r", "f", ValidationSeverity.INFO, "msg", None),
            ],
        )
        assert result.is_valid is True

    def test_is_valid_with_errors(self):
        result = ValidationResult(
            source_id="s1",
            run_id="r1",
            total_records=5,
            validated_at=datetime.utcnow(),
            issues=[
                ValidationIssue("r", "f", ValidationSeverity.ERROR, "msg", None),
            ],
        )
        assert result.is_valid is False

    def test_quality_score_no_records(self):
        result = ValidationResult(
            source_id="s1",
            run_id="r1",
            total_records=0,
            validated_at=datetime.utcnow(),
        )
        assert result.quality_score == 100.0

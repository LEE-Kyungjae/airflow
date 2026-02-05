"""
Data Validator - 데이터 검증 엔진

기능:
- 다중 규칙 적용
- 배치 검증
- 결과 집계
- 검증 프로필 지원
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json

from .rules import (
    ValidationRule,
    ValidationIssue,
    ValidationSeverity,
    EncodingRule,
    DateRule,
    RequiredFieldRule,
    RangeRule,
    FormatRule,
    UniqueRule,
)


@dataclass
class ValidationResult:
    """검증 결과"""
    source_id: str
    run_id: str
    total_records: int
    validated_at: datetime
    issues: List[ValidationIssue] = field(default_factory=list)
    field_stats: Dict[str, Dict] = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        """에러 레벨 이슈 없음"""
        return not any(
            i.severity in (ValidationSeverity.ERROR, ValidationSeverity.CRITICAL)
            for i in self.issues
        )

    @property
    def issue_count_by_severity(self) -> Dict[str, int]:
        counts = {s.value: 0 for s in ValidationSeverity}
        for issue in self.issues:
            counts[issue.severity.value] += 1
        return counts

    @property
    def issue_count_by_rule(self) -> Dict[str, int]:
        counts = {}
        for issue in self.issues:
            counts[issue.rule_name] = counts.get(issue.rule_name, 0) + 1
        return counts

    @property
    def issue_count_by_field(self) -> Dict[str, int]:
        counts = {}
        for issue in self.issues:
            counts[issue.field_name] = counts.get(issue.field_name, 0) + 1
        return counts

    @property
    def quality_score(self) -> float:
        """품질 점수 (0-100)"""
        if self.total_records == 0:
            return 100.0

        # 가중치
        weights = {
            ValidationSeverity.INFO: 0,
            ValidationSeverity.WARNING: 0.5,
            ValidationSeverity.ERROR: 2,
            ValidationSeverity.CRITICAL: 5,
        }

        penalty = sum(weights.get(i.severity, 1) for i in self.issues)
        max_penalty = self.total_records * 5  # 모든 레코드가 CRITICAL인 경우

        score = max(0, 100 - (penalty / max_penalty * 100))
        return round(score, 2)

    def to_dict(self) -> Dict:
        return {
            "source_id": self.source_id,
            "run_id": self.run_id,
            "total_records": self.total_records,
            "validated_at": self.validated_at.isoformat(),
            "is_valid": self.is_valid,
            "quality_score": self.quality_score,
            "issue_summary": {
                "total": len(self.issues),
                "by_severity": self.issue_count_by_severity,
                "by_rule": self.issue_count_by_rule,
                "by_field": self.issue_count_by_field,
            },
            "issues": [
                {
                    "rule_name": i.rule_name,
                    "field_name": i.field_name,
                    "severity": i.severity.value,
                    "message": i.message,
                    "actual_value": str(i.actual_value)[:200] if i.actual_value else None,
                    "expected": i.expected,
                    "row_index": i.row_index,
                    "suggestion": i.suggestion,
                }
                for i in self.issues[:1000]  # 최대 1000개
            ],
            "field_stats": self.field_stats,
        }


class ValidationProfile:
    """검증 프로필 - 필드별 규칙 설정"""

    # 기본 프로필
    DEFAULT_PROFILES = {
        "strict": {
            "encoding": True,
            "date_future": False,
            "required_fields": True,
            "severity": ValidationSeverity.ERROR,
        },
        "lenient": {
            "encoding": True,
            "date_future": True,
            "required_fields": False,
            "severity": ValidationSeverity.WARNING,
        },
        "encoding_only": {
            "encoding": True,
            "date_future": True,
            "required_fields": False,
            "severity": ValidationSeverity.ERROR,
        },
    }

    def __init__(self, name: str = "custom"):
        self.name = name
        self.field_rules: Dict[str, List[ValidationRule]] = {}
        self.global_rules: List[ValidationRule] = []

    def add_field_rule(self, field_name: str, rule: ValidationRule):
        """필드별 규칙 추가"""
        if field_name not in self.field_rules:
            self.field_rules[field_name] = []
        self.field_rules[field_name].append(rule)

    def add_global_rule(self, rule: ValidationRule):
        """전역 규칙 추가 (모든 필드에 적용)"""
        self.global_rules.append(rule)

    @classmethod
    def create_default(cls, profile_name: str = "strict") -> "ValidationProfile":
        """기본 프로필 생성"""
        config = cls.DEFAULT_PROFILES.get(profile_name, cls.DEFAULT_PROFILES["strict"])
        profile = cls(profile_name)

        if config["encoding"]:
            profile.add_global_rule(EncodingRule(
                severity=config["severity"]
            ))

        return profile


class DataValidator:
    """데이터 검증 엔진"""

    def __init__(self, profile: ValidationProfile = None):
        self.profile = profile or ValidationProfile.create_default("strict")
        self._unique_rules: Dict[str, UniqueRule] = {}

    def _get_rules_for_field(self, field_name: str) -> List[ValidationRule]:
        """필드에 적용할 규칙 목록"""
        rules = list(self.profile.global_rules)
        if field_name in self.profile.field_rules:
            rules.extend(self.profile.field_rules[field_name])
        return [r for r in rules if r.enabled]

    def validate_record(
        self,
        record: Dict[str, Any],
        row_index: int = None,
        context: Dict = None
    ) -> List[ValidationIssue]:
        """단일 레코드 검증"""
        issues = []

        for field_name, value in record.items():
            rules = self._get_rules_for_field(field_name)

            for rule in rules:
                issue = rule.validate(value, field_name, row_index, context)
                if issue:
                    issues.append(issue)

        return issues

    def validate_batch(
        self,
        records: List[Dict[str, Any]],
        source_id: str,
        run_id: str,
        context: Dict = None
    ) -> ValidationResult:
        """배치 데이터 검증"""
        all_issues = []
        field_stats = {}

        # Reset unique rules
        for rule in self.profile.global_rules:
            if isinstance(rule, UniqueRule):
                rule.reset()

        for field_rules in self.profile.field_rules.values():
            for rule in field_rules:
                if isinstance(rule, UniqueRule):
                    rule.reset()

        # Validate each record
        for idx, record in enumerate(records):
            # Collect field statistics
            for field_name, value in record.items():
                if field_name not in field_stats:
                    field_stats[field_name] = {
                        "total": 0,
                        "null_count": 0,
                        "empty_count": 0,
                        "unique_values": set(),
                    }

                stats = field_stats[field_name]
                stats["total"] += 1

                if value is None:
                    stats["null_count"] += 1
                elif isinstance(value, str) and value.strip() == "":
                    stats["empty_count"] += 1
                else:
                    # Track unique values (limit to prevent memory issues)
                    if len(stats["unique_values"]) < 10000:
                        try:
                            stats["unique_values"].add(str(value)[:100])
                        except:
                            pass

            # Validate record
            issues = self.validate_record(record, row_index=idx, context=context)
            all_issues.extend(issues)

        # Finalize field stats
        for field_name, stats in field_stats.items():
            stats["unique_count"] = len(stats["unique_values"])
            stats["null_rate"] = round(stats["null_count"] / stats["total"] * 100, 2) if stats["total"] > 0 else 0
            stats["empty_rate"] = round(stats["empty_count"] / stats["total"] * 100, 2) if stats["total"] > 0 else 0
            del stats["unique_values"]  # Remove set before serialization

        return ValidationResult(
            source_id=source_id,
            run_id=run_id,
            total_records=len(records),
            validated_at=datetime.utcnow(),
            issues=all_issues,
            field_stats=field_stats,
        )

    @classmethod
    def create_for_source(cls, source_config: Dict) -> "DataValidator":
        """소스 설정 기반 검증기 생성"""
        profile = ValidationProfile("source_specific")

        # 기본 인코딩 검사
        profile.add_global_rule(EncodingRule())

        # 필드별 규칙 설정
        for field in source_config.get("fields", []):
            field_name = field["name"]
            data_type = field.get("data_type", "string")

            # 필수 필드
            if field.get("required", False):
                profile.add_field_rule(
                    field_name,
                    RequiredFieldRule(name=f"required_{field_name}")
                )

            # 날짜 필드
            if data_type == "date":
                profile.add_field_rule(
                    field_name,
                    DateRule(
                        name=f"date_{field_name}",
                        allow_future=field.get("allow_future", False)
                    )
                )

            # 숫자 필드
            if data_type == "number":
                profile.add_field_rule(
                    field_name,
                    RangeRule(
                        name=f"range_{field_name}",
                        min_value=field.get("min_value"),
                        max_value=field.get("max_value"),
                        allow_negative=field.get("allow_negative", True)
                    )
                )

            # 포맷 검증
            if "pattern" in field:
                profile.add_field_rule(
                    field_name,
                    FormatRule(
                        name=f"format_{field_name}",
                        pattern=field["pattern"]
                    )
                )

            # 고유값 검증
            if field.get("unique", False):
                profile.add_field_rule(
                    field_name,
                    UniqueRule(name=f"unique_{field_name}")
                )

        return cls(profile)

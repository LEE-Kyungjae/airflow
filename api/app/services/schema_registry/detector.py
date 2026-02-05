"""
Schema Detector - 데이터에서 스키마 자동 감지

기능:
- 데이터 샘플에서 스키마 추론
- 필드 타입 자동 감지
- 패턴 인식
- 통계 기반 nullable/required 판단
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from collections import Counter
import logging

from .models import Schema, FieldSchema, FieldType, DataCategory

logger = logging.getLogger(__name__)


# 날짜/시간 패턴
DATE_PATTERNS = [
    (r'^\d{4}-\d{2}-\d{2}$', '%Y-%m-%d'),
    (r'^\d{4}/\d{2}/\d{2}$', '%Y/%m/%d'),
    (r'^\d{2}-\d{2}-\d{4}$', '%d-%m-%Y'),
    (r'^\d{2}/\d{2}/\d{4}$', '%d/%m/%Y'),
    (r'^\d{4}\.\d{2}\.\d{2}$', '%Y.%m.%d'),
    # 한국어 날짜
    (r'^\d{4}년\s*\d{1,2}월\s*\d{1,2}일$', '%Y년 %m월 %d일'),
]

DATETIME_PATTERNS = [
    (r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', '%Y-%m-%dT%H:%M:%S'),
    (r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', '%Y-%m-%d %H:%M:%S'),
    (r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}', '%Y/%m/%d %H:%M:%S'),
    (r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$', '%Y-%m-%dT%H:%M:%SZ'),
    (r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+', '%Y-%m-%dT%H:%M:%S'),
]

# 특수 패턴 (문자열에서 인식)
SPECIAL_PATTERNS = {
    'email': r'^[\w\.-]+@[\w\.-]+\.\w+$',
    'url': r'^https?://[\w\.-]+',
    'phone_kr': r'^0\d{1,2}-\d{3,4}-\d{4}$',
    'phone_intl': r'^\+\d{1,3}[\s-]?\d{1,4}[\s-]?\d{1,4}[\s-]?\d{1,4}$',
    'uuid': r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$',
    'ip_address': r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$',
    'korean_name': r'^[가-힣]{2,5}$',
    'stock_code_kr': r'^\d{6}$',
    'currency_code': r'^[A-Z]{3}$',
}


@dataclass
class FieldStats:
    """필드 통계 정보"""
    name: str
    total_count: int = 0
    null_count: int = 0
    empty_count: int = 0
    type_counts: Dict[str, int] = field(default_factory=dict)
    unique_values: Set[str] = field(default_factory=set)
    min_length: int = float('inf')
    max_length: int = 0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    sample_values: List[Any] = field(default_factory=list)
    detected_patterns: Dict[str, int] = field(default_factory=dict)

    @property
    def null_rate(self) -> float:
        """null 비율"""
        return self.null_count / self.total_count if self.total_count > 0 else 0

    @property
    def empty_rate(self) -> float:
        """빈 값 비율 (null + empty string)"""
        return (self.null_count + self.empty_count) / self.total_count if self.total_count > 0 else 0

    @property
    def unique_rate(self) -> float:
        """고유값 비율"""
        non_null = self.total_count - self.null_count - self.empty_count
        return len(self.unique_values) / non_null if non_null > 0 else 0

    @property
    def dominant_type(self) -> str:
        """가장 많이 나타난 타입"""
        if not self.type_counts:
            return "string"
        return max(self.type_counts, key=self.type_counts.get)

    @property
    def is_likely_id(self) -> bool:
        """ID 필드일 가능성 (고유값 비율 높음)"""
        return self.unique_rate > 0.95 and self.null_rate < 0.01

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "total_count": self.total_count,
            "null_rate": round(self.null_rate, 4),
            "empty_rate": round(self.empty_rate, 4),
            "unique_rate": round(self.unique_rate, 4),
            "dominant_type": self.dominant_type,
            "type_distribution": dict(self.type_counts),
            "detected_patterns": dict(self.detected_patterns),
            "min_length": self.min_length if self.min_length != float('inf') else None,
            "max_length": self.max_length if self.max_length > 0 else None,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "sample_values": self.sample_values[:5],
        }


class SchemaDetector:
    """스키마 자동 감지기"""

    def __init__(
        self,
        sample_size: int = 1000,
        required_threshold: float = 0.95,
        unique_threshold: float = 0.99,
        type_threshold: float = 0.8
    ):
        """
        Args:
            sample_size: 분석할 최대 레코드 수
            required_threshold: 필수 필드 판단 기준 (non-null 비율)
            unique_threshold: 고유값 필드 판단 기준
            type_threshold: 타입 결정 기준 (해당 타입의 최소 비율)
        """
        self.sample_size = sample_size
        self.required_threshold = required_threshold
        self.unique_threshold = unique_threshold
        self.type_threshold = type_threshold

    def detect_from_data(
        self,
        data: List[Dict[str, Any]],
        source_fields: List[Dict] = None,
        data_category: DataCategory = None
    ) -> Schema:
        """
        데이터에서 스키마 감지

        Args:
            data: 데이터 레코드 목록
            source_fields: 소스에서 정의한 필드 목록 (힌트)
            data_category: 데이터 카테고리 (힌트)

        Returns:
            감지된 Schema
        """
        if not data:
            return Schema(fields=[], data_category=data_category)

        # 샘플링
        sample = data[:self.sample_size]

        # 필드 통계 수집
        field_stats = self._collect_stats(sample)

        # 소스 필드 힌트 적용
        hints = {}
        if source_fields:
            for f in source_fields:
                hints[f.get("name", f.get("field_name", ""))] = f

        # 스키마 생성
        fields = []
        for name, stats in field_stats.items():
            # 메타 필드 제외
            if name.startswith('_'):
                continue

            field_schema = self._stats_to_field_schema(stats, hints.get(name))
            fields.append(field_schema)

        # 이름순 정렬
        fields.sort(key=lambda f: f.name)

        return Schema(
            fields=fields,
            data_category=data_category,
            metadata={
                "detected_at": datetime.utcnow().isoformat(),
                "sample_size": len(sample),
                "total_records": len(data),
                "detection_method": "statistical",
            }
        )

    def detect_category(self, data: List[Dict[str, Any]]) -> Optional[DataCategory]:
        """
        데이터에서 카테고리 추론

        Args:
            data: 데이터 레코드 목록

        Returns:
            추론된 DataCategory
        """
        if not data:
            return None

        sample = data[0]
        fields = set(sample.keys())

        # 필드 조합으로 카테고리 추론
        category_indicators = {
            DataCategory.NEWS_ARTICLE: {'title', 'content', 'published_at', 'summary'},
            DataCategory.STOCK_PRICE: {'stock_code', 'price', 'volume', 'high', 'low'},
            DataCategory.EXCHANGE_RATE: {'currency_code', 'base_rate', 'buy_rate', 'sell_rate'},
            DataCategory.MARKET_INDEX: {'index_code', 'value', 'change_rate'},
            DataCategory.ANNOUNCEMENT: {'announcement_type', 'company_name', 'stock_code'},
            DataCategory.FINANCIAL_DATA: {'price', 'change', 'volume', 'change_rate'},
        }

        best_match = None
        best_score = 0

        for category, indicators in category_indicators.items():
            score = len(fields & indicators) / len(indicators)
            if score > best_score:
                best_score = score
                best_match = category

        if best_score >= 0.3:
            return best_match

        return DataCategory.GENERIC

    def analyze_field(self, field_name: str, values: List[Any]) -> FieldStats:
        """
        단일 필드 분석

        Args:
            field_name: 필드 이름
            values: 필드 값 목록

        Returns:
            FieldStats
        """
        stats = FieldStats(name=field_name)

        for value in values:
            stats.total_count += 1
            self._analyze_value(value, stats)

        return stats

    def _collect_stats(self, data: List[Dict[str, Any]]) -> Dict[str, FieldStats]:
        """필드별 통계 수집"""
        stats: Dict[str, FieldStats] = {}

        for record in data:
            for name, value in record.items():
                if name not in stats:
                    stats[name] = FieldStats(name=name)

                s = stats[name]
                s.total_count += 1
                self._analyze_value(value, s)

        return stats

    def _analyze_value(self, value: Any, stats: FieldStats):
        """단일 값 분석"""
        # Null 체크
        if value is None:
            stats.null_count += 1
            return

        # 빈 문자열 체크
        if isinstance(value, str) and value.strip() == "":
            stats.empty_count += 1
            return

        # 타입 감지
        detected_type = self._detect_type(value)
        stats.type_counts[detected_type] = stats.type_counts.get(detected_type, 0) + 1

        # 문자열 통계
        if isinstance(value, str):
            stats.min_length = min(stats.min_length, len(value))
            stats.max_length = max(stats.max_length, len(value))

            # 패턴 감지
            for pattern_name, pattern in SPECIAL_PATTERNS.items():
                if re.match(pattern, value, re.IGNORECASE):
                    stats.detected_patterns[pattern_name] = stats.detected_patterns.get(pattern_name, 0) + 1

        # 숫자 통계
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if stats.min_value is None or value < stats.min_value:
                stats.min_value = value
            if stats.max_value is None or value > stats.max_value:
                stats.max_value = value

        # 유니크 값 (제한적 추적)
        if len(stats.unique_values) < 10000:
            try:
                stats.unique_values.add(str(value)[:100])
            except Exception:
                pass

        # 샘플 값
        if len(stats.sample_values) < 10:
            stats.sample_values.append(value)

    def _detect_type(self, value: Any) -> str:
        """값의 타입 감지"""
        if value is None:
            return "null"

        if isinstance(value, bool):
            return "boolean"

        if isinstance(value, int):
            return "integer"

        if isinstance(value, float):
            return "float"

        if isinstance(value, list):
            return "array"

        if isinstance(value, dict):
            return "object"

        if isinstance(value, datetime):
            return "datetime"

        # 문자열 세부 타입
        if isinstance(value, str):
            value_str = value.strip()

            # 날짜시간
            if self._is_datetime_string(value_str):
                return "datetime"
            if self._is_date_string(value_str):
                return "date"

            # 숫자 문자열
            if self._is_integer_string(value_str):
                return "integer"
            if self._is_float_string(value_str):
                return "float"

            # 불리언 문자열
            if value_str.lower() in ('true', 'false', 'yes', 'no', '1', '0'):
                return "boolean"

            return "string"

        return "any"

    def _is_integer_string(self, s: str) -> bool:
        """정수 문자열 체크"""
        try:
            int(s)
            return '.' not in s and 'e' not in s.lower()
        except ValueError:
            return False

    def _is_float_string(self, s: str) -> bool:
        """실수 문자열 체크"""
        try:
            float(s)
            return True
        except ValueError:
            return False

    def _is_date_string(self, s: str) -> bool:
        """날짜 문자열 체크"""
        for pattern, _ in DATE_PATTERNS:
            if re.match(pattern, s):
                return True
        return False

    def _is_datetime_string(self, s: str) -> bool:
        """날짜시간 문자열 체크"""
        for pattern, _ in DATETIME_PATTERNS:
            if re.match(pattern, s):
                return True
        return False

    def _stats_to_field_schema(
        self,
        stats: FieldStats,
        hint: Dict = None
    ) -> FieldSchema:
        """통계에서 FieldSchema 생성"""
        hint = hint or {}

        # 타입 결정
        field_type = self._determine_type(stats, hint.get("data_type"))

        # 필수 여부
        non_null_rate = 1 - stats.null_rate
        required = non_null_rate >= self.required_threshold

        # 힌트에서 오버라이드
        if "required" in hint:
            required = hint["required"]

        # 패턴 감지
        pattern = None
        if stats.detected_patterns:
            dominant_pattern = max(stats.detected_patterns, key=stats.detected_patterns.get)
            if stats.detected_patterns[dominant_pattern] > stats.total_count * 0.8:
                pattern = SPECIAL_PATTERNS.get(dominant_pattern)

        # 설명 생성
        description = hint.get("description", "")
        if not description and stats.detected_patterns:
            dominant = max(stats.detected_patterns, key=stats.detected_patterns.get)
            description = f"Detected pattern: {dominant}"

        return FieldSchema(
            name=stats.name,
            field_type=field_type,
            required=required,
            nullable=stats.null_count > 0,
            pattern=pattern,
            min_value=stats.min_value if field_type in (FieldType.INTEGER, FieldType.FLOAT) else None,
            max_value=stats.max_value if field_type in (FieldType.INTEGER, FieldType.FLOAT) else None,
            min_length=stats.min_length if field_type == FieldType.STRING and stats.min_length != float('inf') else None,
            max_length=stats.max_length if field_type == FieldType.STRING and stats.max_length > 0 else None,
            description=description,
            examples=stats.sample_values[:3],
        )

    def _determine_type(self, stats: FieldStats, hint_type: str = None) -> FieldType:
        """타입 결정"""
        # 힌트 우선
        if hint_type:
            type_map = {
                "string": FieldType.STRING,
                "integer": FieldType.INTEGER,
                "int": FieldType.INTEGER,
                "number": FieldType.FLOAT,
                "float": FieldType.FLOAT,
                "boolean": FieldType.BOOLEAN,
                "bool": FieldType.BOOLEAN,
                "date": FieldType.DATE,
                "datetime": FieldType.DATETIME,
                "array": FieldType.ARRAY,
                "list": FieldType.ARRAY,
                "object": FieldType.OBJECT,
                "dict": FieldType.OBJECT,
            }
            if hint_type.lower() in type_map:
                return type_map[hint_type.lower()]

        # 통계 기반 추론
        dominant = stats.dominant_type

        # 타입 매핑
        type_mapping = {
            "integer": FieldType.INTEGER,
            "float": FieldType.FLOAT,
            "boolean": FieldType.BOOLEAN,
            "date": FieldType.DATE,
            "datetime": FieldType.DATETIME,
            "array": FieldType.ARRAY,
            "object": FieldType.OBJECT,
            "string": FieldType.STRING,
        }

        return type_mapping.get(dominant, FieldType.STRING)

    def compare_schemas(
        self,
        expected: Schema,
        actual: Schema
    ) -> Dict[str, Any]:
        """
        두 스키마 비교

        Args:
            expected: 예상 스키마
            actual: 실제 스키마

        Returns:
            비교 결과
        """
        expected_fields = {f.name: f for f in expected.fields}
        actual_fields = {f.name: f for f in actual.fields}

        expected_names = set(expected_fields.keys())
        actual_names = set(actual_fields.keys())

        type_mismatches = []
        for name in expected_names & actual_names:
            if expected_fields[name].field_type != actual_fields[name].field_type:
                type_mismatches.append({
                    "field": name,
                    "expected": expected_fields[name].field_type.value,
                    "actual": actual_fields[name].field_type.value,
                })

        return {
            "matched_fields": list(expected_names & actual_names),
            "missing_fields": list(expected_names - actual_names),
            "extra_fields": list(actual_names - expected_names),
            "type_mismatches": type_mismatches,
            "match_rate": len(expected_names & actual_names) / len(expected_names) if expected_names else 1.0,
        }

    def generate_report(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        데이터 분석 리포트 생성

        Args:
            data: 분석할 데이터

        Returns:
            분석 리포트
        """
        sample = data[:self.sample_size]
        field_stats = self._collect_stats(sample)

        return {
            "summary": {
                "total_records": len(data),
                "analyzed_records": len(sample),
                "total_fields": len(field_stats),
                "detected_category": self.detect_category(data).value if self.detect_category(data) else None,
            },
            "fields": {
                name: stats.to_dict()
                for name, stats in field_stats.items()
                if not name.startswith('_')
            },
            "detected_schema": self.detect_from_data(data).to_dict(),
            "generated_at": datetime.utcnow().isoformat(),
        }

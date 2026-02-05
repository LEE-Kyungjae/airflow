"""
Data Deduplicator - 데이터 중복 제거

기능:
- 여러 중복 제거 전략 지원
- 해시 기반 빠른 중복 감지
- 퍼지 매칭 (유사도 기반)
- 배치 및 스트림 처리
"""

import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DeduplicationStrategy(str, Enum):
    """중복 제거 전략"""
    EXACT_MATCH = "exact_match"         # 완전 일치
    KEY_MATCH = "key_match"             # 특정 필드 기반
    HASH_MATCH = "hash_match"           # 해시 기반
    FUZZY_MATCH = "fuzzy_match"         # 유사도 기반
    COMPOSITE = "composite"             # 복합 전략


@dataclass
class DeduplicationResult:
    """중복 제거 결과"""
    total_records: int
    unique_records: int
    duplicates_removed: int
    duplicates_by_reason: Dict[str, int] = field(default_factory=dict)
    processing_time_ms: int = 0
    sample_duplicates: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def duplicate_rate(self) -> float:
        return self.duplicates_removed / self.total_records if self.total_records > 0 else 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_records": self.total_records,
            "unique_records": self.unique_records,
            "duplicates_removed": self.duplicates_removed,
            "duplicate_rate": round(self.duplicate_rate * 100, 2),
            "duplicates_by_reason": self.duplicates_by_reason,
            "processing_time_ms": self.processing_time_ms,
            "sample_duplicates": self.sample_duplicates[:10],
        }


@dataclass
class DeduplicationConfig:
    """중복 제거 설정"""
    strategy: DeduplicationStrategy = DeduplicationStrategy.KEY_MATCH
    key_fields: List[str] = field(default_factory=list)
    hash_fields: List[str] = field(default_factory=list)
    fuzzy_threshold: float = 0.9  # 유사도 임계값 (0-1)
    fuzzy_fields: List[str] = field(default_factory=list)
    keep: str = "first"  # first, last, newest, oldest
    timestamp_field: str = "created_at"
    case_sensitive: bool = True
    ignore_null: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "key_fields": self.key_fields,
            "hash_fields": self.hash_fields,
            "fuzzy_threshold": self.fuzzy_threshold,
            "fuzzy_fields": self.fuzzy_fields,
            "keep": self.keep,
            "timestamp_field": self.timestamp_field,
            "case_sensitive": self.case_sensitive,
            "ignore_null": self.ignore_null,
        }


class DataDeduplicator:
    """데이터 중복 제거기"""

    def __init__(self, config: DeduplicationConfig = None):
        self.config = config or DeduplicationConfig()
        self._seen_hashes: Set[str] = set()
        self._seen_keys: Dict[str, int] = {}  # key -> index

    def reset(self):
        """상태 초기화"""
        self._seen_hashes.clear()
        self._seen_keys.clear()

    def deduplicate(
        self,
        records: List[Dict[str, Any]],
        config: DeduplicationConfig = None
    ) -> Tuple[List[Dict[str, Any]], DeduplicationResult]:
        """
        레코드 중복 제거

        Args:
            records: 입력 레코드 목록
            config: 중복 제거 설정 (없으면 기본 설정 사용)

        Returns:
            (중복 제거된 레코드, 결과 통계)
        """
        import time
        start_time = time.time()

        self.reset()
        cfg = config or self.config

        unique_records = []
        duplicates = []
        duplicate_reasons = {}

        for idx, record in enumerate(records):
            is_duplicate, reason = self._is_duplicate(record, idx, cfg)

            if is_duplicate:
                duplicates.append({
                    "index": idx,
                    "reason": reason,
                    "record_preview": {k: str(v)[:50] for k, v in list(record.items())[:3]}
                })
                duplicate_reasons[reason] = duplicate_reasons.get(reason, 0) + 1
            else:
                unique_records.append(record)

        processing_time = int((time.time() - start_time) * 1000)

        result = DeduplicationResult(
            total_records=len(records),
            unique_records=len(unique_records),
            duplicates_removed=len(duplicates),
            duplicates_by_reason=duplicate_reasons,
            processing_time_ms=processing_time,
            sample_duplicates=duplicates[:100],
        )

        logger.info(
            f"Deduplication completed: {result.total_records} -> {result.unique_records} "
            f"({result.duplicate_rate:.1%} duplicates)"
        )

        return unique_records, result

    def _is_duplicate(
        self,
        record: Dict[str, Any],
        index: int,
        config: DeduplicationConfig
    ) -> Tuple[bool, str]:
        """
        레코드 중복 여부 확인

        Returns:
            (중복 여부, 사유)
        """
        if config.strategy == DeduplicationStrategy.EXACT_MATCH:
            return self._check_exact_match(record)

        elif config.strategy == DeduplicationStrategy.KEY_MATCH:
            return self._check_key_match(record, index, config)

        elif config.strategy == DeduplicationStrategy.HASH_MATCH:
            return self._check_hash_match(record, config)

        elif config.strategy == DeduplicationStrategy.FUZZY_MATCH:
            return self._check_fuzzy_match(record, config)

        elif config.strategy == DeduplicationStrategy.COMPOSITE:
            # 복합 전략: 순차적으로 검사
            if config.key_fields:
                is_dup, reason = self._check_key_match(record, index, config)
                if is_dup:
                    return is_dup, reason

            if config.hash_fields:
                is_dup, reason = self._check_hash_match(record, config)
                if is_dup:
                    return is_dup, reason

            if config.fuzzy_fields:
                is_dup, reason = self._check_fuzzy_match(record, config)
                if is_dup:
                    return is_dup, reason

            return False, ""

        return False, ""

    def _check_exact_match(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        """완전 일치 검사"""
        import json
        record_str = json.dumps(record, sort_keys=True, default=str)
        record_hash = hashlib.sha256(record_str.encode()).hexdigest()

        if record_hash in self._seen_hashes:
            return True, "exact_match"

        self._seen_hashes.add(record_hash)
        return False, ""

    def _check_key_match(
        self,
        record: Dict[str, Any],
        index: int,
        config: DeduplicationConfig
    ) -> Tuple[bool, str]:
        """키 기반 중복 검사"""
        if not config.key_fields:
            return False, ""

        # 키 값 추출
        key_values = []
        for field in config.key_fields:
            value = record.get(field)

            if value is None and config.ignore_null:
                return False, ""  # null 값은 중복으로 처리하지 않음

            if isinstance(value, str) and not config.case_sensitive:
                value = value.lower()

            key_values.append(str(value))

        key = "|".join(key_values)

        if key in self._seen_keys:
            return True, f"key_match:{','.join(config.key_fields)}"

        self._seen_keys[key] = index
        return False, ""

    def _check_hash_match(
        self,
        record: Dict[str, Any],
        config: DeduplicationConfig
    ) -> Tuple[bool, str]:
        """해시 기반 중복 검사"""
        fields = config.hash_fields or list(record.keys())

        # 해시 대상 값 수집
        hash_values = []
        for field in fields:
            value = record.get(field, "")

            if isinstance(value, str) and not config.case_sensitive:
                value = value.lower()

            hash_values.append(str(value))

        content_hash = hashlib.sha256("|".join(hash_values).encode()).hexdigest()[:16]

        if content_hash in self._seen_hashes:
            return True, "hash_match"

        self._seen_hashes.add(content_hash)
        return False, ""

    def _check_fuzzy_match(
        self,
        record: Dict[str, Any],
        config: DeduplicationConfig
    ) -> Tuple[bool, str]:
        """유사도 기반 중복 검사 (간단한 구현)"""
        if not config.fuzzy_fields:
            return False, ""

        # 현재 레코드의 fuzzy 값
        current_values = []
        for field in config.fuzzy_fields:
            value = str(record.get(field, ""))
            if not config.case_sensitive:
                value = value.lower()
            current_values.append(value)

        current_text = " ".join(current_values)

        # 기존 해시와 비교 (간단한 ngram 유사도)
        for seen_hash in self._seen_hashes:
            if seen_hash.startswith("fuzzy:"):
                seen_text = seen_hash[6:]
                similarity = self._calculate_similarity(current_text, seen_text)
                if similarity >= config.fuzzy_threshold:
                    return True, f"fuzzy_match:{similarity:.2f}"

        # 새 해시 추가
        self._seen_hashes.add(f"fuzzy:{current_text[:200]}")
        return False, ""

    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """간단한 자카드 유사도 계산"""
        if not text1 or not text2:
            return 0.0

        # 2-gram 집합
        def get_ngrams(text: str, n: int = 2) -> Set[str]:
            text = text.strip()
            if len(text) < n:
                return {text}
            return {text[i:i+n] for i in range(len(text) - n + 1)}

        set1 = get_ngrams(text1)
        set2 = get_ngrams(text2)

        intersection = len(set1 & set2)
        union = len(set1 | set2)

        return intersection / union if union > 0 else 0.0

    def find_duplicates_in_batch(
        self,
        records: List[Dict[str, Any]],
        config: DeduplicationConfig = None
    ) -> Dict[str, List[int]]:
        """
        배치 내 중복 그룹 찾기

        Returns:
            {key: [중복 레코드 인덱스 목록]}
        """
        cfg = config or self.config
        groups: Dict[str, List[int]] = {}

        for idx, record in enumerate(records):
            # 키 생성
            if cfg.key_fields:
                key_values = [str(record.get(f, "")) for f in cfg.key_fields]
                key = "|".join(key_values)
            else:
                import json
                key = hashlib.sha256(
                    json.dumps(record, sort_keys=True, default=str).encode()
                ).hexdigest()[:16]

            if key not in groups:
                groups[key] = []
            groups[key].append(idx)

        # 중복만 필터링
        return {k: v for k, v in groups.items() if len(v) > 1}

    def merge_duplicates(
        self,
        records: List[Dict[str, Any]],
        duplicate_indices: List[int],
        merge_strategy: str = "first",
        merge_fields: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        중복 레코드 병합

        Args:
            records: 전체 레코드 목록
            duplicate_indices: 중복 레코드 인덱스
            merge_strategy: 병합 전략 (first, last, combine)
            merge_fields: 필드별 병합 전략 {field: "first"|"last"|"max"|"min"|"concat"|"sum"}

        Returns:
            병합된 레코드
        """
        if not duplicate_indices:
            return {}

        duplicate_records = [records[i] for i in duplicate_indices]

        if merge_strategy == "first":
            return duplicate_records[0].copy()

        elif merge_strategy == "last":
            return duplicate_records[-1].copy()

        elif merge_strategy == "combine":
            merged = duplicate_records[0].copy()
            merge_fields = merge_fields or {}

            for record in duplicate_records[1:]:
                for field, value in record.items():
                    if value is None:
                        continue

                    strategy = merge_fields.get(field, "first")

                    if strategy == "first" and merged.get(field) is not None:
                        continue
                    elif strategy == "last":
                        merged[field] = value
                    elif strategy == "max":
                        if merged.get(field) is None or value > merged[field]:
                            merged[field] = value
                    elif strategy == "min":
                        if merged.get(field) is None or value < merged[field]:
                            merged[field] = value
                    elif strategy == "concat":
                        if merged.get(field):
                            merged[field] = f"{merged[field]}; {value}"
                        else:
                            merged[field] = value
                    elif strategy == "sum":
                        try:
                            merged[field] = (merged.get(field) or 0) + value
                        except TypeError:
                            merged[field] = value

            return merged

        return duplicate_records[0].copy()


class IncrementalDeduplicator:
    """증분 중복 제거 (스트림 처리용)"""

    def __init__(
        self,
        mongo_service=None,
        config: DeduplicationConfig = None,
        window_size: int = 10000
    ):
        """
        Args:
            mongo_service: MongoDB 서비스 (영구 저장용)
            config: 중복 제거 설정
            window_size: 메모리 내 보관할 최대 해시 수
        """
        self.mongo = mongo_service
        self.config = config or DeduplicationConfig()
        self.window_size = window_size
        self._recent_hashes: List[str] = []

    def _get_collection(self):
        if self.mongo:
            return self.mongo.db.dedup_hashes
        return None

    def is_duplicate(
        self,
        record: Dict[str, Any],
        source_id: str
    ) -> bool:
        """
        실시간 중복 체크

        Args:
            record: 레코드
            source_id: 소스 ID

        Returns:
            중복 여부
        """
        record_hash = self._compute_hash(record)

        # 메모리 캐시 확인
        if record_hash in self._recent_hashes:
            return True

        # DB 확인 (있으면)
        collection = self._get_collection()
        if collection:
            existing = collection.find_one({
                "source_id": source_id,
                "hash": record_hash
            })
            if existing:
                return True

            # 새 해시 저장
            collection.insert_one({
                "source_id": source_id,
                "hash": record_hash,
                "created_at": datetime.utcnow()
            })

        # 메모리 캐시 추가
        self._recent_hashes.append(record_hash)
        if len(self._recent_hashes) > self.window_size:
            self._recent_hashes.pop(0)

        return False

    def _compute_hash(self, record: Dict[str, Any]) -> str:
        """레코드 해시 계산"""
        if self.config.key_fields:
            values = [str(record.get(f, "")) for f in self.config.key_fields]
            content = "|".join(values)
        else:
            import json
            content = json.dumps(record, sort_keys=True, default=str)

        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def cleanup_old_hashes(self, source_id: str, days: int = 7) -> int:
        """오래된 해시 정리"""
        from datetime import timedelta

        collection = self._get_collection()
        if not collection:
            return 0

        cutoff = datetime.utcnow() - timedelta(days=days)

        result = collection.delete_many({
            "source_id": source_id,
            "created_at": {"$lt": cutoff}
        })

        return result.deleted_count

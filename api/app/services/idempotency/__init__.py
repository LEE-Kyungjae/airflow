"""
Idempotency System - 멱등성 보장

ETL 파이프라인의 안전한 재시도와 정확히 한 번(Exactly-Once) 처리 보장
"""

from .manager import (
    IdempotencyManager,
    IdempotencyKey,
    ExecutionState,
    CheckpointData,
)
from .deduplicator import (
    DataDeduplicator,
    DeduplicationStrategy,
    DeduplicationResult,
)

__all__ = [
    "IdempotencyManager",
    "IdempotencyKey",
    "ExecutionState",
    "CheckpointData",
    "DataDeduplicator",
    "DeduplicationStrategy",
    "DeduplicationResult",
]
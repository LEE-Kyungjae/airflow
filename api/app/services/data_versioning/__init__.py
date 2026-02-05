"""
Data Versioning Service - 데이터 버전 관리 시스템

기능:
- 데이터 스냅샷 생성 및 복원
- 변경 이력 추적 (diff)
- 버전 롤백
- 브랜치/태그 개념 (선택)
- data_lineage 컬렉션 연동

MongoDB 컬렉션:
- data_snapshots: 스냅샷 메타데이터
- data_versions: 버전별 데이터 (또는 델타)
- version_history: 변경 로그
"""

from .versioning import (
    DataVersionManager,
    VersionInfo,
    VersionStatus,
    VersionType,
)
from .snapshot import (
    SnapshotManager,
    Snapshot,
    SnapshotStatus,
    SnapshotType,
)
from .diff import (
    DiffEngine,
    DiffResult,
    ChangeType,
    FieldChange,
    RecordChange,
)
from .history import (
    HistoryTracker,
    HistoryEntry,
    HistoryFilter,
    HistoryStats,
)

__all__ = [
    # Versioning
    "DataVersionManager",
    "VersionInfo",
    "VersionStatus",
    "VersionType",
    # Snapshot
    "SnapshotManager",
    "Snapshot",
    "SnapshotStatus",
    "SnapshotType",
    # Diff
    "DiffEngine",
    "DiffResult",
    "ChangeType",
    "FieldChange",
    "RecordChange",
    # History
    "HistoryTracker",
    "HistoryEntry",
    "HistoryFilter",
    "HistoryStats",
]

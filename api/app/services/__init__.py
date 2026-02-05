"""Services for API business logic."""

from .mongo_service import MongoService
from .airflow_trigger import AirflowTrigger

# Streaming module (Lambda Architecture - Speed Layer)
from . import streaming

# Data Versioning module
from .data_versioning import (
    DataVersionManager,
    VersionInfo,
    VersionStatus,
    VersionType,
    SnapshotManager,
    Snapshot,
    SnapshotStatus,
    SnapshotType,
    DiffEngine,
    DiffResult,
    ChangeType,
    FieldChange,
    RecordChange,
    HistoryTracker,
    HistoryEntry,
    HistoryFilter,
    HistoryStats,
)

__all__ = [
    # Core Services
    'MongoService',
    'AirflowTrigger',
    'streaming',
    # Data Versioning
    'DataVersionManager',
    'VersionInfo',
    'VersionStatus',
    'VersionType',
    # Snapshot
    'SnapshotManager',
    'Snapshot',
    'SnapshotStatus',
    'SnapshotType',
    # Diff
    'DiffEngine',
    'DiffResult',
    'ChangeType',
    'FieldChange',
    'RecordChange',
    # History
    'HistoryTracker',
    'HistoryEntry',
    'HistoryFilter',
    'HistoryStats',
]

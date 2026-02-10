"""
Data Versioning API Router.

This module provides REST API endpoints for data version management,
including version creation, comparison, rollback, snapshots, and history tracking.
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from app.auth.dependencies import require_auth, require_scope, AuthContext
from app.services.mongo_service import MongoService
from app.services.data_versioning import (
    DataVersionManager,
    SnapshotManager,
    DiffEngine,
    HistoryTracker,
    VersionStatus,
    VersionType,
    SnapshotType,
    SnapshotStatus,
)
from app.core import get_logger

logger = get_logger(__name__)

router = APIRouter()


# ============================================================
# MongoDB Dependency
# ============================================================

def get_mongo():
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


# ============================================================
# Pydantic Models
# ============================================================

class VersionCreateRequest(BaseModel):
    data: List[dict]
    version_type: str = Field(default="full", description="full, delta, or incremental")
    description: str = ""
    tags: Optional[List[str]] = None
    branch: str = "main"


class VersionResponse(BaseModel):
    version_id: str
    source_id: str
    version_number: int
    version_type: str
    status: str
    record_count: int
    data_hash: str
    size_bytes: int
    changes_summary: dict
    branch: str
    tags: List[str]
    created_at: datetime
    created_by: str
    description: str


class VersionListResponse(BaseModel):
    versions: List[VersionResponse]
    total: int


class DiffResponse(BaseModel):
    version_a_id: str
    version_b_id: str
    added_count: int
    modified_count: int
    deleted_count: int
    unchanged_count: int
    added_records: List[dict] = []
    modified_records: List[dict] = []
    deleted_records: List[dict] = []


class SnapshotCreateRequest(BaseModel):
    name: str
    description: str = ""
    snapshot_type: str = "full"


class SnapshotResponse(BaseModel):
    snapshot_id: str
    source_id: str
    snapshot_type: str
    status: str
    record_count: int
    original_size_bytes: int
    created_at: datetime
    created_by: str
    description: str


class SnapshotListResponse(BaseModel):
    snapshots: List[SnapshotResponse]
    total: int


class RollbackRequest(BaseModel):
    target_version_id: str
    reason: str = ""


class HistoryFilterRequest(BaseModel):
    action: Optional[str] = None
    actor: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: int = 50


class HistoryResponse(BaseModel):
    entry_id: str
    source_id: str
    version_id: Optional[str]
    action: str
    actor: str
    summary: str
    timestamp: datetime
    success: bool


class HistoryListResponse(BaseModel):
    entries: List[HistoryResponse]
    total: int


class StatsResponse(BaseModel):
    source_id: str
    total_versions: int
    total_changes: int
    by_action: dict
    by_actor: dict
    recent_activity: List[dict]


# ============================================================
# Version Management Endpoints
# ============================================================

@router.get(
    "/{source_id}",
    response_model=VersionListResponse,
    summary="List Versions",
    description="Get a list of versions for a specific data source."
)
async def list_versions(
    source_id: str,
    branch: Optional[str] = Query(default=None, description="Filter by branch"),
    status: Optional[str] = Query(default=None, description="Filter by status"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
) -> VersionListResponse:
    """List all versions for a data source with optional filtering."""
    try:
        version_manager = DataVersionManager(mongo_service=mongo)

        # Parse status if provided
        version_status = None
        if status:
            try:
                version_status = VersionStatus(status)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

        versions = version_manager.list_versions(
            source_id=source_id,
            branch=branch,
            status=version_status,
            limit=limit,
            skip=offset,
        )

        return VersionListResponse(
            versions=[
                VersionResponse(
                    version_id=v.version_id,
                    source_id=v.source_id,
                    version_number=v.version_number,
                    version_type=v.version_type.value,
                    status=v.status.value,
                    record_count=v.record_count,
                    data_hash=v.data_hash,
                    size_bytes=v.size_bytes,
                    changes_summary=v.changes_summary,
                    branch=v.branch,
                    tags=v.tags,
                    created_at=v.created_at,
                    created_by=v.created_by,
                    description=v.description,
                )
                for v in versions
            ],
            total=len(versions)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list versions for source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list versions: {str(e)}")


@router.post(
    "/{source_id}",
    response_model=VersionResponse,
    summary="Create Version",
    description="Create a new version for a data source."
)
async def create_version(
    source_id: str,
    request: VersionCreateRequest,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write")),
) -> VersionResponse:
    """Create a new version of the data."""
    try:
        version_manager = DataVersionManager(mongo_service=mongo)

        # Parse version type
        try:
            version_type = VersionType(request.version_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid version_type: {request.version_type}")

        version_info = version_manager.create_version(
            source_id=source_id,
            data=request.data,
            version_type=version_type,
            branch=request.branch,
            created_by=auth.user_id or "system",
            description=request.description,
            tags=request.tags,
        )

        logger.info(f"Created version {version_info.version_number} for source {source_id}")

        return VersionResponse(
            version_id=version_info.version_id,
            source_id=version_info.source_id,
            version_number=version_info.version_number,
            version_type=version_info.version_type.value,
            status=version_info.status.value,
            record_count=version_info.record_count,
            data_hash=version_info.data_hash,
            size_bytes=version_info.size_bytes,
            changes_summary=version_info.changes_summary,
            branch=version_info.branch,
            tags=version_info.tags,
            created_at=version_info.created_at,
            created_by=version_info.created_by,
            description=version_info.description,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create version for source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create version: {str(e)}")


@router.get(
    "/{source_id}/latest",
    response_model=VersionResponse,
    summary="Get Latest Version",
    description="Get the latest version for a data source."
)
async def get_latest_version(
    source_id: str,
    branch: str = Query(default="main"),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
) -> VersionResponse:
    """Get the latest version of the data."""
    try:
        version_manager = DataVersionManager(mongo_service=mongo)
        version_info = version_manager.get_latest_version(source_id=source_id, branch=branch)

        if not version_info:
            raise HTTPException(
                status_code=404,
                detail=f"No versions found for source {source_id} on branch {branch}"
            )

        return VersionResponse(
            version_id=version_info.version_id,
            source_id=version_info.source_id,
            version_number=version_info.version_number,
            version_type=version_info.version_type.value,
            status=version_info.status.value,
            record_count=version_info.record_count,
            data_hash=version_info.data_hash,
            size_bytes=version_info.size_bytes,
            changes_summary=version_info.changes_summary,
            branch=version_info.branch,
            tags=version_info.tags,
            created_at=version_info.created_at,
            created_by=version_info.created_by,
            description=version_info.description,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get latest version for source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get latest version: {str(e)}")


# ============================================================
# Comparison and Diff Endpoints
# ============================================================

@router.get(
    "/{source_id}/diff",
    response_model=DiffResponse,
    summary="Compare Versions",
    description="Compare two versions and get the differences."
)
async def diff_versions(
    source_id: str,
    version_a: str = Query(..., description="First version ID to compare"),
    version_b: str = Query(..., description="Second version ID to compare"),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
) -> DiffResponse:
    """Compare two versions and return the differences."""
    try:
        diff_engine = DiffEngine(mongo_service=mongo)
        diff_result = diff_engine.diff_versions(
            version_a_id=version_a,
            version_b_id=version_b,
        )

        logger.info(f"Compared versions {version_a} and {version_b} for source {source_id}")

        return DiffResponse(
            version_a_id=version_a,
            version_b_id=version_b,
            added_count=diff_result.added_count,
            modified_count=diff_result.modified_count,
            deleted_count=diff_result.deleted_count,
            unchanged_count=diff_result.unchanged_count,
            added_records=[r.to_dict() for r in diff_result.added_records[:100]],
            modified_records=[r.to_dict() for r in diff_result.modified_records[:100]],
            deleted_records=[r.to_dict() for r in diff_result.deleted_records[:100]],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to diff versions for source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to compare versions: {str(e)}")


# ============================================================
# Snapshot Endpoints
# ============================================================

@router.post(
    "/{source_id}/snapshot",
    response_model=SnapshotResponse,
    summary="Create Snapshot",
    description="Create a snapshot of the current data state."
)
async def create_snapshot(
    source_id: str,
    request: SnapshotCreateRequest,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write")),
) -> SnapshotResponse:
    """Create a snapshot of the data."""
    try:
        snapshot_manager = SnapshotManager(mongo_service=mongo)

        # Parse snapshot type
        try:
            snapshot_type = SnapshotType(request.snapshot_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid snapshot_type: {request.snapshot_type}")

        snapshot = snapshot_manager.create_snapshot(
            source_id=source_id,
            name=request.name,
            description=request.description,
            snapshot_type=snapshot_type,
        )

        logger.info(f"Created snapshot {snapshot.snapshot_id} for source {source_id}")

        return SnapshotResponse(
            snapshot_id=snapshot.snapshot_id,
            source_id=snapshot.source_id,
            snapshot_type=snapshot.snapshot_type.value,
            status=snapshot.status.value,
            record_count=snapshot.record_count,
            original_size_bytes=snapshot.original_size_bytes,
            created_at=snapshot.created_at,
            created_by=snapshot.created_by,
            description=snapshot.description,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create snapshot for source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create snapshot: {str(e)}")


# ============================================================
# Rollback Endpoint
# ============================================================

@router.post(
    "/{source_id}/rollback",
    response_model=VersionResponse,
    summary="Rollback to Version",
    description="Rollback data to a specific version."
)
async def rollback_version(
    source_id: str,
    request: RollbackRequest,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write")),
) -> VersionResponse:
    """Rollback to a specific version."""
    try:
        version_manager = DataVersionManager(mongo_service=mongo)

        new_version = version_manager.rollback_to_version(
            source_id=source_id,
            target_version_id=request.target_version_id,
            created_by=auth.user_id or "system",
            reason=request.reason,
        )

        logger.info(f"Rolled back source {source_id} to version {request.target_version_id}")

        return VersionResponse(
            version_id=new_version.version_id,
            source_id=new_version.source_id,
            version_number=new_version.version_number,
            version_type=new_version.version_type.value,
            status=new_version.status.value,
            record_count=new_version.record_count,
            data_hash=new_version.data_hash,
            size_bytes=new_version.size_bytes,
            changes_summary=new_version.changes_summary,
            branch=new_version.branch,
            tags=new_version.tags,
            created_at=new_version.created_at,
            created_by=new_version.created_by,
            description=new_version.description,
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to rollback source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to rollback: {str(e)}")


# ============================================================
# History Endpoints
# ============================================================

@router.get(
    "/{source_id}/history",
    response_model=HistoryListResponse,
    summary="Get Change History",
    description="Get the change history for a data source."
)
async def get_history(
    source_id: str,
    action: Optional[str] = Query(default=None, description="Filter by action type"),
    actor: Optional[str] = Query(default=None, description="Filter by actor"),
    limit: int = Query(default=50, ge=1, le=200),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
) -> HistoryListResponse:
    """Get change history for a data source."""
    try:
        history_tracker = HistoryTracker(mongo_service=mongo)

        # Build filter
        from app.services.data_versioning.history import HistoryFilter
        history_filter = HistoryFilter(
            source_id=source_id,
            action=action,
            actor=actor,
            limit=limit,
        )

        entries = history_tracker.get_history(filter=history_filter)

        return HistoryListResponse(
            entries=[
                HistoryResponse(
                    entry_id=e.entry_id,
                    source_id=e.source_id,
                    version_id=e.version_id,
                    action=e.action.value,
                    actor=e.actor,
                    summary=e.summary,
                    timestamp=e.timestamp,
                    success=e.success,
                )
                for e in entries
            ],
            total=len(entries)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get history for source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get history: {str(e)}")

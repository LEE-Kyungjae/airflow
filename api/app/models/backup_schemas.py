"""
Pydantic schemas for Backup API request/response models.

These models define the structure of data for backup-related API endpoints,
providing validation, serialization, and OpenAPI documentation.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field


# ============================================================
# Enumerations
# ============================================================

class BackupTypeEnum(str, Enum):
    """Backup type enumeration."""
    FULL = "full"
    INCREMENTAL = "incremental"
    COLLECTION = "collection"


class BackupStatusEnum(str, Enum):
    """Backup job status enumeration."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPRESSING = "compressing"
    UPLOADING = "uploading"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"


class CloudProviderEnum(str, Enum):
    """Cloud storage provider enumeration."""
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"


# ============================================================
# Request Models
# ============================================================

class BackupTriggerRequest(BaseModel):
    """
    Request model for triggering a manual backup.

    Can trigger full database backup or specific collections only.
    """
    backup_type: BackupTypeEnum = Field(
        default=BackupTypeEnum.FULL,
        description="Type of backup: full, incremental, or collection"
    )
    collections: Optional[List[str]] = Field(
        None,
        description="List of collection names to backup (required if backup_type is 'collection')"
    )
    upload_to_cloud: bool = Field(
        default=True,
        description="Whether to upload backup to configured cloud storage"
    )
    skip_cleanup: bool = Field(
        default=False,
        description="Skip cleanup of old backups after this backup"
    )
    description: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional description for this backup"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "backup_type": "full",
                    "upload_to_cloud": True,
                    "skip_cleanup": False
                },
                {
                    "backup_type": "collection",
                    "collections": ["sources", "crawlers"],
                    "upload_to_cloud": True,
                    "description": "Pre-migration backup of critical collections"
                }
            ]
        }
    }


class RestoreTriggerRequest(BaseModel):
    """
    Request model for triggering a restore operation.

    WARNING: Restore operations can overwrite existing data.
    """
    backup_id: Optional[str] = Field(
        None,
        description="Backup ID to restore from (alternative to backup_path)"
    )
    backup_path: Optional[str] = Field(
        None,
        description="Local path to backup file (alternative to backup_id)"
    )
    s3_path: Optional[str] = Field(
        None,
        description="S3 path to backup file (s3://bucket/key)"
    )
    target_db: Optional[str] = Field(
        None,
        description="Target database name. If not specified, restores to original database"
    )
    collections: Optional[List[str]] = Field(
        None,
        description="Specific collections to restore. If not specified, restores all collections"
    )
    drop_existing: bool = Field(
        default=False,
        description="Whether to drop existing collections before restore. Use with caution!"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "backup_id": "full_20250205_020000",
                    "target_db": "crawler_system_restored",
                    "drop_existing": False
                },
                {
                    "s3_path": "s3://my-backups/mongodb/full_20250205_020000.tar.gz",
                    "collections": ["sources"],
                    "drop_existing": True
                }
            ]
        }
    }


class BackupConfigUpdate(BaseModel):
    """
    Request model for updating backup configuration.
    """
    schedule: Optional[str] = Field(
        None,
        description="Cron expression for backup schedule"
    )
    retention_days: Optional[int] = Field(
        None,
        ge=1,
        le=365,
        description="Number of days to retain backups"
    )
    cloud_provider: Optional[CloudProviderEnum] = Field(
        None,
        description="Cloud storage provider (local, s3, gcs, azure)"
    )
    s3_bucket: Optional[str] = Field(
        None,
        description="S3 bucket name for backups"
    )
    s3_prefix: Optional[str] = Field(
        None,
        description="S3 prefix (folder path) for backups"
    )
    gcs_bucket: Optional[str] = Field(
        None,
        description="GCS bucket name for backups"
    )
    gcs_prefix: Optional[str] = Field(
        None,
        description="GCS prefix (folder path) for backups"
    )
    compression_enabled: Optional[bool] = Field(
        None,
        description="Whether to compress backups"
    )
    verify_after_backup: Optional[bool] = Field(
        None,
        description="Whether to verify backups after creation"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "schedule": "0 3 * * *",
                "retention_days": 30,
                "cloud_provider": "s3",
                "compression_enabled": True
            }
        }
    }


# ============================================================
# Response Models
# ============================================================

class BackupInfo(BaseModel):
    """
    Response model for backup metadata information.
    """
    backup_id: str = Field(
        ...,
        description="Unique backup identifier"
    )
    backup_type: str = Field(
        ...,
        description="Type of backup (full, incremental, collection)"
    )
    created_at: datetime = Field(
        ...,
        description="Backup creation timestamp"
    )
    size_bytes: int = Field(
        ...,
        ge=0,
        description="Backup size in bytes"
    )
    collections: List[str] = Field(
        ...,
        description="List of collections included in backup"
    )
    location: str = Field(
        ...,
        description="Storage location (local, s3, gcs, azure)"
    )
    path: str = Field(
        ...,
        description="Full path to backup file"
    )
    verified: bool = Field(
        ...,
        description="Whether backup has been verified"
    )
    checksum: Optional[str] = Field(
        None,
        description="SHA-256 checksum of backup file"
    )
    mongo_version: Optional[str] = Field(
        None,
        description="MongoDB version at time of backup"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional backup metadata"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "backup_id": "full_20250205_020000",
                "backup_type": "full",
                "created_at": "2025-02-05T02:00:00Z",
                "size_bytes": 157286400,
                "collections": ["sources", "crawlers", "crawl_results", "error_logs"],
                "location": "s3",
                "path": "s3://my-backups/mongodb-backups/full_20250205_020000.tar.gz",
                "verified": True,
                "checksum": "abc123def456...",
                "mongo_version": "7.0.4",
                "metadata": {"compression": True}
            }
        }
    }


class BackupStatus(BaseModel):
    """
    Response model for backup job status.
    """
    backup_id: str = Field(
        ...,
        description="Backup job identifier"
    )
    status: BackupStatusEnum = Field(
        ...,
        description="Current job status"
    )
    progress: float = Field(
        ...,
        ge=0,
        le=1,
        description="Progress percentage (0.0 to 1.0)"
    )
    started_at: Optional[datetime] = Field(
        None,
        description="Job start timestamp"
    )
    completed_at: Optional[datetime] = Field(
        None,
        description="Job completion timestamp"
    )
    error: Optional[str] = Field(
        None,
        description="Error message if job failed"
    )
    current_step: Optional[str] = Field(
        None,
        description="Current step description"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "backup_id": "full_20250205_020000",
                "status": "in_progress",
                "progress": 0.45,
                "started_at": "2025-02-05T02:00:00Z",
                "completed_at": None,
                "error": None,
                "current_step": "Compressing backup"
            }
        }
    }


class BackupJobResponse(BaseModel):
    """
    Response model for backup trigger operations.
    """
    success: bool = Field(
        ...,
        description="Whether the backup job was successfully triggered"
    )
    backup_id: Optional[str] = Field(
        None,
        description="Generated backup ID"
    )
    dag_run_id: Optional[str] = Field(
        None,
        description="Airflow DAG run ID"
    )
    message: str = Field(
        ...,
        description="Status message"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "backup_id": "full_20250205_143000",
                "dag_run_id": "manual__2025-02-05T14:30:00+00:00",
                "message": "Backup job triggered successfully"
            }
        }
    }


class RestoreJobResponse(BaseModel):
    """
    Response model for restore trigger operations.
    """
    success: bool = Field(
        ...,
        description="Whether the restore job was successfully triggered"
    )
    dag_run_id: Optional[str] = Field(
        None,
        description="Airflow DAG run ID"
    )
    target_db: Optional[str] = Field(
        None,
        description="Target database name"
    )
    message: str = Field(
        ...,
        description="Status message"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "dag_run_id": "manual__2025-02-05T14:30:00+00:00",
                "target_db": "crawler_system_restored",
                "message": "Restore job triggered successfully. Monitor DAG for progress."
            }
        }
    }


class BackupConfigResponse(BaseModel):
    """
    Response model for backup configuration.
    """
    schedule: str = Field(
        ...,
        description="Cron expression for backup schedule"
    )
    retention_days: int = Field(
        ...,
        description="Number of days to retain backups"
    )
    cloud_enabled: bool = Field(
        ...,
        description="Whether cloud storage is enabled"
    )
    cloud_provider: Optional[str] = Field(
        None,
        description="Configured cloud provider"
    )
    s3_bucket: Optional[str] = Field(
        None,
        description="S3 bucket name (masked if configured)"
    )
    gcs_bucket: Optional[str] = Field(
        None,
        description="GCS bucket name (masked if configured)"
    )
    compression_enabled: bool = Field(
        ...,
        description="Whether compression is enabled"
    )
    verify_after_backup: bool = Field(
        ...,
        description="Whether verification is enabled"
    )
    last_backup: Optional[datetime] = Field(
        None,
        description="Timestamp of last successful backup"
    )
    next_backup: Optional[datetime] = Field(
        None,
        description="Estimated timestamp of next scheduled backup"
    )
    backup_dir: str = Field(
        ...,
        description="Local backup directory path"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "schedule": "0 2 * * *",
                "retention_days": 30,
                "cloud_enabled": True,
                "cloud_provider": "s3",
                "s3_bucket": "my-backups",
                "gcs_bucket": None,
                "compression_enabled": True,
                "verify_after_backup": True,
                "last_backup": "2025-02-05T02:00:00Z",
                "next_backup": "2025-02-06T02:00:00Z",
                "backup_dir": "/data/backups"
            }
        }
    }


class BackupListResponse(BaseModel):
    """
    Response model for listing backups.
    """
    backups: List[BackupInfo] = Field(
        ...,
        description="List of backup records"
    )
    total: int = Field(
        ...,
        ge=0,
        description="Total number of backups"
    )
    page: int = Field(
        default=1,
        ge=1,
        description="Current page number"
    )
    page_size: int = Field(
        default=20,
        ge=1,
        description="Items per page"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "backups": [],
                "total": 25,
                "page": 1,
                "page_size": 20
            }
        }
    }


class BackupStatsResponse(BaseModel):
    """
    Response model for backup statistics.
    """
    total_backups: int = Field(
        ...,
        ge=0,
        description="Total number of backups"
    )
    total_size_bytes: int = Field(
        ...,
        ge=0,
        description="Total size of all backups in bytes"
    )
    oldest_backup: Optional[datetime] = Field(
        None,
        description="Timestamp of oldest backup"
    )
    newest_backup: Optional[datetime] = Field(
        None,
        description="Timestamp of newest backup"
    )
    by_type: Dict[str, int] = Field(
        default_factory=dict,
        description="Backup count by type"
    )
    by_location: Dict[str, int] = Field(
        default_factory=dict,
        description="Backup count by storage location"
    )
    avg_backup_time_seconds: Optional[float] = Field(
        None,
        description="Average backup duration in seconds"
    )
    success_rate: Optional[float] = Field(
        None,
        ge=0,
        le=100,
        description="Backup success rate percentage"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "total_backups": 30,
                "total_size_bytes": 4718592000,
                "oldest_backup": "2025-01-06T02:00:00Z",
                "newest_backup": "2025-02-05T02:00:00Z",
                "by_type": {"full": 30, "collection": 5},
                "by_location": {"local": 30, "s3": 30},
                "avg_backup_time_seconds": 180.5,
                "success_rate": 98.5
            }
        }
    }


class BackupDeleteResponse(BaseModel):
    """
    Response model for backup deletion.
    """
    success: bool = Field(
        ...,
        description="Whether deletion was successful"
    )
    backup_id: str = Field(
        ...,
        description="Deleted backup ID"
    )
    message: str = Field(
        ...,
        description="Status message"
    )
    deleted_locations: List[str] = Field(
        default_factory=list,
        description="Locations where backup was deleted (local, s3, gcs)"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "backup_id": "full_20250101_020000",
                "message": "Backup deleted successfully",
                "deleted_locations": ["local", "s3"]
            }
        }
    }

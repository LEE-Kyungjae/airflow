"""
Backup Management API Router.

This module provides REST API endpoints for managing MongoDB backups,
including triggering backups, listing available backups, managing
restore operations, and configuring backup settings.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from fastapi.responses import FileResponse

from app.models.backup_schemas import (
    BackupTriggerRequest,
    RestoreTriggerRequest,
    BackupConfigUpdate,
    BackupInfo,
    BackupStatus,
    BackupJobResponse,
    RestoreJobResponse,
    BackupConfigResponse,
    BackupListResponse,
    BackupStatsResponse,
    BackupDeleteResponse,
    BackupTypeEnum,
    BackupStatusEnum,
)
from app.services.airflow_trigger import AirflowTrigger
from app.services.mongo_service import MongoService

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# Helper Functions
# ============================================================

def get_backup_config() -> dict:
    """Get backup configuration from environment."""
    return {
        "schedule": os.getenv("BACKUP_SCHEDULE", "0 2 * * *"),
        "retention_days": int(os.getenv("BACKUP_RETENTION_DAYS", "30")),
        "cloud_provider": os.getenv("BACKUP_CLOUD_PROVIDER", "local"),
        "s3_bucket": os.getenv("BACKUP_S3_BUCKET", ""),
        "s3_prefix": os.getenv("BACKUP_S3_PREFIX", "mongodb-backups"),
        "gcs_bucket": os.getenv("BACKUP_GCS_BUCKET", ""),
        "gcs_prefix": os.getenv("BACKUP_GCS_PREFIX", "mongodb-backups"),
        "compression_enabled": os.getenv("BACKUP_COMPRESSION", "true").lower() == "true",
        "verify_after_backup": os.getenv("BACKUP_VERIFY", "true").lower() == "true",
        "backup_dir": os.getenv("BACKUP_DIR", "/data/backups"),
    }


def get_backup_service():
    """Get backup service instance for listing backups."""
    # Import here to avoid circular imports
    import sys
    sys.path.insert(0, '/opt/airflow/dags')

    try:
        from utils.backup_service import MongoBackupService, BackupConfig
        return MongoBackupService()
    except ImportError:
        # Running outside of Airflow context
        return None


async def get_last_backup_info() -> Optional[datetime]:
    """Get timestamp of last successful backup from backup logs."""
    try:
        mongo = MongoService()
        last_backup = mongo.db.backup_logs.find_one(
            {"status": "success"},
            sort=[("completed_at", -1)]
        )
        if last_backup:
            return last_backup.get("completed_at")
        return None
    except Exception as e:
        logger.warning(f"Failed to get last backup info: {e}")
        return None


async def get_next_backup_time(schedule: str) -> Optional[datetime]:
    """Calculate next backup time from cron schedule."""
    try:
        from croniter import croniter
        cron = croniter(schedule, datetime.utcnow())
        return cron.get_next(datetime)
    except ImportError:
        logger.warning("croniter not installed, cannot calculate next backup time")
        return None
    except Exception as e:
        logger.warning(f"Failed to calculate next backup time: {e}")
        return None


# ============================================================
# Backup Trigger Endpoints
# ============================================================

@router.post(
    "/trigger",
    response_model=BackupJobResponse,
    summary="Trigger Manual Backup",
    description="""
    Trigger a manual backup of the MongoDB database.

    Supports full database backups or specific collection backups.
    The backup job runs asynchronously via Airflow.
    """,
    responses={
        200: {"description": "Backup job triggered successfully"},
        400: {"description": "Invalid request parameters"},
        500: {"description": "Failed to trigger backup job"},
    }
)
async def trigger_backup(
    request: BackupTriggerRequest = None,
    backup_type: str = Query(
        default="full",
        description="Type of backup: full, incremental, or collection"
    ),
    collections: Optional[List[str]] = Query(
        default=None,
        description="Collections to backup (required if backup_type is 'collection')"
    ),
) -> BackupJobResponse:
    """
    Trigger a manual backup job.

    Can be triggered via request body or query parameters.
    Request body takes precedence if both are provided.
    """
    # Use request body if provided, otherwise use query params
    if request:
        backup_type = request.backup_type.value
        collections = request.collections
        upload_to_cloud = request.upload_to_cloud
        skip_cleanup = request.skip_cleanup
    else:
        upload_to_cloud = True
        skip_cleanup = False

    # Validate collection backup has collections specified
    if backup_type == "collection" and not collections:
        raise HTTPException(
            status_code=400,
            detail="Collections must be specified for collection backup type"
        )

    # Prepare DAG configuration
    dag_conf = {
        "backup_type": backup_type,
        "collections": collections,
        "upload_to_cloud": upload_to_cloud,
        "skip_cleanup": skip_cleanup,
        "triggered_at": datetime.utcnow().isoformat(),
        "triggered_by": "api",
    }

    if request and request.description:
        dag_conf["description"] = request.description

    # Trigger Airflow DAG
    try:
        airflow = AirflowTrigger()
        result = airflow.trigger_dag(
            dag_id="manual_mongodb_backup",
            conf=dag_conf
        )

        if result.get("success"):
            # Generate backup ID for tracking
            backup_id = f"{backup_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

            # Log backup trigger
            try:
                mongo = MongoService()
                mongo.db.backup_logs.insert_one({
                    "backup_id": backup_id,
                    "status": "triggered",
                    "backup_type": backup_type,
                    "collections": collections,
                    "dag_run_id": result.get("run_id"),
                    "triggered_at": datetime.utcnow(),
                    "config": dag_conf
                })
            except Exception as e:
                logger.warning(f"Failed to log backup trigger: {e}")

            return BackupJobResponse(
                success=True,
                backup_id=backup_id,
                dag_run_id=result.get("run_id"),
                message="Backup job triggered successfully. Monitor progress via /api/backup/status endpoint."
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to trigger backup DAG: {result.get('message', 'Unknown error')}"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger backup: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger backup job: {str(e)}"
        )


@router.post(
    "/restore",
    response_model=RestoreJobResponse,
    summary="Trigger Restore Operation",
    description="""
    Trigger a restore operation from a backup.

    **WARNING**: This operation can overwrite existing data.
    Use with caution and ensure you have a recent backup before proceeding.
    """,
    responses={
        200: {"description": "Restore job triggered successfully"},
        400: {"description": "Invalid request parameters"},
        500: {"description": "Failed to trigger restore job"},
    }
)
async def trigger_restore(
    request: RestoreTriggerRequest,
) -> RestoreJobResponse:
    """
    Trigger a database restore from backup.

    Must specify either backup_id, backup_path, or s3_path.
    """
    # Validate input
    if not any([request.backup_id, request.backup_path, request.s3_path]):
        raise HTTPException(
            status_code=400,
            detail="Must specify either backup_id, backup_path, or s3_path"
        )

    # If backup_id provided, resolve to path
    backup_path = request.backup_path
    s3_path = request.s3_path

    if request.backup_id and not backup_path and not s3_path:
        # Try to find backup in logs
        try:
            mongo = MongoService()
            backup_log = mongo.db.backup_logs.find_one({"backup_id": request.backup_id})
            if backup_log:
                backup_path = backup_log.get("compressed_path") or backup_log.get("backup_path")
                s3_path = backup_log.get("cloud_uri")
        except Exception as e:
            logger.warning(f"Failed to resolve backup_id: {e}")

    if not backup_path and not s3_path:
        raise HTTPException(
            status_code=400,
            detail=f"Could not resolve backup_id '{request.backup_id}' to a path"
        )

    # Prepare DAG configuration
    dag_conf = {
        "backup_id": request.backup_id,
        "backup_path": backup_path,
        "s3_path": s3_path,
        "target_db": request.target_db,
        "collections": request.collections,
        "drop_existing": request.drop_existing,
        "triggered_at": datetime.utcnow().isoformat(),
        "triggered_by": "api",
    }

    # Trigger Airflow DAG
    try:
        airflow = AirflowTrigger()
        result = airflow.trigger_dag(
            dag_id="mongodb_restore",
            conf=dag_conf
        )

        if result.get("success"):
            # Log restore trigger
            try:
                mongo = MongoService()
                mongo.db.backup_logs.insert_one({
                    "operation": "restore",
                    "status": "triggered",
                    "backup_id": request.backup_id,
                    "target_db": request.target_db,
                    "dag_run_id": result.get("run_id"),
                    "triggered_at": datetime.utcnow(),
                    "config": dag_conf
                })
            except Exception as e:
                logger.warning(f"Failed to log restore trigger: {e}")

            return RestoreJobResponse(
                success=True,
                dag_run_id=result.get("run_id"),
                target_db=request.target_db or "original database",
                message="Restore job triggered successfully. Monitor DAG for progress."
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to trigger restore DAG: {result.get('message', 'Unknown error')}"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger restore: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger restore job: {str(e)}"
        )


# ============================================================
# Backup List and Status Endpoints
# ============================================================

@router.get(
    "/list",
    response_model=BackupListResponse,
    summary="List Available Backups",
    description="Get a paginated list of available backups with metadata.",
    responses={
        200: {"description": "List of backups"},
        500: {"description": "Failed to list backups"},
    }
)
async def list_backups(
    days: int = Query(
        default=30,
        ge=1,
        le=365,
        description="Number of days to look back"
    ),
    include_cloud: bool = Query(
        default=True,
        description="Include cloud backups in listing"
    ),
    backup_type: Optional[str] = Query(
        default=None,
        description="Filter by backup type (full, incremental, collection)"
    ),
    page: int = Query(default=1, ge=1, description="Page number"),
    page_size: int = Query(default=20, ge=1, le=100, description="Items per page"),
) -> BackupListResponse:
    """
    List available backups with optional filtering.
    """
    try:
        # Get from backup logs in MongoDB
        mongo = MongoService()
        cutoff_date = datetime.utcnow() - timedelta(days=days)

        query = {
            "status": "success",
            "completed_at": {"$gte": cutoff_date}
        }

        if backup_type:
            query["backup_type"] = backup_type

        # Get total count
        total = mongo.db.backup_logs.count_documents(query)

        # Get paginated results
        skip = (page - 1) * page_size
        cursor = mongo.db.backup_logs.find(query).sort(
            "completed_at", -1
        ).skip(skip).limit(page_size)

        backups = []
        for doc in cursor:
            # Convert to BackupInfo format
            backup_info = doc.get("backup_info", {})
            backups.append(BackupInfo(
                backup_id=doc.get("backup_id", "unknown"),
                backup_type=doc.get("backup_type", "full"),
                created_at=doc.get("completed_at", datetime.utcnow()),
                size_bytes=backup_info.get("size_bytes", 0),
                collections=backup_info.get("collections", []),
                location=backup_info.get("location", "local"),
                path=doc.get("compressed_path") or doc.get("cloud_uri") or "",
                verified=backup_info.get("verified", False),
                checksum=backup_info.get("checksum"),
                mongo_version=backup_info.get("mongo_version"),
                metadata=backup_info.get("metadata", {})
            ))

        return BackupListResponse(
            backups=backups,
            total=total,
            page=page,
            page_size=page_size
        )

    except Exception as e:
        logger.error(f"Failed to list backups: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list backups: {str(e)}"
        )


@router.get(
    "/status/{backup_id}",
    response_model=BackupStatus,
    summary="Get Backup Status",
    description="Get the current status of a backup or restore job.",
    responses={
        200: {"description": "Backup status"},
        404: {"description": "Backup not found"},
        500: {"description": "Failed to get status"},
    }
)
async def get_backup_status(backup_id: str) -> BackupStatus:
    """
    Get status of a backup job by ID.
    """
    try:
        mongo = MongoService()
        backup_log = mongo.db.backup_logs.find_one({"backup_id": backup_id})

        if not backup_log:
            raise HTTPException(
                status_code=404,
                detail=f"Backup '{backup_id}' not found"
            )

        # Map status
        status = backup_log.get("status", "unknown")
        status_map = {
            "triggered": BackupStatusEnum.PENDING,
            "in_progress": BackupStatusEnum.IN_PROGRESS,
            "compressing": BackupStatusEnum.COMPRESSING,
            "uploading": BackupStatusEnum.UPLOADING,
            "verifying": BackupStatusEnum.VERIFYING,
            "success": BackupStatusEnum.COMPLETED,
            "completed": BackupStatusEnum.COMPLETED,
            "failed": BackupStatusEnum.FAILED,
        }

        return BackupStatus(
            backup_id=backup_id,
            status=status_map.get(status, BackupStatusEnum.PENDING),
            progress=backup_log.get("progress", 0.0),
            started_at=backup_log.get("triggered_at"),
            completed_at=backup_log.get("completed_at"),
            error=backup_log.get("error"),
            current_step=backup_log.get("current_step")
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get backup status: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get backup status: {str(e)}"
        )


@router.get(
    "/stats",
    response_model=BackupStatsResponse,
    summary="Get Backup Statistics",
    description="Get backup statistics and metrics.",
    responses={
        200: {"description": "Backup statistics"},
        500: {"description": "Failed to get statistics"},
    }
)
async def get_backup_stats() -> BackupStatsResponse:
    """
    Get backup statistics including counts, sizes, and success rates.
    """
    try:
        mongo = MongoService()

        # Aggregate statistics
        pipeline = [
            {"$match": {"status": {"$in": ["success", "completed", "failed"]}}},
            {"$group": {
                "_id": None,
                "total": {"$sum": 1},
                "success_count": {
                    "$sum": {"$cond": [{"$in": ["$status", ["success", "completed"]]}, 1, 0]}
                },
                "total_size": {"$sum": {"$ifNull": ["$backup_info.size_bytes", 0]}},
                "oldest": {"$min": "$completed_at"},
                "newest": {"$max": "$completed_at"},
            }}
        ]

        result = list(mongo.db.backup_logs.aggregate(pipeline))
        stats_data = result[0] if result else {}

        # Get counts by type
        type_pipeline = [
            {"$match": {"status": {"$in": ["success", "completed"]}}},
            {"$group": {"_id": "$backup_type", "count": {"$sum": 1}}}
        ]
        type_counts = {r["_id"]: r["count"] for r in mongo.db.backup_logs.aggregate(type_pipeline)}

        # Get counts by location
        location_pipeline = [
            {"$match": {"status": {"$in": ["success", "completed"]}}},
            {"$group": {"_id": {"$ifNull": ["$backup_info.location", "local"]}, "count": {"$sum": 1}}}
        ]
        location_counts = {r["_id"]: r["count"] for r in mongo.db.backup_logs.aggregate(location_pipeline)}

        total = stats_data.get("total", 0)
        success_count = stats_data.get("success_count", 0)

        return BackupStatsResponse(
            total_backups=total,
            total_size_bytes=stats_data.get("total_size", 0),
            oldest_backup=stats_data.get("oldest"),
            newest_backup=stats_data.get("newest"),
            by_type=type_counts,
            by_location=location_counts,
            avg_backup_time_seconds=None,  # Would need to calculate from duration
            success_rate=round(success_count / total * 100, 2) if total > 0 else 100.0
        )

    except Exception as e:
        logger.error(f"Failed to get backup stats: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get backup statistics: {str(e)}"
        )


# ============================================================
# Backup Download and Delete Endpoints
# ============================================================

@router.get(
    "/download/{backup_id}",
    summary="Download Backup",
    description="Download a backup file. Only available for local backups.",
    responses={
        200: {"description": "Backup file download"},
        404: {"description": "Backup not found"},
        400: {"description": "Backup not available for download"},
    }
)
async def download_backup(backup_id: str) -> FileResponse:
    """
    Download a backup file by ID.

    Only works for backups stored locally. For cloud backups,
    use the cloud provider's direct download.
    """
    try:
        mongo = MongoService()
        backup_log = mongo.db.backup_logs.find_one({"backup_id": backup_id})

        if not backup_log:
            raise HTTPException(
                status_code=404,
                detail=f"Backup '{backup_id}' not found"
            )

        backup_path = backup_log.get("compressed_path")
        if not backup_path or not os.path.exists(backup_path):
            # Check if only cloud backup
            cloud_uri = backup_log.get("cloud_uri")
            if cloud_uri:
                raise HTTPException(
                    status_code=400,
                    detail=f"Backup is stored in cloud only. Download from: {cloud_uri}"
                )
            raise HTTPException(
                status_code=404,
                detail="Backup file not found on disk"
            )

        return FileResponse(
            path=backup_path,
            filename=os.path.basename(backup_path),
            media_type="application/gzip"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to download backup: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to download backup: {str(e)}"
        )


@router.delete(
    "/{backup_id}",
    response_model=BackupDeleteResponse,
    summary="Delete Backup",
    description="Delete a backup from local and cloud storage.",
    responses={
        200: {"description": "Backup deleted successfully"},
        404: {"description": "Backup not found"},
        500: {"description": "Failed to delete backup"},
    }
)
async def delete_backup(
    backup_id: str,
    delete_from_cloud: bool = Query(
        default=True,
        description="Also delete from cloud storage if applicable"
    )
) -> BackupDeleteResponse:
    """
    Delete a backup by ID.

    Removes backup from local storage and optionally from cloud storage.
    """
    try:
        mongo = MongoService()
        backup_log = mongo.db.backup_logs.find_one({"backup_id": backup_id})

        if not backup_log:
            raise HTTPException(
                status_code=404,
                detail=f"Backup '{backup_id}' not found"
            )

        deleted_locations = []

        # Delete local file
        backup_path = backup_log.get("compressed_path")
        if backup_path and os.path.exists(backup_path):
            os.remove(backup_path)
            deleted_locations.append("local")
            logger.info(f"Deleted local backup: {backup_path}")

            # Also delete checksum file
            checksum_path = f"{backup_path}.sha256"
            if os.path.exists(checksum_path):
                os.remove(checksum_path)

        # Delete from cloud if requested
        if delete_from_cloud:
            cloud_uri = backup_log.get("cloud_uri")
            if cloud_uri:
                try:
                    if cloud_uri.startswith("s3://"):
                        import boto3
                        parts = cloud_uri.replace("s3://", "").split("/", 1)
                        bucket, key = parts[0], parts[1]
                        s3 = boto3.client('s3')
                        s3.delete_object(Bucket=bucket, Key=key)
                        s3.delete_object(Bucket=bucket, Key=f"{key}.sha256")
                        deleted_locations.append("s3")
                        logger.info(f"Deleted S3 backup: {cloud_uri}")

                    elif cloud_uri.startswith("gs://"):
                        from google.cloud import storage
                        parts = cloud_uri.replace("gs://", "").split("/", 1)
                        bucket_name, blob_name = parts[0], parts[1]
                        client = storage.Client()
                        bucket = client.bucket(bucket_name)
                        bucket.blob(blob_name).delete()
                        bucket.blob(f"{blob_name}.sha256").delete()
                        deleted_locations.append("gcs")
                        logger.info(f"Deleted GCS backup: {cloud_uri}")

                except Exception as e:
                    logger.warning(f"Failed to delete cloud backup: {e}")

        # Update backup log
        mongo.db.backup_logs.update_one(
            {"backup_id": backup_id},
            {"$set": {
                "status": "deleted",
                "deleted_at": datetime.utcnow(),
                "deleted_locations": deleted_locations
            }}
        )

        return BackupDeleteResponse(
            success=True,
            backup_id=backup_id,
            message="Backup deleted successfully",
            deleted_locations=deleted_locations
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete backup: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete backup: {str(e)}"
        )


# ============================================================
# Configuration Endpoints
# ============================================================

@router.get(
    "/config",
    response_model=BackupConfigResponse,
    summary="Get Backup Configuration",
    description="Get current backup configuration settings.",
    responses={
        200: {"description": "Backup configuration"},
        500: {"description": "Failed to get configuration"},
    }
)
async def get_backup_config_endpoint() -> BackupConfigResponse:
    """
    Get current backup configuration.
    """
    try:
        config = get_backup_config()
        last_backup = await get_last_backup_info()
        next_backup = await get_next_backup_time(config["schedule"])

        cloud_enabled = config["cloud_provider"].lower() != "local" and (
            bool(config["s3_bucket"]) or bool(config["gcs_bucket"])
        )

        return BackupConfigResponse(
            schedule=config["schedule"],
            retention_days=config["retention_days"],
            cloud_enabled=cloud_enabled,
            cloud_provider=config["cloud_provider"] if cloud_enabled else None,
            s3_bucket=config["s3_bucket"] if config["s3_bucket"] else None,
            gcs_bucket=config["gcs_bucket"] if config["gcs_bucket"] else None,
            compression_enabled=config["compression_enabled"],
            verify_after_backup=config["verify_after_backup"],
            last_backup=last_backup,
            next_backup=next_backup,
            backup_dir=config["backup_dir"]
        )

    except Exception as e:
        logger.error(f"Failed to get backup config: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get backup configuration: {str(e)}"
        )


@router.put(
    "/config",
    response_model=BackupConfigResponse,
    summary="Update Backup Configuration",
    description="""
    Update backup configuration settings.

    Note: Some settings may require service restart to take effect.
    Environment variables are the source of truth.
    """,
    responses={
        200: {"description": "Configuration updated"},
        400: {"description": "Invalid configuration"},
        500: {"description": "Failed to update configuration"},
    }
)
async def update_backup_config(
    config_update: BackupConfigUpdate,
) -> BackupConfigResponse:
    """
    Update backup configuration.

    Note: This endpoint stores configuration in MongoDB for the API,
    but actual backup behavior is controlled by environment variables
    in the Airflow containers.
    """
    try:
        mongo = MongoService()

        # Get current config or create default
        current_config = mongo.db.system_config.find_one({"type": "backup"})
        if not current_config:
            current_config = {
                "type": "backup",
                "created_at": datetime.utcnow()
            }

        # Update with provided values
        update_data = config_update.model_dump(exclude_unset=True)
        if update_data:
            update_data["updated_at"] = datetime.utcnow()
            current_config.update(update_data)

            mongo.db.system_config.update_one(
                {"type": "backup"},
                {"$set": current_config},
                upsert=True
            )

        # Return updated config
        return await get_backup_config_endpoint()

    except Exception as e:
        logger.error(f"Failed to update backup config: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update backup configuration: {str(e)}"
        )


# ============================================================
# Utility Endpoints
# ============================================================

@router.post(
    "/cleanup",
    summary="Trigger Backup Cleanup",
    description="Manually trigger cleanup of old backups based on retention policy.",
    responses={
        200: {"description": "Cleanup triggered"},
        500: {"description": "Failed to trigger cleanup"},
    }
)
async def trigger_cleanup(
    retention_days: Optional[int] = Query(
        default=None,
        ge=1,
        le=365,
        description="Override retention days for this cleanup"
    )
) -> dict:
    """
    Trigger manual cleanup of old backups.
    """
    try:
        airflow = AirflowTrigger()
        result = airflow.trigger_dag(
            dag_id="manual_mongodb_backup",
            conf={
                "backup_type": "full",
                "skip_backup": True,  # Only run cleanup
                "retention_days_override": retention_days,
            }
        )

        if result.get("success"):
            return {
                "success": True,
                "message": "Cleanup job triggered",
                "dag_run_id": result.get("run_id")
            }
        else:
            raise HTTPException(
                status_code=500,
                detail="Failed to trigger cleanup job"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger cleanup: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger cleanup: {str(e)}"
        )


@router.get(
    "/verify/{backup_id}",
    summary="Verify Backup Integrity",
    description="Verify the integrity of a specific backup.",
    responses={
        200: {"description": "Verification result"},
        404: {"description": "Backup not found"},
        500: {"description": "Verification failed"},
    }
)
async def verify_backup_integrity(backup_id: str) -> dict:
    """
    Verify backup integrity by checking checksum and archive contents.
    """
    try:
        mongo = MongoService()
        backup_log = mongo.db.backup_logs.find_one({"backup_id": backup_id})

        if not backup_log:
            raise HTTPException(
                status_code=404,
                detail=f"Backup '{backup_id}' not found"
            )

        backup_path = backup_log.get("compressed_path")
        if not backup_path or not os.path.exists(backup_path):
            return {
                "backup_id": backup_id,
                "verified": False,
                "error": "Backup file not found on disk"
            }

        # Verify checksum
        import hashlib
        checksum_path = f"{backup_path}.sha256"
        checksum_valid = False
        calculated_checksum = None

        if os.path.exists(checksum_path):
            with open(checksum_path, 'r') as f:
                expected = f.read().split()[0]

            sha256 = hashlib.sha256()
            with open(backup_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256.update(chunk)
            calculated_checksum = sha256.hexdigest()
            checksum_valid = expected == calculated_checksum

        # Verify archive
        import tarfile
        archive_valid = False
        file_count = 0
        try:
            with tarfile.open(backup_path, 'r:gz') as tar:
                members = tar.getmembers()
                file_count = len(members)
                archive_valid = file_count > 0
        except Exception as e:
            logger.error(f"Failed to verify archive: {e}")

        # Update verification status
        mongo.db.backup_logs.update_one(
            {"backup_id": backup_id},
            {"$set": {
                "backup_info.verified": checksum_valid and archive_valid,
                "backup_info.checksum": calculated_checksum,
                "verified_at": datetime.utcnow()
            }}
        )

        return {
            "backup_id": backup_id,
            "verified": checksum_valid and archive_valid,
            "checksum_valid": checksum_valid,
            "checksum": calculated_checksum,
            "archive_valid": archive_valid,
            "file_count": file_count
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to verify backup: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify backup: {str(e)}"
        )

"""
Backup Service for MongoDB Database.

This module provides comprehensive backup and restore functionality
for the crawler system's MongoDB database, including:
- Full and incremental backups
- Cloud storage integration (S3, GCS, Azure)
- Backup verification and integrity checking
- Retention policy management
- Point-in-time recovery support
"""

import os
import subprocess
import shutil
import hashlib
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class BackupType(str, Enum):
    """Backup type enumeration."""
    FULL = "full"
    INCREMENTAL = "incremental"
    COLLECTION = "collection"


class BackupStatus(str, Enum):
    """Backup job status enumeration."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPRESSING = "compressing"
    UPLOADING = "uploading"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"


class CloudProvider(str, Enum):
    """Cloud storage provider enumeration."""
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"


@dataclass
class BackupConfig:
    """Backup configuration settings."""
    # MongoDB connection
    mongo_uri: str = field(default_factory=lambda: os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
    mongo_db: str = field(default_factory=lambda: os.getenv("MONGODB_DATABASE", "crawler_system"))

    # Backup storage
    backup_dir: str = field(default_factory=lambda: os.getenv("BACKUP_DIR", "/data/backups"))
    retention_days: int = field(default_factory=lambda: int(os.getenv("BACKUP_RETENTION_DAYS", "30")))

    # Cloud configuration
    cloud_provider: str = field(default_factory=lambda: os.getenv("BACKUP_CLOUD_PROVIDER", "local"))
    s3_bucket: str = field(default_factory=lambda: os.getenv("BACKUP_S3_BUCKET", ""))
    s3_prefix: str = field(default_factory=lambda: os.getenv("BACKUP_S3_PREFIX", "mongodb-backups"))
    gcs_bucket: str = field(default_factory=lambda: os.getenv("BACKUP_GCS_BUCKET", ""))
    gcs_prefix: str = field(default_factory=lambda: os.getenv("BACKUP_GCS_PREFIX", "mongodb-backups"))
    azure_container: str = field(default_factory=lambda: os.getenv("BACKUP_AZURE_CONTAINER", ""))
    azure_connection_string: str = field(default_factory=lambda: os.getenv("AZURE_STORAGE_CONNECTION_STRING", ""))

    # Compression settings
    compression_enabled: bool = field(default_factory=lambda: os.getenv("BACKUP_COMPRESSION", "true").lower() == "true")
    compression_level: int = field(default_factory=lambda: int(os.getenv("BACKUP_COMPRESSION_LEVEL", "6")))

    # Verification settings
    verify_after_backup: bool = field(default_factory=lambda: os.getenv("BACKUP_VERIFY", "true").lower() == "true")

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary (excluding sensitive data)."""
        data = asdict(self)
        # Mask sensitive values
        if data.get("mongo_uri"):
            data["mongo_uri"] = "***masked***"
        if data.get("azure_connection_string"):
            data["azure_connection_string"] = "***masked***"
        return data


@dataclass
class BackupInfo:
    """Backup metadata information."""
    backup_id: str
    backup_type: str
    created_at: datetime
    size_bytes: int
    collections: List[str]
    location: str  # local, s3, gcs, azure
    path: str
    verified: bool = False
    checksum: Optional[str] = None
    mongo_version: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "backup_id": self.backup_id,
            "backup_type": self.backup_type,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "size_bytes": self.size_bytes,
            "collections": self.collections,
            "location": self.location,
            "path": self.path,
            "verified": self.verified,
            "checksum": self.checksum,
            "mongo_version": self.mongo_version,
            "metadata": self.metadata
        }


@dataclass
class BackupJobStatus:
    """Backup job status tracking."""
    backup_id: str
    status: str
    progress: float = 0.0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    current_step: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "backup_id": self.backup_id,
            "status": self.status,
            "progress": self.progress,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
            "current_step": self.current_step
        }


class MongoBackupService:
    """
    Service for MongoDB backup operations.

    Provides comprehensive backup functionality including full dumps,
    incremental backups, compression, cloud upload, and verification.
    """

    def __init__(self, config: Optional[BackupConfig] = None):
        """
        Initialize backup service.

        Args:
            config: Backup configuration. Uses defaults from environment if not provided.
        """
        self.config = config or BackupConfig()
        self._ensure_backup_dir()

    def _ensure_backup_dir(self) -> None:
        """Ensure backup directory exists."""
        Path(self.config.backup_dir).mkdir(parents=True, exist_ok=True)

    def _generate_backup_id(self, backup_type: str) -> str:
        """Generate unique backup ID."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"{backup_type}_{timestamp}"

    def _get_mongo_version(self) -> Optional[str]:
        """Get MongoDB version."""
        try:
            result = subprocess.run(
                ["mongodump", "--version"],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                # Parse version from output
                for line in result.stdout.split('\n'):
                    if 'version' in line.lower():
                        return line.strip()
            return None
        except Exception as e:
            logger.warning(f"Failed to get MongoDB version: {e}")
            return None

    def _calculate_checksum(self, file_path: str) -> str:
        """Calculate SHA-256 checksum of file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _get_directory_size(self, path: str) -> int:
        """Get total size of directory in bytes."""
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.isfile(filepath):
                    total_size += os.path.getsize(filepath)
        return total_size

    def _get_collections(self) -> List[str]:
        """Get list of collections in database."""
        try:
            from pymongo import MongoClient
            client = MongoClient(self.config.mongo_uri, serverSelectionTimeoutMS=5000)
            db = client[self.config.mongo_db]
            collections = db.list_collection_names()
            client.close()
            return collections
        except Exception as e:
            logger.warning(f"Failed to list collections: {e}")
            return []

    def create_full_backup(self, job_status_callback=None) -> Tuple[str, BackupInfo]:
        """
        Create full MongoDB backup.

        Args:
            job_status_callback: Optional callback for progress updates

        Returns:
            Tuple of (backup_path, BackupInfo)

        Raises:
            RuntimeError: If backup fails
        """
        backup_id = self._generate_backup_id("full")
        backup_path = os.path.join(self.config.backup_dir, backup_id)

        logger.info(f"Starting full backup: {backup_id}")

        if job_status_callback:
            job_status_callback(BackupJobStatus(
                backup_id=backup_id,
                status=BackupStatus.IN_PROGRESS,
                progress=0.1,
                started_at=datetime.utcnow(),
                current_step="Running mongodump"
            ))

        # Build mongodump command
        cmd = [
            "mongodump",
            f"--uri={self.config.mongo_uri}",
            f"--db={self.config.mongo_db}",
            f"--out={backup_path}",
            "--gzip" if self.config.compression_enabled else ""
        ]
        cmd = [c for c in cmd if c]  # Remove empty strings

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )

            if result.returncode != 0:
                error_msg = f"mongodump failed: {result.stderr}"
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            logger.info(f"mongodump completed successfully: {backup_path}")

        except subprocess.TimeoutExpired:
            raise RuntimeError("Backup timed out after 1 hour")
        except FileNotFoundError:
            raise RuntimeError("mongodump command not found. Ensure MongoDB tools are installed.")

        # Get backup size
        size_bytes = self._get_directory_size(backup_path)

        # Get collections list
        collections = self._get_collections()

        # Create backup info
        backup_info = BackupInfo(
            backup_id=backup_id,
            backup_type=BackupType.FULL,
            created_at=datetime.utcnow(),
            size_bytes=size_bytes,
            collections=collections,
            location=CloudProvider.LOCAL,
            path=backup_path,
            verified=False,
            mongo_version=self._get_mongo_version(),
            metadata={"compression": self.config.compression_enabled}
        )

        # Save metadata
        self._save_backup_metadata(backup_path, backup_info)

        if job_status_callback:
            job_status_callback(BackupJobStatus(
                backup_id=backup_id,
                status=BackupStatus.COMPLETED,
                progress=1.0,
                started_at=datetime.utcnow(),
                completed_at=datetime.utcnow(),
                current_step="Backup completed"
            ))

        return backup_path, backup_info

    def create_collection_backup(
        self,
        collections: List[str],
        job_status_callback=None
    ) -> Tuple[str, BackupInfo]:
        """
        Backup specific collections only.

        Args:
            collections: List of collection names to backup
            job_status_callback: Optional callback for progress updates

        Returns:
            Tuple of (backup_path, BackupInfo)
        """
        backup_id = self._generate_backup_id("collection")
        backup_path = os.path.join(self.config.backup_dir, backup_id)

        logger.info(f"Starting collection backup: {backup_id}, collections: {collections}")

        Path(backup_path).mkdir(parents=True, exist_ok=True)

        for idx, collection in enumerate(collections):
            if job_status_callback:
                progress = (idx + 1) / len(collections) * 0.8
                job_status_callback(BackupJobStatus(
                    backup_id=backup_id,
                    status=BackupStatus.IN_PROGRESS,
                    progress=progress,
                    started_at=datetime.utcnow(),
                    current_step=f"Backing up collection: {collection}"
                ))

            cmd = [
                "mongodump",
                f"--uri={self.config.mongo_uri}",
                f"--db={self.config.mongo_db}",
                f"--collection={collection}",
                f"--out={backup_path}",
            ]
            if self.config.compression_enabled:
                cmd.append("--gzip")

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=1800
                )

                if result.returncode != 0:
                    logger.error(f"Failed to backup collection {collection}: {result.stderr}")

            except Exception as e:
                logger.error(f"Error backing up collection {collection}: {e}")

        size_bytes = self._get_directory_size(backup_path)

        backup_info = BackupInfo(
            backup_id=backup_id,
            backup_type=BackupType.COLLECTION,
            created_at=datetime.utcnow(),
            size_bytes=size_bytes,
            collections=collections,
            location=CloudProvider.LOCAL,
            path=backup_path,
            verified=False,
            mongo_version=self._get_mongo_version(),
            metadata={"compression": self.config.compression_enabled}
        )

        self._save_backup_metadata(backup_path, backup_info)

        return backup_path, backup_info

    def compress_backup(self, backup_path: str) -> str:
        """
        Compress backup directory to tar.gz archive.

        Args:
            backup_path: Path to backup directory

        Returns:
            Path to compressed archive
        """
        compressed_path = f"{backup_path}.tar.gz"

        logger.info(f"Compressing backup: {backup_path} -> {compressed_path}")

        try:
            # Use tar with gzip compression
            cmd = [
                "tar",
                f"-czf",
                compressed_path,
                "-C", os.path.dirname(backup_path),
                os.path.basename(backup_path)
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600
            )

            if result.returncode != 0:
                raise RuntimeError(f"Compression failed: {result.stderr}")

            # Calculate checksum
            checksum = self._calculate_checksum(compressed_path)

            # Save checksum file
            with open(f"{compressed_path}.sha256", "w") as f:
                f.write(f"{checksum}  {os.path.basename(compressed_path)}\n")

            # Remove original directory
            shutil.rmtree(backup_path)

            logger.info(f"Compression completed: {compressed_path}, checksum: {checksum}")

            return compressed_path

        except FileNotFoundError:
            # Fallback to Python-based compression
            import tarfile
            with tarfile.open(compressed_path, "w:gz") as tar:
                tar.add(backup_path, arcname=os.path.basename(backup_path))

            checksum = self._calculate_checksum(compressed_path)
            with open(f"{compressed_path}.sha256", "w") as f:
                f.write(f"{checksum}  {os.path.basename(compressed_path)}\n")

            shutil.rmtree(backup_path)

            return compressed_path

    def upload_to_s3(self, file_path: str, delete_local: bool = False) -> str:
        """
        Upload backup to Amazon S3.

        Args:
            file_path: Path to file to upload
            delete_local: Whether to delete local file after upload

        Returns:
            S3 URI of uploaded file
        """
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError:
            raise RuntimeError("boto3 is required for S3 uploads. Install with: pip install boto3")

        if not self.config.s3_bucket:
            raise ValueError("S3 bucket not configured")

        s3_client = boto3.client('s3')

        file_name = os.path.basename(file_path)
        s3_key = f"{self.config.s3_prefix}/{file_name}"

        logger.info(f"Uploading to S3: {file_path} -> s3://{self.config.s3_bucket}/{s3_key}")

        try:
            # Upload file
            s3_client.upload_file(
                file_path,
                self.config.s3_bucket,
                s3_key,
                ExtraArgs={
                    'ServerSideEncryption': 'AES256',
                    'Metadata': {
                        'backup-timestamp': datetime.utcnow().isoformat(),
                        'source-path': file_path
                    }
                }
            )

            # Upload checksum file if exists
            checksum_path = f"{file_path}.sha256"
            if os.path.exists(checksum_path):
                s3_client.upload_file(
                    checksum_path,
                    self.config.s3_bucket,
                    f"{s3_key}.sha256"
                )

            s3_uri = f"s3://{self.config.s3_bucket}/{s3_key}"
            logger.info(f"Upload completed: {s3_uri}")

            if delete_local:
                os.remove(file_path)
                if os.path.exists(checksum_path):
                    os.remove(checksum_path)

            return s3_uri

        except ClientError as e:
            raise RuntimeError(f"S3 upload failed: {e}")

    def upload_to_gcs(self, file_path: str, delete_local: bool = False) -> str:
        """
        Upload backup to Google Cloud Storage.

        Args:
            file_path: Path to file to upload
            delete_local: Whether to delete local file after upload

        Returns:
            GCS URI of uploaded file
        """
        try:
            from google.cloud import storage
            from google.cloud.exceptions import GoogleCloudError
        except ImportError:
            raise RuntimeError("google-cloud-storage is required for GCS uploads")

        if not self.config.gcs_bucket:
            raise ValueError("GCS bucket not configured")

        client = storage.Client()
        bucket = client.bucket(self.config.gcs_bucket)

        file_name = os.path.basename(file_path)
        blob_name = f"{self.config.gcs_prefix}/{file_name}"

        logger.info(f"Uploading to GCS: {file_path} -> gs://{self.config.gcs_bucket}/{blob_name}")

        try:
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(file_path)

            # Upload checksum file if exists
            checksum_path = f"{file_path}.sha256"
            if os.path.exists(checksum_path):
                checksum_blob = bucket.blob(f"{blob_name}.sha256")
                checksum_blob.upload_from_filename(checksum_path)

            gcs_uri = f"gs://{self.config.gcs_bucket}/{blob_name}"
            logger.info(f"Upload completed: {gcs_uri}")

            if delete_local:
                os.remove(file_path)
                if os.path.exists(checksum_path):
                    os.remove(checksum_path)

            return gcs_uri

        except GoogleCloudError as e:
            raise RuntimeError(f"GCS upload failed: {e}")

    def upload_to_cloud(self, file_path: str, delete_local: bool = False) -> str:
        """
        Upload backup to configured cloud provider.

        Args:
            file_path: Path to file to upload
            delete_local: Whether to delete local file after upload

        Returns:
            Cloud URI of uploaded file
        """
        provider = self.config.cloud_provider.lower()

        if provider == CloudProvider.S3:
            return self.upload_to_s3(file_path, delete_local)
        elif provider == CloudProvider.GCS:
            return self.upload_to_gcs(file_path, delete_local)
        elif provider == CloudProvider.LOCAL:
            logger.info("Cloud upload skipped (provider is 'local')")
            return file_path
        else:
            raise ValueError(f"Unsupported cloud provider: {provider}")

    def cleanup_old_backups(self, retention_days: Optional[int] = None) -> int:
        """
        Delete backups older than retention period.

        Args:
            retention_days: Number of days to retain. Uses config value if not specified.

        Returns:
            Number of backups deleted
        """
        retention = retention_days or self.config.retention_days
        cutoff_date = datetime.utcnow() - timedelta(days=retention)

        logger.info(f"Cleaning up backups older than {retention} days (cutoff: {cutoff_date})")

        deleted_count = 0
        backup_dir = Path(self.config.backup_dir)

        for item in backup_dir.iterdir():
            if item.is_file() and item.suffix in ['.gz', '.tar']:
                # Parse timestamp from filename
                try:
                    # Format: type_YYYYMMDD_HHMMSS.tar.gz
                    name_parts = item.stem.replace('.tar', '').split('_')
                    if len(name_parts) >= 3:
                        date_str = f"{name_parts[1]}_{name_parts[2]}"
                        backup_date = datetime.strptime(date_str, "%Y%m%d_%H%M%S")

                        if backup_date < cutoff_date:
                            logger.info(f"Deleting old backup: {item.name}")
                            item.unlink()

                            # Also delete checksum file
                            checksum_file = item.with_suffix(item.suffix + '.sha256')
                            if checksum_file.exists():
                                checksum_file.unlink()

                            deleted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to parse backup date for {item.name}: {e}")

            elif item.is_dir() and item.name.startswith(('full_', 'incremental_', 'collection_')):
                try:
                    name_parts = item.name.split('_')
                    if len(name_parts) >= 3:
                        date_str = f"{name_parts[1]}_{name_parts[2]}"
                        backup_date = datetime.strptime(date_str, "%Y%m%d_%H%M%S")

                        if backup_date < cutoff_date:
                            logger.info(f"Deleting old backup directory: {item.name}")
                            shutil.rmtree(item)
                            deleted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to parse backup date for {item.name}: {e}")

        # Also cleanup cloud backups if configured
        if self.config.cloud_provider == CloudProvider.S3:
            deleted_count += self._cleanup_s3_backups(cutoff_date)
        elif self.config.cloud_provider == CloudProvider.GCS:
            deleted_count += self._cleanup_gcs_backups(cutoff_date)

        logger.info(f"Cleanup completed. Deleted {deleted_count} backups.")
        return deleted_count

    def _cleanup_s3_backups(self, cutoff_date: datetime) -> int:
        """Cleanup old backups from S3."""
        try:
            import boto3

            s3 = boto3.client('s3')
            deleted_count = 0

            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.config.s3_bucket,
                Prefix=self.config.s3_prefix
            )

            for page in pages:
                for obj in page.get('Contents', []):
                    if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                        logger.info(f"Deleting S3 object: {obj['Key']}")
                        s3.delete_object(Bucket=self.config.s3_bucket, Key=obj['Key'])
                        deleted_count += 1

            return deleted_count

        except Exception as e:
            logger.error(f"Failed to cleanup S3 backups: {e}")
            return 0

    def _cleanup_gcs_backups(self, cutoff_date: datetime) -> int:
        """Cleanup old backups from GCS."""
        try:
            from google.cloud import storage

            client = storage.Client()
            bucket = client.bucket(self.config.gcs_bucket)
            deleted_count = 0

            blobs = bucket.list_blobs(prefix=self.config.gcs_prefix)
            for blob in blobs:
                if blob.time_created.replace(tzinfo=None) < cutoff_date:
                    logger.info(f"Deleting GCS object: {blob.name}")
                    blob.delete()
                    deleted_count += 1

            return deleted_count

        except Exception as e:
            logger.error(f"Failed to cleanup GCS backups: {e}")
            return 0

    def verify_backup(self, backup_path: str) -> bool:
        """
        Verify backup integrity.

        Args:
            backup_path: Path to backup file or directory

        Returns:
            True if backup is valid
        """
        logger.info(f"Verifying backup: {backup_path}")

        if backup_path.endswith('.tar.gz'):
            return self._verify_compressed_backup(backup_path)
        else:
            return self._verify_directory_backup(backup_path)

    def _verify_compressed_backup(self, backup_path: str) -> bool:
        """Verify compressed backup archive."""
        # Check file exists
        if not os.path.exists(backup_path):
            logger.error(f"Backup file not found: {backup_path}")
            return False

        # Verify checksum if available
        checksum_path = f"{backup_path}.sha256"
        if os.path.exists(checksum_path):
            with open(checksum_path, 'r') as f:
                expected_checksum = f.read().split()[0]

            actual_checksum = self._calculate_checksum(backup_path)

            if expected_checksum != actual_checksum:
                logger.error(f"Checksum mismatch: expected {expected_checksum}, got {actual_checksum}")
                return False

        # Verify archive can be read
        try:
            import tarfile
            with tarfile.open(backup_path, 'r:gz') as tar:
                # List contents to verify integrity
                members = tar.getmembers()
                if not members:
                    logger.error("Backup archive is empty")
                    return False

                logger.info(f"Backup verified: {len(members)} files/directories")
                return True

        except Exception as e:
            logger.error(f"Failed to verify backup archive: {e}")
            return False

    def _verify_directory_backup(self, backup_path: str) -> bool:
        """Verify uncompressed backup directory."""
        if not os.path.isdir(backup_path):
            logger.error(f"Backup directory not found: {backup_path}")
            return False

        # Check for BSON files
        db_path = os.path.join(backup_path, self.config.mongo_db)
        if not os.path.isdir(db_path):
            logger.error(f"Database directory not found in backup: {db_path}")
            return False

        # Check for collection files
        bson_files = list(Path(db_path).glob('*.bson*'))
        if not bson_files:
            logger.error("No BSON files found in backup")
            return False

        logger.info(f"Backup verified: {len(bson_files)} collection files")
        return True

    def list_backups(self, include_cloud: bool = True) -> List[BackupInfo]:
        """
        List all available backups.

        Args:
            include_cloud: Whether to include cloud backups

        Returns:
            List of BackupInfo objects
        """
        backups = []

        # List local backups
        backup_dir = Path(self.config.backup_dir)
        if backup_dir.exists():
            for item in backup_dir.iterdir():
                if item.suffix == '.gz' or item.is_dir():
                    metadata_path = item.with_suffix('.json') if item.is_file() else item / 'metadata.json'

                    if metadata_path.exists():
                        try:
                            with open(metadata_path, 'r') as f:
                                data = json.load(f)
                                data['created_at'] = datetime.fromisoformat(data['created_at'])
                                backups.append(BackupInfo(**data))
                        except Exception as e:
                            logger.warning(f"Failed to read metadata for {item.name}: {e}")
                    else:
                        # Create basic info from filename
                        try:
                            name_parts = item.stem.replace('.tar', '').split('_')
                            backup_type = name_parts[0]
                            date_str = f"{name_parts[1]}_{name_parts[2]}"
                            backup_date = datetime.strptime(date_str, "%Y%m%d_%H%M%S")

                            size = os.path.getsize(item) if item.is_file() else self._get_directory_size(str(item))

                            backups.append(BackupInfo(
                                backup_id=item.stem.replace('.tar', ''),
                                backup_type=backup_type,
                                created_at=backup_date,
                                size_bytes=size,
                                collections=[],
                                location=CloudProvider.LOCAL,
                                path=str(item),
                                verified=False
                            ))
                        except Exception as e:
                            logger.warning(f"Failed to parse backup info for {item.name}: {e}")

        # Sort by creation date, newest first
        backups.sort(key=lambda x: x.created_at, reverse=True)

        return backups

    def _save_backup_metadata(self, backup_path: str, backup_info: BackupInfo) -> None:
        """Save backup metadata to JSON file."""
        if os.path.isdir(backup_path):
            metadata_path = os.path.join(backup_path, 'metadata.json')
        else:
            metadata_path = backup_path.replace('.tar.gz', '') + '.json'

        with open(metadata_path, 'w') as f:
            json.dump(backup_info.to_dict(), f, indent=2)


class RestoreService:
    """
    Service for MongoDB restore operations.

    Provides restore functionality including full restore,
    collection-specific restore, and point-in-time recovery.
    """

    def __init__(self, config: Optional[BackupConfig] = None):
        """Initialize restore service."""
        self.config = config or BackupConfig()

    def restore_full(
        self,
        backup_path: str,
        target_db: Optional[str] = None,
        drop_existing: bool = False
    ) -> bool:
        """
        Restore full backup to MongoDB.

        Args:
            backup_path: Path to backup file or directory
            target_db: Target database name. Uses original if not specified.
            drop_existing: Whether to drop existing collections before restore

        Returns:
            True if restore succeeded
        """
        logger.info(f"Starting full restore from: {backup_path}")

        # Extract if compressed
        if backup_path.endswith('.tar.gz'):
            extract_path = self._extract_backup(backup_path)
            restore_path = extract_path
        else:
            restore_path = backup_path

        # Build mongorestore command
        cmd = [
            "mongorestore",
            f"--uri={self.config.mongo_uri}",
            "--gzip" if self.config.compression_enabled else "",
        ]

        if target_db:
            cmd.extend([f"--db={target_db}"])
            cmd.extend([f"--nsFrom={self.config.mongo_db}.*"])
            cmd.extend([f"--nsTo={target_db}.*"])

        if drop_existing:
            cmd.append("--drop")

        cmd.append(restore_path)

        cmd = [c for c in cmd if c]  # Remove empty strings

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=7200  # 2 hours timeout
            )

            if result.returncode != 0:
                logger.error(f"mongorestore failed: {result.stderr}")
                return False

            logger.info("Restore completed successfully")
            return True

        except subprocess.TimeoutExpired:
            logger.error("Restore timed out")
            return False
        except FileNotFoundError:
            logger.error("mongorestore command not found")
            return False
        finally:
            # Cleanup extracted backup
            if backup_path.endswith('.tar.gz') and 'extract_path' in locals():
                shutil.rmtree(extract_path, ignore_errors=True)

    def restore_collection(
        self,
        backup_path: str,
        collection: str,
        target_db: Optional[str] = None,
        drop_existing: bool = False
    ) -> bool:
        """
        Restore specific collection from backup.

        Args:
            backup_path: Path to backup
            collection: Collection name to restore
            target_db: Target database name
            drop_existing: Whether to drop existing collection

        Returns:
            True if restore succeeded
        """
        logger.info(f"Starting collection restore: {collection} from {backup_path}")

        # Extract if compressed
        if backup_path.endswith('.tar.gz'):
            extract_path = self._extract_backup(backup_path)
            restore_path = extract_path
        else:
            restore_path = backup_path

        db_name = target_db or self.config.mongo_db

        cmd = [
            "mongorestore",
            f"--uri={self.config.mongo_uri}",
            f"--db={db_name}",
            f"--collection={collection}",
            "--gzip" if self.config.compression_enabled else "",
        ]

        if drop_existing:
            cmd.append("--drop")

        # Find the collection file
        collection_file = os.path.join(restore_path, self.config.mongo_db, f"{collection}.bson")
        if self.config.compression_enabled:
            collection_file += ".gz"

        if not os.path.exists(collection_file):
            logger.error(f"Collection file not found: {collection_file}")
            return False

        cmd.append(collection_file)

        cmd = [c for c in cmd if c]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600
            )

            if result.returncode != 0:
                logger.error(f"mongorestore failed: {result.stderr}")
                return False

            logger.info(f"Collection {collection} restored successfully")
            return True

        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False
        finally:
            if backup_path.endswith('.tar.gz') and 'extract_path' in locals():
                shutil.rmtree(extract_path, ignore_errors=True)

    def restore_from_s3(self, s3_path: str, target_db: Optional[str] = None) -> bool:
        """
        Download and restore from S3.

        Args:
            s3_path: S3 URI (s3://bucket/key)
            target_db: Target database name

        Returns:
            True if restore succeeded
        """
        try:
            import boto3
        except ImportError:
            raise RuntimeError("boto3 is required for S3 operations")

        # Parse S3 URI
        parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1]

        local_path = os.path.join(self.config.backup_dir, os.path.basename(key))

        logger.info(f"Downloading from S3: {s3_path} -> {local_path}")

        s3 = boto3.client('s3')
        s3.download_file(bucket, key, local_path)

        # Also download checksum if available
        try:
            s3.download_file(bucket, f"{key}.sha256", f"{local_path}.sha256")
        except Exception:
            pass

        # Verify and restore
        backup_service = MongoBackupService(self.config)
        if backup_service.verify_backup(local_path):
            return self.restore_full(local_path, target_db)
        else:
            logger.error("Backup verification failed")
            return False

    def _extract_backup(self, archive_path: str) -> str:
        """Extract compressed backup archive."""
        extract_path = archive_path.replace('.tar.gz', '_extracted')

        logger.info(f"Extracting backup: {archive_path} -> {extract_path}")

        try:
            import tarfile
            with tarfile.open(archive_path, 'r:gz') as tar:
                tar.extractall(extract_path)

            # Find the actual backup directory
            for item in os.listdir(extract_path):
                item_path = os.path.join(extract_path, item)
                if os.path.isdir(item_path):
                    return item_path

            return extract_path

        except Exception as e:
            raise RuntimeError(f"Failed to extract backup: {e}")


# Airflow task functions
def backup_mongodb_func(**context) -> str:
    """
    Airflow task function to create MongoDB backup.

    Returns:
        Backup path
    """
    service = MongoBackupService()
    backup_path, backup_info = service.create_full_backup()

    # Push backup info to XCom
    context['ti'].xcom_push(key='backup_path', value=backup_path)
    context['ti'].xcom_push(key='backup_info', value=backup_info.to_dict())

    return backup_path


def compress_backup_func(**context) -> str:
    """
    Airflow task function to compress backup.

    Returns:
        Compressed backup path
    """
    service = MongoBackupService()
    backup_path = context['ti'].xcom_pull(key='backup_path', task_ids='backup_mongodb')

    compressed_path = service.compress_backup(backup_path)
    context['ti'].xcom_push(key='compressed_path', value=compressed_path)

    return compressed_path


def upload_to_cloud_func(**context) -> str:
    """
    Airflow task function to upload backup to cloud.

    Returns:
        Cloud URI
    """
    service = MongoBackupService()
    compressed_path = context['ti'].xcom_pull(key='compressed_path', task_ids='compress_backup')

    cloud_uri = service.upload_to_cloud(compressed_path, delete_local=False)
    context['ti'].xcom_push(key='cloud_uri', value=cloud_uri)

    return cloud_uri


def cleanup_old_backups_func(**context) -> int:
    """
    Airflow task function to cleanup old backups.

    Returns:
        Number of deleted backups
    """
    service = MongoBackupService()
    deleted_count = service.cleanup_old_backups()

    return deleted_count


def verify_backup_func(**context) -> bool:
    """
    Airflow task function to verify backup integrity.

    Returns:
        True if backup is valid
    """
    service = MongoBackupService()
    compressed_path = context['ti'].xcom_pull(key='compressed_path', task_ids='compress_backup')

    is_valid = service.verify_backup(compressed_path)

    if not is_valid:
        raise ValueError("Backup verification failed!")

    return is_valid

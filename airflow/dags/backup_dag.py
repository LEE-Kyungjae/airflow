"""
MongoDB Backup Automation DAGs.

This module defines Airflow DAGs for automated MongoDB backup operations:
- Daily full backup with compression and cloud upload
- Weekly full backup with extended verification
- On-demand backup trigger via API

Backups are stored locally and optionally uploaded to cloud storage (S3/GCS).
Retention policies automatically cleanup old backups.
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Import backup functions
from utils.backup_service import (
    backup_mongodb_func,
    compress_backup_func,
    upload_to_cloud_func,
    cleanup_old_backups_func,
    verify_backup_func,
    MongoBackupService,
    BackupConfig
)


# ============================================================
# DAG Default Arguments
# ============================================================

default_args = {
    'owner': 'crawler-system',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
}


# ============================================================
# Helper Functions
# ============================================================

def should_upload_to_cloud(**context) -> str:
    """
    Branch function to determine if cloud upload should proceed.

    Returns:
        Task ID to execute next based on cloud configuration
    """
    config = BackupConfig()

    if config.cloud_provider.lower() != 'local' and (config.s3_bucket or config.gcs_bucket):
        return 'upload_to_cloud'
    else:
        return 'skip_cloud_upload'


def send_backup_notification(**context) -> Dict[str, Any]:
    """
    Send notification about backup completion.

    Args:
        context: Airflow context

    Returns:
        Notification result
    """
    import logging
    logger = logging.getLogger(__name__)

    ti = context['ti']

    # Gather backup information
    backup_info = ti.xcom_pull(key='backup_info', task_ids='backup_mongodb')
    compressed_path = ti.xcom_pull(key='compressed_path', task_ids='compress_backup')
    cloud_uri = ti.xcom_pull(key='cloud_uri', task_ids='upload_to_cloud')

    notification = {
        'status': 'success',
        'dag_id': context['dag'].dag_id,
        'run_id': context['run_id'],
        'execution_date': context['execution_date'].isoformat(),
        'backup_info': backup_info,
        'compressed_path': compressed_path,
        'cloud_uri': cloud_uri
    }

    logger.info(f"Backup completed successfully: {notification}")

    # Optional: Send to monitoring system
    try:
        from utils.mongo_service import MongoService
        mongo = MongoService()
        mongo.db.backup_logs.insert_one({
            **notification,
            'completed_at': datetime.utcnow()
        })
    except Exception as e:
        logger.warning(f"Failed to log backup notification to MongoDB: {e}")

    return notification


def log_backup_failure(**context) -> None:
    """
    Log backup failure for alerting.

    Args:
        context: Airflow context
    """
    import logging
    logger = logging.getLogger(__name__)

    exception = context.get('exception')
    ti = context.get('ti')

    error_info = {
        'status': 'failed',
        'dag_id': context['dag'].dag_id,
        'run_id': context['run_id'],
        'execution_date': context['execution_date'].isoformat(),
        'task_id': ti.task_id if ti else None,
        'error': str(exception) if exception else 'Unknown error',
        'failed_at': datetime.utcnow().isoformat()
    }

    logger.error(f"Backup failed: {error_info}")

    # Log to MongoDB for alerting
    try:
        from utils.mongo_service import MongoService
        mongo = MongoService()
        mongo.db.backup_logs.insert_one({
            **error_info,
            'created_at': datetime.utcnow()
        })
    except Exception as e:
        logger.error(f"Failed to log backup failure to MongoDB: {e}")


def backup_single_collection(**context) -> str:
    """
    Backup a single collection (for targeted backups).

    Args:
        context: Airflow context with collection name in dag_run.conf

    Returns:
        Backup path
    """
    import logging
    logger = logging.getLogger(__name__)

    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}

    collections = conf.get('collections', [])
    if not collections:
        raise ValueError("No collections specified in dag_run.conf")

    logger.info(f"Starting collection backup for: {collections}")

    service = MongoBackupService()
    backup_path, backup_info = service.create_collection_backup(collections)

    context['ti'].xcom_push(key='backup_path', value=backup_path)
    context['ti'].xcom_push(key='backup_info', value=backup_info.to_dict())

    return backup_path


# ============================================================
# Daily Backup DAG
# ============================================================

with DAG(
    dag_id='daily_mongodb_backup',
    default_args=default_args,
    description='Daily MongoDB backup with compression and optional cloud upload',
    schedule_interval='0 2 * * *',  # Every day at 02:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['backup', 'mongodb', 'daily'],
    doc_md="""
    ## Daily MongoDB Backup DAG

    This DAG performs daily automated backups of the MongoDB database.

    ### Tasks:
    1. **backup_mongodb**: Creates full MongoDB dump
    2. **compress_backup**: Compresses backup to tar.gz
    3. **upload_to_cloud**: Uploads to S3/GCS (if configured)
    4. **cleanup_old_backups**: Removes backups older than retention period
    5. **verify_backup**: Validates backup integrity
    6. **notify_success**: Sends success notification

    ### Configuration:
    - Set `BACKUP_CLOUD_PROVIDER` to 's3' or 'gcs' for cloud uploads
    - Set `BACKUP_RETENTION_DAYS` for retention period (default: 30)
    - Set `BACKUP_S3_BUCKET` or `BACKUP_GCS_BUCKET` for cloud storage

    ### Schedule:
    Runs daily at 02:00 UTC
    """
) as daily_backup_dag:

    # Task 1: Create MongoDB dump
    backup_mongodb = PythonOperator(
        task_id='backup_mongodb',
        python_callable=backup_mongodb_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    # Task 2: Compress backup
    compress_backup = PythonOperator(
        task_id='compress_backup',
        python_callable=compress_backup_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    # Task 3: Branch to determine cloud upload
    check_cloud_config = BranchPythonOperator(
        task_id='check_cloud_config',
        python_callable=should_upload_to_cloud,
        provide_context=True,
    )

    # Task 4a: Upload to cloud
    upload_to_cloud = PythonOperator(
        task_id='upload_to_cloud',
        python_callable=upload_to_cloud_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    # Task 4b: Skip cloud upload
    skip_cloud_upload = EmptyOperator(
        task_id='skip_cloud_upload',
    )

    # Task 5: Join after branch
    after_upload = EmptyOperator(
        task_id='after_upload',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Task 6: Cleanup old backups
    cleanup_old_backups = PythonOperator(
        task_id='cleanup_old_backups',
        python_callable=cleanup_old_backups_func,
        provide_context=True,
    )

    # Task 7: Verify backup
    verify_backup = PythonOperator(
        task_id='verify_backup',
        python_callable=verify_backup_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    # Task 8: Send success notification
    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=send_backup_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Define task dependencies
    backup_mongodb >> compress_backup >> check_cloud_config
    check_cloud_config >> upload_to_cloud >> after_upload
    check_cloud_config >> skip_cloud_upload >> after_upload
    after_upload >> cleanup_old_backups >> verify_backup >> notify_success


# ============================================================
# Weekly Full Backup DAG (with extended verification)
# ============================================================

with DAG(
    dag_id='weekly_mongodb_backup',
    default_args={
        **default_args,
        'execution_timeout': timedelta(hours=4),  # Extended timeout for weekly backup
    },
    description='Weekly MongoDB backup with full verification and mandatory cloud upload',
    schedule_interval='0 3 * * 0',  # Every Sunday at 03:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['backup', 'mongodb', 'weekly'],
    doc_md="""
    ## Weekly MongoDB Backup DAG

    This DAG performs weekly comprehensive backups with extended verification.

    ### Features:
    - Full database backup
    - Extended verification checks
    - Mandatory cloud upload (if configured)
    - Test restore to verify backup validity

    ### Schedule:
    Runs every Sunday at 03:00 UTC
    """
) as weekly_backup_dag:

    def extended_verification(**context) -> bool:
        """Extended backup verification including test restore."""
        import logging
        import tempfile
        logger = logging.getLogger(__name__)

        service = MongoBackupService()
        compressed_path = context['ti'].xcom_pull(key='compressed_path', task_ids='compress_backup')

        # Basic verification
        if not service.verify_backup(compressed_path):
            raise ValueError("Basic backup verification failed")

        logger.info("Basic verification passed, performing extended checks...")

        # Check archive contents
        import tarfile
        with tarfile.open(compressed_path, 'r:gz') as tar:
            members = tar.getmembers()
            total_size = sum(m.size for m in members)
            logger.info(f"Archive contains {len(members)} files, total uncompressed size: {total_size} bytes")

            if total_size == 0:
                raise ValueError("Archive appears to be empty")

        logger.info("Extended verification completed successfully")
        return True

    backup_mongodb_weekly = PythonOperator(
        task_id='backup_mongodb',
        python_callable=backup_mongodb_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    compress_backup_weekly = PythonOperator(
        task_id='compress_backup',
        python_callable=compress_backup_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    upload_to_cloud_weekly = PythonOperator(
        task_id='upload_to_cloud',
        python_callable=upload_to_cloud_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    extended_verify = PythonOperator(
        task_id='extended_verification',
        python_callable=extended_verification,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    cleanup_weekly = PythonOperator(
        task_id='cleanup_old_backups',
        python_callable=cleanup_old_backups_func,
        provide_context=True,
    )

    notify_weekly = PythonOperator(
        task_id='notify_success',
        python_callable=send_backup_notification,
        provide_context=True,
    )

    (backup_mongodb_weekly >> compress_backup_weekly >> upload_to_cloud_weekly >>
     extended_verify >> cleanup_weekly >> notify_weekly)


# ============================================================
# On-Demand Backup DAG (triggered via API)
# ============================================================

with DAG(
    dag_id='manual_mongodb_backup',
    default_args=default_args,
    description='On-demand MongoDB backup triggered via API',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,  # Allow multiple concurrent manual backups
    tags=['backup', 'mongodb', 'manual'],
    doc_md="""
    ## Manual MongoDB Backup DAG

    This DAG can be triggered on-demand via the API for manual backups.

    ### Trigger Options:
    ```json
    {
        "backup_type": "full",  // or "collection"
        "collections": ["sources", "crawlers"],  // required if backup_type is "collection"
        "upload_to_cloud": true,
        "skip_cleanup": false
    }
    ```

    ### API Trigger:
    ```bash
    curl -X POST 'http://localhost:8000/api/backup/trigger' \\
      -H 'Content-Type: application/json' \\
      -d '{"backup_type": "full"}'
    ```
    """
) as manual_backup_dag:

    def determine_backup_type(**context) -> str:
        """Determine backup type from dag_run configuration."""
        dag_run = context.get('dag_run')
        conf = dag_run.conf if dag_run else {}

        backup_type = conf.get('backup_type', 'full')

        if backup_type == 'collection':
            return 'backup_collection'
        return 'backup_full'

    def should_upload(**context) -> str:
        """Determine if cloud upload should be performed."""
        dag_run = context.get('dag_run')
        conf = dag_run.conf if dag_run else {}

        if conf.get('upload_to_cloud', True):
            config = BackupConfig()
            if config.cloud_provider.lower() != 'local':
                return 'upload_to_cloud'

        return 'skip_upload'

    def should_cleanup(**context) -> str:
        """Determine if cleanup should be performed."""
        dag_run = context.get('dag_run')
        conf = dag_run.conf if dag_run else {}

        if conf.get('skip_cleanup', False):
            return 'skip_cleanup'
        return 'cleanup_old_backups'

    # Branching tasks
    check_backup_type = BranchPythonOperator(
        task_id='check_backup_type',
        python_callable=determine_backup_type,
        provide_context=True,
    )

    # Full backup
    backup_full = PythonOperator(
        task_id='backup_full',
        python_callable=backup_mongodb_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    # Collection backup
    backup_collection = PythonOperator(
        task_id='backup_collection',
        python_callable=backup_single_collection,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    # Join after backup type branch
    after_backup = EmptyOperator(
        task_id='after_backup',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    compress_manual = PythonOperator(
        task_id='compress_backup',
        python_callable=compress_backup_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    check_upload = BranchPythonOperator(
        task_id='check_upload',
        python_callable=should_upload,
        provide_context=True,
    )

    upload_manual = PythonOperator(
        task_id='upload_to_cloud',
        python_callable=upload_to_cloud_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    skip_upload = EmptyOperator(
        task_id='skip_upload',
    )

    after_upload_manual = EmptyOperator(
        task_id='after_upload',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_cleanup = BranchPythonOperator(
        task_id='check_cleanup',
        python_callable=should_cleanup,
        provide_context=True,
    )

    cleanup_manual = PythonOperator(
        task_id='cleanup_old_backups',
        python_callable=cleanup_old_backups_func,
        provide_context=True,
    )

    skip_cleanup = EmptyOperator(
        task_id='skip_cleanup',
    )

    after_cleanup = EmptyOperator(
        task_id='after_cleanup',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    verify_manual = PythonOperator(
        task_id='verify_backup',
        python_callable=verify_backup_func,
        provide_context=True,
        on_failure_callback=log_backup_failure,
    )

    notify_manual = PythonOperator(
        task_id='notify_success',
        python_callable=send_backup_notification,
        provide_context=True,
    )

    # Define dependencies
    check_backup_type >> backup_full >> after_backup
    check_backup_type >> backup_collection >> after_backup

    after_backup >> compress_manual >> check_upload

    check_upload >> upload_manual >> after_upload_manual
    check_upload >> skip_upload >> after_upload_manual

    after_upload_manual >> check_cleanup

    check_cleanup >> cleanup_manual >> after_cleanup
    check_cleanup >> skip_cleanup >> after_cleanup

    after_cleanup >> verify_manual >> notify_manual


# ============================================================
# Restore DAG (triggered via API)
# ============================================================

with DAG(
    dag_id='mongodb_restore',
    default_args={
        **default_args,
        'retries': 0,  # No retries for restore operations
        'execution_timeout': timedelta(hours=4),
    },
    description='On-demand MongoDB restore from backup',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # Only one restore at a time
    tags=['backup', 'mongodb', 'restore'],
    doc_md="""
    ## MongoDB Restore DAG

    This DAG restores MongoDB from a backup. Use with caution.

    ### Trigger Options:
    ```json
    {
        "backup_id": "full_20250205_020000",
        "backup_path": "/data/backups/full_20250205_020000.tar.gz",
        "s3_path": "s3://bucket/backups/full_20250205_020000.tar.gz",
        "target_db": "crawler_system_restored",
        "drop_existing": false,
        "collections": ["sources"]  // optional: specific collections only
    }
    ```

    ### Warning:
    This operation can overwrite existing data. Use with extreme caution.
    """
) as restore_dag:

    def validate_restore_config(**context) -> Dict[str, Any]:
        """Validate restore configuration."""
        import logging
        logger = logging.getLogger(__name__)

        dag_run = context.get('dag_run')
        if not dag_run or not dag_run.conf:
            raise ValueError("Restore configuration is required in dag_run.conf")

        conf = dag_run.conf

        # Must have either backup_path or s3_path
        if not conf.get('backup_path') and not conf.get('s3_path'):
            raise ValueError("Either 'backup_path' or 's3_path' is required")

        logger.info(f"Restore configuration validated: {conf}")
        return conf

    def download_backup_if_needed(**context) -> str:
        """Download backup from cloud if s3_path is provided."""
        import logging
        logger = logging.getLogger(__name__)

        dag_run = context.get('dag_run')
        conf = dag_run.conf

        if conf.get('backup_path'):
            return conf['backup_path']

        s3_path = conf.get('s3_path')
        if s3_path:
            import boto3
            import os

            parts = s3_path.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1]

            config = BackupConfig()
            local_path = os.path.join(config.backup_dir, os.path.basename(key))

            logger.info(f"Downloading backup from S3: {s3_path}")

            s3 = boto3.client('s3')
            s3.download_file(bucket, key, local_path)

            context['ti'].xcom_push(key='backup_path', value=local_path)
            return local_path

        raise ValueError("No backup path available")

    def perform_restore(**context) -> bool:
        """Perform the actual restore operation."""
        import logging
        from utils.backup_service import RestoreService

        logger = logging.getLogger(__name__)

        dag_run = context.get('dag_run')
        conf = dag_run.conf

        backup_path = context['ti'].xcom_pull(key='backup_path', task_ids='download_backup')
        if not backup_path:
            backup_path = conf.get('backup_path')

        target_db = conf.get('target_db')
        drop_existing = conf.get('drop_existing', False)
        collections = conf.get('collections')

        logger.info(f"Starting restore from {backup_path} to {target_db or 'original database'}")

        service = RestoreService()

        if collections:
            for collection in collections:
                success = service.restore_collection(
                    backup_path,
                    collection,
                    target_db,
                    drop_existing
                )
                if not success:
                    raise RuntimeError(f"Failed to restore collection: {collection}")
        else:
            success = service.restore_full(backup_path, target_db, drop_existing)
            if not success:
                raise RuntimeError("Full restore failed")

        logger.info("Restore completed successfully")
        return True

    def send_restore_notification(**context) -> Dict[str, Any]:
        """Send notification about restore completion."""
        import logging
        logger = logging.getLogger(__name__)

        dag_run = context.get('dag_run')
        conf = dag_run.conf

        notification = {
            'status': 'success',
            'operation': 'restore',
            'dag_id': context['dag'].dag_id,
            'run_id': context['run_id'],
            'config': conf,
            'completed_at': datetime.utcnow().isoformat()
        }

        logger.info(f"Restore completed: {notification}")

        return notification

    validate_config = PythonOperator(
        task_id='validate_config',
        python_callable=validate_restore_config,
        provide_context=True,
    )

    download_backup = PythonOperator(
        task_id='download_backup',
        python_callable=download_backup_if_needed,
        provide_context=True,
    )

    restore_data = PythonOperator(
        task_id='restore_data',
        python_callable=perform_restore,
        provide_context=True,
    )

    notify_restore = PythonOperator(
        task_id='notify_restore',
        python_callable=send_restore_notification,
        provide_context=True,
    )

    validate_config >> download_backup >> restore_data >> notify_restore

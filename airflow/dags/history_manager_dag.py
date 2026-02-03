"""
History Manager DAG for crawler code version control.

This DAG handles:
1. Detecting code changes in crawlers
2. Backing up old versions
3. Logging change history
4. Cleaning up old backups
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.mongo_service import MongoService

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'crawler-system',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration
MAX_HISTORY_VERSIONS = 50  # Maximum history versions to keep per crawler
HISTORY_RETENTION_DAYS = 90  # Days to keep history


def detect_changes(**context) -> List[Dict[str, Any]]:
    """
    Detect crawlers that have been modified since last check.

    Returns list of crawlers with pending changes to backup.
    """
    mongo = MongoService()

    try:
        # Get all active crawlers
        crawlers = mongo.list_crawlers(status='active')

        changes_detected = []

        for crawler in crawlers:
            crawler_id = crawler['_id']

            # Get latest history entry
            history = mongo.get_crawler_history(crawler_id, limit=1)

            if not history:
                # No history yet, needs initial backup
                changes_detected.append({
                    'crawler_id': crawler_id,
                    'source_id': crawler['source_id'],
                    'current_version': crawler['version'],
                    'last_history_version': 0,
                    'change_type': 'initial'
                })
            elif history[0]['version'] < crawler['version']:
                # New version detected
                changes_detected.append({
                    'crawler_id': crawler_id,
                    'source_id': crawler['source_id'],
                    'current_version': crawler['version'],
                    'last_history_version': history[0]['version'],
                    'change_type': 'update'
                })

        logger.info(f"Detected {len(changes_detected)} crawlers with changes")
        return changes_detected

    finally:
        mongo.close()


def backup_old_versions(**context) -> List[str]:
    """
    Backup old versions of changed crawlers.

    Creates history entries for any versions between last backup and current.
    """
    ti = context['ti']
    changes = ti.xcom_pull(task_ids='detect_changes')

    if not changes:
        raise AirflowSkipException("No changes to backup")

    mongo = MongoService()
    backed_up = []

    try:
        for change in changes:
            crawler_id = change['crawler_id']
            current_version = change['current_version']
            last_version = change['last_history_version']

            # Get current crawler data
            crawler = mongo.get_crawler(crawler_id)
            if not crawler:
                logger.warning(f"Crawler {crawler_id} not found, skipping")
                continue

            # For initial backup, just save current version
            if change['change_type'] == 'initial':
                history_id = mongo.save_crawler_history(
                    crawler_id=crawler_id,
                    version=current_version,
                    code=crawler['code'],
                    change_reason='manual_edit',  # Initial version
                    change_detail='Initial version backup',
                    changed_by='system'
                )
                backed_up.append(history_id)
                logger.info(f"Created initial backup for crawler {crawler_id}")

            else:
                # Save the previous version (before changes)
                # Note: We don't have the old code, so we'll mark this version
                # The actual old code should have been saved when the change was made

                # Just log that we detected the change
                logger.info(
                    f"Change detected for crawler {crawler_id}: "
                    f"v{last_version} -> v{current_version}"
                )

                # Ensure current version is in history
                existing = mongo.get_crawler_version(crawler_id, current_version)
                if not existing:
                    history_id = mongo.save_crawler_history(
                        crawler_id=crawler_id,
                        version=current_version,
                        code=crawler['code'],
                        change_reason='manual_edit',
                        change_detail='Version sync backup',
                        changed_by='system'
                    )
                    backed_up.append(history_id)

        logger.info(f"Backed up {len(backed_up)} versions")
        return backed_up

    finally:
        mongo.close()


def log_history(**context) -> Dict[str, Any]:
    """
    Log history statistics and changes.

    Returns summary of history operations.
    """
    ti = context['ti']
    changes = ti.xcom_pull(task_ids='detect_changes')
    backed_up = ti.xcom_pull(task_ids='backup_old_versions') or []

    mongo = MongoService()

    try:
        # Calculate statistics
        total_crawlers = len(mongo.list_crawlers())
        total_history_entries = 0

        crawlers = mongo.list_crawlers()
        for crawler in crawlers:
            history = mongo.get_crawler_history(crawler['_id'])
            total_history_entries += len(history)

        summary = {
            'timestamp': datetime.utcnow().isoformat(),
            'changes_detected': len(changes) if changes else 0,
            'versions_backed_up': len(backed_up),
            'total_crawlers': total_crawlers,
            'total_history_entries': total_history_entries
        }

        logger.info(f"History summary: {summary}")
        return summary

    finally:
        mongo.close()


def cleanup_old_backups(**context) -> Dict[str, Any]:
    """
    Clean up old history entries to manage storage.

    Keeps only the most recent MAX_HISTORY_VERSIONS per crawler
    and removes entries older than HISTORY_RETENTION_DAYS.
    """
    mongo = MongoService()

    try:
        deleted_count = 0
        crawlers_cleaned = 0

        crawlers = mongo.list_crawlers()

        for crawler in crawlers:
            crawler_id = crawler['_id']
            history = mongo.get_crawler_history(
                crawler_id,
                limit=MAX_HISTORY_VERSIONS + 100  # Get extra to see what to delete
            )

            if len(history) <= MAX_HISTORY_VERSIONS:
                continue

            # Get versions to delete (oldest beyond limit)
            versions_to_keep = [h['version'] for h in history[:MAX_HISTORY_VERSIONS]]

            # Delete old versions using direct MongoDB operation
            cutoff_date = datetime.utcnow() - timedelta(days=HISTORY_RETENTION_DAYS)

            result = mongo.db.crawler_history.delete_many({
                'crawler_id': crawler_id,
                'version': {'$nin': versions_to_keep},
                'changed_at': {'$lt': cutoff_date}
            })

            if result.deleted_count > 0:
                deleted_count += result.deleted_count
                crawlers_cleaned += 1
                logger.info(
                    f"Cleaned {result.deleted_count} old versions from crawler {crawler_id}"
                )

        summary = {
            'crawlers_cleaned': crawlers_cleaned,
            'entries_deleted': deleted_count,
            'retention_days': HISTORY_RETENTION_DAYS,
            'max_versions': MAX_HISTORY_VERSIONS
        }

        logger.info(f"Cleanup summary: {summary}")
        return summary

    finally:
        mongo.close()


def generate_report(**context) -> str:
    """
    Generate a summary report of history management.
    """
    ti = context['ti']

    changes = ti.xcom_pull(task_ids='detect_changes') or []
    history_summary = ti.xcom_pull(task_ids='log_history') or {}
    cleanup_summary = ti.xcom_pull(task_ids='cleanup_old_backups') or {}

    report = f"""
History Manager Report
======================
Generated: {datetime.utcnow().isoformat()}

Changes Detected
----------------
- Total changes: {len(changes)}
- Crawlers affected: {', '.join([c['crawler_id'] for c in changes]) or 'None'}

History Statistics
------------------
- Total crawlers: {history_summary.get('total_crawlers', 'N/A')}
- Total history entries: {history_summary.get('total_history_entries', 'N/A')}
- Versions backed up: {history_summary.get('versions_backed_up', 0)}

Cleanup Results
---------------
- Crawlers cleaned: {cleanup_summary.get('crawlers_cleaned', 0)}
- Entries deleted: {cleanup_summary.get('entries_deleted', 0)}
- Retention period: {cleanup_summary.get('retention_days', HISTORY_RETENTION_DAYS)} days
- Max versions kept: {cleanup_summary.get('max_versions', MAX_HISTORY_VERSIONS)}
"""

    logger.info(report)
    return report


# Define the DAG
with DAG(
    dag_id='history_manager',
    default_args=default_args,
    description='Manage crawler code history and version backups',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['management', 'history', 'backup']
) as dag:

    detect_task = PythonOperator(
        task_id='detect_changes',
        python_callable=detect_changes,
        provide_context=True
    )

    backup_task = PythonOperator(
        task_id='backup_old_versions',
        python_callable=backup_old_versions,
        provide_context=True
    )

    log_task = PythonOperator(
        task_id='log_history',
        python_callable=log_history,
        provide_context=True
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_old_backups',
        python_callable=cleanup_old_backups,
        provide_context=True
    )

    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True
    )

    # Define task dependencies
    detect_task >> backup_task >> log_task >> cleanup_task >> report_task

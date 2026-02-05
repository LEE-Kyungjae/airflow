"""
Utility modules for Airflow DAGs.

This package contains shared services and utilities used by the crawler DAGs.
"""

from .gpt_service import GPTService
from .mongo_service import MongoService
from .error_handler import ErrorHandler, ErrorCode
from .code_validator import CodeValidator
from .playwright_executor import (
    PlaywrightExecutor,
    ExecutorConfig,
    SourceConfig,
    ExecutionResult,
    PageType,
    CrawlerType,
    run_playwright_crawl,
    run_playwright_batch
)
from .backup_service import (
    MongoBackupService,
    RestoreService,
    BackupConfig,
    BackupInfo,
    BackupJobStatus,
    BackupType,
    BackupStatus,
    CloudProvider,
    backup_mongodb_func,
    compress_backup_func,
    upload_to_cloud_func,
    cleanup_old_backups_func,
    verify_backup_func,
)

__all__ = [
    'GPTService',
    'MongoService',
    'ErrorHandler',
    'ErrorCode',
    'CodeValidator',
    # Playwright executor
    'PlaywrightExecutor',
    'ExecutorConfig',
    'SourceConfig',
    'ExecutionResult',
    'PageType',
    'CrawlerType',
    'run_playwright_crawl',
    'run_playwright_batch',
    # Backup service
    'MongoBackupService',
    'RestoreService',
    'BackupConfig',
    'BackupInfo',
    'BackupJobStatus',
    'BackupType',
    'BackupStatus',
    'CloudProvider',
    'backup_mongodb_func',
    'compress_backup_func',
    'upload_to_cloud_func',
    'cleanup_old_backups_func',
    'verify_backup_func',
]

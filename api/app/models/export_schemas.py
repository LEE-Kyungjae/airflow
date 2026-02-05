"""
Pydantic schemas for data export functionality.

These models define the structure of export requests and responses,
providing validation and OpenAPI documentation for the export API.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


# ============================================================
# Export Request Models
# ============================================================

class ExportRequest(BaseModel):
    """
    Request model for asynchronous data export.

    Used for large dataset exports that run as background jobs.
    Supports filtering by date range, source, and field selection.
    """
    collection: str = Field(
        ...,
        description="Target collection name (e.g., 'crawl_results', 'data_reviews', 'sources')"
    )
    format: str = Field(
        "csv",
        pattern="^(csv|excel|json)$",
        description="Export format: csv, excel, or json"
    )
    query: Optional[Dict[str, Any]] = Field(
        None,
        description="MongoDB query filter for selecting documents"
    )
    fields: Optional[List[str]] = Field(
        None,
        description="List of fields to include in export. If None, all fields are exported."
    )
    date_from: Optional[datetime] = Field(
        None,
        description="Start date filter (inclusive)"
    )
    date_to: Optional[datetime] = Field(
        None,
        description="End date filter (inclusive)"
    )
    limit: int = Field(
        100000,
        ge=1,
        le=1000000,
        description="Maximum number of records to export (default 100K, max 1M)"
    )
    encoding: str = Field(
        "utf-8-sig",
        pattern="^(utf-8|utf-8-sig|euc-kr|cp949)$",
        description="Character encoding for CSV export"
    )
    date_field: str = Field(
        "created_at",
        description="Field name to use for date filtering"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "collection": "crawl_results",
                    "format": "csv",
                    "fields": ["source_id", "status", "record_count", "executed_at"],
                    "date_from": "2025-01-01T00:00:00Z",
                    "date_to": "2025-02-01T00:00:00Z",
                    "limit": 10000,
                    "encoding": "utf-8-sig"
                },
                {
                    "collection": "data_reviews",
                    "format": "excel",
                    "query": {"review_status": "approved"},
                    "limit": 50000
                }
            ]
        }
    }


class CrawlResultExportRequest(BaseModel):
    """
    Specialized request model for crawl result exports.

    Provides convenient parameters specific to crawl result data.
    """
    source_id: Optional[str] = Field(
        None,
        description="Filter by source ID"
    )
    status: Optional[str] = Field(
        None,
        pattern="^(success|failed|partial)$",
        description="Filter by execution status"
    )
    date_from: Optional[datetime] = Field(
        None,
        description="Start date filter (based on executed_at)"
    )
    date_to: Optional[datetime] = Field(
        None,
        description="End date filter (based on executed_at)"
    )
    include_data: bool = Field(
        False,
        description="Include extracted data in export (can be large)"
    )
    include_metadata: bool = Field(
        False,
        description="Include execution metadata (run_id, error details)"
    )
    limit: int = Field(
        10000,
        ge=1,
        le=100000,
        description="Maximum records to export"
    )


class ReviewExportRequest(BaseModel):
    """
    Specialized request model for review data exports.

    Provides parameters specific to data review records.
    """
    source_id: Optional[str] = Field(
        None,
        description="Filter by source ID"
    )
    status: Optional[str] = Field(
        None,
        pattern="^(pending|approved|rejected|on_hold|needs_correction|corrected)$",
        description="Filter by review status"
    )
    date_from: Optional[datetime] = Field(
        None,
        description="Start date filter (based on created_at)"
    )
    date_to: Optional[datetime] = Field(
        None,
        description="End date filter (based on created_at)"
    )
    include_corrections: bool = Field(
        True,
        description="Include correction history in export"
    )
    include_confidence: bool = Field(
        True,
        description="Include confidence scores"
    )
    limit: int = Field(
        10000,
        ge=1,
        le=100000,
        description="Maximum records to export"
    )


# ============================================================
# Export Response Models
# ============================================================

class ExportJobResponse(BaseModel):
    """
    Response model for async export job creation.

    Returned when initiating a background export operation.
    """
    job_id: str = Field(
        ...,
        description="Unique identifier for tracking the export job"
    )
    status: str = Field(
        ...,
        pattern="^(pending|processing|completed|failed)$",
        description="Initial job status (usually 'pending')"
    )
    created_at: datetime = Field(
        ...,
        description="Job creation timestamp"
    )
    estimated_records: int = Field(
        ...,
        ge=0,
        description="Estimated number of records to export"
    )
    format: str = Field(
        ...,
        description="Requested export format"
    )
    collection: str = Field(
        ...,
        description="Target collection being exported"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "job_id": "export_20250205_143000_abc123",
                "status": "pending",
                "created_at": "2025-02-05T14:30:00Z",
                "estimated_records": 15000,
                "format": "csv",
                "collection": "crawl_results"
            }
        }
    }


class ExportJobStatus(BaseModel):
    """
    Response model for export job status queries.

    Provides detailed progress information and download URL when complete.
    """
    job_id: str = Field(
        ...,
        description="Job identifier"
    )
    status: str = Field(
        ...,
        pattern="^(pending|processing|completed|failed|expired)$",
        description="Current job status"
    )
    progress: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Progress percentage (0.0 to 1.0)"
    )
    records_processed: int = Field(
        ...,
        ge=0,
        description="Number of records processed so far"
    )
    total_records: int = Field(
        ...,
        ge=0,
        description="Total records to process"
    )
    file_size: Optional[int] = Field(
        None,
        ge=0,
        description="Final file size in bytes (available when completed)"
    )
    download_url: Optional[str] = Field(
        None,
        description="Download URL (available when completed, expires after 24 hours)"
    )
    error: Optional[str] = Field(
        None,
        description="Error message if job failed"
    )
    created_at: datetime = Field(
        ...,
        description="Job creation timestamp"
    )
    started_at: Optional[datetime] = Field(
        None,
        description="Processing start timestamp"
    )
    completed_at: Optional[datetime] = Field(
        None,
        description="Job completion timestamp"
    )
    expires_at: Optional[datetime] = Field(
        None,
        description="Download link expiration time"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "job_id": "export_20250205_143000_abc123",
                    "status": "processing",
                    "progress": 0.45,
                    "records_processed": 4500,
                    "total_records": 10000,
                    "file_size": None,
                    "download_url": None,
                    "error": None,
                    "created_at": "2025-02-05T14:30:00Z",
                    "started_at": "2025-02-05T14:30:01Z",
                    "completed_at": None,
                    "expires_at": None
                },
                {
                    "job_id": "export_20250205_143000_abc123",
                    "status": "completed",
                    "progress": 1.0,
                    "records_processed": 10000,
                    "total_records": 10000,
                    "file_size": 2456789,
                    "download_url": "/api/export/download/export_20250205_143000_abc123",
                    "error": None,
                    "created_at": "2025-02-05T14:30:00Z",
                    "started_at": "2025-02-05T14:30:01Z",
                    "completed_at": "2025-02-05T14:35:00Z",
                    "expires_at": "2025-02-06T14:35:00Z"
                }
            ]
        }
    }


class ExportListResponse(BaseModel):
    """
    Response model for listing export jobs.

    Provides paginated list of user's export jobs.
    """
    jobs: List[ExportJobStatus] = Field(
        ...,
        description="List of export jobs"
    )
    total: int = Field(
        ...,
        ge=0,
        description="Total number of jobs"
    )
    page: int = Field(
        ...,
        ge=1,
        description="Current page number"
    )
    page_size: int = Field(
        ...,
        ge=1,
        description="Items per page"
    )


# ============================================================
# Export Configuration Models
# ============================================================

class ExportFieldMapping(BaseModel):
    """
    Field mapping configuration for exports.

    Allows renaming and transforming fields in the export output.
    """
    source_field: str = Field(
        ...,
        description="Original field name in the database"
    )
    target_field: str = Field(
        ...,
        description="Field name in the export file"
    )
    format: Optional[str] = Field(
        None,
        description="Format string for value transformation (e.g., date format)"
    )
    default_value: Optional[Any] = Field(
        None,
        description="Default value if field is missing"
    )


class ExportTemplate(BaseModel):
    """
    Reusable export template configuration.

    Allows saving and reusing common export configurations.
    """
    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Template name"
    )
    description: Optional[str] = Field(
        None,
        max_length=500,
        description="Template description"
    )
    collection: str = Field(
        ...,
        description="Target collection"
    )
    format: str = Field(
        "csv",
        description="Default export format"
    )
    fields: Optional[List[str]] = Field(
        None,
        description="Fields to include"
    )
    field_mappings: Optional[List[ExportFieldMapping]] = Field(
        None,
        description="Custom field mappings"
    )
    default_filters: Optional[Dict[str, Any]] = Field(
        None,
        description="Default query filters"
    )
    encoding: str = Field(
        "utf-8-sig",
        description="Default encoding"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "name": "Monthly Crawl Report",
                "description": "Standard monthly crawl results export",
                "collection": "crawl_results",
                "format": "excel",
                "fields": ["source_id", "status", "record_count", "executed_at"],
                "encoding": "utf-8-sig"
            }
        }
    }


# ============================================================
# Export Statistics Models
# ============================================================

class ExportStats(BaseModel):
    """
    Statistics about export operations.

    Provides usage metrics for monitoring and capacity planning.
    """
    total_exports_today: int = Field(
        ...,
        ge=0,
        description="Total exports created today"
    )
    total_exports_this_month: int = Field(
        ...,
        ge=0,
        description="Total exports created this month"
    )
    total_records_exported_today: int = Field(
        ...,
        ge=0,
        description="Total records exported today"
    )
    average_export_time_ms: float = Field(
        ...,
        ge=0,
        description="Average export processing time in milliseconds"
    )
    active_jobs: int = Field(
        ...,
        ge=0,
        description="Currently processing export jobs"
    )
    pending_jobs: int = Field(
        ...,
        ge=0,
        description="Jobs waiting to be processed"
    )
    storage_used_bytes: int = Field(
        ...,
        ge=0,
        description="Total storage used by export files"
    )

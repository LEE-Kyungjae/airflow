"""
Pydantic schemas for API request/response models.

These models define the structure of data for the REST API,
providing validation, serialization, and OpenAPI documentation.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any, Generic, TypeVar
from pydantic import BaseModel, Field, HttpUrl

T = TypeVar('T')


# ============================================================
# Common/Base Models
# ============================================================

class FieldDefinition(BaseModel):
    """
    Field definition for data extraction configuration.

    Defines how to extract a specific field from the source document,
    including selector paths, data types, and extraction patterns.
    """
    name: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Unique field identifier used as key in extracted data"
    )
    selector: Optional[str] = Field(
        None,
        description="CSS selector for HTML, XPath for XML, or column name for tabular data"
    )
    data_type: str = Field(
        "string",
        pattern="^(string|number|date|boolean|currency)$",
        description="Expected data type for validation and parsing"
    )
    is_list: bool = Field(
        False,
        description="Set to true when extracting multiple values (e.g., list items)"
    )
    attribute: Optional[str] = Field(
        None,
        description="HTML attribute to extract (e.g., 'href', 'src'). If None, extracts text content"
    )
    pattern: Optional[str] = Field(
        None,
        description="Regular expression pattern for extracting or validating text"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "title",
                    "selector": "h1.article-title",
                    "data_type": "string"
                },
                {
                    "name": "price",
                    "selector": "span.price-value",
                    "data_type": "currency",
                    "pattern": r"[\d,]+\.?\d*"
                },
                {
                    "name": "links",
                    "selector": "a.nav-link",
                    "attribute": "href",
                    "is_list": True
                }
            ]
        }
    }


# ============================================================
# Source Models
# ============================================================

class SourceCreate(BaseModel):
    """
    Request model for registering a new crawling source.

    Creating a source triggers automatic crawler code generation via
    the source_manager DAG. The generated crawler will be scheduled
    according to the provided cron expression.
    """
    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Human-readable source name. Must be unique across all sources."
    )
    url: str = Field(
        ...,
        description="Target URL to crawl. Supports http/https protocols."
    )
    type: str = Field(
        ...,
        pattern="^(html|pdf|excel|csv|api)$",
        description="Source document type. Determines the extraction engine used."
    )
    fields: List[FieldDefinition] = Field(
        ...,
        min_length=1,
        max_length=50,
        description="List of fields to extract from the source. At least one field required."
    )
    schedule: str = Field(
        ...,
        description="Cron expression for scheduling crawls (e.g., '0 9 * * *' for daily at 9 AM)"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "Government Statistics Portal",
                    "url": "https://example.gov/statistics/monthly",
                    "type": "html",
                    "fields": [
                        {"name": "indicator_name", "selector": "td.indicator", "data_type": "string"},
                        {"name": "value", "selector": "td.value", "data_type": "number"},
                        {"name": "period", "selector": "td.period", "data_type": "date"}
                    ],
                    "schedule": "0 9 1 * *"
                },
                {
                    "name": "Financial Report PDF",
                    "url": "https://company.com/reports/quarterly.pdf",
                    "type": "pdf",
                    "fields": [
                        {"name": "revenue", "data_type": "currency", "pattern": r"Revenue[:\s]*([\d,]+)"},
                        {"name": "net_income", "data_type": "currency", "pattern": r"Net Income[:\s]*([\d,]+)"}
                    ],
                    "schedule": "0 6 * * MON"
                }
            ]
        }
    }


class SourceUpdate(BaseModel):
    """
    Request model for partially updating an existing source.

    All fields are optional. Only provided fields will be updated.
    Changing fields or type may trigger crawler code regeneration.
    """
    name: Optional[str] = Field(
        None,
        min_length=1,
        max_length=100,
        description="Updated source name"
    )
    url: Optional[str] = Field(
        None,
        description="Updated target URL"
    )
    type: Optional[str] = Field(
        None,
        pattern="^(html|pdf|excel|csv|api)$",
        description="Updated source type"
    )
    fields: Optional[List[FieldDefinition]] = Field(
        None,
        description="Updated field definitions"
    )
    schedule: Optional[str] = Field(
        None,
        description="Updated cron schedule"
    )
    status: Optional[str] = Field(
        None,
        pattern="^(active|inactive)$",
        description="Source status. Set to 'inactive' to pause crawling."
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "schedule": "0 */6 * * *",
                    "status": "active"
                },
                {
                    "fields": [
                        {"name": "title", "selector": "h1.new-title", "data_type": "string"}
                    ]
                }
            ]
        }
    }


class SourceResponse(BaseModel):
    """
    Response model for source data.

    Contains full source configuration along with runtime statistics
    including execution history and error tracking.
    """
    id: str = Field(
        ...,
        alias="_id",
        description="Unique source identifier (MongoDB ObjectId)"
    )
    name: str = Field(
        ...,
        description="Human-readable source name"
    )
    url: str = Field(
        ...,
        description="Target URL being crawled"
    )
    type: str = Field(
        ...,
        description="Source document type (html, pdf, excel, csv, api)"
    )
    fields: List[FieldDefinition] = Field(
        ...,
        description="Configured extraction fields"
    )
    schedule: str = Field(
        ...,
        description="Cron schedule expression"
    )
    status: str = Field(
        ...,
        description="Current status: active, inactive, or error"
    )
    last_run: Optional[datetime] = Field(
        None,
        description="Timestamp of the most recent crawl attempt"
    )
    last_success: Optional[datetime] = Field(
        None,
        description="Timestamp of the most recent successful crawl"
    )
    error_count: int = Field(
        0,
        description="Number of consecutive errors since last success"
    )
    created_at: datetime = Field(
        ...,
        description="Source creation timestamp"
    )
    updated_at: Optional[datetime] = Field(
        None,
        description="Last modification timestamp"
    )

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "_id": "507f1f77bcf86cd799439011",
                "name": "Government Statistics Portal",
                "url": "https://example.gov/statistics",
                "type": "html",
                "fields": [
                    {"name": "indicator", "selector": "td.indicator", "data_type": "string"}
                ],
                "schedule": "0 9 * * *",
                "status": "active",
                "last_run": "2025-02-05T09:00:00Z",
                "last_success": "2025-02-05T09:00:00Z",
                "error_count": 0,
                "created_at": "2025-01-15T10:30:00Z",
                "updated_at": "2025-02-01T14:20:00Z"
            }
        }
    }


# ============================================================
# Crawler Models
# ============================================================

class CrawlerResponse(BaseModel):
    """
    Response model for crawler data.

    Crawlers are auto-generated Python code modules that perform
    the actual data extraction. Each source has one active crawler
    with version history tracking.
    """
    id: str = Field(
        ...,
        alias="_id",
        description="Unique crawler identifier"
    )
    source_id: str = Field(
        ...,
        description="Associated source identifier"
    )
    version: int = Field(
        ...,
        ge=1,
        description="Crawler version number, incremented on each code update"
    )
    status: str = Field(
        ...,
        description="Crawler status: active, testing, or deprecated"
    )
    dag_id: Optional[str] = Field(
        None,
        description="Airflow DAG identifier for scheduling"
    )
    created_at: datetime = Field(
        ...,
        description="Crawler creation timestamp"
    )
    created_by: str = Field(
        ...,
        description="Creator: 'gpt' for AI-generated, 'manual' for human-written"
    )
    code: Optional[str] = Field(
        None,
        description="Python source code. Excluded from list views for performance."
    )

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "_id": "507f1f77bcf86cd799439012",
                "source_id": "507f1f77bcf86cd799439011",
                "version": 3,
                "status": "active",
                "dag_id": "crawler_gov_stats",
                "created_at": "2025-02-01T10:00:00Z",
                "created_by": "gpt"
            }
        }
    }


class CrawlerHistoryResponse(BaseModel):
    """
    Response model for crawler version history.

    Tracks all code changes with full version history,
    enabling rollback to any previous version.
    """
    id: str = Field(
        ...,
        alias="_id",
        description="History record identifier"
    )
    crawler_id: str = Field(
        ...,
        description="Parent crawler identifier"
    )
    version: int = Field(
        ...,
        ge=1,
        description="Version number at time of change"
    )
    code: str = Field(
        ...,
        description="Complete Python source code for this version"
    )
    change_reason: str = Field(
        ...,
        description="Reason for change: error_recovery, structure_change, manual_edit, rollback"
    )
    change_detail: Optional[str] = Field(
        None,
        description="Detailed description of what changed and why"
    )
    changed_at: datetime = Field(
        ...,
        description="Timestamp when this version was created"
    )
    changed_by: str = Field(
        ...,
        description="Author: 'gpt', 'user', or 'system'"
    )

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "_id": "507f1f77bcf86cd799439013",
                "crawler_id": "507f1f77bcf86cd799439012",
                "version": 2,
                "code": "# Crawler code...",
                "change_reason": "error_recovery",
                "change_detail": "Fixed CSS selector for updated page structure",
                "changed_at": "2025-02-03T15:30:00Z",
                "changed_by": "gpt"
            }
        }
    }


# ============================================================
# Crawl Result Models
# ============================================================

class CrawlResultResponse(BaseModel):
    """
    Response model for individual crawl execution results.

    Contains extracted data, execution metrics, and error details
    for a single crawl run.
    """
    id: str = Field(
        ...,
        alias="_id",
        description="Crawl result identifier"
    )
    source_id: str = Field(
        ...,
        description="Source that was crawled"
    )
    crawler_id: str = Field(
        ...,
        description="Crawler version that executed"
    )
    run_id: Optional[str] = Field(
        None,
        description="Airflow DAG run identifier for tracing"
    )
    status: str = Field(
        ...,
        description="Result status: success, failed, partial"
    )
    data: Optional[Dict[str, Any]] = Field(
        None,
        description="Extracted data records"
    )
    record_count: int = Field(
        0,
        ge=0,
        description="Number of records extracted"
    )
    error_code: Optional[str] = Field(
        None,
        description="Error classification code if failed"
    )
    error_message: Optional[str] = Field(
        None,
        description="Human-readable error description"
    )
    execution_time_ms: int = Field(
        0,
        ge=0,
        description="Total execution time in milliseconds"
    )
    executed_at: datetime = Field(
        ...,
        description="Crawl execution timestamp"
    )

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "_id": "507f1f77bcf86cd799439014",
                "source_id": "507f1f77bcf86cd799439011",
                "crawler_id": "507f1f77bcf86cd799439012",
                "run_id": "scheduled__2025-02-05T09:00:00+00:00",
                "status": "success",
                "data": [{"indicator": "GDP", "value": 1234.5}],
                "record_count": 1,
                "execution_time_ms": 2500,
                "executed_at": "2025-02-05T09:00:02Z"
            }
        }
    }


# ============================================================
# Error Log Models
# ============================================================

class ErrorLogResponse(BaseModel):
    """
    Response model for error log entries.

    Captures detailed error information with classification for
    automated recovery and manual resolution tracking.
    """
    id: str = Field(
        ...,
        alias="_id",
        description="Error log identifier"
    )
    source_id: str = Field(
        ...,
        description="Source where error occurred"
    )
    crawler_id: Optional[str] = Field(
        None,
        description="Crawler that encountered the error"
    )
    run_id: Optional[str] = Field(
        None,
        description="Airflow run ID for context"
    )
    error_code: str = Field(
        ...,
        description="Standardized error code (e.g., SELECTOR_NOT_FOUND, TIMEOUT)"
    )
    error_type: str = Field(
        ...,
        description="Error category: network, parsing, validation, system"
    )
    message: str = Field(
        ...,
        description="Human-readable error message"
    )
    stack_trace: Optional[str] = Field(
        None,
        description="Full Python stack trace for debugging"
    )
    auto_recoverable: bool = Field(
        ...,
        description="Whether automatic recovery was attempted"
    )
    resolved: bool = Field(
        ...,
        description="Whether the error has been resolved"
    )
    resolved_at: Optional[datetime] = Field(
        None,
        description="Resolution timestamp"
    )
    resolution_method: Optional[str] = Field(
        None,
        description="How resolved: auto (self-healing), manual, retry"
    )
    resolution_detail: Optional[str] = Field(
        None,
        description="Details about the resolution"
    )
    created_at: datetime = Field(
        ...,
        description="Error occurrence timestamp"
    )

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "_id": "507f1f77bcf86cd799439015",
                "source_id": "507f1f77bcf86cd799439011",
                "crawler_id": "507f1f77bcf86cd799439012",
                "run_id": "scheduled__2025-02-05T09:00:00+00:00",
                "error_code": "SELECTOR_NOT_FOUND",
                "error_type": "parsing",
                "message": "CSS selector 'div.old-class' not found in document",
                "auto_recoverable": True,
                "resolved": False,
                "created_at": "2025-02-05T09:00:05Z"
            }
        }
    }


# ============================================================
# Dashboard & Statistics Models
# ============================================================

class SourceStats(BaseModel):
    """Aggregated statistics for sources."""
    total: int = Field(..., description="Total number of registered sources")
    active: int = Field(..., description="Sources currently active and crawling")
    error: int = Field(..., description="Sources in error state")


class CrawlerStats(BaseModel):
    """Aggregated statistics for crawlers."""
    total: int = Field(..., description="Total number of crawlers")
    active: int = Field(..., description="Currently active crawlers")


class ExecutionStats(BaseModel):
    """Statistics for recent crawl executions."""
    total: int = Field(..., description="Total executions in period")
    success: int = Field(..., description="Successful executions")
    failed: int = Field(..., description="Failed executions")
    success_rate: float = Field(
        ...,
        ge=0,
        le=100,
        description="Success rate as percentage (0-100)"
    )


class DashboardResponse(BaseModel):
    """
    Comprehensive dashboard overview data.

    Provides system-wide statistics for monitoring
    the health and performance of the crawling system.
    """
    sources: SourceStats = Field(
        ...,
        description="Source statistics"
    )
    crawlers: CrawlerStats = Field(
        ...,
        description="Crawler statistics"
    )
    recent_executions: ExecutionStats = Field(
        ...,
        description="Execution statistics for the last 24 hours"
    )
    unresolved_errors: int = Field(
        ...,
        ge=0,
        description="Number of unresolved error logs"
    )
    timestamp: datetime = Field(
        ...,
        description="Data retrieval timestamp"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "sources": {"total": 50, "active": 45, "error": 2},
                "crawlers": {"total": 50, "active": 48},
                "recent_executions": {"total": 200, "success": 190, "failed": 10, "success_rate": 95.0},
                "unresolved_errors": 3,
                "timestamp": "2025-02-05T12:00:00Z"
            }
        }
    }


class TriggerResponse(BaseModel):
    """
    Response model for Airflow DAG trigger operations.

    Returned when triggering crawls, regeneration, or other
    Airflow-based operations.
    """
    success: bool = Field(
        ...,
        description="Whether the trigger was successful"
    )
    dag_id: str = Field(
        ...,
        description="Triggered DAG identifier"
    )
    run_id: Optional[str] = Field(
        None,
        description="Airflow run ID if trigger succeeded"
    )
    message: str = Field(
        ...,
        description="Status message with details"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "dag_id": "crawler_gov_stats",
                "run_id": "manual__2025-02-05T12:30:00+00:00",
                "message": "DAG triggered successfully"
            }
        }
    }


class PaginatedResponse(BaseModel, Generic[T]):
    """
    Generic paginated response wrapper.

    Used for list endpoints that support pagination.
    """
    items: List[T] = Field(
        ...,
        description="List of items for current page"
    )
    total: int = Field(
        ...,
        ge=0,
        description="Total number of items across all pages"
    )
    page: int = Field(
        ...,
        ge=1,
        description="Current page number (1-indexed)"
    )
    page_size: int = Field(
        ...,
        ge=1,
        description="Number of items per page"
    )
    total_pages: int = Field(
        ...,
        ge=0,
        description="Total number of pages"
    )


# ============================================================
# Data Review/Verification Models
# ============================================================

class SourceHighlight(BaseModel):
    """
    Visual highlight region on source document.

    Used to show reviewers exactly where extracted data came from,
    supporting both HTML and PDF document types.
    """
    field: str = Field(
        ...,
        description="Field name this highlight corresponds to"
    )
    bbox: Optional[Dict[str, float]] = Field(
        None,
        description="Bounding box coordinates {x, y, width, height} for PDF regions"
    )
    page: Optional[int] = Field(
        None,
        ge=1,
        description="Page number for multi-page PDFs"
    )
    selector: Optional[str] = Field(
        None,
        description="CSS selector path for HTML elements"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"field": "revenue", "bbox": {"x": 100, "y": 200, "width": 150, "height": 20}, "page": 1},
                {"field": "title", "selector": "h1.article-title"}
            ]
        }
    }


class FieldCorrection(BaseModel):
    """
    Correction record for a single extracted field.

    Tracks original vs corrected values with justification
    for audit trail and model improvement.
    """
    field: str = Field(
        ...,
        description="Name of the field being corrected"
    )
    original_value: Optional[Any] = Field(
        None,
        description="Original value extracted by the crawler"
    )
    corrected_value: Optional[Any] = Field(
        None,
        description="Corrected value provided by reviewer"
    )
    reason: Optional[str] = Field(
        None,
        max_length=500,
        description="Explanation of why correction was needed"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "field": "revenue",
                "original_value": "1,234,567",
                "corrected_value": "12,345,670",
                "reason": "OCR misread digit - verified against PDF"
            }
        }
    }


class ReviewStatusUpdate(BaseModel):
    """
    Request model for updating data review status.

    Supports various review outcomes including approval,
    hold for investigation, and corrections.
    """
    status: str = Field(
        ...,
        pattern="^(approved|rejected|on_hold|needs_correction|corrected)$",
        description="New review status: approved, rejected, on_hold, needs_correction, or corrected"
    )
    corrections: Optional[List[FieldCorrection]] = Field(
        None,
        description="List of field corrections (required for 'corrected' status)"
    )
    notes: Optional[str] = Field(
        None,
        max_length=2000,
        description="Reviewer notes or comments"
    )
    rejection_reason: Optional[str] = Field(
        None,
        pattern="^(data_error|source_changed|source_not_updated|other)$",
        description="Reason for rejection: data_error, source_changed, source_not_updated, other"
    )
    rejection_notes: Optional[str] = Field(
        None,
        max_length=2000,
        description="Additional notes for rejection"
    )
    review_duration_ms: Optional[int] = Field(
        None,
        ge=0,
        description="Time spent reviewing in milliseconds (for analytics)"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "status": "approved",
                    "notes": "Data verified against source document"
                },
                {
                    "status": "corrected",
                    "corrections": [
                        {"field": "amount", "original_value": "100", "corrected_value": "1000", "reason": "Missing zero"}
                    ],
                    "review_duration_ms": 45000
                }
            ]
        }
    }


class DataReviewResponse(BaseModel):
    """
    Complete response model for data review records.

    Contains original extracted data, confidence metrics,
    correction history, and review status tracking.
    """
    id: str = Field(
        ...,
        alias="_id",
        description="Review record identifier"
    )
    crawl_result_id: str = Field(
        ...,
        description="Associated crawl result"
    )
    source_id: str = Field(
        ...,
        description="Source that produced this data"
    )
    data_record_index: int = Field(
        0,
        ge=0,
        description="Index within crawl result when multiple records extracted"
    )
    review_status: str = Field(
        ...,
        description="Current status: pending, approved, on_hold, needs_correction, corrected"
    )
    reviewer_id: Optional[str] = Field(
        None,
        description="ID of the reviewer who processed this record"
    )
    reviewed_at: Optional[datetime] = Field(
        None,
        description="Timestamp of review completion"
    )
    original_data: Dict[str, Any] = Field(
        default_factory=dict,
        description="Original extracted data as key-value pairs"
    )
    corrected_data: Optional[Dict[str, Any]] = Field(
        None,
        description="Corrected data after reviewer modifications"
    )
    corrections: List[FieldCorrection] = Field(
        default_factory=list,
        description="List of field-level corrections made"
    )
    source_highlights: List[SourceHighlight] = Field(
        default_factory=list,
        description="Visual markers showing extraction locations"
    )
    confidence_score: Optional[float] = Field(
        None,
        ge=0,
        le=1,
        description="Overall extraction confidence (0-1)"
    )
    ocr_confidence: Optional[float] = Field(
        None,
        ge=0,
        le=1,
        description="OCR-specific confidence for PDF/image sources"
    )
    ai_confidence: Optional[float] = Field(
        None,
        ge=0,
        le=1,
        description="AI model confidence for extracted values"
    )
    needs_number_review: bool = Field(
        False,
        description="Flag for records with uncertain numeric values"
    )
    uncertain_numbers: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Details of uncertain numeric extractions"
    )
    notes: Optional[str] = Field(
        None,
        description="Reviewer comments and notes"
    )
    created_at: datetime = Field(
        ...,
        description="Record creation timestamp"
    )
    updated_at: Optional[datetime] = Field(
        None,
        description="Last update timestamp"
    )

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "_id": "507f1f77bcf86cd799439016",
                "crawl_result_id": "507f1f77bcf86cd799439014",
                "source_id": "507f1f77bcf86cd799439011",
                "data_record_index": 0,
                "review_status": "pending",
                "original_data": {"indicator": "GDP", "value": 1234.5},
                "confidence_score": 0.85,
                "needs_number_review": True,
                "created_at": "2025-02-05T09:00:05Z"
            }
        }
    }


class ReviewQueueItem(BaseModel):
    """
    Review queue item with enriched source information.

    Combines review data with source context for the review UI.
    """
    review: DataReviewResponse = Field(
        ...,
        description="The review record data"
    )
    source_name: str = Field(
        ...,
        description="Human-readable source name"
    )
    source_type: str = Field(
        ...,
        description="Source document type"
    )
    source_url: str = Field(
        ...,
        description="Source URL for reference"
    )
    total_in_queue: int = Field(
        ...,
        ge=0,
        description="Total items in the review queue"
    )
    current_position: int = Field(
        ...,
        ge=1,
        description="Position of this item in the queue"
    )


class ReviewSessionStats(BaseModel):
    """
    Statistics for a reviewer's session.

    Tracks productivity metrics for review session analytics.
    """
    total_reviewed: int = Field(
        ...,
        ge=0,
        description="Total records reviewed in session"
    )
    approved: int = Field(
        ...,
        ge=0,
        description="Records approved without changes"
    )
    on_hold: int = Field(
        ...,
        ge=0,
        description="Records put on hold"
    )
    needs_correction: int = Field(
        ...,
        ge=0,
        description="Records flagged for correction"
    )
    corrected: int = Field(
        ...,
        ge=0,
        description="Records with corrections applied"
    )
    avg_review_time_ms: float = Field(
        ...,
        ge=0,
        description="Average time per review in milliseconds"
    )
    session_duration_ms: int = Field(
        ...,
        ge=0,
        description="Total session duration in milliseconds"
    )


class ReviewDashboardResponse(BaseModel):
    """
    Dashboard data for the review system.

    Provides overview metrics for review queue management
    and reviewer performance tracking.
    """
    pending_count: int = Field(
        ...,
        ge=0,
        description="Number of records awaiting review"
    )
    today_reviewed: int = Field(
        ...,
        ge=0,
        description="Records reviewed today"
    )
    approval_rate: float = Field(
        ...,
        ge=0,
        le=100,
        description="Percentage of records approved without correction"
    )
    avg_confidence: float = Field(
        ...,
        ge=0,
        le=1,
        description="Average confidence score across pending reviews"
    )
    needs_number_review_count: int = Field(
        ...,
        ge=0,
        description="Records flagged for numeric verification"
    )
    by_source: List[Dict[str, Any]] = Field(
        ...,
        description="Pending count breakdown by source"
    )
    recent_reviews: List[DataReviewResponse] = Field(
        ...,
        description="Most recently completed reviews"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "pending_count": 150,
                "today_reviewed": 45,
                "approval_rate": 92.5,
                "avg_confidence": 0.87,
                "needs_number_review_count": 12,
                "by_source": [
                    {"source_id": "507f...", "source_name": "Gov Stats", "pending_count": 50}
                ],
                "recent_reviews": []
            }
        }
    }


# ============================================================
# Bulk Review Operation Models
# ============================================================

class BulkApproveRequest(BaseModel):
    """
    Request model for bulk approval of multiple review records.

    Allows batch approval of up to 100 records at once.
    Each record will be promoted from staging to production.
    """
    review_ids: List[str] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="List of review record IDs to approve (max 100)"
    )
    comment: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional comment for all approved records"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "review_ids": ["507f1f77bcf86cd799439016", "507f1f77bcf86cd799439017"],
                "comment": "Batch approved after quality verification"
            }
        }
    }


class BulkRejectRequest(BaseModel):
    """
    Request model for bulk rejection of multiple review records.

    Allows batch rejection of up to 100 records at once.
    Rejected records will not be promoted to production.
    """
    review_ids: List[str] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="List of review record IDs to reject (max 100)"
    )
    reason: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Reason for rejection (required)"
    )
    comment: Optional[str] = Field(
        None,
        max_length=500,
        description="Additional comment for rejected records"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "review_ids": ["507f1f77bcf86cd799439018", "507f1f77bcf86cd799439019"],
                "reason": "Data quality below threshold",
                "comment": "OCR confidence too low for reliable extraction"
            }
        }
    }


class BulkFilterRequest(BaseModel):
    """
    Request model for filter-based bulk approval.

    Allows approval of records matching specific criteria
    instead of specifying individual IDs.
    """
    source_id: Optional[str] = Field(
        None,
        description="Filter by source ID"
    )
    confidence_min: Optional[float] = Field(
        None,
        ge=0,
        le=1,
        description="Minimum confidence score (0-1)"
    )
    date_from: Optional[datetime] = Field(
        None,
        description="Start date for created_at filter"
    )
    date_to: Optional[datetime] = Field(
        None,
        description="End date for created_at filter"
    )
    limit: int = Field(
        100,
        ge=1,
        le=1000,
        description="Maximum number of records to process (default 100, max 1000)"
    )
    comment: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional comment for all approved records"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "source_id": "507f1f77bcf86cd799439011",
                "confidence_min": 0.9,
                "date_from": "2025-02-01T00:00:00Z",
                "date_to": "2025-02-05T23:59:59Z",
                "limit": 500,
                "comment": "High confidence batch approval"
            }
        }
    }


class BulkOperationResult(BaseModel):
    """
    Response model for bulk operation results.

    Provides detailed statistics about the bulk operation
    including success/failure counts and error details.
    """
    total: int = Field(
        ...,
        ge=0,
        description="Total number of records processed"
    )
    success: int = Field(
        ...,
        ge=0,
        description="Number of successfully processed records"
    )
    failed: int = Field(
        ...,
        ge=0,
        description="Number of failed records"
    )
    failed_ids: List[str] = Field(
        default_factory=list,
        description="List of IDs that failed to process"
    )
    errors: List[str] = Field(
        default_factory=list,
        description="List of error messages for failed records"
    )
    job_id: Optional[str] = Field(
        None,
        description="Job ID for async operations (if applicable)"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "total": 50,
                "success": 48,
                "failed": 2,
                "failed_ids": ["507f1f77bcf86cd799439020", "507f1f77bcf86cd799439021"],
                "errors": ["Record not found", "Already processed"],
                "job_id": None
            }
        }
    }


class BulkJobStatus(BaseModel):
    """
    Response model for async bulk job status.

    Used to track progress of long-running bulk operations.
    """
    job_id: str = Field(
        ...,
        description="Unique job identifier"
    )
    status: str = Field(
        ...,
        pattern="^(pending|processing|completed|failed)$",
        description="Current job status"
    )
    operation: str = Field(
        ...,
        description="Type of bulk operation (approve, reject, filter_approve)"
    )
    total: int = Field(
        ...,
        ge=0,
        description="Total records to process"
    )
    processed: int = Field(
        ...,
        ge=0,
        description="Records processed so far"
    )
    success: int = Field(
        ...,
        ge=0,
        description="Successful operations"
    )
    failed: int = Field(
        ...,
        ge=0,
        description="Failed operations"
    )
    started_at: datetime = Field(
        ...,
        description="Job start timestamp"
    )
    completed_at: Optional[datetime] = Field(
        None,
        description="Job completion timestamp"
    )
    error_message: Optional[str] = Field(
        None,
        description="Error message if job failed"
    )
    result: Optional[BulkOperationResult] = Field(
        None,
        description="Final result when completed"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "job_id": "bulk_approve_20250205_123456",
                "status": "processing",
                "operation": "approve",
                "total": 100,
                "processed": 45,
                "success": 44,
                "failed": 1,
                "started_at": "2025-02-05T12:34:56Z",
                "completed_at": None,
                "error_message": None,
                "result": None
            }
        }
    }

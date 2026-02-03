"""
Pydantic schemas for API request/response models.

These models define the structure of data for the REST API.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any, Generic, TypeVar
from pydantic import BaseModel, Field, HttpUrl

T = TypeVar('T')


class FieldDefinition(BaseModel):
    """Field definition for data extraction."""
    name: str = Field(..., description="Field name")
    selector: Optional[str] = Field(None, description="CSS selector or column name")
    data_type: str = Field("string", description="Data type: string, number, date")
    is_list: bool = Field(False, description="Whether field extracts multiple values")
    attribute: Optional[str] = Field(None, description="HTML attribute to extract")
    pattern: Optional[str] = Field(None, description="Regex pattern for text extraction")


class SourceCreate(BaseModel):
    """Request model for creating a new source."""
    name: str = Field(..., min_length=1, max_length=100, description="Source name")
    url: str = Field(..., description="Target URL to crawl")
    type: str = Field(..., pattern="^(html|pdf|excel|csv)$", description="Data type")
    fields: List[FieldDefinition] = Field(..., min_length=1, description="Fields to extract")
    schedule: str = Field(..., description="Cron expression for scheduling")

    model_config = {
        "json_schema_extra": {
            "example": {
                "name": "Example News Site",
                "url": "https://example.com/news",
                "type": "html",
                "fields": [
                    {"name": "title", "selector": "h1.article-title", "data_type": "string"},
                    {"name": "date", "selector": "span.publish-date", "data_type": "date"},
                    {"name": "content", "selector": "div.article-body", "data_type": "string"}
                ],
                "schedule": "0 9 * * *"
            }
        }
    }


class SourceUpdate(BaseModel):
    """Request model for updating a source."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    url: Optional[str] = None
    type: Optional[str] = Field(None, pattern="^(html|pdf|excel|csv)$")
    fields: Optional[List[FieldDefinition]] = None
    schedule: Optional[str] = None
    status: Optional[str] = Field(None, pattern="^(active|inactive)$")


class SourceResponse(BaseModel):
    """Response model for source data."""
    id: str = Field(..., alias="_id")
    name: str
    url: str
    type: str
    fields: List[FieldDefinition]
    schedule: str
    status: str
    last_run: Optional[datetime] = None
    last_success: Optional[datetime] = None
    error_count: int = 0
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = {"populate_by_name": True}


class CrawlerResponse(BaseModel):
    """Response model for crawler data."""
    id: str = Field(..., alias="_id")
    source_id: str
    version: int
    status: str
    dag_id: Optional[str] = None
    created_at: datetime
    created_by: str
    code: Optional[str] = None  # Optional, may be excluded for list views

    model_config = {"populate_by_name": True}


class CrawlerHistoryResponse(BaseModel):
    """Response model for crawler history."""
    id: str = Field(..., alias="_id")
    crawler_id: str
    version: int
    code: str
    change_reason: str
    change_detail: Optional[str] = None
    changed_at: datetime
    changed_by: str

    model_config = {"populate_by_name": True}


class CrawlResultResponse(BaseModel):
    """Response model for crawl results."""
    id: str = Field(..., alias="_id")
    source_id: str
    crawler_id: str
    run_id: Optional[str] = None
    status: str
    data: Optional[Dict[str, Any]] = None
    record_count: int = 0
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    execution_time_ms: int = 0
    executed_at: datetime

    model_config = {"populate_by_name": True}


class ErrorLogResponse(BaseModel):
    """Response model for error logs."""
    id: str = Field(..., alias="_id")
    source_id: str
    crawler_id: Optional[str] = None
    run_id: Optional[str] = None
    error_code: str
    error_type: str
    message: str
    stack_trace: Optional[str] = None
    auto_recoverable: bool
    resolved: bool
    resolved_at: Optional[datetime] = None
    resolution_method: Optional[str] = None
    resolution_detail: Optional[str] = None
    created_at: datetime

    model_config = {"populate_by_name": True}


class SourceStats(BaseModel):
    """Statistics for sources."""
    total: int
    active: int
    error: int


class CrawlerStats(BaseModel):
    """Statistics for crawlers."""
    total: int
    active: int


class ExecutionStats(BaseModel):
    """Statistics for recent executions."""
    total: int
    success: int
    failed: int
    success_rate: float


class DashboardResponse(BaseModel):
    """Response model for dashboard data."""
    sources: SourceStats
    crawlers: CrawlerStats
    recent_executions: ExecutionStats
    unresolved_errors: int
    timestamp: datetime


class TriggerResponse(BaseModel):
    """Response model for DAG trigger."""
    success: bool
    dag_id: str
    run_id: Optional[str] = None
    message: str


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response."""
    items: List[T]
    total: int
    page: int
    page_size: int
    total_pages: int

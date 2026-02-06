"""
Export Router for data export endpoints.

Provides REST API endpoints for exporting data to CSV, Excel, and JSON formats.
Supports both streaming downloads and asynchronous batch exports.
"""

import os
import logging
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Query, BackgroundTasks, HTTPException, Depends
from fastapi.responses import StreamingResponse, FileResponse, Response
from bson import ObjectId

from ..services.mongo_service import MongoService
from ..services.export_service import (
    ExportService,
    CrawlResultExporter,
    ReviewDataExporter
)
from ..models.export_schemas import (
    ExportRequest,
    ExportJobResponse,
    ExportJobStatus
)
from app.auth.dependencies import require_auth, require_scope, AuthContext

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# Dependency Injection
# ============================================================

def get_mongo_service():
    """Dependency for MongoDB service."""
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


def get_export_service(mongo: MongoService = Depends(get_mongo_service)):
    """Dependency for Export service."""
    return ExportService(mongo)


# ============================================================
# CSV Export Endpoints
# ============================================================

@router.get(
    "/csv",
    summary="Export data to CSV",
    description="""
    Export data from a collection to CSV format with streaming download.

    **Supported collections:**
    - `crawl_results`: Crawl execution results
    - `data_reviews`: Data review records
    - `sources`: Registered crawling sources
    - `error_logs`: Error log entries

    **Encoding options:**
    - `utf-8-sig`: UTF-8 with BOM (recommended for Excel on Windows)
    - `utf-8`: Standard UTF-8
    - `euc-kr`: Korean encoding (legacy systems)

    **Notes:**
    - Large exports (>10,000 records) may take time
    - For very large datasets, use the async export endpoint
    """,
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "CSV file stream",
            "content": {"text/csv": {}}
        },
        400: {"description": "Invalid parameters"},
        404: {"description": "Collection not found"}
    }
)
async def export_csv(
    collection: str = Query(
        ...,
        description="Collection name to export",
        example="crawl_results"
    ),
    source_id: Optional[str] = Query(
        None,
        description="Filter by source ID"
    ),
    date_from: Optional[datetime] = Query(
        None,
        description="Start date filter (ISO 8601 format)"
    ),
    date_to: Optional[datetime] = Query(
        None,
        description="End date filter (ISO 8601 format)"
    ),
    fields: Optional[List[str]] = Query(
        None,
        description="Fields to include in export"
    ),
    limit: int = Query(
        10000,
        ge=1,
        le=100000,
        description="Maximum records to export"
    ),
    encoding: str = Query(
        "utf-8-sig",
        regex="^(utf-8|utf-8-sig|euc-kr|cp949)$",
        description="Character encoding"
    ),
    export_service: ExportService = Depends(get_export_service),
    auth: AuthContext = Depends(require_auth),
):
    """
    Stream CSV export of collection data.

    The response is streamed in chunks for memory efficiency.
    """
    # Validate collection
    valid_collections = [
        "crawl_results", "data_reviews", "sources",
        "error_logs", "crawlers", "crawler_history"
    ]
    if collection not in valid_collections:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid collection. Must be one of: {', '.join(valid_collections)}"
        )

    # Build query
    query = _build_query(source_id, date_from, date_to, collection)

    # Generate filename
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{collection}_{timestamp}.csv"

    # Create streaming response
    async def csv_generator():
        async for chunk in export_service.stream_csv_async(
            collection=collection,
            query=query,
            fields=fields,
            encoding=encoding,
            limit=limit
        ):
            yield chunk

    return StreamingResponse(
        csv_generator(),
        media_type="text/csv; charset=" + encoding,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Total-Limit": str(limit)
        }
    )


# ============================================================
# Excel Export Endpoints
# ============================================================

@router.get(
    "/excel",
    summary="Export data to Excel",
    description="""
    Export data from a collection to Excel format (.xlsx).

    **Features:**
    - Formatted header row with styling
    - Auto-adjusted column widths
    - Frozen header row for easy scrolling

    **Limitations:**
    - Maximum 50,000 records (Excel performance)
    - File generated in memory before download
    - For larger exports, use CSV format
    """,
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Excel file download",
            "content": {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}
            }
        },
        400: {"description": "Invalid parameters"},
        413: {"description": "Request too large"}
    }
)
async def export_excel(
    collection: str = Query(
        ...,
        description="Collection name to export"
    ),
    source_id: Optional[str] = Query(
        None,
        description="Filter by source ID"
    ),
    date_from: Optional[datetime] = Query(
        None,
        description="Start date filter"
    ),
    date_to: Optional[datetime] = Query(
        None,
        description="End date filter"
    ),
    fields: Optional[List[str]] = Query(
        None,
        description="Fields to include"
    ),
    limit: int = Query(
        10000,
        ge=1,
        le=50000,
        description="Maximum records (lower limit for Excel)"
    ),
    sheet_name: str = Query(
        "Data",
        max_length=31,
        description="Excel worksheet name"
    ),
    export_service: ExportService = Depends(get_export_service),
    auth: AuthContext = Depends(require_auth),
):
    """
    Generate and download Excel file.

    Note: Excel files are generated in memory, so large exports
    may use significant memory. Use CSV for very large datasets.
    """
    # Validate collection
    valid_collections = [
        "crawl_results", "data_reviews", "sources",
        "error_logs", "crawlers", "crawler_history"
    ]
    if collection not in valid_collections:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid collection. Must be one of: {', '.join(valid_collections)}"
        )

    # Build query
    query = _build_query(source_id, date_from, date_to, collection)

    try:
        # Generate Excel
        excel_bytes = await export_service.generate_excel_async(
            collection=collection,
            query=query,
            fields=fields,
            sheet_name=sheet_name,
            limit=limit
        )

        # Generate filename
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{collection}_{timestamp}.xlsx"

        return Response(
            content=excel_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(len(excel_bytes))
            }
        )

    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="Excel export requires openpyxl. Please contact administrator."
        )
    except MemoryError:
        raise HTTPException(
            status_code=413,
            detail="Export too large. Please reduce the limit or use CSV format."
        )


# ============================================================
# Async Export Endpoints
# ============================================================

@router.post(
    "/async",
    summary="Create async export job",
    description="""
    Create a background export job for large datasets.

    **Use this endpoint when:**
    - Exporting more than 50,000 records
    - Export may take longer than request timeout
    - You need to track export progress

    **Workflow:**
    1. POST to this endpoint to create job
    2. Poll GET /export/jobs/{job_id} for status
    3. Download from GET /export/download/{job_id} when complete

    **Job expiration:**
    - Completed exports are available for 24 hours
    - After expiration, files are automatically deleted
    """,
    response_model=ExportJobResponse,
    responses={
        201: {"description": "Export job created"},
        400: {"description": "Invalid request"},
        503: {"description": "Export service unavailable"}
    }
)
async def create_async_export(
    request: ExportRequest,
    background_tasks: BackgroundTasks,
    export_service: ExportService = Depends(get_export_service),
    auth: AuthContext = Depends(require_scope("write")),
):
    """
    Create an asynchronous export job.

    The export runs in the background and results can be
    downloaded when complete.
    """
    # Validate collection
    valid_collections = [
        "crawl_results", "data_reviews", "sources",
        "error_logs", "crawlers", "crawler_history"
    ]
    if request.collection not in valid_collections:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid collection. Must be one of: {', '.join(valid_collections)}"
        )

    # Build query from request
    query = request.query or {}
    if request.date_from or request.date_to:
        date_field = request.date_field
        query[date_field] = {}
        if request.date_from:
            query[date_field]["$gte"] = request.date_from
        if request.date_to:
            query[date_field]["$lte"] = request.date_to

    # Create job
    job_id = export_service.create_export_job(
        collection=request.collection,
        format=request.format,
        query=query,
        fields=request.fields,
        limit=request.limit,
        encoding=request.encoding
    )

    # Get job details
    job = export_service.get_export_job(job_id)

    # Schedule background processing
    background_tasks.add_task(export_service.process_export_job, job_id)

    return ExportJobResponse(
        job_id=job_id,
        status="pending",
        created_at=job["created_at"],
        estimated_records=job["total_records"],
        format=request.format,
        collection=request.collection
    )


@router.get(
    "/jobs/{job_id}",
    summary="Get export job status",
    description="Check the status and progress of an async export job.",
    response_model=ExportJobStatus,
    responses={
        200: {"description": "Job status"},
        404: {"description": "Job not found"}
    }
)
async def get_export_job_status(
    job_id: str,
    export_service: ExportService = Depends(get_export_service),
    auth: AuthContext = Depends(require_auth),
):
    """
    Get status of an async export job.

    Returns progress information and download URL when complete.
    """
    job = export_service.get_export_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")

    # Build download URL if completed
    download_url = None
    if job.get("status") == "completed":
        download_url = f"/api/export/download/{job_id}"

    return ExportJobStatus(
        job_id=job["job_id"],
        status=job.get("status", "pending"),
        progress=job.get("progress", 0.0),
        records_processed=job.get("records_processed", 0),
        total_records=job.get("total_records", 0),
        file_size=job.get("file_size"),
        download_url=download_url,
        error=job.get("error"),
        created_at=job["created_at"],
        started_at=job.get("started_at"),
        completed_at=job.get("completed_at"),
        expires_at=job.get("expires_at")
    )


@router.get(
    "/download/{job_id}",
    summary="Download export file",
    description="""
    Download the completed export file.

    **Notes:**
    - Only available for completed jobs
    - Files expire after 24 hours
    - Each file can be downloaded multiple times until expiration
    """,
    response_class=FileResponse,
    responses={
        200: {"description": "Export file download"},
        404: {"description": "Job or file not found"},
        400: {"description": "Job not completed"}
    }
)
async def download_export_file(
    job_id: str,
    export_service: ExportService = Depends(get_export_service),
    auth: AuthContext = Depends(require_auth),
):
    """
    Download a completed export file.
    """
    job = export_service.get_export_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")

    if job.get("status") != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Export not ready. Current status: {job.get('status')}"
        )

    file_path = export_service.get_export_file_path(job_id)
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Export file not found or expired")

    # Determine media type based on format
    format = job.get("format", "csv")
    if format == "excel":
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ext = ".xlsx"
    elif format == "json":
        media_type = "application/json"
        ext = ".json"
    else:
        media_type = "text/csv"
        ext = ".csv"

    filename = f"{job.get('collection', 'export')}_{job_id}{ext}"

    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename
    )


# ============================================================
# Specialized Export Endpoints
# ============================================================

@router.get(
    "/crawl-results/{source_id}",
    summary="Export crawl results for a source",
    description="""
    Export crawl execution results for a specific source.

    Provides a convenient endpoint for exporting all crawl results
    from a single source with source-specific filtering options.
    """,
    response_class=StreamingResponse,
    responses={
        200: {"description": "Export file stream"},
        404: {"description": "Source not found"}
    }
)
async def export_crawl_results(
    source_id: str,
    format: str = Query(
        "csv",
        regex="^(csv|excel|json)$",
        description="Export format"
    ),
    date_from: Optional[datetime] = Query(
        None,
        description="Start date filter"
    ),
    date_to: Optional[datetime] = Query(
        None,
        description="End date filter"
    ),
    status: Optional[str] = Query(
        None,
        regex="^(success|failed|partial)$",
        description="Filter by execution status"
    ),
    include_data: bool = Query(
        False,
        description="Include extracted data (can be large)"
    ),
    include_metadata: bool = Query(
        False,
        description="Include execution metadata"
    ),
    limit: int = Query(
        10000,
        ge=1,
        le=100000,
        description="Maximum records"
    ),
    mongo: MongoService = Depends(get_mongo_service),
    export_service: ExportService = Depends(get_export_service),
    auth: AuthContext = Depends(require_auth),
):
    """
    Export crawl results for a specific source.
    """
    # Verify source exists
    try:
        source = mongo.get_source(source_id)
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")
    except Exception:
        raise HTTPException(status_code=404, detail="Source not found")

    # Build query
    exporter = CrawlResultExporter(export_service)
    query = exporter.build_query(
        source_id=source_id,
        status=status,
        date_from=date_from,
        date_to=date_to
    )

    # Get fields
    fields = exporter.get_export_fields(
        include_data=include_data,
        include_metadata=include_metadata
    )

    # Generate filename
    source_name = source.get("name", "unknown").replace(" ", "_")[:30]
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if format == "excel":
        # Excel export
        ext = "xlsx"
        excel_limit = min(limit, 50000)
        excel_bytes = await export_service.generate_excel_async(
            collection="crawl_results",
            query=query,
            fields=fields,
            sheet_name=source_name,
            limit=excel_limit
        )
        return Response(
            content=excel_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="crawl_results_{source_name}_{timestamp}.{ext}"'
            }
        )
    elif format == "json":
        # JSON export - use streaming
        ext = "json"

        async def json_generator():
            import json
            projection = {field: 1 for field in fields} if fields else None
            cursor = mongo.db.crawl_results.find(query, projection).limit(limit)

            yield b'[\n'
            first = True
            for doc in cursor:
                if not first:
                    yield b',\n'
                first = False
                # Serialize document
                serialized = export_service._serialize_document(doc)
                yield json.dumps(serialized, ensure_ascii=False, default=str).encode('utf-8')
            yield b'\n]'

        return StreamingResponse(
            json_generator(),
            media_type="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="crawl_results_{source_name}_{timestamp}.{ext}"'
            }
        )
    else:
        # CSV export (default)
        ext = "csv"

        async def csv_generator():
            async for chunk in export_service.stream_csv_async(
                collection="crawl_results",
                query=query,
                fields=fields,
                limit=limit
            ):
                yield chunk

        return StreamingResponse(
            csv_generator(),
            media_type="text/csv; charset=utf-8-sig",
            headers={
                "Content-Disposition": f'attachment; filename="crawl_results_{source_name}_{timestamp}.{ext}"'
            }
        )


@router.get(
    "/reviews",
    summary="Export review data",
    description="""
    Export data review records with filtering options.

    Useful for:
    - Auditing review decisions
    - Training data extraction for ML models
    - Quality assurance reporting
    """,
    response_class=StreamingResponse,
    responses={
        200: {"description": "Export file stream"}
    }
)
async def export_reviews(
    status: Optional[str] = Query(
        None,
        regex="^(pending|approved|rejected|on_hold|needs_correction|corrected)$",
        description="Filter by review status"
    ),
    source_id: Optional[str] = Query(
        None,
        description="Filter by source ID"
    ),
    date_from: Optional[datetime] = Query(
        None,
        description="Start date filter"
    ),
    date_to: Optional[datetime] = Query(
        None,
        description="End date filter"
    ),
    format: str = Query(
        "csv",
        regex="^(csv|excel)$",
        description="Export format"
    ),
    include_corrections: bool = Query(
        True,
        description="Include correction history"
    ),
    include_confidence: bool = Query(
        True,
        description="Include confidence scores"
    ),
    limit: int = Query(
        10000,
        ge=1,
        le=100000,
        description="Maximum records"
    ),
    export_service: ExportService = Depends(get_export_service),
    auth: AuthContext = Depends(require_auth),
):
    """
    Export review data with various filter options.
    """
    # Build query
    exporter = ReviewDataExporter(export_service)
    query = exporter.build_query(
        source_id=source_id,
        status=status,
        date_from=date_from,
        date_to=date_to
    )

    # Get fields
    fields = exporter.get_export_fields(
        include_corrections=include_corrections,
        include_confidence=include_confidence
    )

    # Generate filename
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    status_suffix = f"_{status}" if status else ""

    if format == "excel":
        excel_limit = min(limit, 50000)
        excel_bytes = await export_service.generate_excel_async(
            collection="data_reviews",
            query=query,
            fields=fields,
            sheet_name="Reviews",
            limit=excel_limit
        )
        return Response(
            content=excel_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="reviews{status_suffix}_{timestamp}.xlsx"'
            }
        )
    else:
        async def csv_generator():
            async for chunk in export_service.stream_csv_async(
                collection="data_reviews",
                query=query,
                fields=fields,
                limit=limit
            ):
                yield chunk

        return StreamingResponse(
            csv_generator(),
            media_type="text/csv; charset=utf-8-sig",
            headers={
                "Content-Disposition": f'attachment; filename="reviews{status_suffix}_{timestamp}.csv"'
            }
        )


# ============================================================
# Utility Endpoints
# ============================================================

@router.get(
    "/jobs",
    summary="List export jobs",
    description="List recent export jobs with pagination.",
    responses={
        200: {"description": "List of export jobs"}
    }
)
async def list_export_jobs(
    status: Optional[str] = Query(
        None,
        regex="^(pending|processing|completed|failed|expired)$",
        description="Filter by job status"
    ),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(20, ge=1, le=100, description="Maximum records to return"),
    mongo: MongoService = Depends(get_mongo_service),
    auth: AuthContext = Depends(require_auth),
):
    """
    List export jobs with optional status filter.
    """
    query = {}
    if status:
        query["status"] = status

    cursor = mongo.db.export_jobs.find(query).sort("created_at", -1).skip(skip).limit(limit)
    total = mongo.db.export_jobs.count_documents(query)

    jobs = []
    for doc in cursor:
        doc["job_id"] = doc.pop("_id")
        if doc.get("status") == "completed":
            doc["download_url"] = f"/api/export/download/{doc['job_id']}"
        jobs.append(doc)

    return {
        "jobs": jobs,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@router.delete(
    "/jobs/{job_id}",
    summary="Cancel export job",
    description="Cancel a pending or processing export job.",
    responses={
        200: {"description": "Job cancelled"},
        404: {"description": "Job not found"},
        400: {"description": "Job cannot be cancelled"}
    }
)
async def cancel_export_job(
    job_id: str,
    mongo: MongoService = Depends(get_mongo_service),
    auth: AuthContext = Depends(require_scope("write")),
):
    """
    Cancel an export job.

    Only pending or processing jobs can be cancelled.
    """
    job = mongo.db.export_jobs.find_one({"_id": job_id})
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")

    if job.get("status") in ["completed", "failed", "expired"]:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job with status: {job.get('status')}"
        )

    mongo.db.export_jobs.update_one(
        {"_id": job_id},
        {"$set": {
            "status": "failed",
            "error": "Cancelled by user",
            "completed_at": datetime.utcnow()
        }}
    )

    return {"message": "Export job cancelled", "job_id": job_id}


@router.post(
    "/cleanup",
    summary="Cleanup expired exports",
    description="Manually trigger cleanup of expired export files. Normally runs automatically.",
    responses={
        200: {"description": "Cleanup completed"}
    }
)
async def cleanup_exports(
    export_service: ExportService = Depends(get_export_service),
    auth: AuthContext = Depends(require_scope("write")),
):
    """
    Manually trigger cleanup of expired export files.
    """
    cleaned = export_service.cleanup_expired_exports()
    return {
        "message": "Cleanup completed",
        "files_removed": cleaned
    }


# ============================================================
# Helper Functions
# ============================================================

def _build_query(
    source_id: Optional[str],
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    collection: str
) -> dict:
    """
    Build MongoDB query from common parameters.

    Args:
        source_id: Optional source ID filter
        date_from: Optional start date
        date_to: Optional end date
        collection: Target collection name

    Returns:
        MongoDB query dictionary
    """
    query = {}

    if source_id:
        try:
            query["source_id"] = ObjectId(source_id)
        except Exception:
            query["source_id"] = source_id

    # Determine date field based on collection
    date_field_map = {
        "crawl_results": "executed_at",
        "data_reviews": "created_at",
        "error_logs": "created_at",
        "sources": "created_at",
        "crawlers": "created_at",
        "crawler_history": "changed_at"
    }
    date_field = date_field_map.get(collection, "created_at")

    if date_from or date_to:
        query[date_field] = {}
        if date_from:
            query[date_field]["$gte"] = date_from
        if date_to:
            query[date_field]["$lte"] = date_to

    return query

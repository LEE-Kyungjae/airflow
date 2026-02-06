"""
Sources Router.

Endpoints for managing crawling sources. Sources define target URLs,
extraction fields, and scheduling for automated data collection.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Depends, status

from app.models.schemas import SourceCreate, SourceUpdate, SourceResponse, TriggerResponse
from app.services.mongo_service import MongoService
from app.services.airflow_trigger import AirflowTrigger
from app.auth.dependencies import require_auth, optional_auth, require_scope, AuthContext

logger = logging.getLogger(__name__)
router = APIRouter()


# ============================================================
# Common Error Response Definitions
# ============================================================

ERROR_RESPONSES = {
    404: {
        "description": "Source not found",
        "content": {
            "application/json": {
                "example": {"detail": "Source not found"}
            }
        }
    },
    409: {
        "description": "Resource conflict",
        "content": {
            "application/json": {
                "example": {"detail": "Source 'Example Source' already exists"}
            }
        }
    },
    400: {
        "description": "Bad request",
        "content": {
            "application/json": {
                "example": {"detail": "No update data provided"}
            }
        }
    },
    401: {
        "description": "Authentication required",
        "content": {
            "application/json": {
                "example": {"detail": "Not authenticated"}
            }
        }
    },
    403: {
        "description": "Insufficient permissions",
        "content": {
            "application/json": {
                "example": {"detail": "Insufficient scope. Required: write"}
            }
        }
    }
}


def get_mongo():
    """Dependency for MongoDB connection with automatic cleanup."""
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


@router.post(
    "",
    response_model=TriggerResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new crawling source",
    description="""
Register a new crawling source and trigger automatic crawler code generation.

**Process:**
1. Validates source configuration against schema rules
2. Checks for duplicate source names
3. Registers the source in MongoDB
4. Triggers the `source_manager` DAG to generate crawler code

**Required scope:** `write`
""",
    response_description="Trigger response with DAG run information",
    responses={
        201: {
            "description": "Source created successfully",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "dag_id": "source_manager",
                        "run_id": "manual__2025-02-05T12:00:00+00:00",
                        "message": "Source created with ID: 507f1f77bcf86cd799439011. DAG triggered successfully"
                    }
                }
            }
        },
        409: ERROR_RESPONSES[409],
        401: ERROR_RESPONSES[401],
        403: ERROR_RESPONSES[403]
    }
)
async def create_source(
    source: SourceCreate,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write"))
):
    """
    Create a new crawling source and trigger code generation.

    This will:
    1. Register the source in MongoDB
    2. Trigger the source_manager DAG to generate crawler code
    """
    # Check if source already exists
    existing = mongo.get_source_by_name(source.name)
    if existing:
        raise HTTPException(status_code=409, detail=f"Source '{source.name}' already exists")

    # Create source
    source_data = source.model_dump()
    source_id = mongo.create_source(source_data)

    # Trigger Airflow DAG
    airflow = AirflowTrigger()
    trigger_conf = {
        "source_id": source_id,
        **source_data
    }

    result = await airflow.trigger_dag("source_manager", conf=trigger_conf)

    if not result["success"]:
        # Source was created but DAG trigger failed
        logger.warning(f"Source created but DAG trigger failed: {result['message']}")

    return TriggerResponse(
        success=result["success"],
        dag_id="source_manager",
        run_id=result.get("run_id"),
        message=f"Source created with ID: {source_id}. {result['message']}"
    )


@router.get(
    "",
    response_model=List[SourceResponse],
    summary="List all sources",
    description="""
Retrieve a paginated list of all registered crawling sources.

Supports filtering by status and pagination through `skip` and `limit` parameters.
Results are ordered by creation date (newest first).

**Performance Tip:** Set `include_crawler_info=True` to include active crawler
details in a single optimized query instead of making N+1 separate calls.
""",
    response_description="List of source configurations with runtime statistics"
)
async def list_sources(
    status: Optional[str] = Query(
        None,
        pattern="^(active|inactive|error)$",
        description="Filter by source status: active, inactive, or error"
    ),
    skip: int = Query(
        0,
        ge=0,
        description="Number of records to skip for pagination"
    ),
    limit: int = Query(
        100,
        ge=1,
        le=500,
        description="Maximum number of records to return (max 500)"
    ),
    include_crawler_info: bool = Query(
        False,
        description="Include active crawler info via $lookup aggregation (optimized)"
    ),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth)
):
    """
    List all sources with optional filtering.

    N+1 optimization: When include_crawler_info=True, uses MongoDB $lookup
    to fetch source and crawler data in a single aggregation pipeline.
    """
    if include_crawler_info:
        # 최적화된 메서드 사용: 소스 + 활성 크롤러 정보를 단일 쿼리로 조회
        sources = mongo.list_sources_with_crawler_info(status=status, skip=skip, limit=limit)
    else:
        sources = mongo.list_sources(status=status, skip=skip, limit=limit)
    return sources


@router.get(
    "/{source_id}",
    response_model=SourceResponse,
    summary="Get source by ID",
    description="Retrieve detailed information about a specific crawling source including configuration and runtime statistics.",
    response_description="Complete source configuration and statistics",
    responses={404: ERROR_RESPONSES[404]}
)
async def get_source(
    source_id: str,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth)
):
    """
    Get a specific source by ID.

    Returns the complete source configuration including:
    - Extraction field definitions
    - Schedule configuration
    - Runtime statistics (last run, error count, etc.)
    """
    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source not found"
        )
    return source


@router.put(
    "/{source_id}",
    response_model=SourceResponse,
    summary="Update source configuration",
    description="""
Update an existing source's configuration.

Only provided fields will be updated (partial update supported).
Changing `fields` or `type` may trigger automatic crawler code regeneration.

**Required scope:** `write`
""",
    response_description="Updated source configuration",
    responses={
        400: ERROR_RESPONSES[400],
        404: ERROR_RESPONSES[404],
        401: ERROR_RESPONSES[401],
        403: ERROR_RESPONSES[403]
    }
)
async def update_source(
    source_id: str,
    update: SourceUpdate,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write"))
):
    """
    Update a source.

    Supports partial updates - only include fields you want to change.
    """
    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source not found"
        )

    update_data = update.model_dump(exclude_unset=True)
    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No update data provided"
        )

    mongo.update_source(source_id, update_data)
    return mongo.get_source(source_id)


@router.delete(
    "/{source_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a source",
    description="""
Delete a source and all associated data.

**Warning:** This operation is irreversible and will delete:
- The source configuration
- Associated crawler code and version history
- All crawl results
- Related error logs

**Required scope:** `delete`
""",
    responses={
        204: {"description": "Source deleted successfully"},
        404: ERROR_RESPONSES[404],
        401: ERROR_RESPONSES[401],
        403: ERROR_RESPONSES[403]
    }
)
async def delete_source(
    source_id: str,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("delete"))
):
    """Delete a source and all related data."""
    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source not found"
        )

    mongo.delete_source(source_id)


@router.post(
    "/{source_id}/trigger",
    response_model=TriggerResponse,
    summary="Trigger manual crawl",
    description="""
Manually trigger an immediate crawl for the specified source.

This bypasses the regular cron schedule and runs the crawler immediately.
Useful for testing changes or urgent data updates.

**Required scope:** `write`

**Prerequisites:**
- Source must have an active crawler
- Crawler must have a registered DAG ID
""",
    response_description="Trigger response with run information",
    responses={
        200: {
            "description": "Crawl triggered successfully",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "dag_id": "crawler_gov_stats",
                        "run_id": "manual__2025-02-05T12:30:00+00:00",
                        "message": "DAG triggered successfully"
                    }
                }
            }
        },
        400: {
            "description": "Cannot trigger crawl",
            "content": {
                "application/json": {
                    "examples": {
                        "no_crawler": {"value": {"detail": "No active crawler for this source"}},
                        "no_dag": {"value": {"detail": "Crawler has no DAG ID"}}
                    }
                }
            }
        },
        404: ERROR_RESPONSES[404],
        401: ERROR_RESPONSES[401],
        403: ERROR_RESPONSES[403]
    }
)
async def trigger_source_crawl(
    source_id: str,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write"))
):
    """Manually trigger a crawl for a source."""
    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source not found"
        )

    crawler = mongo.get_active_crawler(source_id)
    if not crawler:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No active crawler for this source"
        )

    dag_id = crawler.get('dag_id')
    if not dag_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Crawler has no DAG ID"
        )

    airflow = AirflowTrigger()
    result = await airflow.trigger_dag(dag_id)

    return TriggerResponse(
        success=result["success"],
        dag_id=dag_id,
        run_id=result.get("run_id"),
        message=result["message"]
    )


@router.get(
    "/{source_id}/results",
    summary="Get crawl results for a source",
    description="""
Retrieve the crawl execution history and results for a specific source.

Results are ordered by execution time (most recent first) and include
extracted data, execution metrics, and error information if applicable.
""",
    response_description="List of crawl results with extracted data",
    responses={
        200: {
            "description": "Crawl results retrieved",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "_id": "507f1f77bcf86cd799439014",
                            "source_id": "507f1f77bcf86cd799439011",
                            "status": "success",
                            "record_count": 25,
                            "execution_time_ms": 2500,
                            "executed_at": "2025-02-05T09:00:02Z"
                        }
                    ]
                }
            }
        },
        404: ERROR_RESPONSES[404]
    }
)
async def get_source_results(
    source_id: str,
    skip: int = Query(
        0,
        ge=0,
        description="Number of records to skip for pagination"
    ),
    limit: int = Query(
        20,
        ge=1,
        le=100,
        description="Maximum number of results to return (max 100)"
    ),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth)
):
    """
    Get crawl results for a source.

    Returns execution history including status, record count,
    execution time, and error details for failed runs.
    """
    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source not found"
        )

    results = mongo.get_crawl_results(source_id=source_id, skip=skip, limit=limit)
    return results

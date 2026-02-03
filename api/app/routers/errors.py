"""
Errors Router.

Endpoints for managing error logs and recovery.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Depends

from app.models.schemas import ErrorLogResponse, TriggerResponse
from app.services.mongo_service import MongoService
from app.services.airflow_trigger import AirflowTrigger

logger = logging.getLogger(__name__)
router = APIRouter()


def get_mongo():
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


@router.get("", response_model=List[ErrorLogResponse])
async def list_errors(
    resolved: Optional[bool] = None,
    source_id: Optional[str] = None,
    error_code: Optional[str] = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    mongo: MongoService = Depends(get_mongo)
):
    """List error logs with optional filtering."""
    errors = mongo.list_errors(
        resolved=resolved,
        source_id=source_id,
        skip=skip,
        limit=limit
    )
    return errors


@router.get("/unresolved", response_model=List[ErrorLogResponse])
async def list_unresolved_errors(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    mongo: MongoService = Depends(get_mongo)
):
    """Get all unresolved errors."""
    errors = mongo.list_errors(resolved=False, skip=skip, limit=limit)
    return errors


@router.get("/stats")
async def get_error_stats(mongo: MongoService = Depends(get_mongo)):
    """Get error statistics."""
    total = mongo.count_errors()
    resolved = mongo.count_errors(resolved=True)
    unresolved = mongo.count_errors(resolved=False)

    # Count by error code
    pipeline = [
        {"$group": {"_id": "$error_code", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    by_code = list(mongo.db.error_logs.aggregate(pipeline))

    return {
        "total": total,
        "resolved": resolved,
        "unresolved": unresolved,
        "by_error_code": {item["_id"]: item["count"] for item in by_code}
    }


@router.get("/{error_id}", response_model=ErrorLogResponse)
async def get_error(error_id: str, mongo: MongoService = Depends(get_mongo)):
    """Get a specific error by ID."""
    error = mongo.get_error(error_id)
    if not error:
        raise HTTPException(status_code=404, detail="Error not found")
    return error


@router.post("/{error_id}/resolve")
async def resolve_error(
    error_id: str,
    method: str = Query("manual", pattern="^(auto|manual)$"),
    detail: str = Query(""),
    mongo: MongoService = Depends(get_mongo)
):
    """Mark an error as resolved."""
    error = mongo.get_error(error_id)
    if not error:
        raise HTTPException(status_code=404, detail="Error not found")

    if error.get('resolved'):
        raise HTTPException(status_code=400, detail="Error already resolved")

    mongo.resolve_error(error_id, method, detail)

    return {"success": True, "message": "Error marked as resolved"}


@router.post("/{error_id}/retry", response_model=TriggerResponse)
async def retry_error(error_id: str, mongo: MongoService = Depends(get_mongo)):
    """
    Retry crawling for an error (one-click recovery).

    This will:
    1. Get the source and crawler associated with the error
    2. Trigger the crawler DAG to retry
    """
    error = mongo.get_error(error_id)
    if not error:
        raise HTTPException(status_code=404, detail="Error not found")

    source_id = error.get('source_id')
    if not source_id:
        raise HTTPException(status_code=400, detail="Error has no associated source")

    # Get the active crawler
    crawler = mongo.get_active_crawler(source_id)
    if not crawler:
        raise HTTPException(status_code=400, detail="No active crawler found")

    dag_id = crawler.get('dag_id')
    if not dag_id:
        raise HTTPException(status_code=400, detail="Crawler has no DAG ID")

    # Trigger the DAG
    airflow = AirflowTrigger()
    result = await airflow.trigger_dag(dag_id)

    if result["success"]:
        # Mark error as resolved with auto method
        mongo.resolve_error(error_id, "auto", f"Retry triggered: {result.get('run_id')}")

    return TriggerResponse(
        success=result["success"],
        dag_id=dag_id,
        run_id=result.get("run_id"),
        message=result["message"]
    )


@router.post("/{error_id}/regenerate", response_model=TriggerResponse)
async def regenerate_crawler_code(error_id: str, mongo: MongoService = Depends(get_mongo)):
    """
    Regenerate crawler code for an error.

    This triggers the source_manager DAG to regenerate the crawler
    code using GPT, which may fix structural issues.
    """
    error = mongo.get_error(error_id)
    if not error:
        raise HTTPException(status_code=404, detail="Error not found")

    source_id = error.get('source_id')
    if not source_id:
        raise HTTPException(status_code=400, detail="Error has no associated source")

    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    # Trigger source_manager to regenerate
    airflow = AirflowTrigger()
    conf = {
        "source_id": source_id,
        "name": source['name'],
        "url": source['url'],
        "type": source['type'],
        "fields": source['fields'],
        "schedule": source['schedule'],
        "regenerate": True
    }

    result = await airflow.trigger_dag("source_manager", conf=conf)

    if result["success"]:
        mongo.resolve_error(error_id, "auto", "Regeneration triggered")

    return TriggerResponse(
        success=result["success"],
        dag_id="source_manager",
        run_id=result.get("run_id"),
        message=result["message"]
    )

"""
Sources Router.

Endpoints for managing crawling sources.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Depends

from app.models.schemas import SourceCreate, SourceUpdate, SourceResponse, TriggerResponse
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


@router.post("", response_model=TriggerResponse, status_code=201)
async def create_source(source: SourceCreate, mongo: MongoService = Depends(get_mongo)):
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


@router.get("", response_model=List[SourceResponse])
async def list_sources(
    status: Optional[str] = Query(None, pattern="^(active|inactive|error)$"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    mongo: MongoService = Depends(get_mongo)
):
    """List all sources with optional filtering."""
    sources = mongo.list_sources(status=status, skip=skip, limit=limit)
    return sources


@router.get("/{source_id}", response_model=SourceResponse)
async def get_source(source_id: str, mongo: MongoService = Depends(get_mongo)):
    """Get a specific source by ID."""
    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


@router.put("/{source_id}", response_model=SourceResponse)
async def update_source(
    source_id: str,
    update: SourceUpdate,
    mongo: MongoService = Depends(get_mongo)
):
    """Update a source."""
    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    update_data = update.model_dump(exclude_unset=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No update data provided")

    mongo.update_source(source_id, update_data)
    return mongo.get_source(source_id)


@router.delete("/{source_id}", status_code=204)
async def delete_source(source_id: str, mongo: MongoService = Depends(get_mongo)):
    """Delete a source and all related data."""
    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    mongo.delete_source(source_id)


@router.post("/{source_id}/trigger", response_model=TriggerResponse)
async def trigger_source_crawl(source_id: str, mongo: MongoService = Depends(get_mongo)):
    """Manually trigger a crawl for a source."""
    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    crawler = mongo.get_active_crawler(source_id)
    if not crawler:
        raise HTTPException(status_code=400, detail="No active crawler for this source")

    dag_id = crawler.get('dag_id')
    if not dag_id:
        raise HTTPException(status_code=400, detail="Crawler has no DAG ID")

    airflow = AirflowTrigger()
    result = await airflow.trigger_dag(dag_id)

    return TriggerResponse(
        success=result["success"],
        dag_id=dag_id,
        run_id=result.get("run_id"),
        message=result["message"]
    )


@router.get("/{source_id}/results")
async def get_source_results(
    source_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    mongo: MongoService = Depends(get_mongo)
):
    """Get crawl results for a source."""
    source = mongo.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    results = mongo.get_crawl_results(source_id=source_id, skip=skip, limit=limit)
    return results

"""
Crawlers Router.

Endpoints for managing crawlers and their history.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Depends

from app.models.schemas import CrawlerResponse, CrawlerHistoryResponse
from app.services.mongo_service import MongoService

logger = logging.getLogger(__name__)
router = APIRouter()


def get_mongo():
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


@router.get("", response_model=List[CrawlerResponse])
async def list_crawlers(
    source_id: Optional[str] = None,
    status: Optional[str] = Query(None, pattern="^(active|testing|deprecated)$"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    mongo: MongoService = Depends(get_mongo)
):
    """List all crawlers with optional filtering."""
    crawlers = mongo.list_crawlers(source_id=source_id, status=status, skip=skip, limit=limit)

    # Exclude code from list view for performance
    for crawler in crawlers:
        crawler.pop('code', None)

    return crawlers


@router.get("/{crawler_id}", response_model=CrawlerResponse)
async def get_crawler(crawler_id: str, mongo: MongoService = Depends(get_mongo)):
    """Get a specific crawler by ID, including its code."""
    crawler = mongo.get_crawler(crawler_id)
    if not crawler:
        raise HTTPException(status_code=404, detail="Crawler not found")
    return crawler


@router.get("/{crawler_id}/code")
async def get_crawler_code(crawler_id: str, mongo: MongoService = Depends(get_mongo)):
    """Get just the crawler code."""
    crawler = mongo.get_crawler(crawler_id)
    if not crawler:
        raise HTTPException(status_code=404, detail="Crawler not found")

    return {
        "crawler_id": crawler_id,
        "version": crawler.get("version"),
        "code": crawler.get("code")
    }


@router.get("/{crawler_id}/history", response_model=List[CrawlerHistoryResponse])
async def get_crawler_history(
    crawler_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    mongo: MongoService = Depends(get_mongo)
):
    """Get version history for a crawler."""
    crawler = mongo.get_crawler(crawler_id)
    if not crawler:
        raise HTTPException(status_code=404, detail="Crawler not found")

    history = mongo.get_crawler_history(crawler_id, skip=skip, limit=limit)
    return history


@router.get("/{crawler_id}/history/{version}")
async def get_crawler_version(
    crawler_id: str,
    version: int,
    mongo: MongoService = Depends(get_mongo)
):
    """Get a specific version of crawler code."""
    crawler = mongo.get_crawler(crawler_id)
    if not crawler:
        raise HTTPException(status_code=404, detail="Crawler not found")

    history = mongo.get_crawler_history(crawler_id)
    for h in history:
        if h.get('version') == version:
            return h

    raise HTTPException(status_code=404, detail=f"Version {version} not found")


@router.post("/{crawler_id}/rollback/{version}")
async def rollback_crawler(
    crawler_id: str,
    version: int,
    mongo: MongoService = Depends(get_mongo)
):
    """Rollback crawler to a previous version."""
    crawler = mongo.get_crawler(crawler_id)
    if not crawler:
        raise HTTPException(status_code=404, detail="Crawler not found")

    # Find the target version
    history = mongo.get_crawler_history(crawler_id)
    target = None
    for h in history:
        if h.get('version') == version:
            target = h
            break

    if not target:
        raise HTTPException(status_code=404, detail=f"Version {version} not found")

    # Save current version to history first
    current_version = crawler.get('version', 1)
    mongo.db.crawler_history.insert_one({
        'crawler_id': crawler_id,
        'version': current_version,
        'code': crawler.get('code'),
        'change_reason': 'manual_edit',
        'change_detail': f'Pre-rollback backup before restoring v{version}',
        'changed_by': 'user'
    })

    # Update crawler with old code
    new_version = current_version + 1
    mongo.db.crawlers.update_one(
        {'_id': crawler_id},
        {'$set': {
            'code': target.get('code'),
            'version': new_version,
            'created_by': 'manual'
        }}
    )

    return {
        "success": True,
        "message": f"Rolled back to version {version}",
        "new_version": new_version
    }

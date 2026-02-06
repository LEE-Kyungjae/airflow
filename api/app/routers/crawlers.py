"""
Crawlers Router.

Endpoints for managing crawlers and their version history.
Crawlers are AI-generated Python code modules that perform data extraction.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Depends, status

from app.models.schemas import CrawlerResponse, CrawlerHistoryResponse
from app.services.mongo_service import MongoService
from app.auth.dependencies import require_auth, require_scope, AuthContext

logger = logging.getLogger(__name__)
router = APIRouter()


# ============================================================
# Common Error Response Definitions
# ============================================================

ERROR_RESPONSES = {
    404: {
        "description": "Resource not found",
        "content": {
            "application/json": {
                "example": {"detail": "Crawler not found"}
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


@router.get(
    "",
    response_model=List[CrawlerResponse],
    summary="List all crawlers",
    description="""
Retrieve a paginated list of all crawler instances.

Crawlers are auto-generated Python modules responsible for data extraction.
Each source typically has one active crawler with version history.

**Performance Notes:**
- The `code` field is excluded from list responses for performance
- Set `include_source_info=True` to join source data in a single query
""",
    response_description="List of crawler metadata (code excluded)"
)
async def list_crawlers(
    source_id: Optional[str] = Query(
        None,
        description="Filter by associated source ID"
    ),
    status: Optional[str] = Query(
        None,
        pattern="^(active|testing|deprecated)$",
        description="Filter by crawler status: active, testing, or deprecated"
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
    include_source_info: bool = Query(
        False,
        description="Include source info via $lookup aggregation (optimized)"
    ),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
):
    """
    List all crawlers with optional filtering.

    N+1 optimization: When include_source_info=True, uses MongoDB $lookup
    to fetch crawler and source data in a single aggregation pipeline.
    Code field is excluded from list views for performance.
    """
    if include_source_info:
        # 최적화된 메서드 사용: 크롤러 + 소스 정보를 단일 쿼리로 조회
        # code 필드는 aggregation pipeline에서 이미 제외됨
        crawlers = mongo.list_crawlers_with_source_info(
            source_id=source_id, status=status, skip=skip, limit=limit
        )
    else:
        crawlers = mongo.list_crawlers(source_id=source_id, status=status, skip=skip, limit=limit)
        # Exclude code from list view for performance
        for crawler in crawlers:
            crawler.pop('code', None)

    return crawlers


@router.get(
    "/{crawler_id}",
    response_model=CrawlerResponse,
    summary="Get crawler by ID",
    description="Retrieve complete crawler information including the Python source code.",
    response_description="Full crawler details with code",
    responses={404: ERROR_RESPONSES[404]}
)
async def get_crawler(
    crawler_id: str,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
):
    """
    Get a specific crawler by ID, including its code.

    Returns the complete crawler record including:
    - Metadata (version, status, DAG ID)
    - Full Python source code
    - Creation and update timestamps
    """
    crawler = mongo.get_crawler(crawler_id)
    if not crawler:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Crawler not found"
        )
    return crawler


@router.get(
    "/{crawler_id}/code",
    summary="Get crawler code only",
    description="""
Retrieve only the Python source code for a crawler.

This is a lightweight endpoint when you only need the code,
not the full crawler metadata.
""",
    response_description="Crawler code with version",
    responses={
        200: {
            "description": "Crawler code retrieved",
            "content": {
                "application/json": {
                    "example": {
                        "crawler_id": "507f1f77bcf86cd799439012",
                        "version": 3,
                        "code": "# Auto-generated crawler\\nimport requests\\n..."
                    }
                }
            }
        },
        404: ERROR_RESPONSES[404]
    }
)
async def get_crawler_code(
    crawler_id: str,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
):
    """
    Get just the crawler code.

    Returns a minimal response with only the code and version,
    useful for code editors and diff tools.
    """
    crawler = mongo.get_crawler(crawler_id)
    if not crawler:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Crawler not found"
        )

    return {
        "crawler_id": crawler_id,
        "version": crawler.get("version"),
        "code": crawler.get("code")
    }


@router.get(
    "/{crawler_id}/history",
    response_model=List[CrawlerHistoryResponse],
    summary="Get crawler version history",
    description="""
Retrieve the complete version history for a crawler.

Each version record includes:
- Full source code at that version
- Change reason and details
- Timestamp and author

Useful for auditing changes and finding versions to rollback to.
""",
    response_description="List of version history records",
    responses={404: ERROR_RESPONSES[404]}
)
async def get_crawler_history(
    crawler_id: str,
    skip: int = Query(
        0,
        ge=0,
        description="Number of records to skip"
    ),
    limit: int = Query(
        50,
        ge=1,
        le=100,
        description="Maximum number of history records (max 100)"
    ),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
):
    """
    Get version history for a crawler.

    Returns all historical versions ordered by version number (descending).
    Each record contains the complete code at that point in time.
    """
    crawler = mongo.get_crawler(crawler_id)
    if not crawler:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Crawler not found"
        )

    history = mongo.get_crawler_history(crawler_id, skip=skip, limit=limit)
    return history


@router.get("/{crawler_id}/history/{version}")
async def get_crawler_version(
    crawler_id: str,
    version: int,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
):
    """
    Get a specific version of crawler code.

    N+1 최적화: 전체 히스토리를 조회 후 반복하는 대신
    MongoDB에서 직접 특정 버전을 조회하여 O(n) -> O(1) 개선
    """
    crawler = mongo.get_crawler(crawler_id)
    if not crawler:
        raise HTTPException(status_code=404, detail="Crawler not found")

    # 최적화: 직접 버전 조회 (반복 조회 대신 인덱스 활용)
    history_version = mongo.get_crawler_version(crawler_id, version)
    if not history_version:
        raise HTTPException(status_code=404, detail=f"Version {version} not found")

    return history_version


@router.post("/{crawler_id}/rollback/{version}")
async def rollback_crawler(
    crawler_id: str,
    version: int,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write")),
):
    """
    Rollback crawler to a previous version.

    N+1 최적화: 전체 히스토리를 조회 후 반복하는 대신
    MongoDB에서 직접 특정 버전을 조회하여 O(n) -> O(1) 개선
    """
    crawler = mongo.get_crawler(crawler_id)
    if not crawler:
        raise HTTPException(status_code=404, detail="Crawler not found")

    # 최적화: 직접 버전 조회 (반복 조회 대신 인덱스 활용)
    target = mongo.get_crawler_version(crawler_id, version)
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

"""
Production Data API Router.

PostgreSQL 기반 프로덕션 데이터 조회 + 공통코드 관리 API.
승인된 데이터만 PostgreSQL에 저장되며, 이 라우터로 조회/통계를 제공한다.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ..services.postgres_service import get_pg, PostgresService
from ..services.pg_schema import DOMAIN_TABLE_MAP

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# Health / Status
# ============================================================

@router.get("/status")
async def pg_status(pg: PostgresService = Depends(get_pg)):
    """Check PostgreSQL connection status."""
    return {
        "available": pg.is_available,
        "dsn_host": pg.dsn.split("@")[-1].split("/")[0] if pg.is_available else None,
    }


# ============================================================
# 공통코드 조회
# ============================================================

@router.get("/common-codes")
async def list_code_groups(pg: PostgresService = Depends(get_pg)):
    """Get all common code groups with counts."""
    if not pg.is_available:
        raise HTTPException(503, "PostgreSQL is not available")

    groups = await pg.get_all_code_groups()
    return {"groups": groups}


@router.get("/common-codes/{group_code}")
async def get_codes(group_code: str, pg: PostgresService = Depends(get_pg)):
    """Get all codes in a specific group."""
    if not pg.is_available:
        raise HTTPException(503, "PostgreSQL is not available")

    codes = await pg.get_common_codes(group_code.upper())
    if not codes:
        raise HTTPException(404, f"Code group '{group_code}' not found or empty")
    return {"group_code": group_code.upper(), "codes": codes}


# ============================================================
# 도메인 데이터 조회
# ============================================================

@router.get("/domains")
async def list_domains(pg: PostgresService = Depends(get_pg)):
    """Get available domain tables and their row counts."""
    if not pg.is_available:
        raise HTTPException(503, "PostgreSQL is not available")

    stats = await pg.get_domain_stats()
    return {"domains": stats}


@router.get("/domains/{category}")
async def query_domain(
    category: str,
    source_id: Optional[int] = Query(None, description="Filter by PG source_id"),
    search: Optional[str] = Query(None, description="Full-text search"),
    order_by: str = Query("promoted_at DESC", description="ORDER BY clause"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    pg: PostgresService = Depends(get_pg),
):
    """
    Query production data for a specific domain.

    Categories: NEWS, FINANCE, ANNOUNCEMENT, GENERIC
    """
    if not pg.is_available:
        raise HTTPException(503, "PostgreSQL is not available")

    table_name = DOMAIN_TABLE_MAP.get(category.upper())
    if not table_name:
        raise HTTPException(400, f"Unknown category: {category}. Valid: {list(DOMAIN_TABLE_MAP.keys())}")

    filters = {}
    if source_id is not None:
        filters["source_id"] = source_id

    offset = (page - 1) * page_size

    rows, total = await pg.query_domain_data(
        table_name=table_name,
        filters=filters,
        search=search,
        order_by=order_by,
        limit=page_size,
        offset=offset,
    )

    # Serialize datetime/date objects
    serialized = []
    for row in rows:
        item = {}
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                item[k] = v.isoformat()
            else:
                item[k] = v
        serialized.append(item)

    total_pages = (total + page_size - 1) // page_size if total > 0 else 0

    return {
        "category": category.upper(),
        "table_name": table_name,
        "items": serialized,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


# ============================================================
# 프로모션 이력
# ============================================================

@router.get("/promotions")
async def promotion_history(
    source_id: Optional[int] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    pg: PostgresService = Depends(get_pg),
):
    """Get recent promotion log entries."""
    if not pg.is_available:
        raise HTTPException(503, "PostgreSQL is not available")

    rows = await pg.get_promotion_history(source_id=source_id, limit=limit)

    serialized = []
    for row in rows:
        item = {}
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                item[k] = v.isoformat()
            else:
                item[k] = v
        serialized.append(item)

    return {"promotions": serialized, "count": len(serialized)}


# ============================================================
# 소스 마스터 조회
# ============================================================

@router.get("/sources")
async def list_pg_sources(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    pg: PostgresService = Depends(get_pg),
):
    """List sources registered in PostgreSQL."""
    if not pg.is_available:
        raise HTTPException(503, "PostgreSQL is not available")

    offset = (page - 1) * page_size

    rows, total = await pg.query_domain_data(
        table_name="tb_source_master",
        order_by="id ASC",
        limit=page_size,
        offset=offset,
    )

    serialized = []
    for row in rows:
        item = {}
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                item[k] = v.isoformat()
            else:
                item[k] = v
        serialized.append(item)

    return {
        "items": serialized,
        "total": total,
        "page": page,
        "page_size": page_size,
    }

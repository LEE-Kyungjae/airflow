"""
Data Catalog Router - 데이터 카탈로그 API 엔드포인트

데이터셋 메타데이터 관리, 검색, 리니지 조회를 위한 REST API 제공
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.auth.dependencies import require_auth, require_scope, require_admin, AuthContext
from app.services.mongo_service import MongoService
from app.services.data_catalog import (
    DataCatalog,
    CatalogSearch,
    DataLineageService,
    DatasetType,
    DatasetStatus,
    SearchFilter,
    SortOption,
)
from app.core import get_logger

logger = get_logger(__name__)

router = APIRouter()


# ==================== Dependency ====================

def get_mongo():
    """MongoDB 서비스 의존성"""
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


# ==================== Pydantic Models ====================

class DatasetCreateRequest(BaseModel):
    """데이터셋 생성 요청"""
    name: str = Field(..., description="데이터셋 이름")
    dataset_type: DatasetType = Field(..., description="데이터셋 유형")
    description: str = Field(default="", description="설명")
    domain: str = Field(default="", description="비즈니스 도메인")
    tags: List[str] = Field(default_factory=list, description="태그 목록")
    collection_name: Optional[str] = Field(None, description="MongoDB 컬렉션 이름")


class DatasetUpdateRequest(BaseModel):
    """데이터셋 수정 요청"""
    display_name: Optional[str] = None
    description: Optional[str] = None
    domain: Optional[str] = None
    status: Optional[DatasetStatus] = None


class DatasetListResponse(BaseModel):
    """데이터셋 목록 응답"""
    datasets: List[Dict[str, Any]]
    total: int
    page: int
    page_size: int


class SearchRequest(BaseModel):
    """검색 요청"""
    query: str = Field(..., description="검색 쿼리")
    dataset_types: List[DatasetType] = Field(default_factory=list)
    domains: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    min_quality_score: Optional[float] = None


# ==================== Endpoints ====================

@router.get("/", response_model=DatasetListResponse)
async def list_datasets(
    dataset_type: Optional[DatasetType] = Query(None, description="데이터셋 유형 필터"),
    status: Optional[DatasetStatus] = Query(None, description="상태 필터"),
    domain: Optional[str] = Query(None, description="도메인 필터"),
    tags: Optional[List[str]] = Query(None, description="태그 필터"),
    search: Optional[str] = Query(None, description="검색어"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    page_size: int = Query(50, ge=1, le=100, description="페이지 크기"),
    auth: AuthContext = Depends(require_auth),
    mongo: MongoService = Depends(get_mongo),
):
    """
    데이터셋 목록 조회

    필터와 검색어를 사용하여 데이터셋 목록을 조회합니다.
    """
    try:
        catalog = DataCatalog(mongo_service=mongo)

        skip = (page - 1) * page_size
        datasets, total = catalog.list_datasets(
            dataset_type=dataset_type,
            status=status,
            domain=domain,
            tags=tags,
            search_query=search,
            skip=skip,
            limit=page_size,
        )

        return DatasetListResponse(
            datasets=[ds.to_dict() for ds in datasets],
            total=total,
            page=page,
            page_size=page_size,
        )
    except Exception as e:
        logger.error(f"Failed to list datasets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", status_code=201)
async def create_dataset(
    request: DatasetCreateRequest,
    auth: AuthContext = Depends(require_scope("write")),
    mongo: MongoService = Depends(get_mongo),
):
    """
    새 데이터셋 생성

    데이터셋 메타데이터를 생성합니다.
    """
    try:
        catalog = DataCatalog(mongo_service=mongo)

        dataset = catalog.create_dataset(
            name=request.name,
            dataset_type=request.dataset_type,
            description=request.description,
            domain=request.domain,
            tags=request.tags,
            collection_name=request.collection_name,
            created_by=auth.user_id or "system",
        )

        logger.info(f"Created dataset: {dataset.id} by {auth.user_id}")
        return dataset.to_dict()

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_datasets(
    query: str = Query(..., min_length=1, description="검색 쿼리"),
    dataset_types: Optional[List[DatasetType]] = Query(None, description="데이터셋 유형 필터"),
    domains: Optional[List[str]] = Query(None, description="도메인 필터"),
    tags: Optional[List[str]] = Query(None, description="태그 필터"),
    min_quality_score: Optional[float] = Query(None, ge=0, le=100, description="최소 품질 점수"),
    limit: int = Query(20, ge=1, le=100, description="최대 결과 수"),
    auth: AuthContext = Depends(require_auth),
    mongo: MongoService = Depends(get_mongo),
):
    """
    데이터셋 검색

    전문 검색으로 데이터셋을 찾습니다.
    """
    try:
        search = CatalogSearch(mongo_service=mongo)

        search_filter = SearchFilter(
            dataset_types=dataset_types or [],
            domains=domains or [],
            tags=tags or [],
            min_quality_score=min_quality_score,
        )

        result = search.search(
            query=query,
            filters=search_filter,
            limit=limit,
        )

        return {
            "datasets": result.datasets,
            "total_results": result.total_results,
            "facets": result.facets,
            "query": query,
        }

    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_statistics(
    auth: AuthContext = Depends(require_auth),
    mongo: MongoService = Depends(get_mongo),
):
    """
    카탈로그 통계 조회

    데이터셋, 컬럼, 태그 등의 통계를 조회합니다.
    """
    try:
        catalog = DataCatalog(mongo_service=mongo)
        stats = catalog.get_statistics()
        return stats.to_dict()

    except Exception as e:
        logger.error(f"Failed to get statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/register-collections")
async def register_existing_collections(
    auth: AuthContext = Depends(require_admin),
    mongo: MongoService = Depends(get_mongo),
):
    """
    기존 MongoDB 컬렉션 자동 등록

    시스템 컬렉션과 staging_ 프리픽스 컬렉션을 자동으로 카탈로그에 등록합니다.
    관리자 권한 필요.
    """
    try:
        catalog = DataCatalog(mongo_service=mongo)
        registered = catalog.register_existing_collections()

        logger.info(f"Auto-registered {len(registered)} collections by {auth.user_id}")

        return {
            "registered_count": len(registered),
            "datasets": [ds.to_dict() for ds in registered],
        }

    except Exception as e:
        logger.error(f"Auto-registration failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{dataset_id}")
async def get_dataset(
    dataset_id: str,
    auth: AuthContext = Depends(require_auth),
    mongo: MongoService = Depends(get_mongo),
):
    """
    데이터셋 상세 조회

    ID로 데이터셋 메타데이터를 조회합니다.
    """
    try:
        catalog = DataCatalog(mongo_service=mongo)
        dataset = catalog.get_dataset(dataset_id)

        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")

        catalog.record_access(dataset_id)
        return dataset.to_dict()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get dataset {dataset_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{dataset_id}")
async def update_dataset(
    dataset_id: str,
    request: DatasetUpdateRequest,
    auth: AuthContext = Depends(require_scope("write")),
    mongo: MongoService = Depends(get_mongo),
):
    """
    데이터셋 수정

    데이터셋 메타데이터를 수정합니다.
    """
    try:
        catalog = DataCatalog(mongo_service=mongo)

        updates = request.dict(exclude_unset=True)
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        if "status" in updates:
            updates["status"] = updates["status"].value

        success = catalog.update_dataset(
            dataset_id=dataset_id,
            updates=updates,
            updated_by=auth.user_id or "system",
        )

        if not success:
            raise HTTPException(status_code=404, detail="Dataset not found")

        logger.info(f"Updated dataset {dataset_id} by {auth.user_id}")

        updated_dataset = catalog.get_dataset(dataset_id)
        return updated_dataset.to_dict() if updated_dataset else {"success": True}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update dataset {dataset_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{dataset_id}")
async def delete_dataset(
    dataset_id: str,
    auth: AuthContext = Depends(require_scope("write")),
    mongo: MongoService = Depends(get_mongo),
):
    """
    데이터셋 삭제

    데이터셋 메타데이터와 관련 컬럼 정보를 삭제합니다.
    """
    try:
        catalog = DataCatalog(mongo_service=mongo)

        success = catalog.delete_dataset(dataset_id)

        if not success:
            raise HTTPException(status_code=404, detail="Dataset not found")

        logger.info(f"Deleted dataset {dataset_id} by {auth.user_id}")

        return {"success": True, "dataset_id": dataset_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete dataset {dataset_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

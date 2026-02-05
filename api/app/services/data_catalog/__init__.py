"""
Data Catalog Service - 데이터 카탈로그 및 메타데이터 관리

데이터 발견, 이해, 신뢰를 위한 중앙 집중식 메타데이터 관리 시스템.

주요 기능:
1. 데이터셋 메타데이터 관리 (설명, 소유자, 태그)
2. 데이터 리니지 시각화 정보
3. 컬럼 레벨 문서화
4. 데이터 품질 메트릭 연동
5. 검색 및 발견 기능

사용 예시:
    from api.app.services.data_catalog import DataCatalog, CatalogSearch, DataLineageService
    from api.app.services.mongo_service import MongoService

    # 카탈로그 초기화
    mongo = MongoService()
    catalog = DataCatalog(mongo)
    search = CatalogSearch(mongo)
    lineage = DataLineageService(mongo, catalog)

    # 기존 컬렉션 자동 등록
    registered = catalog.register_existing_collections()

    # 데이터셋 생성
    dataset = catalog.create_dataset(
        name="my_dataset",
        dataset_type=DatasetType.STAGING,
        description="My staging dataset",
        domain="analytics",
        tags=["staging", "daily"],
    )

    # 검색
    results = search.search("news", filters=SearchFilter(
        dataset_types=[DatasetType.FINAL],
        domains=["news"],
    ))

    # 리니지 그래프 조회
    graph = lineage.build_lineage_graph(
        root_dataset_id=dataset.id,
        direction="both",
    )

    # 영향도 분석
    impact = lineage.analyze_impact(dataset.id)

MongoDB 컬렉션:
- data_catalog: 데이터셋 메타데이터
- data_columns: 컬럼 정보
- data_tags: 태그 관리
- data_lineage: 리니지 엣지
- column_lineage: 컬럼 레벨 리니지
"""

# Models
from .models import (
    # Enums
    DatasetType,
    DatasetStatus,
    ColumnType,
    SensitivityLevel,
    TagCategory,
    # Data Classes
    Tag,
    QualityMetrics,
    ColumnStatistics,
    Column,
    DatasetOwner,
    LineageNode,
    Dataset,
    CatalogStatistics,
)

# Catalog Service
from .catalog import DataCatalog

# Lineage Service
from .lineage import (
    # Enums
    RelationshipType,
    NodeType,
    # Data Classes
    LineageEdge,
    LineageGraphNode,
    LineageGraphEdge,
    LineageGraph,
    ColumnLineage,
    ImpactAnalysis,
    # Service
    DataLineageService,
)

# Search Service
from .search import (
    # Enums
    SortOption,
    # Data Classes
    SearchFilter,
    SearchFacet,
    SearchResult,
    ColumnSearchResult,
    Suggestion,
    # Service
    CatalogSearch,
)

__all__ = [
    # === Models ===
    # Enums
    "DatasetType",
    "DatasetStatus",
    "ColumnType",
    "SensitivityLevel",
    "TagCategory",
    # Data Classes
    "Tag",
    "QualityMetrics",
    "ColumnStatistics",
    "Column",
    "DatasetOwner",
    "LineageNode",
    "Dataset",
    "CatalogStatistics",

    # === Catalog ===
    "DataCatalog",

    # === Lineage ===
    # Enums
    "RelationshipType",
    "NodeType",
    # Data Classes
    "LineageEdge",
    "LineageGraphNode",
    "LineageGraphEdge",
    "LineageGraph",
    "ColumnLineage",
    "ImpactAnalysis",
    # Service
    "DataLineageService",

    # === Search ===
    # Enums
    "SortOption",
    # Data Classes
    "SearchFilter",
    "SearchFacet",
    "SearchResult",
    "ColumnSearchResult",
    "Suggestion",
    # Service
    "CatalogSearch",
]


# Version
__version__ = "1.0.0"

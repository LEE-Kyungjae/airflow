"""
Data Catalog Search - 메타데이터 검색 및 발견

주요 기능:
1. 전문 검색 (Full-text search)
2. 패싯 검색 (Faceted search)
3. 유사 데이터셋 추천
4. 인기/최근 데이터셋
5. 검색 자동완성
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import Counter
from bson import ObjectId

from .models import (
    Dataset,
    DatasetType,
    DatasetStatus,
    Column,
    Tag,
    TagCategory,
    SensitivityLevel,
)

logger = logging.getLogger(__name__)


class SortOption(str, Enum):
    """정렬 옵션"""
    RELEVANCE = "relevance"
    NAME_ASC = "name_asc"
    NAME_DESC = "name_desc"
    CREATED_ASC = "created_asc"
    CREATED_DESC = "created_desc"
    UPDATED_DESC = "updated_desc"
    QUALITY_DESC = "quality_desc"
    POPULARITY = "popularity"
    RECORD_COUNT = "record_count"


@dataclass
class SearchFilter:
    """검색 필터"""
    dataset_types: List[DatasetType] = field(default_factory=list)
    statuses: List[DatasetStatus] = field(default_factory=list)
    domains: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    owners: List[str] = field(default_factory=list)
    sensitivity_levels: List[SensitivityLevel] = field(default_factory=list)
    min_quality_score: Optional[float] = None
    max_quality_score: Optional[float] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    updated_after: Optional[datetime] = None
    has_documentation: Optional[bool] = None

    def to_mongo_query(self) -> Dict[str, Any]:
        """MongoDB 쿼리로 변환"""
        query = {}

        if self.dataset_types:
            query["dataset_type"] = {"$in": [t.value for t in self.dataset_types]}

        if self.statuses:
            query["status"] = {"$in": [s.value for s in self.statuses]}

        if self.domains:
            query["domain"] = {"$in": self.domains}

        if self.tags:
            query["tags"] = {"$all": self.tags}

        if self.owners:
            query["owners.user_id"] = {"$in": self.owners}

        if self.sensitivity_levels:
            query["sensitivity"] = {"$in": [s.value for s in self.sensitivity_levels]}

        if self.min_quality_score is not None:
            query.setdefault("quality_metrics.overall_score", {})["$gte"] = self.min_quality_score

        if self.max_quality_score is not None:
            query.setdefault("quality_metrics.overall_score", {})["$lte"] = self.max_quality_score

        if self.created_after:
            query.setdefault("created_at", {})["$gte"] = self.created_after

        if self.created_before:
            query.setdefault("created_at", {})["$lte"] = self.created_before

        if self.updated_after:
            query.setdefault("updated_at", {})["$gte"] = self.updated_after

        if self.has_documentation is not None:
            if self.has_documentation:
                query["$or"] = [
                    {"description": {"$ne": ""}},
                    {"columns.description": {"$ne": ""}},
                ]
            else:
                query["description"] = ""

        return query


@dataclass
class SearchFacet:
    """검색 패싯"""
    name: str
    field: str
    values: List[Dict[str, Any]] = field(default_factory=list)  # [{value, count}]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "field": self.field,
            "values": self.values,
        }


@dataclass
class SearchResult:
    """검색 결과"""
    datasets: List[Dataset]
    total: int
    page: int
    page_size: int
    query: str
    facets: List[SearchFacet] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)
    search_time_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "datasets": [d.to_dict() for d in self.datasets],
            "total": self.total,
            "page": self.page,
            "page_size": self.page_size,
            "total_pages": (self.total + self.page_size - 1) // self.page_size,
            "query": self.query,
            "facets": [f.to_dict() for f in self.facets],
            "suggestions": self.suggestions,
            "search_time_ms": round(self.search_time_ms, 2),
        }


@dataclass
class ColumnSearchResult:
    """컬럼 검색 결과"""
    dataset_id: str
    dataset_name: str
    column: Column
    match_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "dataset_name": self.dataset_name,
            "column": self.column.to_dict(),
            "match_score": self.match_score,
        }


@dataclass
class Suggestion:
    """검색 제안"""
    text: str
    suggestion_type: str  # "dataset", "column", "tag", "domain"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "text": self.text,
            "type": self.suggestion_type,
            "metadata": self.metadata,
        }


class CatalogSearch:
    """데이터 카탈로그 검색 서비스"""

    # 검색 가중치
    WEIGHT_NAME = 3.0
    WEIGHT_DISPLAY_NAME = 2.5
    WEIGHT_DESCRIPTION = 1.5
    WEIGHT_COLUMN_NAME = 1.0
    WEIGHT_TAG = 0.8
    WEIGHT_DOMAIN = 0.5

    def __init__(self, mongo_service=None):
        """
        초기화

        Args:
            mongo_service: MongoService 인스턴스
        """
        self.mongo = mongo_service
        self._search_history: List[Dict] = []

    def _get_catalog_collection(self):
        """카탈로그 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_catalog
        return None

    def _get_columns_collection(self):
        """컬럼 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_columns
        return None

    def _get_tags_collection(self):
        """태그 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_tags
        return None

    def _serialize_id(self, doc: Dict) -> Dict:
        """ObjectId를 문자열로 변환"""
        if doc and "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    # ==================== Main Search ====================

    def search(
        self,
        query: str,
        filters: SearchFilter = None,
        sort_by: SortOption = SortOption.RELEVANCE,
        page: int = 1,
        page_size: int = 20,
        include_facets: bool = True,
    ) -> SearchResult:
        """
        데이터셋 검색

        Args:
            query: 검색어
            filters: 검색 필터
            sort_by: 정렬 옵션
            page: 페이지 번호
            page_size: 페이지 크기
            include_facets: 패싯 포함 여부

        Returns:
            SearchResult
        """
        start_time = datetime.utcnow()
        collection = self._get_catalog_collection()

        if not collection:
            return SearchResult(
                datasets=[],
                total=0,
                page=page,
                page_size=page_size,
                query=query,
            )

        # MongoDB 쿼리 구성
        mongo_query = {}

        if filters:
            mongo_query.update(filters.to_mongo_query())

        # 텍스트 검색
        if query:
            # 정규식 기반 검색 (텍스트 인덱스 대신)
            search_regex = {"$regex": query, "$options": "i"}
            text_conditions = [
                {"name": search_regex},
                {"display_name": search_regex},
                {"description": search_regex},
                {"domain": search_regex},
                {"tags": search_regex},
                {"columns.name": search_regex},
                {"columns.description": search_regex},
                {"columns.business_name": search_regex},
            ]

            if "$or" in mongo_query:
                # 기존 $or과 결합
                mongo_query = {
                    "$and": [
                        {"$or": mongo_query.pop("$or")},
                        {"$or": text_conditions},
                    ]
                }
            else:
                mongo_query["$or"] = text_conditions

        # 정렬
        sort_spec = self._get_sort_spec(sort_by, query)

        # 페이지네이션
        skip = (page - 1) * page_size

        # 쿼리 실행
        total = collection.count_documents(mongo_query)
        cursor = collection.find(mongo_query).sort(sort_spec).skip(skip).limit(page_size)

        datasets = [Dataset.from_dict(self._serialize_id(doc)) for doc in cursor]

        # 관련도 점수 계산 및 정렬 (RELEVANCE인 경우)
        if sort_by == SortOption.RELEVANCE and query:
            datasets = self._sort_by_relevance(datasets, query)

        # 패싯 생성
        facets = []
        if include_facets:
            facets = self._build_facets(mongo_query if not query else {})

        # 제안어 생성
        suggestions = []
        if query and total == 0:
            suggestions = self._get_suggestions(query)

        # 검색 기록
        self._record_search(query, total)

        end_time = datetime.utcnow()
        search_time = (end_time - start_time).total_seconds() * 1000

        return SearchResult(
            datasets=datasets,
            total=total,
            page=page,
            page_size=page_size,
            query=query,
            facets=facets,
            suggestions=suggestions,
            search_time_ms=search_time,
        )

    def _get_sort_spec(self, sort_by: SortOption, query: str = None) -> List[Tuple[str, int]]:
        """정렬 스펙 생성"""
        sort_mapping = {
            SortOption.NAME_ASC: [("name", 1)],
            SortOption.NAME_DESC: [("name", -1)],
            SortOption.CREATED_ASC: [("created_at", 1)],
            SortOption.CREATED_DESC: [("created_at", -1)],
            SortOption.UPDATED_DESC: [("updated_at", -1)],
            SortOption.QUALITY_DESC: [("quality_metrics.overall_score", -1)],
            SortOption.POPULARITY: [("access_count", -1)],
            SortOption.RECORD_COUNT: [("record_count", -1)],
            SortOption.RELEVANCE: [("created_at", -1)],  # 기본값, 후처리에서 재정렬
        }
        return sort_mapping.get(sort_by, [("created_at", -1)])

    def _sort_by_relevance(self, datasets: List[Dataset], query: str) -> List[Dataset]:
        """관련도 기반 정렬"""
        query_lower = query.lower()
        query_terms = set(query_lower.split())

        def calculate_score(dataset: Dataset) -> float:
            score = 0.0

            # 이름 매칭
            if query_lower in dataset.name.lower():
                score += self.WEIGHT_NAME * (2.0 if dataset.name.lower() == query_lower else 1.0)

            # 표시 이름 매칭
            if dataset.display_name and query_lower in dataset.display_name.lower():
                score += self.WEIGHT_DISPLAY_NAME

            # 설명 매칭
            if dataset.description and query_lower in dataset.description.lower():
                score += self.WEIGHT_DESCRIPTION

            # 도메인 매칭
            if dataset.domain and query_lower in dataset.domain.lower():
                score += self.WEIGHT_DOMAIN

            # 태그 매칭
            for tag in dataset.tags:
                if query_lower in tag.lower():
                    score += self.WEIGHT_TAG

            # 컬럼 매칭
            for col in dataset.columns:
                if query_lower in col.name.lower():
                    score += self.WEIGHT_COLUMN_NAME
                if col.description and query_lower in col.description.lower():
                    score += self.WEIGHT_COLUMN_NAME * 0.5

            # 개별 단어 매칭 보너스
            all_text = f"{dataset.name} {dataset.display_name} {dataset.description}".lower()
            matched_terms = sum(1 for term in query_terms if term in all_text)
            score += matched_terms * 0.3

            return score

        scored_datasets = [(dataset, calculate_score(dataset)) for dataset in datasets]
        scored_datasets.sort(key=lambda x: x[1], reverse=True)

        return [d[0] for d in scored_datasets]

    def _build_facets(self, base_query: Dict) -> List[SearchFacet]:
        """검색 패싯 생성"""
        collection = self._get_catalog_collection()
        if not collection:
            return []

        facets = []

        # 데이터셋 타입 패싯
        type_pipeline = [
            {"$match": base_query} if base_query else {"$match": {}},
            {"$group": {"_id": "$dataset_type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        type_results = list(collection.aggregate(type_pipeline))
        facets.append(SearchFacet(
            name="데이터셋 유형",
            field="dataset_type",
            values=[{"value": r["_id"], "count": r["count"]} for r in type_results if r["_id"]],
        ))

        # 도메인 패싯
        domain_pipeline = [
            {"$match": base_query} if base_query else {"$match": {}},
            {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": ""}}},
            {"$sort": {"count": -1}},
            {"$limit": 10},
        ]
        domain_results = list(collection.aggregate(domain_pipeline))
        facets.append(SearchFacet(
            name="도메인",
            field="domain",
            values=[{"value": r["_id"], "count": r["count"]} for r in domain_results],
        ))

        # 상태 패싯
        status_pipeline = [
            {"$match": base_query} if base_query else {"$match": {}},
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        status_results = list(collection.aggregate(status_pipeline))
        facets.append(SearchFacet(
            name="상태",
            field="status",
            values=[{"value": r["_id"], "count": r["count"]} for r in status_results if r["_id"]],
        ))

        # 태그 패싯
        tag_pipeline = [
            {"$match": base_query} if base_query else {"$match": {}},
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 15},
        ]
        tag_results = list(collection.aggregate(tag_pipeline))
        facets.append(SearchFacet(
            name="태그",
            field="tags",
            values=[{"value": r["_id"], "count": r["count"]} for r in tag_results],
        ))

        # 품질 점수 범위 패싯
        quality_pipeline = [
            {"$match": base_query} if base_query else {"$match": {}},
            {
                "$bucket": {
                    "groupBy": "$quality_metrics.overall_score",
                    "boundaries": [0, 50, 70, 85, 95, 101],
                    "default": "unknown",
                    "output": {"count": {"$sum": 1}},
                }
            },
        ]
        quality_results = list(collection.aggregate(quality_pipeline))

        quality_labels = {
            0: "0-50 (Low)",
            50: "50-70 (Medium)",
            70: "70-85 (Good)",
            85: "85-95 (High)",
            95: "95-100 (Excellent)",
            "unknown": "Unknown",
        }
        facets.append(SearchFacet(
            name="품질 점수",
            field="quality_score",
            values=[
                {"value": quality_labels.get(r["_id"], str(r["_id"])), "count": r["count"]}
                for r in quality_results
            ],
        ))

        return facets

    def _get_suggestions(self, query: str) -> List[str]:
        """검색어 제안"""
        collection = self._get_catalog_collection()
        if not collection:
            return []

        suggestions = []

        # 유사한 이름 찾기
        pipeline = [
            {"$match": {"name": {"$regex": f".*{query[:3]}.*", "$options": "i"}}},
            {"$group": {"_id": "$name"}},
            {"$limit": 5},
        ]
        name_suggestions = [r["_id"] for r in collection.aggregate(pipeline)]
        suggestions.extend(name_suggestions)

        # 태그에서 찾기
        tags_collection = self._get_tags_collection()
        if tags_collection:
            tag_results = tags_collection.find(
                {"name": {"$regex": f".*{query}.*", "$options": "i"}},
                {"name": 1}
            ).limit(3)
            suggestions.extend([t["name"] for t in tag_results])

        return list(set(suggestions))[:5]

    def _record_search(self, query: str, result_count: int):
        """검색 기록"""
        self._search_history.append({
            "query": query,
            "result_count": result_count,
            "timestamp": datetime.utcnow(),
        })
        # 최근 1000개만 유지
        if len(self._search_history) > 1000:
            self._search_history = self._search_history[-1000:]

    # ==================== Column Search ====================

    def search_columns(
        self,
        query: str,
        dataset_ids: List[str] = None,
        data_types: List[str] = None,
        page: int = 1,
        page_size: int = 50,
    ) -> Tuple[List[ColumnSearchResult], int]:
        """
        컬럼 검색

        Args:
            query: 검색어
            dataset_ids: 데이터셋 ID 필터
            data_types: 데이터 타입 필터
            page: 페이지 번호
            page_size: 페이지 크기

        Returns:
            (컬럼 검색 결과 목록, 전체 개수)
        """
        collection = self._get_columns_collection()
        catalog_collection = self._get_catalog_collection()

        if not collection or not catalog_collection:
            return [], 0

        mongo_query: Dict[str, Any] = {}

        # 텍스트 검색
        if query:
            search_regex = {"$regex": query, "$options": "i"}
            mongo_query["$or"] = [
                {"name": search_regex},
                {"description": search_regex},
                {"business_name": search_regex},
                {"business_definition": search_regex},
            ]

        # 데이터셋 필터
        if dataset_ids:
            mongo_query["dataset_id"] = {"$in": dataset_ids}

        # 데이터 타입 필터
        if data_types:
            mongo_query["data_type"] = {"$in": data_types}

        # 쿼리 실행
        total = collection.count_documents(mongo_query)
        skip = (page - 1) * page_size
        cursor = collection.find(mongo_query).skip(skip).limit(page_size)

        results = []
        dataset_cache: Dict[str, str] = {}

        for doc in cursor:
            dataset_id = doc.get("dataset_id", "")

            # 데이터셋 이름 캐싱
            if dataset_id not in dataset_cache:
                dataset_doc = catalog_collection.find_one(
                    {"_id": ObjectId(dataset_id)},
                    {"name": 1}
                )
                dataset_cache[dataset_id] = dataset_doc["name"] if dataset_doc else "Unknown"

            column = Column.from_dict(doc)

            # 관련도 점수 계산
            score = self._calculate_column_relevance(column, query) if query else 0

            results.append(ColumnSearchResult(
                dataset_id=dataset_id,
                dataset_name=dataset_cache[dataset_id],
                column=column,
                match_score=score,
            ))

        # 관련도 기반 정렬
        if query:
            results.sort(key=lambda x: x.match_score, reverse=True)

        return results, total

    def _calculate_column_relevance(self, column: Column, query: str) -> float:
        """컬럼 관련도 점수 계산"""
        query_lower = query.lower()
        score = 0.0

        if query_lower in column.name.lower():
            score += 3.0 if column.name.lower() == query_lower else 2.0

        if column.business_name and query_lower in column.business_name.lower():
            score += 1.5

        if column.description and query_lower in column.description.lower():
            score += 1.0

        if column.business_definition and query_lower in column.business_definition.lower():
            score += 0.8

        return score

    # ==================== Autocomplete ====================

    def autocomplete(
        self,
        prefix: str,
        limit: int = 10,
    ) -> List[Suggestion]:
        """
        자동완성 제안

        Args:
            prefix: 접두어
            limit: 최대 개수

        Returns:
            제안 목록
        """
        if len(prefix) < 2:
            return []

        suggestions = []
        prefix_lower = prefix.lower()

        catalog_collection = self._get_catalog_collection()
        tags_collection = self._get_tags_collection()
        columns_collection = self._get_columns_collection()

        # 데이터셋 이름
        if catalog_collection:
            dataset_cursor = catalog_collection.find(
                {"name": {"$regex": f"^{prefix}", "$options": "i"}},
                {"name": 1, "display_name": 1}
            ).limit(limit)

            for doc in dataset_cursor:
                suggestions.append(Suggestion(
                    text=doc["name"],
                    suggestion_type="dataset",
                    metadata={"display_name": doc.get("display_name", "")},
                ))

        # 태그
        if tags_collection:
            tag_cursor = tags_collection.find(
                {"name": {"$regex": f"^{prefix}", "$options": "i"}},
                {"name": 1, "category": 1}
            ).limit(limit // 2)

            for doc in tag_cursor:
                suggestions.append(Suggestion(
                    text=doc["name"],
                    suggestion_type="tag",
                    metadata={"category": doc.get("category", "")},
                ))

        # 컬럼 이름
        if columns_collection:
            column_cursor = columns_collection.find(
                {"name": {"$regex": f"^{prefix}", "$options": "i"}},
                {"name": 1, "data_type": 1}
            ).limit(limit // 2)

            seen_columns = set()
            for doc in column_cursor:
                col_name = doc["name"]
                if col_name not in seen_columns:
                    seen_columns.add(col_name)
                    suggestions.append(Suggestion(
                        text=col_name,
                        suggestion_type="column",
                        metadata={"data_type": doc.get("data_type", "")},
                    ))

        # 도메인
        if catalog_collection:
            domain_pipeline = [
                {"$match": {"domain": {"$regex": f"^{prefix}", "$options": "i"}}},
                {"$group": {"_id": "$domain"}},
                {"$limit": limit // 3},
            ]
            domain_results = catalog_collection.aggregate(domain_pipeline)

            for doc in domain_results:
                if doc["_id"]:
                    suggestions.append(Suggestion(
                        text=doc["_id"],
                        suggestion_type="domain",
                    ))

        # 중복 제거 및 정렬
        seen = set()
        unique_suggestions = []
        for s in suggestions:
            key = (s.text.lower(), s.suggestion_type)
            if key not in seen:
                seen.add(key)
                unique_suggestions.append(s)

        # 접두어 정확 매칭 우선
        unique_suggestions.sort(
            key=lambda x: (
                0 if x.text.lower().startswith(prefix_lower) else 1,
                x.text.lower()
            )
        )

        return unique_suggestions[:limit]

    # ==================== Similar Datasets ====================

    def find_similar_datasets(
        self,
        dataset_id: str,
        limit: int = 5,
    ) -> List[Tuple[Dataset, float]]:
        """
        유사 데이터셋 찾기

        Args:
            dataset_id: 기준 데이터셋 ID
            limit: 최대 개수

        Returns:
            (데이터셋, 유사도 점수) 튜플 목록
        """
        collection = self._get_catalog_collection()
        if not collection:
            return []

        # 기준 데이터셋 조회
        try:
            target_doc = collection.find_one({"_id": ObjectId(dataset_id)})
        except:
            return []

        if not target_doc:
            return []

        target = Dataset.from_dict(self._serialize_id(target_doc))

        # 유사성 기준
        # 1. 같은 도메인
        # 2. 공통 태그
        # 3. 컬럼 이름 유사성
        # 4. 같은 데이터셋 타입

        candidates = []
        target_columns = set(target.get_column_names())
        target_tags = set(target.tags)

        # 후보 조회 (같은 도메인 또는 공통 태그)
        query = {
            "_id": {"$ne": ObjectId(dataset_id)},
            "$or": [
                {"domain": target.domain} if target.domain else {"domain": {"$exists": False}},
                {"tags": {"$in": list(target_tags)}} if target_tags else {"tags": {"$exists": False}},
            ],
        }

        cursor = collection.find(query).limit(100)

        for doc in cursor:
            candidate = Dataset.from_dict(self._serialize_id(doc))
            score = self._calculate_similarity(target, candidate, target_columns, target_tags)

            if score > 0:
                candidates.append((candidate, score))

        # 점수 기준 정렬
        candidates.sort(key=lambda x: x[1], reverse=True)

        return candidates[:limit]

    def _calculate_similarity(
        self,
        target: Dataset,
        candidate: Dataset,
        target_columns: Set[str],
        target_tags: Set[str],
    ) -> float:
        """유사도 점수 계산"""
        score = 0.0

        # 도메인 일치 (30점)
        if target.domain and target.domain == candidate.domain:
            score += 30

        # 서브도메인 일치 (10점)
        if target.subdomain and target.subdomain == candidate.subdomain:
            score += 10

        # 같은 데이터셋 타입 (15점)
        if target.dataset_type == candidate.dataset_type:
            score += 15

        # 공통 태그 (태그당 5점, 최대 20점)
        candidate_tags = set(candidate.tags)
        common_tags = target_tags & candidate_tags
        score += min(len(common_tags) * 5, 20)

        # 공통 컬럼 이름 (컬럼당 3점, 최대 25점)
        candidate_columns = set(candidate.get_column_names())
        common_columns = target_columns & candidate_columns
        score += min(len(common_columns) * 3, 25)

        return score

    # ==================== Popular & Recent ====================

    def get_popular_datasets(
        self,
        limit: int = 10,
        days: int = 30,
    ) -> List[Dataset]:
        """
        인기 데이터셋 조회

        Args:
            limit: 최대 개수
            days: 기간 (일)

        Returns:
            데이터셋 목록
        """
        collection = self._get_catalog_collection()
        if not collection:
            return []

        since = datetime.utcnow() - timedelta(days=days)

        cursor = collection.find({
            "status": DatasetStatus.ACTIVE.value,
            "last_accessed_at": {"$gte": since},
        }).sort("access_count", -1).limit(limit)

        return [Dataset.from_dict(self._serialize_id(doc)) for doc in cursor]

    def get_recent_datasets(
        self,
        limit: int = 10,
    ) -> List[Dataset]:
        """
        최근 생성된 데이터셋 조회

        Args:
            limit: 최대 개수

        Returns:
            데이터셋 목록
        """
        collection = self._get_catalog_collection()
        if not collection:
            return []

        cursor = collection.find({}).sort("created_at", -1).limit(limit)

        return [Dataset.from_dict(self._serialize_id(doc)) for doc in cursor]

    def get_recently_updated_datasets(
        self,
        limit: int = 10,
    ) -> List[Dataset]:
        """
        최근 업데이트된 데이터셋 조회

        Args:
            limit: 최대 개수

        Returns:
            데이터셋 목록
        """
        collection = self._get_catalog_collection()
        if not collection:
            return []

        cursor = collection.find({
            "updated_at": {"$ne": None}
        }).sort("updated_at", -1).limit(limit)

        return [Dataset.from_dict(self._serialize_id(doc)) for doc in cursor]

    # ==================== Search Analytics ====================

    def get_popular_searches(
        self,
        limit: int = 10,
        days: int = 7,
    ) -> List[Dict[str, Any]]:
        """
        인기 검색어 조회

        Args:
            limit: 최대 개수
            days: 기간 (일)

        Returns:
            인기 검색어 목록
        """
        since = datetime.utcnow() - timedelta(days=days)

        recent_searches = [
            h["query"] for h in self._search_history
            if h["timestamp"] >= since and h["query"]
        ]

        counter = Counter(recent_searches)
        return [
            {"query": query, "count": count}
            for query, count in counter.most_common(limit)
        ]

    def get_zero_result_searches(
        self,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        검색 결과가 없었던 검색어 조회

        Args:
            limit: 최대 개수

        Returns:
            검색어 목록
        """
        zero_results = [
            h["query"] for h in self._search_history
            if h["result_count"] == 0 and h["query"]
        ]

        counter = Counter(zero_results)
        return [
            {"query": query, "count": count}
            for query, count in counter.most_common(limit)
        ]

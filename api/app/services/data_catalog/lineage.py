"""
Data Lineage - 데이터 흐름 추적 및 시각화

주요 기능:
1. 데이터 리니지 그래프 관리
2. 업스트림/다운스트림 추적
3. 컬럼 레벨 리니지
4. 영향도 분석
5. 시각화용 데이터 제공
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
from bson import ObjectId
from bson.errors import InvalidId

from .models import (
    Dataset,
    LineageNode,
    Column,
)

logger = logging.getLogger(__name__)


class RelationshipType(str, Enum):
    """관계 유형"""
    DERIVES_FROM = "derives_from"       # 파생 관계
    AGGREGATES = "aggregates"           # 집계 관계
    FILTERS = "filters"                 # 필터 관계
    JOINS = "joins"                     # 조인 관계
    TRANSFORMS = "transforms"           # 변환 관계
    COPIES = "copies"                   # 복사 관계


class NodeType(str, Enum):
    """노드 유형"""
    SOURCE = "source"           # 원본 소스
    STAGING = "staging"         # 스테이징
    TRANSFORM = "transform"     # 변환
    AGGREGATION = "aggregation" # 집계
    FINAL = "final"             # 최종


@dataclass
class LineageEdge:
    """리니지 엣지 (관계)"""
    source_id: str              # 소스 데이터셋 ID
    target_id: str              # 타겟 데이터셋 ID
    relationship: RelationshipType
    transformation_logic: str = ""  # 변환 로직 설명
    column_mappings: Dict[str, List[str]] = field(default_factory=dict)  # target_col -> [source_cols]
    job_id: Optional[str] = None    # Airflow DAG ID 등
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = "system"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "relationship": self.relationship.value,
            "transformation_logic": self.transformation_logic,
            "column_mappings": self.column_mappings,
            "job_id": self.job_id,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "created_by": self.created_by,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LineageEdge":
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.utcnow()

        return cls(
            source_id=data["source_id"],
            target_id=data["target_id"],
            relationship=RelationshipType(data.get("relationship", "derives_from")),
            transformation_logic=data.get("transformation_logic", ""),
            column_mappings=data.get("column_mappings", {}),
            job_id=data.get("job_id"),
            created_at=created_at,
            created_by=data.get("created_by", "system"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class LineageGraphNode:
    """리니지 그래프 노드 (시각화용)"""
    id: str
    name: str
    display_name: str
    node_type: NodeType
    domain: str = ""
    quality_score: float = 0.0
    record_count: int = 0
    position: Dict[str, float] = field(default_factory=dict)  # {x, y}
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "display_name": self.display_name,
            "node_type": self.node_type.value,
            "domain": self.domain,
            "quality_score": self.quality_score,
            "record_count": self.record_count,
            "position": self.position,
            "metadata": self.metadata,
        }


@dataclass
class LineageGraphEdge:
    """리니지 그래프 엣지 (시각화용)"""
    source: str
    target: str
    relationship: str
    label: str = ""
    animated: bool = False      # 실시간 데이터 흐름 표시
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "relationship": self.relationship,
            "label": self.label,
            "animated": self.animated,
            "metadata": self.metadata,
        }


@dataclass
class LineageGraph:
    """리니지 그래프 (시각화용 전체 구조)"""
    nodes: List[LineageGraphNode] = field(default_factory=list)
    edges: List[LineageGraphEdge] = field(default_factory=list)
    root_id: Optional[str] = None
    depth: int = 0
    generated_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "root_id": self.root_id,
            "depth": self.depth,
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "generated_at": self.generated_at.isoformat(),
        }


@dataclass
class ColumnLineage:
    """컬럼 레벨 리니지"""
    target_dataset_id: str
    target_column: str
    source_columns: List[Dict[str, str]] = field(default_factory=list)  # [{dataset_id, column}]
    transformation: str = ""
    expression: str = ""  # SQL/Python 표현식

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target_dataset_id": self.target_dataset_id,
            "target_column": self.target_column,
            "source_columns": self.source_columns,
            "transformation": self.transformation,
            "expression": self.expression,
        }


@dataclass
class ImpactAnalysis:
    """영향도 분석 결과"""
    source_dataset_id: str
    affected_datasets: List[Dict[str, Any]] = field(default_factory=list)  # [{id, name, depth, impact_type}]
    affected_columns: List[Dict[str, Any]] = field(default_factory=list)   # [{dataset_id, column, impact}]
    total_affected: int = 0
    max_depth: int = 0
    critical_paths: List[List[str]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_dataset_id": self.source_dataset_id,
            "affected_datasets": self.affected_datasets,
            "affected_columns": self.affected_columns,
            "total_affected": self.total_affected,
            "max_depth": self.max_depth,
            "critical_paths": self.critical_paths,
        }


class DataLineageService:
    """데이터 리니지 서비스"""

    def __init__(self, mongo_service=None, catalog=None):
        """
        초기화

        Args:
            mongo_service: MongoService 인스턴스
            catalog: DataCatalog 인스턴스
        """
        self.mongo = mongo_service
        self.catalog = catalog
        self._edge_cache: Dict[str, List[LineageEdge]] = {}

    def _get_lineage_collection(self):
        """리니지 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_lineage
        return None

    def _get_column_lineage_collection(self):
        """컬럼 리니지 컬렉션"""
        if self.mongo:
            return self.mongo.db.column_lineage
        return None

    # ==================== Edge Management ====================

    def create_edge(
        self,
        source_id: str,
        target_id: str,
        relationship: RelationshipType,
        transformation_logic: str = "",
        column_mappings: Dict[str, List[str]] = None,
        job_id: str = None,
        created_by: str = "system",
    ) -> LineageEdge:
        """
        리니지 엣지 생성

        Args:
            source_id: 소스 데이터셋 ID
            target_id: 타겟 데이터셋 ID
            relationship: 관계 유형
            transformation_logic: 변환 로직 설명
            column_mappings: 컬럼 매핑 (target_col -> [source_cols])
            job_id: 작업 ID
            created_by: 생성자

        Returns:
            생성된 LineageEdge
        """
        collection = self._get_lineage_collection()

        edge = LineageEdge(
            source_id=source_id,
            target_id=target_id,
            relationship=relationship,
            transformation_logic=transformation_logic,
            column_mappings=column_mappings or {},
            job_id=job_id,
            created_at=datetime.utcnow(),
            created_by=created_by,
        )

        if collection:
            # 중복 체크
            existing = collection.find_one({
                "source_id": source_id,
                "target_id": target_id,
            })

            if existing:
                # 업데이트
                collection.update_one(
                    {"_id": existing["_id"]},
                    {"$set": edge.to_dict()}
                )
                logger.info(f"Updated lineage edge: {source_id} -> {target_id}")
            else:
                # 새로 생성
                collection.insert_one(edge.to_dict())
                logger.info(f"Created lineage edge: {source_id} -> {target_id}")

            # 데이터셋 업데이트
            self._update_dataset_lineage(source_id, target_id, relationship.value, transformation_logic)

        # 캐시 무효화
        self._edge_cache.pop(source_id, None)
        self._edge_cache.pop(target_id, None)

        return edge

    def _update_dataset_lineage(
        self,
        source_id: str,
        target_id: str,
        relationship: str,
        transformation: str,
    ):
        """데이터셋의 upstream/downstream 정보 업데이트"""
        if not self.catalog:
            return

        # 소스의 downstream 추가
        source_dataset = self.catalog.get_dataset(source_id)
        target_dataset = self.catalog.get_dataset(target_id)

        if source_dataset:
            downstream_node = LineageNode(
                dataset_id=target_id,
                dataset_name=target_dataset.name if target_dataset else "",
                relationship="downstream",
                transformation=transformation,
            )

            # 기존 downstream에서 중복 제거 후 추가
            existing_downstream = [
                n for n in source_dataset.downstream
                if n.dataset_id != target_id
            ]
            existing_downstream.append(downstream_node)

            self.catalog.update_dataset(source_id, {
                "downstream": [n.to_dict() for n in existing_downstream]
            })

        if target_dataset:
            upstream_node = LineageNode(
                dataset_id=source_id,
                dataset_name=source_dataset.name if source_dataset else "",
                relationship="upstream",
                transformation=transformation,
            )

            # 기존 upstream에서 중복 제거 후 추가
            existing_upstream = [
                n for n in target_dataset.upstream
                if n.dataset_id != source_id
            ]
            existing_upstream.append(upstream_node)

            self.catalog.update_dataset(target_id, {
                "upstream": [n.to_dict() for n in existing_upstream]
            })

    def get_edge(self, source_id: str, target_id: str) -> Optional[LineageEdge]:
        """
        특정 엣지 조회

        Args:
            source_id: 소스 데이터셋 ID
            target_id: 타겟 데이터셋 ID

        Returns:
            LineageEdge 또는 None
        """
        collection = self._get_lineage_collection()
        if not collection:
            return None

        doc = collection.find_one({
            "source_id": source_id,
            "target_id": target_id,
        })

        if doc:
            return LineageEdge.from_dict(doc)

        return None

    def get_outgoing_edges(self, dataset_id: str) -> List[LineageEdge]:
        """
        데이터셋에서 나가는 엣지 목록 (downstream)

        Args:
            dataset_id: 데이터셋 ID

        Returns:
            엣지 목록
        """
        cache_key = f"out_{dataset_id}"
        if cache_key in self._edge_cache:
            return self._edge_cache[cache_key]

        collection = self._get_lineage_collection()
        if not collection:
            return []

        cursor = collection.find({"source_id": dataset_id})
        edges = [LineageEdge.from_dict(doc) for doc in cursor]
        self._edge_cache[cache_key] = edges

        return edges

    def get_incoming_edges(self, dataset_id: str) -> List[LineageEdge]:
        """
        데이터셋으로 들어오는 엣지 목록 (upstream)

        Args:
            dataset_id: 데이터셋 ID

        Returns:
            엣지 목록
        """
        cache_key = f"in_{dataset_id}"
        if cache_key in self._edge_cache:
            return self._edge_cache[cache_key]

        collection = self._get_lineage_collection()
        if not collection:
            return []

        cursor = collection.find({"target_id": dataset_id})
        edges = [LineageEdge.from_dict(doc) for doc in cursor]
        self._edge_cache[cache_key] = edges

        return edges

    def delete_edge(self, source_id: str, target_id: str) -> bool:
        """
        엣지 삭제

        Args:
            source_id: 소스 데이터셋 ID
            target_id: 타겟 데이터셋 ID

        Returns:
            삭제 성공 여부
        """
        collection = self._get_lineage_collection()
        if not collection:
            return False

        result = collection.delete_one({
            "source_id": source_id,
            "target_id": target_id,
        })

        if result.deleted_count > 0:
            # 캐시 무효화
            self._edge_cache.pop(f"out_{source_id}", None)
            self._edge_cache.pop(f"in_{target_id}", None)
            logger.info(f"Deleted lineage edge: {source_id} -> {target_id}")
            return True

        return False

    # ==================== Column Lineage ====================

    def create_column_lineage(
        self,
        target_dataset_id: str,
        target_column: str,
        source_columns: List[Dict[str, str]],
        transformation: str = "",
        expression: str = "",
    ) -> ColumnLineage:
        """
        컬럼 리니지 생성

        Args:
            target_dataset_id: 타겟 데이터셋 ID
            target_column: 타겟 컬럼 이름
            source_columns: 소스 컬럼 목록 [{dataset_id, column}]
            transformation: 변환 설명
            expression: SQL/Python 표현식

        Returns:
            생성된 ColumnLineage
        """
        collection = self._get_column_lineage_collection()

        lineage = ColumnLineage(
            target_dataset_id=target_dataset_id,
            target_column=target_column,
            source_columns=source_columns,
            transformation=transformation,
            expression=expression,
        )

        if collection:
            # Upsert
            collection.update_one(
                {
                    "target_dataset_id": target_dataset_id,
                    "target_column": target_column,
                },
                {"$set": lineage.to_dict()},
                upsert=True,
            )

        return lineage

    def get_column_lineage(
        self,
        dataset_id: str,
        column_name: str = None,
    ) -> List[ColumnLineage]:
        """
        컬럼 리니지 조회

        Args:
            dataset_id: 데이터셋 ID
            column_name: 컬럼 이름 (None이면 모든 컬럼)

        Returns:
            컬럼 리니지 목록
        """
        collection = self._get_column_lineage_collection()
        if not collection:
            return []

        query = {"target_dataset_id": dataset_id}
        if column_name:
            query["target_column"] = column_name

        cursor = collection.find(query)
        return [ColumnLineage(**{k: v for k, v in doc.items() if k != "_id"}) for doc in cursor]

    def trace_column_origin(
        self,
        dataset_id: str,
        column_name: str,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        컬럼 원본 추적 (업스트림으로 거슬러 올라감)

        Args:
            dataset_id: 데이터셋 ID
            column_name: 컬럼 이름
            max_depth: 최대 추적 깊이

        Returns:
            원본 추적 경로 목록
        """
        results = []
        visited = set()

        def trace(ds_id: str, col: str, depth: int, path: List[Dict]):
            if depth > max_depth:
                return
            if (ds_id, col) in visited:
                return

            visited.add((ds_id, col))
            lineages = self.get_column_lineage(ds_id, col)

            if not lineages:
                # 원본 도달
                results.append({
                    "origin_dataset_id": ds_id,
                    "origin_column": col,
                    "path": path,
                    "depth": depth,
                })
                return

            for lineage in lineages:
                for source in lineage.source_columns:
                    new_path = path + [{
                        "dataset_id": ds_id,
                        "column": col,
                        "transformation": lineage.transformation,
                    }]
                    trace(
                        source["dataset_id"],
                        source["column"],
                        depth + 1,
                        new_path,
                    )

        trace(dataset_id, column_name, 0, [])
        return results

    # ==================== Graph Building ====================

    def build_lineage_graph(
        self,
        root_dataset_id: str,
        direction: str = "both",
        max_depth: int = 5,
    ) -> LineageGraph:
        """
        리니지 그래프 생성

        Args:
            root_dataset_id: 루트 데이터셋 ID
            direction: 방향 ("upstream", "downstream", "both")
            max_depth: 최대 깊이

        Returns:
            LineageGraph
        """
        nodes_map: Dict[str, LineageGraphNode] = {}
        edges_list: List[LineageGraphEdge] = []
        visited: Set[str] = set()
        actual_depth = 0

        def add_node(dataset_id: str, depth: int) -> Optional[LineageGraphNode]:
            if dataset_id in nodes_map:
                return nodes_map[dataset_id]

            dataset = self.catalog.get_dataset(dataset_id) if self.catalog else None
            if not dataset:
                return None

            # 노드 타입 결정
            node_type = self._determine_node_type(dataset)

            node = LineageGraphNode(
                id=dataset.id,
                name=dataset.name,
                display_name=dataset.display_name or dataset.name,
                node_type=node_type,
                domain=dataset.domain,
                quality_score=dataset.quality_metrics.overall_score if dataset.quality_metrics else 0,
                record_count=dataset.record_count,
                position={"x": depth * 200, "y": len(nodes_map) * 100},
            )

            nodes_map[dataset_id] = node
            return node

        def traverse(dataset_id: str, depth: int, is_upstream: bool):
            nonlocal actual_depth

            if depth > max_depth:
                return
            if dataset_id in visited:
                return

            visited.add(dataset_id)
            actual_depth = max(actual_depth, depth)

            # 현재 노드 추가
            add_node(dataset_id, depth if not is_upstream else -depth)

            # 관련 엣지 조회
            if is_upstream:
                edges = self.get_incoming_edges(dataset_id)
                for edge in edges:
                    source_node = add_node(edge.source_id, -(depth + 1))
                    if source_node:
                        edges_list.append(LineageGraphEdge(
                            source=edge.source_id,
                            target=dataset_id,
                            relationship=edge.relationship.value,
                            label=edge.transformation_logic[:50] if edge.transformation_logic else "",
                        ))
                        traverse(edge.source_id, depth + 1, True)
            else:
                edges = self.get_outgoing_edges(dataset_id)
                for edge in edges:
                    target_node = add_node(edge.target_id, depth + 1)
                    if target_node:
                        edges_list.append(LineageGraphEdge(
                            source=dataset_id,
                            target=edge.target_id,
                            relationship=edge.relationship.value,
                            label=edge.transformation_logic[:50] if edge.transformation_logic else "",
                        ))
                        traverse(edge.target_id, depth + 1, False)

        # 루트 노드 추가
        add_node(root_dataset_id, 0)

        # 탐색
        if direction in ("upstream", "both"):
            traverse(root_dataset_id, 0, True)

        visited.clear()

        if direction in ("downstream", "both"):
            traverse(root_dataset_id, 0, False)

        return LineageGraph(
            nodes=list(nodes_map.values()),
            edges=edges_list,
            root_id=root_dataset_id,
            depth=actual_depth,
            generated_at=datetime.utcnow(),
        )

    def _determine_node_type(self, dataset: Dataset) -> NodeType:
        """데이터셋에서 노드 타입 결정"""
        type_mapping = {
            "source": NodeType.SOURCE,
            "staging": NodeType.STAGING,
            "transformed": NodeType.TRANSFORM,
            "aggregated": NodeType.AGGREGATION,
            "final": NodeType.FINAL,
        }
        return type_mapping.get(dataset.dataset_type.value, NodeType.SOURCE)

    # ==================== Impact Analysis ====================

    def analyze_impact(
        self,
        dataset_id: str,
        include_columns: bool = True,
        max_depth: int = 10,
    ) -> ImpactAnalysis:
        """
        영향도 분석 (downstream 영향 파악)

        Args:
            dataset_id: 데이터셋 ID
            include_columns: 컬럼 레벨 분석 포함 여부
            max_depth: 최대 깊이

        Returns:
            ImpactAnalysis
        """
        affected_datasets = []
        affected_columns = []
        critical_paths = []
        visited: Set[str] = set()
        actual_max_depth = 0

        def analyze(ds_id: str, depth: int, path: List[str]):
            nonlocal actual_max_depth

            if depth > max_depth:
                return
            if ds_id in visited and ds_id != dataset_id:
                return

            visited.add(ds_id)
            actual_max_depth = max(actual_max_depth, depth)

            edges = self.get_outgoing_edges(ds_id)

            for edge in edges:
                target_dataset = self.catalog.get_dataset(edge.target_id) if self.catalog else None

                affected_datasets.append({
                    "id": edge.target_id,
                    "name": target_dataset.name if target_dataset else "Unknown",
                    "depth": depth + 1,
                    "impact_type": edge.relationship.value,
                    "transformation": edge.transformation_logic,
                })

                # 컬럼 영향 분석
                if include_columns and edge.column_mappings:
                    for target_col, source_cols in edge.column_mappings.items():
                        affected_columns.append({
                            "dataset_id": edge.target_id,
                            "column": target_col,
                            "source_columns": source_cols,
                            "impact": "direct" if depth == 0 else "indirect",
                        })

                new_path = path + [edge.target_id]

                # 리프 노드이면 critical path 기록
                downstream = self.get_outgoing_edges(edge.target_id)
                if not downstream:
                    critical_paths.append(new_path)

                analyze(edge.target_id, depth + 1, new_path)

        analyze(dataset_id, 0, [dataset_id])

        return ImpactAnalysis(
            source_dataset_id=dataset_id,
            affected_datasets=affected_datasets,
            affected_columns=affected_columns,
            total_affected=len(affected_datasets),
            max_depth=actual_max_depth,
            critical_paths=critical_paths[:10],  # 상위 10개만
        )

    def find_root_sources(self, dataset_id: str) -> List[str]:
        """
        루트 소스 찾기 (업스트림이 없는 최상위 데이터셋)

        Args:
            dataset_id: 데이터셋 ID

        Returns:
            루트 소스 데이터셋 ID 목록
        """
        roots = []
        visited: Set[str] = set()

        def find_roots(ds_id: str):
            if ds_id in visited:
                return

            visited.add(ds_id)
            incoming = self.get_incoming_edges(ds_id)

            if not incoming:
                roots.append(ds_id)
            else:
                for edge in incoming:
                    find_roots(edge.source_id)

        find_roots(dataset_id)
        return roots

    def find_leaf_targets(self, dataset_id: str) -> List[str]:
        """
        리프 타겟 찾기 (다운스트림이 없는 최종 데이터셋)

        Args:
            dataset_id: 데이터셋 ID

        Returns:
            리프 타겟 데이터셋 ID 목록
        """
        leaves = []
        visited: Set[str] = set()

        def find_leaves(ds_id: str):
            if ds_id in visited:
                return

            visited.add(ds_id)
            outgoing = self.get_outgoing_edges(ds_id)

            if not outgoing:
                leaves.append(ds_id)
            else:
                for edge in outgoing:
                    find_leaves(edge.target_id)

        find_leaves(dataset_id)
        return leaves

    # ==================== Path Finding ====================

    def find_paths(
        self,
        source_id: str,
        target_id: str,
        max_depth: int = 10,
    ) -> List[List[str]]:
        """
        두 데이터셋 간의 모든 경로 찾기

        Args:
            source_id: 시작 데이터셋 ID
            target_id: 목표 데이터셋 ID
            max_depth: 최대 깊이

        Returns:
            경로 목록 (각 경로는 데이터셋 ID 리스트)
        """
        paths = []

        def dfs(current: str, path: List[str], depth: int):
            if depth > max_depth:
                return

            if current == target_id:
                paths.append(path.copy())
                return

            edges = self.get_outgoing_edges(current)
            for edge in edges:
                if edge.target_id not in path:  # 순환 방지
                    path.append(edge.target_id)
                    dfs(edge.target_id, path, depth + 1)
                    path.pop()

        dfs(source_id, [source_id], 0)
        return paths

    def get_shortest_path(
        self,
        source_id: str,
        target_id: str,
    ) -> Optional[List[str]]:
        """
        두 데이터셋 간의 최단 경로 찾기 (BFS)

        Args:
            source_id: 시작 데이터셋 ID
            target_id: 목표 데이터셋 ID

        Returns:
            최단 경로 또는 None
        """
        if source_id == target_id:
            return [source_id]

        queue = deque([(source_id, [source_id])])
        visited = {source_id}

        while queue:
            current, path = queue.popleft()

            edges = self.get_outgoing_edges(current)
            for edge in edges:
                if edge.target_id == target_id:
                    return path + [target_id]

                if edge.target_id not in visited:
                    visited.add(edge.target_id)
                    queue.append((edge.target_id, path + [edge.target_id]))

        return None

    # ==================== Auto Detection ====================

    def detect_lineage_from_etl(
        self,
        source_collection: str,
        target_collection: str,
        job_id: str = None,
    ) -> Optional[LineageEdge]:
        """
        ETL 작업에서 리니지 자동 감지

        Args:
            source_collection: 소스 컬렉션 이름
            target_collection: 타겟 컬렉션 이름
            job_id: 작업 ID

        Returns:
            생성된 LineageEdge 또는 None
        """
        if not self.catalog:
            return None

        source_dataset = self.catalog.get_dataset_by_collection(source_collection)
        target_dataset = self.catalog.get_dataset_by_collection(target_collection)

        if not source_dataset or not target_dataset:
            return None

        # 관계 유형 추론
        relationship = RelationshipType.DERIVES_FROM

        if "staging_" in target_collection:
            relationship = RelationshipType.COPIES
        elif "agg_" in target_collection or "summary_" in target_collection:
            relationship = RelationshipType.AGGREGATES

        return self.create_edge(
            source_id=source_dataset.id,
            target_id=target_dataset.id,
            relationship=relationship,
            job_id=job_id,
            created_by="auto_detection",
        )

    def invalidate_cache(self):
        """캐시 무효화"""
        self._edge_cache.clear()

"""
Data Lineage Tracker - 데이터 리니지 추적기

기능:
- 데이터 흐름 추적 (소스 → 변환 → 대상)
- 필드 레벨 리니지
- 실행별 리니지 기록
- 계보 조회 (upstream/downstream)
"""

import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class NodeType(str, Enum):
    """리니지 노드 타입"""
    SOURCE = "source"           # 원본 데이터 소스 (웹사이트, API 등)
    DATASET = "dataset"         # 데이터셋 (컬렉션, 테이블)
    FIELD = "field"             # 필드 (컬럼)
    TRANSFORMATION = "transformation"  # 변환 작업
    MODEL = "model"             # ML 모델
    REPORT = "report"           # 리포트/대시보드


class EdgeType(str, Enum):
    """리니지 엣지 타입"""
    EXTRACT = "extract"         # 추출 (소스 → 데이터셋)
    TRANSFORM = "transform"     # 변환 (데이터셋 → 데이터셋)
    LOAD = "load"               # 적재 (데이터셋 → 대상)
    DERIVE = "derive"           # 파생 (필드 → 필드)
    AGGREGATE = "aggregate"     # 집계
    FILTER = "filter"           # 필터링
    JOIN = "join"               # 조인
    UNION = "union"             # 합집합


@dataclass
class LineageNode:
    """리니지 노드"""
    node_id: str                # 고유 ID
    node_type: NodeType         # 노드 타입
    name: str                   # 이름
    qualified_name: str         # 전체 경로 이름

    # 메타데이터
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)

    # 소스 정보
    source_id: Optional[str] = None
    crawler_id: Optional[str] = None

    # 시간
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        data = asdict(self)
        data["node_type"] = self.node_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LineageNode":
        """딕셔너리에서 생성"""
        data["node_type"] = NodeType(data["node_type"])
        if isinstance(data.get("created_at"), str):
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        if isinstance(data.get("updated_at"), str):
            data["updated_at"] = datetime.fromisoformat(data["updated_at"])
        return cls(**data)

    @staticmethod
    def generate_id(node_type: NodeType, qualified_name: str) -> str:
        """노드 ID 생성"""
        content = f"{node_type.value}:{qualified_name}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]


@dataclass
class LineageEdge:
    """리니지 엣지 (데이터 흐름)"""
    edge_id: str
    source_node_id: str         # 시작 노드
    target_node_id: str         # 끝 노드
    edge_type: EdgeType         # 엣지 타입

    # 변환 정보
    transformation_logic: Optional[str] = None  # 변환 로직 설명
    expression: Optional[str] = None            # SQL/코드 표현식

    # 메타데이터
    properties: Dict[str, Any] = field(default_factory=dict)

    # 시간
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        data = asdict(self)
        data["edge_type"] = self.edge_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LineageEdge":
        """딕셔너리에서 생성"""
        data["edge_type"] = EdgeType(data["edge_type"])
        if isinstance(data.get("created_at"), str):
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        return cls(**data)

    @staticmethod
    def generate_id(source_id: str, target_id: str, edge_type: EdgeType) -> str:
        """엣지 ID 생성"""
        content = f"{source_id}:{target_id}:{edge_type.value}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]


@dataclass
class LineageRun:
    """실행별 리니지 기록"""
    run_id: str
    dag_id: Optional[str] = None
    source_id: Optional[str] = None

    # 실행 정보
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    status: str = "running"  # running, success, failed

    # 리니지 참조
    nodes: List[str] = field(default_factory=list)   # 관련 노드 ID
    edges: List[str] = field(default_factory=list)   # 관련 엣지 ID

    # 통계
    records_read: int = 0
    records_written: int = 0

    # 메타데이터
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return asdict(self)


class LineageTracker:
    """데이터 리니지 추적기"""

    NODES_COLLECTION = "lineage_nodes"
    EDGES_COLLECTION = "lineage_edges"
    RUNS_COLLECTION = "lineage_runs"

    def __init__(self, mongo_service=None):
        """
        Args:
            mongo_service: MongoDB 서비스
        """
        self.mongo = mongo_service
        self._current_run: Optional[LineageRun] = None

    # ==================== Run Management ====================

    @contextmanager
    def track_run(
        self,
        run_id: str,
        dag_id: str = None,
        source_id: str = None
    ):
        """
        리니지 추적 컨텍스트

        사용법:
            with tracker.track_run(run_id, dag_id) as run:
                tracker.add_extraction(...)
                tracker.add_transformation(...)

        Args:
            run_id: 실행 ID
            dag_id: DAG ID
            source_id: 소스 ID

        Yields:
            LineageRun 객체
        """
        self._current_run = LineageRun(
            run_id=run_id,
            dag_id=dag_id,
            source_id=source_id
        )

        try:
            yield self._current_run
            self._current_run.status = "success"
            self._current_run.completed_at = datetime.utcnow()

        except Exception as e:
            self._current_run.status = "failed"
            self._current_run.completed_at = datetime.utcnow()
            self._current_run.metadata["error"] = str(e)
            raise

        finally:
            # 실행 기록 저장
            self._save_run(self._current_run)
            self._current_run = None

    def _save_run(self, run: LineageRun):
        """실행 기록 저장"""
        if not self.mongo:
            return

        try:
            self.mongo.db[self.RUNS_COLLECTION].update_one(
                {"run_id": run.run_id},
                {"$set": run.to_dict()},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Failed to save lineage run: {e}")

    # ==================== Node Operations ====================

    def register_node(self, node: LineageNode) -> str:
        """
        노드 등록

        Args:
            node: 리니지 노드

        Returns:
            노드 ID
        """
        if not self.mongo:
            return node.node_id

        try:
            self.mongo.db[self.NODES_COLLECTION].update_one(
                {"node_id": node.node_id},
                {"$set": node.to_dict()},
                upsert=True
            )

            if self._current_run and node.node_id not in self._current_run.nodes:
                self._current_run.nodes.append(node.node_id)

            logger.debug(f"Registered lineage node: {node.qualified_name}")
            return node.node_id

        except Exception as e:
            logger.error(f"Failed to register node: {e}")
            return node.node_id

    def register_source(
        self,
        source_id: str,
        source_name: str,
        source_url: str = None,
        **properties
    ) -> LineageNode:
        """
        소스 노드 등록

        Args:
            source_id: 소스 ID
            source_name: 소스 이름
            source_url: 소스 URL
            **properties: 추가 속성

        Returns:
            LineageNode
        """
        qualified_name = f"source://{source_id}"
        node_id = LineageNode.generate_id(NodeType.SOURCE, qualified_name)

        node = LineageNode(
            node_id=node_id,
            node_type=NodeType.SOURCE,
            name=source_name,
            qualified_name=qualified_name,
            source_id=source_id,
            properties={
                "url": source_url,
                **properties
            }
        )

        self.register_node(node)
        return node

    def register_dataset(
        self,
        dataset_name: str,
        collection_name: str = None,
        source_id: str = None,
        **properties
    ) -> LineageNode:
        """
        데이터셋 노드 등록

        Args:
            dataset_name: 데이터셋 이름
            collection_name: MongoDB 컬렉션 이름
            source_id: 관련 소스 ID
            **properties: 추가 속성

        Returns:
            LineageNode
        """
        qualified_name = f"dataset://{collection_name or dataset_name}"
        node_id = LineageNode.generate_id(NodeType.DATASET, qualified_name)

        node = LineageNode(
            node_id=node_id,
            node_type=NodeType.DATASET,
            name=dataset_name,
            qualified_name=qualified_name,
            source_id=source_id,
            properties={
                "collection": collection_name,
                **properties
            }
        )

        self.register_node(node)
        return node

    def register_field(
        self,
        field_name: str,
        dataset_name: str,
        data_type: str = None,
        **properties
    ) -> LineageNode:
        """
        필드 노드 등록

        Args:
            field_name: 필드 이름
            dataset_name: 데이터셋 이름
            data_type: 데이터 타입
            **properties: 추가 속성

        Returns:
            LineageNode
        """
        qualified_name = f"field://{dataset_name}.{field_name}"
        node_id = LineageNode.generate_id(NodeType.FIELD, qualified_name)

        node = LineageNode(
            node_id=node_id,
            node_type=NodeType.FIELD,
            name=field_name,
            qualified_name=qualified_name,
            properties={
                "dataset": dataset_name,
                "data_type": data_type,
                **properties
            }
        )

        self.register_node(node)
        return node

    def register_transformation(
        self,
        name: str,
        description: str = None,
        dag_id: str = None,
        task_id: str = None,
        **properties
    ) -> LineageNode:
        """
        변환 노드 등록

        Args:
            name: 변환 이름
            description: 설명
            dag_id: Airflow DAG ID
            task_id: Airflow Task ID
            **properties: 추가 속성

        Returns:
            LineageNode
        """
        qualified_name = f"transform://{dag_id or 'unknown'}/{task_id or name}"
        node_id = LineageNode.generate_id(NodeType.TRANSFORMATION, qualified_name)

        node = LineageNode(
            node_id=node_id,
            node_type=NodeType.TRANSFORMATION,
            name=name,
            qualified_name=qualified_name,
            description=description,
            properties={
                "dag_id": dag_id,
                "task_id": task_id,
                **properties
            }
        )

        self.register_node(node)
        return node

    def get_node(self, node_id: str) -> Optional[LineageNode]:
        """노드 조회"""
        if not self.mongo:
            return None

        try:
            doc = self.mongo.db[self.NODES_COLLECTION].find_one({"node_id": node_id})
            return LineageNode.from_dict(doc) if doc else None
        except Exception as e:
            logger.error(f"Failed to get node: {e}")
            return None

    def find_nodes(
        self,
        node_type: NodeType = None,
        source_id: str = None,
        name_pattern: str = None,
        limit: int = 100
    ) -> List[LineageNode]:
        """노드 검색"""
        if not self.mongo:
            return []

        query = {}
        if node_type:
            query["node_type"] = node_type.value
        if source_id:
            query["source_id"] = source_id
        if name_pattern:
            query["name"] = {"$regex": name_pattern, "$options": "i"}

        try:
            docs = self.mongo.db[self.NODES_COLLECTION].find(query).limit(limit)
            return [LineageNode.from_dict(doc) for doc in docs]
        except Exception as e:
            logger.error(f"Failed to find nodes: {e}")
            return []

    # ==================== Edge Operations ====================

    def add_edge(self, edge: LineageEdge) -> str:
        """
        엣지 추가

        Args:
            edge: 리니지 엣지

        Returns:
            엣지 ID
        """
        if not self.mongo:
            return edge.edge_id

        try:
            self.mongo.db[self.EDGES_COLLECTION].update_one(
                {"edge_id": edge.edge_id},
                {"$set": edge.to_dict()},
                upsert=True
            )

            if self._current_run and edge.edge_id not in self._current_run.edges:
                self._current_run.edges.append(edge.edge_id)

            logger.debug(f"Added lineage edge: {edge.source_node_id} -> {edge.target_node_id}")
            return edge.edge_id

        except Exception as e:
            logger.error(f"Failed to add edge: {e}")
            return edge.edge_id

    def add_extraction(
        self,
        source_node: LineageNode,
        target_dataset: LineageNode,
        transformation_logic: str = None
    ) -> LineageEdge:
        """
        추출 엣지 추가 (소스 → 데이터셋)

        Args:
            source_node: 소스 노드
            target_dataset: 대상 데이터셋
            transformation_logic: 변환 로직 설명

        Returns:
            LineageEdge
        """
        edge_id = LineageEdge.generate_id(
            source_node.node_id,
            target_dataset.node_id,
            EdgeType.EXTRACT
        )

        edge = LineageEdge(
            edge_id=edge_id,
            source_node_id=source_node.node_id,
            target_node_id=target_dataset.node_id,
            edge_type=EdgeType.EXTRACT,
            transformation_logic=transformation_logic or "Data extraction from source"
        )

        self.add_edge(edge)
        return edge

    def add_transformation(
        self,
        source_dataset: LineageNode,
        target_dataset: LineageNode,
        transformation_node: LineageNode = None,
        edge_type: EdgeType = EdgeType.TRANSFORM,
        transformation_logic: str = None,
        expression: str = None
    ) -> LineageEdge:
        """
        변환 엣지 추가

        Args:
            source_dataset: 소스 데이터셋
            target_dataset: 대상 데이터셋
            transformation_node: 변환 노드 (선택)
            edge_type: 엣지 타입
            transformation_logic: 변환 로직 설명
            expression: 표현식

        Returns:
            LineageEdge
        """
        # 변환 노드가 있으면 source → transform → target
        if transformation_node:
            # source → transform
            self.add_edge(LineageEdge(
                edge_id=LineageEdge.generate_id(
                    source_dataset.node_id,
                    transformation_node.node_id,
                    EdgeType.TRANSFORM
                ),
                source_node_id=source_dataset.node_id,
                target_node_id=transformation_node.node_id,
                edge_type=EdgeType.TRANSFORM,
                transformation_logic="Input to transformation"
            ))

            # transform → target
            edge_id = LineageEdge.generate_id(
                transformation_node.node_id,
                target_dataset.node_id,
                edge_type
            )
            source_id = transformation_node.node_id
        else:
            # 직접 연결
            edge_id = LineageEdge.generate_id(
                source_dataset.node_id,
                target_dataset.node_id,
                edge_type
            )
            source_id = source_dataset.node_id

        edge = LineageEdge(
            edge_id=edge_id,
            source_node_id=source_id,
            target_node_id=target_dataset.node_id,
            edge_type=edge_type,
            transformation_logic=transformation_logic,
            expression=expression
        )

        self.add_edge(edge)
        return edge

    def add_field_derivation(
        self,
        source_fields: List[LineageNode],
        target_field: LineageNode,
        expression: str = None,
        transformation_logic: str = None
    ) -> List[LineageEdge]:
        """
        필드 파생 엣지 추가

        Args:
            source_fields: 소스 필드들
            target_field: 대상 필드
            expression: 파생 표현식
            transformation_logic: 변환 로직

        Returns:
            생성된 엣지 목록
        """
        edges = []
        for source_field in source_fields:
            edge_id = LineageEdge.generate_id(
                source_field.node_id,
                target_field.node_id,
                EdgeType.DERIVE
            )

            edge = LineageEdge(
                edge_id=edge_id,
                source_node_id=source_field.node_id,
                target_node_id=target_field.node_id,
                edge_type=EdgeType.DERIVE,
                expression=expression,
                transformation_logic=transformation_logic
            )

            self.add_edge(edge)
            edges.append(edge)

        return edges

    # ==================== Lineage Queries ====================

    def get_upstream(
        self,
        node_id: str,
        depth: int = 10
    ) -> Tuple[List[LineageNode], List[LineageEdge]]:
        """
        업스트림 (상위) 리니지 조회

        Args:
            node_id: 시작 노드 ID
            depth: 최대 깊이

        Returns:
            (노드 목록, 엣지 목록)
        """
        if not self.mongo:
            return [], []

        visited_nodes: Set[str] = set()
        visited_edges: Set[str] = set()
        nodes = []
        edges = []

        def traverse(current_id: str, current_depth: int):
            if current_depth > depth or current_id in visited_nodes:
                return

            visited_nodes.add(current_id)

            # 현재 노드 조회
            node = self.get_node(current_id)
            if node:
                nodes.append(node)

            # 들어오는 엣지 찾기
            incoming = list(self.mongo.db[self.EDGES_COLLECTION].find({
                "target_node_id": current_id
            }))

            for edge_doc in incoming:
                edge = LineageEdge.from_dict(edge_doc)
                if edge.edge_id not in visited_edges:
                    visited_edges.add(edge.edge_id)
                    edges.append(edge)
                    traverse(edge.source_node_id, current_depth + 1)

        traverse(node_id, 0)
        return nodes, edges

    def get_downstream(
        self,
        node_id: str,
        depth: int = 10
    ) -> Tuple[List[LineageNode], List[LineageEdge]]:
        """
        다운스트림 (하위) 리니지 조회

        Args:
            node_id: 시작 노드 ID
            depth: 최대 깊이

        Returns:
            (노드 목록, 엣지 목록)
        """
        if not self.mongo:
            return [], []

        visited_nodes: Set[str] = set()
        visited_edges: Set[str] = set()
        nodes = []
        edges = []

        def traverse(current_id: str, current_depth: int):
            if current_depth > depth or current_id in visited_nodes:
                return

            visited_nodes.add(current_id)

            # 현재 노드 조회
            node = self.get_node(current_id)
            if node:
                nodes.append(node)

            # 나가는 엣지 찾기
            outgoing = list(self.mongo.db[self.EDGES_COLLECTION].find({
                "source_node_id": current_id
            }))

            for edge_doc in outgoing:
                edge = LineageEdge.from_dict(edge_doc)
                if edge.edge_id not in visited_edges:
                    visited_edges.add(edge.edge_id)
                    edges.append(edge)
                    traverse(edge.target_node_id, current_depth + 1)

        traverse(node_id, 0)
        return nodes, edges

    def get_full_lineage(
        self,
        node_id: str,
        upstream_depth: int = 10,
        downstream_depth: int = 10
    ) -> Dict[str, Any]:
        """
        전체 리니지 조회 (업스트림 + 다운스트림)

        Args:
            node_id: 중심 노드 ID
            upstream_depth: 업스트림 깊이
            downstream_depth: 다운스트림 깊이

        Returns:
            전체 리니지 정보
        """
        upstream_nodes, upstream_edges = self.get_upstream(node_id, upstream_depth)
        downstream_nodes, downstream_edges = self.get_downstream(node_id, downstream_depth)

        # 중복 제거
        all_nodes = {n.node_id: n for n in upstream_nodes + downstream_nodes}
        all_edges = {e.edge_id: e for e in upstream_edges + downstream_edges}

        return {
            "center_node_id": node_id,
            "nodes": [n.to_dict() for n in all_nodes.values()],
            "edges": [e.to_dict() for e in all_edges.values()],
            "upstream_count": len(upstream_nodes),
            "downstream_count": len(downstream_nodes)
        }

    def get_run_lineage(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        특정 실행의 리니지 조회

        Args:
            run_id: 실행 ID

        Returns:
            실행 리니지 정보
        """
        if not self.mongo:
            return None

        try:
            run_doc = self.mongo.db[self.RUNS_COLLECTION].find_one({"run_id": run_id})
            if not run_doc:
                return None

            # 관련 노드 조회
            nodes = list(self.mongo.db[self.NODES_COLLECTION].find({
                "node_id": {"$in": run_doc.get("nodes", [])}
            }))

            # 관련 엣지 조회
            edges = list(self.mongo.db[self.EDGES_COLLECTION].find({
                "edge_id": {"$in": run_doc.get("edges", [])}
            }))

            return {
                "run": run_doc,
                "nodes": nodes,
                "edges": edges
            }

        except Exception as e:
            logger.error(f"Failed to get run lineage: {e}")
            return None

    def get_source_lineage(self, source_id: str) -> Dict[str, Any]:
        """
        소스별 리니지 조회

        Args:
            source_id: 소스 ID

        Returns:
            소스 리니지 정보
        """
        # 소스 노드 찾기
        source_nodes = self.find_nodes(node_type=NodeType.SOURCE, source_id=source_id)

        if not source_nodes:
            return {"error": "Source not found", "nodes": [], "edges": []}

        # 첫 번째 소스 노드의 다운스트림 조회
        return self.get_full_lineage(source_nodes[0].node_id)
"""
Lineage Graph - 리니지 그래프 관리

기능:
- 그래프 시각화용 데이터 변환
- 경로 분석
- 그래프 통계
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import logging

from .tracker import LineageNode, LineageEdge, NodeType, EdgeType

logger = logging.getLogger(__name__)


@dataclass
class GraphNode:
    """그래프 시각화용 노드"""
    id: str
    label: str
    type: str
    group: str              # 그룹핑 (소스별, 타입별)
    properties: Dict[str, Any] = field(default_factory=dict)

    # 시각화 속성
    size: int = 20
    color: str = "#1f77b4"
    shape: str = "dot"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "type": self.type,
            "group": self.group,
            "size": self.size,
            "color": self.color,
            "shape": self.shape,
            "properties": self.properties
        }


@dataclass
class GraphEdge:
    """그래프 시각화용 엣지"""
    id: str
    source: str
    target: str
    type: str
    label: str = ""
    properties: Dict[str, Any] = field(default_factory=dict)

    # 시각화 속성
    width: int = 1
    color: str = "#999999"
    dashes: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "target": self.target,
            "type": self.type,
            "label": self.label,
            "width": self.width,
            "color": self.color,
            "dashes": self.dashes,
            "properties": self.properties
        }


class LineageGraph:
    """리니지 그래프 관리 및 분석"""

    # 노드 타입별 시각화 설정
    NODE_STYLES = {
        NodeType.SOURCE: {"color": "#ff7f0e", "shape": "database", "size": 30},
        NodeType.DATASET: {"color": "#2ca02c", "shape": "box", "size": 25},
        NodeType.FIELD: {"color": "#9467bd", "shape": "dot", "size": 15},
        NodeType.TRANSFORMATION: {"color": "#d62728", "shape": "diamond", "size": 25},
        NodeType.MODEL: {"color": "#8c564b", "shape": "triangle", "size": 25},
        NodeType.REPORT: {"color": "#e377c2", "shape": "star", "size": 25},
    }

    # 엣지 타입별 시각화 설정
    EDGE_STYLES = {
        EdgeType.EXTRACT: {"color": "#ff7f0e", "width": 2, "dashes": False},
        EdgeType.TRANSFORM: {"color": "#2ca02c", "width": 2, "dashes": False},
        EdgeType.LOAD: {"color": "#1f77b4", "width": 2, "dashes": False},
        EdgeType.DERIVE: {"color": "#9467bd", "width": 1, "dashes": True},
        EdgeType.AGGREGATE: {"color": "#d62728", "width": 2, "dashes": False},
        EdgeType.FILTER: {"color": "#8c564b", "width": 1, "dashes": True},
        EdgeType.JOIN: {"color": "#e377c2", "width": 2, "dashes": False},
        EdgeType.UNION: {"color": "#7f7f7f", "width": 2, "dashes": False},
    }

    def __init__(self):
        self._nodes: Dict[str, GraphNode] = {}
        self._edges: Dict[str, GraphEdge] = {}
        self._adjacency: Dict[str, Set[str]] = defaultdict(set)  # 출발 -> 도착
        self._reverse_adjacency: Dict[str, Set[str]] = defaultdict(set)  # 도착 -> 출발

    def add_node(self, lineage_node: LineageNode):
        """리니지 노드를 그래프 노드로 추가"""
        style = self.NODE_STYLES.get(lineage_node.node_type, {})

        graph_node = GraphNode(
            id=lineage_node.node_id,
            label=lineage_node.name,
            type=lineage_node.node_type.value,
            group=lineage_node.source_id or lineage_node.node_type.value,
            properties=lineage_node.properties,
            size=style.get("size", 20),
            color=style.get("color", "#1f77b4"),
            shape=style.get("shape", "dot")
        )

        self._nodes[lineage_node.node_id] = graph_node

    def add_edge(self, lineage_edge: LineageEdge):
        """리니지 엣지를 그래프 엣지로 추가"""
        style = self.EDGE_STYLES.get(lineage_edge.edge_type, {})

        graph_edge = GraphEdge(
            id=lineage_edge.edge_id,
            source=lineage_edge.source_node_id,
            target=lineage_edge.target_node_id,
            type=lineage_edge.edge_type.value,
            label=lineage_edge.edge_type.value,
            properties={
                "transformation_logic": lineage_edge.transformation_logic,
                "expression": lineage_edge.expression
            },
            width=style.get("width", 1),
            color=style.get("color", "#999999"),
            dashes=style.get("dashes", False)
        )

        self._edges[lineage_edge.edge_id] = graph_edge
        self._adjacency[lineage_edge.source_node_id].add(lineage_edge.target_node_id)
        self._reverse_adjacency[lineage_edge.target_node_id].add(lineage_edge.source_node_id)

    def build_from_lineage(
        self,
        nodes: List[LineageNode],
        edges: List[LineageEdge]
    ):
        """리니지 데이터로 그래프 구축"""
        self._nodes.clear()
        self._edges.clear()
        self._adjacency.clear()
        self._reverse_adjacency.clear()

        for node in nodes:
            self.add_node(node)

        for edge in edges:
            self.add_edge(edge)

    def to_visualization_format(self) -> Dict[str, Any]:
        """
        시각화 라이브러리용 포맷으로 변환

        vis.js, D3.js, Cytoscape.js 등에서 사용 가능
        """
        return {
            "nodes": [n.to_dict() for n in self._nodes.values()],
            "edges": [e.to_dict() for e in self._edges.values()],
            "statistics": self.get_statistics()
        }

    def to_cytoscape_format(self) -> List[Dict[str, Any]]:
        """Cytoscape.js 포맷으로 변환"""
        elements = []

        for node in self._nodes.values():
            elements.append({
                "data": {
                    "id": node.id,
                    "label": node.label,
                    "type": node.type,
                    "group": node.group
                },
                "classes": node.type
            })

        for edge in self._edges.values():
            elements.append({
                "data": {
                    "id": edge.id,
                    "source": edge.source,
                    "target": edge.target,
                    "label": edge.label,
                    "type": edge.type
                },
                "classes": edge.type
            })

        return elements

    def get_statistics(self) -> Dict[str, Any]:
        """그래프 통계"""
        node_by_type = defaultdict(int)
        for node in self._nodes.values():
            node_by_type[node.type] += 1

        edge_by_type = defaultdict(int)
        for edge in self._edges.values():
            edge_by_type[edge.type] += 1

        # 연결성 분석
        isolated_nodes = [
            n_id for n_id in self._nodes
            if n_id not in self._adjacency and n_id not in self._reverse_adjacency
        ]

        source_nodes = [
            n_id for n_id in self._nodes
            if n_id not in self._reverse_adjacency and n_id in self._adjacency
        ]

        sink_nodes = [
            n_id for n_id in self._nodes
            if n_id in self._reverse_adjacency and n_id not in self._adjacency
        ]

        return {
            "total_nodes": len(self._nodes),
            "total_edges": len(self._edges),
            "nodes_by_type": dict(node_by_type),
            "edges_by_type": dict(edge_by_type),
            "isolated_nodes": len(isolated_nodes),
            "source_nodes": len(source_nodes),
            "sink_nodes": len(sink_nodes),
            "avg_in_degree": sum(len(v) for v in self._reverse_adjacency.values()) / max(len(self._nodes), 1),
            "avg_out_degree": sum(len(v) for v in self._adjacency.values()) / max(len(self._nodes), 1)
        }

    def find_path(
        self,
        start_node_id: str,
        end_node_id: str
    ) -> Optional[List[str]]:
        """
        두 노드 사이의 경로 찾기 (BFS)

        Args:
            start_node_id: 시작 노드 ID
            end_node_id: 끝 노드 ID

        Returns:
            경로 (노드 ID 목록) 또는 None
        """
        if start_node_id not in self._nodes or end_node_id not in self._nodes:
            return None

        if start_node_id == end_node_id:
            return [start_node_id]

        visited = {start_node_id}
        queue = [(start_node_id, [start_node_id])]

        while queue:
            current, path = queue.pop(0)

            for neighbor in self._adjacency.get(current, set()):
                if neighbor == end_node_id:
                    return path + [neighbor]

                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))

        return None

    def find_all_paths(
        self,
        start_node_id: str,
        end_node_id: str,
        max_paths: int = 10
    ) -> List[List[str]]:
        """
        두 노드 사이의 모든 경로 찾기 (DFS)

        Args:
            start_node_id: 시작 노드 ID
            end_node_id: 끝 노드 ID
            max_paths: 최대 경로 수

        Returns:
            경로 목록
        """
        if start_node_id not in self._nodes or end_node_id not in self._nodes:
            return []

        paths = []
        stack = [(start_node_id, [start_node_id])]

        while stack and len(paths) < max_paths:
            current, path = stack.pop()

            if current == end_node_id:
                paths.append(path)
                continue

            for neighbor in self._adjacency.get(current, set()):
                if neighbor not in path:  # 사이클 방지
                    stack.append((neighbor, path + [neighbor]))

        return paths

    def get_connected_components(self) -> List[Set[str]]:
        """
        연결 컴포넌트 찾기 (무방향 그래프로 처리)

        Returns:
            컴포넌트 목록 (각 컴포넌트는 노드 ID 집합)
        """
        visited = set()
        components = []

        def dfs(node_id: str, component: Set[str]):
            if node_id in visited:
                return
            visited.add(node_id)
            component.add(node_id)

            # 양방향 연결 탐색
            for neighbor in self._adjacency.get(node_id, set()):
                dfs(neighbor, component)
            for neighbor in self._reverse_adjacency.get(node_id, set()):
                dfs(neighbor, component)

        for node_id in self._nodes:
            if node_id not in visited:
                component = set()
                dfs(node_id, component)
                components.append(component)

        return components

    def detect_cycles(self) -> List[List[str]]:
        """
        사이클 감지

        Returns:
            사이클 목록 (각 사이클은 노드 ID 목록)
        """
        cycles = []
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node_id: str):
            visited.add(node_id)
            rec_stack.add(node_id)
            path.append(node_id)

            for neighbor in self._adjacency.get(node_id, set()):
                if neighbor not in visited:
                    result = dfs(neighbor)
                    if result:
                        return result
                elif neighbor in rec_stack:
                    # 사이클 발견
                    cycle_start = path.index(neighbor)
                    cycles.append(path[cycle_start:] + [neighbor])

            path.pop()
            rec_stack.remove(node_id)
            return None

        for node_id in self._nodes:
            if node_id not in visited:
                dfs(node_id)

        return cycles

    def get_subgraph(
        self,
        node_ids: Set[str],
        include_connecting_edges: bool = True
    ) -> "LineageGraph":
        """
        서브그래프 추출

        Args:
            node_ids: 포함할 노드 ID 집합
            include_connecting_edges: 노드 간 연결 엣지 포함 여부

        Returns:
            새 LineageGraph 인스턴스
        """
        subgraph = LineageGraph()

        for node_id in node_ids:
            if node_id in self._nodes:
                subgraph._nodes[node_id] = self._nodes[node_id]

        for edge_id, edge in self._edges.items():
            if include_connecting_edges:
                if edge.source in node_ids and edge.target in node_ids:
                    subgraph._edges[edge_id] = edge
                    subgraph._adjacency[edge.source].add(edge.target)
                    subgraph._reverse_adjacency[edge.target].add(edge.source)

        return subgraph

    def topological_sort(self) -> Optional[List[str]]:
        """
        위상 정렬 (DAG인 경우만)

        Returns:
            정렬된 노드 ID 목록 또는 None (사이클 존재 시)
        """
        in_degree = {n_id: 0 for n_id in self._nodes}
        for targets in self._adjacency.values():
            for target in targets:
                if target in in_degree:
                    in_degree[target] += 1

        queue = [n_id for n_id, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            node_id = queue.pop(0)
            result.append(node_id)

            for neighbor in self._adjacency.get(node_id, set()):
                if neighbor in in_degree:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

        if len(result) != len(self._nodes):
            return None  # 사이클 존재

        return result

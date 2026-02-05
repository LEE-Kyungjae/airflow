"""
Impact Analyzer - 영향 분석

기능:
- 변경 영향 분석 (소스/필드 변경 시 영향 범위)
- 의존성 분석
- 데이터 계보 보고서
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import logging

from .tracker import LineageTracker, LineageNode, LineageEdge, NodeType, EdgeType
from .graph import LineageGraph

logger = logging.getLogger(__name__)


class ImpactLevel(str, Enum):
    """영향 수준"""
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ImpactedNode:
    """영향받는 노드"""
    node: LineageNode
    impact_level: ImpactLevel
    impact_reason: str
    distance_from_source: int
    path: List[str] = field(default_factory=list)  # 영향 전파 경로


@dataclass
class ImpactResult:
    """영향 분석 결과"""
    source_node_id: str
    change_type: str  # delete, modify, schema_change
    analysis_time: datetime = field(default_factory=datetime.utcnow)

    # 영향받는 노드
    impacted_nodes: List[ImpactedNode] = field(default_factory=list)

    # 요약
    total_impacted: int = 0
    by_level: Dict[str, int] = field(default_factory=dict)
    by_type: Dict[str, int] = field(default_factory=dict)

    # 권장 조치
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_node_id": self.source_node_id,
            "change_type": self.change_type,
            "analysis_time": self.analysis_time.isoformat(),
            "total_impacted": self.total_impacted,
            "by_level": self.by_level,
            "by_type": self.by_type,
            "impacted_nodes": [
                {
                    "node_id": n.node.node_id,
                    "name": n.node.name,
                    "type": n.node.node_type.value,
                    "impact_level": n.impact_level.value,
                    "impact_reason": n.impact_reason,
                    "distance": n.distance_from_source,
                    "path": n.path
                }
                for n in self.impacted_nodes
            ],
            "recommendations": self.recommendations
        }


class ImpactAnalyzer:
    """영향 분석기"""

    def __init__(self, lineage_tracker: LineageTracker):
        """
        Args:
            lineage_tracker: 리니지 추적기
        """
        self.tracker = lineage_tracker

    def analyze_deletion_impact(
        self,
        node_id: str,
        include_recommendations: bool = True
    ) -> ImpactResult:
        """
        노드 삭제 영향 분석

        Args:
            node_id: 삭제할 노드 ID
            include_recommendations: 권장 조치 포함 여부

        Returns:
            영향 분석 결과
        """
        result = ImpactResult(
            source_node_id=node_id,
            change_type="delete"
        )

        # 다운스트림 노드 조회
        downstream_nodes, downstream_edges = self.tracker.get_downstream(node_id)

        # 영향 분석
        impacted = self._analyze_downstream_impact(
            node_id,
            downstream_nodes,
            downstream_edges,
            change_type="delete"
        )

        result.impacted_nodes = impacted
        result.total_impacted = len(impacted)

        # 레벨별 집계
        for node in impacted:
            level = node.impact_level.value
            result.by_level[level] = result.by_level.get(level, 0) + 1
            node_type = node.node.node_type.value
            result.by_type[node_type] = result.by_type.get(node_type, 0) + 1

        # 권장 조치
        if include_recommendations:
            result.recommendations = self._generate_deletion_recommendations(
                node_id, impacted
            )

        return result

    def analyze_schema_change_impact(
        self,
        node_id: str,
        changed_fields: List[str],
        change_type: str = "modify"  # modify, add, remove
    ) -> ImpactResult:
        """
        스키마 변경 영향 분석

        Args:
            node_id: 변경되는 노드 ID
            changed_fields: 변경되는 필드 목록
            change_type: 변경 유형

        Returns:
            영향 분석 결과
        """
        result = ImpactResult(
            source_node_id=node_id,
            change_type=f"schema_{change_type}"
        )

        # 필드 노드 찾기
        field_nodes = self._find_related_field_nodes(node_id, changed_fields)

        # 각 필드별 다운스트림 분석
        all_impacted: Dict[str, ImpactedNode] = {}

        for field_node in field_nodes:
            downstream_nodes, downstream_edges = self.tracker.get_downstream(
                field_node.node_id
            )

            impacted = self._analyze_downstream_impact(
                field_node.node_id,
                downstream_nodes,
                downstream_edges,
                change_type=f"field_{change_type}"
            )

            for node in impacted:
                if node.node.node_id not in all_impacted:
                    all_impacted[node.node.node_id] = node
                else:
                    # 더 높은 영향 수준으로 업데이트
                    existing = all_impacted[node.node.node_id]
                    if self._compare_impact_level(node.impact_level, existing.impact_level) > 0:
                        all_impacted[node.node.node_id] = node

        result.impacted_nodes = list(all_impacted.values())
        result.total_impacted = len(result.impacted_nodes)

        # 집계
        for node in result.impacted_nodes:
            level = node.impact_level.value
            result.by_level[level] = result.by_level.get(level, 0) + 1
            node_type = node.node.node_type.value
            result.by_type[node_type] = result.by_type.get(node_type, 0) + 1

        # 권장 조치
        result.recommendations = self._generate_schema_change_recommendations(
            changed_fields, change_type, result.impacted_nodes
        )

        return result

    def analyze_source_unavailability(
        self,
        source_id: str
    ) -> ImpactResult:
        """
        소스 불가용 영향 분석

        Args:
            source_id: 소스 ID

        Returns:
            영향 분석 결과
        """
        # 소스 노드 찾기
        source_nodes = self.tracker.find_nodes(
            node_type=NodeType.SOURCE,
            source_id=source_id
        )

        if not source_nodes:
            return ImpactResult(
                source_node_id=source_id,
                change_type="unavailable",
                recommendations=["Source not found in lineage"]
            )

        # 삭제 영향 분석과 동일하게 처리
        result = self.analyze_deletion_impact(
            source_nodes[0].node_id,
            include_recommendations=False
        )
        result.change_type = "source_unavailable"

        # 소스 불가용 특화 권장 조치
        result.recommendations = [
            "데이터 소스 가용성 확인 필요",
            "대체 데이터 소스 검토",
            f"영향받는 파이프라인 {result.total_impacted}개 일시 중지 권장"
        ]

        # 크리티컬 노드가 있으면 추가 권장
        critical_count = result.by_level.get("critical", 0)
        if critical_count > 0:
            result.recommendations.insert(0, f"⚠️ 크리티컬 영향 {critical_count}건 - 즉시 조치 필요")

        return result

    def get_dependency_report(
        self,
        node_id: str
    ) -> Dict[str, Any]:
        """
        노드 의존성 보고서

        Args:
            node_id: 노드 ID

        Returns:
            의존성 보고서
        """
        # 업스트림 (이 노드가 의존하는 것)
        upstream_nodes, upstream_edges = self.tracker.get_upstream(node_id)

        # 다운스트림 (이 노드에 의존하는 것)
        downstream_nodes, downstream_edges = self.tracker.get_downstream(node_id)

        # 그래프 구축
        graph = LineageGraph()
        for node in upstream_nodes + downstream_nodes:
            graph.add_node(node)
        for edge in upstream_edges + downstream_edges:
            graph.add_edge(edge)

        # 통계
        stats = graph.get_statistics()

        return {
            "node_id": node_id,
            "upstream": {
                "count": len(upstream_nodes),
                "by_type": self._count_by_type(upstream_nodes),
                "nodes": [
                    {"id": n.node_id, "name": n.name, "type": n.node_type.value}
                    for n in upstream_nodes
                ]
            },
            "downstream": {
                "count": len(downstream_nodes),
                "by_type": self._count_by_type(downstream_nodes),
                "nodes": [
                    {"id": n.node_id, "name": n.name, "type": n.node_type.value}
                    for n in downstream_nodes
                ]
            },
            "graph_statistics": stats,
            "visualization": graph.to_visualization_format()
        }

    def get_data_flow_report(
        self,
        source_id: str
    ) -> Dict[str, Any]:
        """
        소스별 데이터 흐름 보고서

        Args:
            source_id: 소스 ID

        Returns:
            데이터 흐름 보고서
        """
        lineage = self.tracker.get_source_lineage(source_id)

        if "error" in lineage:
            return lineage

        # 그래프 구축
        graph = LineageGraph()
        nodes = [LineageNode.from_dict(n) for n in lineage.get("nodes", [])]
        edges = [LineageEdge.from_dict(e) for e in lineage.get("edges", [])]

        for node in nodes:
            graph.add_node(node)
        for edge in edges:
            graph.add_edge(edge)

        # 위상 정렬 (데이터 흐름 순서)
        flow_order = graph.topological_sort()

        # 스테이지별 분류
        stages = self._classify_stages(nodes, edges)

        return {
            "source_id": source_id,
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "stages": stages,
            "flow_order": flow_order,
            "cycles_detected": len(graph.detect_cycles()) > 0,
            "graph_statistics": graph.get_statistics(),
            "visualization": graph.to_visualization_format()
        }

    def _analyze_downstream_impact(
        self,
        source_node_id: str,
        downstream_nodes: List[LineageNode],
        downstream_edges: List[LineageEdge],
        change_type: str
    ) -> List[ImpactedNode]:
        """다운스트림 영향 분석"""
        impacted = []

        # 거리 계산
        distances = self._calculate_distances(source_node_id, downstream_edges)

        for node in downstream_nodes:
            if node.node_id == source_node_id:
                continue

            distance = distances.get(node.node_id, 99)

            # 영향 수준 결정
            impact_level = self._determine_impact_level(node, distance, change_type)

            # 영향 사유
            impact_reason = self._generate_impact_reason(node, distance, change_type)

            # 경로 계산
            path = self._find_shortest_path(
                source_node_id, node.node_id, downstream_edges
            )

            impacted.append(ImpactedNode(
                node=node,
                impact_level=impact_level,
                impact_reason=impact_reason,
                distance_from_source=distance,
                path=path
            ))

        # 영향 수준으로 정렬
        impacted.sort(key=lambda x: (
            -self._impact_level_to_int(x.impact_level),
            x.distance_from_source
        ))

        return impacted

    def _determine_impact_level(
        self,
        node: LineageNode,
        distance: int,
        change_type: str
    ) -> ImpactLevel:
        """영향 수준 결정"""
        # 거리 기반 기본 수준
        if distance == 1:
            base_level = ImpactLevel.HIGH
        elif distance <= 3:
            base_level = ImpactLevel.MEDIUM
        else:
            base_level = ImpactLevel.LOW

        # 노드 타입에 따른 조정
        if node.node_type == NodeType.REPORT:
            # 리포트는 영향이 더 큼
            base_level = self._increase_level(base_level)
        elif node.node_type == NodeType.MODEL:
            # ML 모델도 영향이 큼
            base_level = self._increase_level(base_level)

        # 변경 유형에 따른 조정
        if change_type == "delete":
            base_level = self._increase_level(base_level)

        return base_level

    def _increase_level(self, level: ImpactLevel) -> ImpactLevel:
        """영향 수준 증가"""
        order = [ImpactLevel.NONE, ImpactLevel.LOW, ImpactLevel.MEDIUM,
                 ImpactLevel.HIGH, ImpactLevel.CRITICAL]
        idx = order.index(level)
        return order[min(idx + 1, len(order) - 1)]

    def _impact_level_to_int(self, level: ImpactLevel) -> int:
        """영향 수준을 정수로 변환"""
        mapping = {
            ImpactLevel.NONE: 0,
            ImpactLevel.LOW: 1,
            ImpactLevel.MEDIUM: 2,
            ImpactLevel.HIGH: 3,
            ImpactLevel.CRITICAL: 4
        }
        return mapping.get(level, 0)

    def _compare_impact_level(self, a: ImpactLevel, b: ImpactLevel) -> int:
        """영향 수준 비교 (-1, 0, 1)"""
        return self._impact_level_to_int(a) - self._impact_level_to_int(b)

    def _generate_impact_reason(
        self,
        node: LineageNode,
        distance: int,
        change_type: str
    ) -> str:
        """영향 사유 생성"""
        if change_type == "delete":
            return f"Upstream data source will be deleted (distance: {distance})"
        elif change_type.startswith("field_"):
            return f"Field dependency affected by schema change (distance: {distance})"
        elif change_type == "source_unavailable":
            return f"Data source unavailability (distance: {distance})"
        else:
            return f"Data modification impact (distance: {distance})"

    def _calculate_distances(
        self,
        source_id: str,
        edges: List[LineageEdge]
    ) -> Dict[str, int]:
        """거리 계산 (BFS)"""
        adjacency = {}
        for edge in edges:
            if edge.source_node_id not in adjacency:
                adjacency[edge.source_node_id] = []
            adjacency[edge.source_node_id].append(edge.target_node_id)

        distances = {source_id: 0}
        queue = [source_id]

        while queue:
            current = queue.pop(0)
            current_dist = distances[current]

            for neighbor in adjacency.get(current, []):
                if neighbor not in distances:
                    distances[neighbor] = current_dist + 1
                    queue.append(neighbor)

        return distances

    def _find_shortest_path(
        self,
        start_id: str,
        end_id: str,
        edges: List[LineageEdge]
    ) -> List[str]:
        """최단 경로 찾기"""
        adjacency = {}
        for edge in edges:
            if edge.source_node_id not in adjacency:
                adjacency[edge.source_node_id] = []
            adjacency[edge.source_node_id].append(edge.target_node_id)

        visited = {start_id}
        queue = [(start_id, [start_id])]

        while queue:
            current, path = queue.pop(0)

            if current == end_id:
                return path

            for neighbor in adjacency.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))

        return []

    def _find_related_field_nodes(
        self,
        dataset_node_id: str,
        field_names: List[str]
    ) -> List[LineageNode]:
        """관련 필드 노드 찾기"""
        field_nodes = []
        all_fields = self.tracker.find_nodes(node_type=NodeType.FIELD)

        for field_node in all_fields:
            dataset = field_node.properties.get("dataset", "")
            if field_node.name in field_names:
                field_nodes.append(field_node)

        return field_nodes

    def _count_by_type(self, nodes: List[LineageNode]) -> Dict[str, int]:
        """타입별 카운트"""
        counts = {}
        for node in nodes:
            node_type = node.node_type.value
            counts[node_type] = counts.get(node_type, 0) + 1
        return counts

    def _classify_stages(
        self,
        nodes: List[LineageNode],
        edges: List[LineageEdge]
    ) -> List[Dict[str, Any]]:
        """스테이지별 분류"""
        stages = []

        # 소스 스테이지
        source_nodes = [n for n in nodes if n.node_type == NodeType.SOURCE]
        if source_nodes:
            stages.append({
                "stage": "extract",
                "description": "Data extraction from sources",
                "nodes": [{"id": n.node_id, "name": n.name} for n in source_nodes]
            })

        # 변환 스테이지
        transform_nodes = [n for n in nodes if n.node_type == NodeType.TRANSFORMATION]
        if transform_nodes:
            stages.append({
                "stage": "transform",
                "description": "Data transformations",
                "nodes": [{"id": n.node_id, "name": n.name} for n in transform_nodes]
            })

        # 데이터셋 스테이지
        dataset_nodes = [n for n in nodes if n.node_type == NodeType.DATASET]
        if dataset_nodes:
            stages.append({
                "stage": "load",
                "description": "Data loaded to datasets",
                "nodes": [{"id": n.node_id, "name": n.name} for n in dataset_nodes]
            })

        # 리포트 스테이지
        report_nodes = [n for n in nodes if n.node_type == NodeType.REPORT]
        if report_nodes:
            stages.append({
                "stage": "report",
                "description": "Reports and dashboards",
                "nodes": [{"id": n.node_id, "name": n.name} for n in report_nodes]
            })

        return stages

    def _generate_deletion_recommendations(
        self,
        node_id: str,
        impacted: List[ImpactedNode]
    ) -> List[str]:
        """삭제 관련 권장 조치 생성"""
        recommendations = []

        if not impacted:
            recommendations.append("No downstream dependencies found. Safe to delete.")
            return recommendations

        # 크리티컬 영향
        critical = [n for n in impacted if n.impact_level == ImpactLevel.CRITICAL]
        if critical:
            recommendations.append(
                f"⚠️ CRITICAL: {len(critical)} critical dependencies will break. "
                f"Review before deletion."
            )

        # 높은 영향
        high = [n for n in impacted if n.impact_level == ImpactLevel.HIGH]
        if high:
            recommendations.append(
                f"HIGH IMPACT: {len(high)} nodes with high dependency. "
                f"Consider migration plan."
            )

        # 리포트 영향
        reports = [n for n in impacted if n.node.node_type == NodeType.REPORT]
        if reports:
            recommendations.append(
                f"📊 {len(reports)} reports will be affected. "
                f"Notify stakeholders before deletion."
            )

        # 모델 영향
        models = [n for n in impacted if n.node.node_type == NodeType.MODEL]
        if models:
            recommendations.append(
                f"🤖 {len(models)} ML models will be affected. "
                f"Plan retraining strategy."
            )

        recommendations.append(
            f"Total impact: {len(impacted)} downstream nodes. "
            f"Recommended to create backup before deletion."
        )

        return recommendations

    def _generate_schema_change_recommendations(
        self,
        changed_fields: List[str],
        change_type: str,
        impacted: List[ImpactedNode]
    ) -> List[str]:
        """스키마 변경 관련 권장 조치 생성"""
        recommendations = []

        if change_type == "remove":
            recommendations.append(
                f"⚠️ Removing fields: {', '.join(changed_fields)}. "
                f"This may break downstream transformations."
            )

        if change_type == "modify":
            recommendations.append(
                f"Fields being modified: {', '.join(changed_fields)}. "
                f"Verify type compatibility with downstream."
            )

        if impacted:
            recommendations.append(
                f"Schema change affects {len(impacted)} downstream nodes. "
                f"Consider implementing schema versioning."
            )

            # 필드 레벨 영향
            field_impacts = [n for n in impacted if n.node.node_type == NodeType.FIELD]
            if field_impacts:
                recommendations.append(
                    f"{len(field_impacts)} derived fields will need updates."
                )

        return recommendations

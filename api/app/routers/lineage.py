"""
Data Lineage Router - 데이터 리니지 API

데이터 흐름 추적, 영향 분석, 계보 조회 API를 제공합니다.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from app.services.mongo_service import MongoService
from app.services.lineage import (
    LineageTracker,
    LineageNode,
    NodeType,
    EdgeType,
    LineageGraph,
    ImpactAnalyzer,
)
from app.core import get_logger
from app.auth.dependencies import require_auth, require_scope, require_admin, AuthContext

logger = get_logger(__name__)
router = APIRouter()


# ============== Dependencies ==============

def get_mongo():
    """MongoDB 서비스 의존성"""
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


def get_tracker(mongo: MongoService = Depends(get_mongo)) -> LineageTracker:
    """리니지 추적기 의존성"""
    return LineageTracker(mongo_service=mongo)


def get_analyzer(
    tracker: LineageTracker = Depends(get_tracker)
) -> ImpactAnalyzer:
    """영향 분석기 의존성"""
    return ImpactAnalyzer(lineage_tracker=tracker)


# ============== Request/Response Models ==============

class NodeCreateRequest(BaseModel):
    """노드 생성 요청"""
    name: str = Field(..., description="노드 이름")
    node_type: str = Field(..., description="노드 타입 (source, dataset, field, transformation)")
    qualified_name: Optional[str] = Field(None, description="전체 경로 이름")
    description: Optional[str] = None
    source_id: Optional[str] = None
    properties: Dict[str, Any] = Field(default_factory=dict)


class EdgeCreateRequest(BaseModel):
    """엣지 생성 요청"""
    source_node_id: str = Field(..., description="시작 노드 ID")
    target_node_id: str = Field(..., description="끝 노드 ID")
    edge_type: str = Field(..., description="엣지 타입 (extract, transform, load, derive)")
    transformation_logic: Optional[str] = None
    expression: Optional[str] = None


class ImpactAnalysisRequest(BaseModel):
    """영향 분석 요청"""
    node_id: str = Field(..., description="분석할 노드 ID")
    change_type: str = Field("delete", description="변경 유형 (delete, modify, unavailable)")
    changed_fields: Optional[List[str]] = Field(None, description="변경되는 필드 목록 (스키마 변경 시)")


# ============== Endpoints ==============

@router.get("/nodes")
async def list_nodes(
    node_type: Optional[str] = Query(None, description="노드 타입 필터"),
    source_id: Optional[str] = Query(None, description="소스 ID 필터"),
    name_pattern: Optional[str] = Query(None, description="이름 패턴 (정규식)"),
    limit: int = Query(100, ge=1, le=500),
    tracker: LineageTracker = Depends(get_tracker),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    리니지 노드 목록 조회
    """
    type_filter = NodeType(node_type) if node_type else None

    nodes = tracker.find_nodes(
        node_type=type_filter,
        source_id=source_id,
        name_pattern=name_pattern,
        limit=limit
    )

    return {
        "total": len(nodes),
        "nodes": [n.to_dict() for n in nodes]
    }


@router.get("/nodes/{node_id}")
async def get_node(
    node_id: str,
    tracker: LineageTracker = Depends(get_tracker),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    단일 노드 조회
    """
    node = tracker.get_node(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")

    return node.to_dict()


@router.post("/nodes")
async def create_node(
    request: NodeCreateRequest,
    tracker: LineageTracker = Depends(get_tracker),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    리니지 노드 생성
    """
    try:
        node_type = NodeType(request.node_type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid node_type. Must be one of: {[t.value for t in NodeType]}"
        )

    qualified_name = request.qualified_name or f"{node_type.value}://{request.name}"
    node_id = LineageNode.generate_id(node_type, qualified_name)

    node = LineageNode(
        node_id=node_id,
        node_type=node_type,
        name=request.name,
        qualified_name=qualified_name,
        description=request.description,
        source_id=request.source_id,
        properties=request.properties
    )

    tracker.register_node(node)

    return {
        "success": True,
        "node_id": node_id,
        "node": node.to_dict()
    }


@router.post("/edges")
async def create_edge(
    request: EdgeCreateRequest,
    tracker: LineageTracker = Depends(get_tracker),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    리니지 엣지 생성
    """
    try:
        edge_type = EdgeType(request.edge_type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid edge_type. Must be one of: {[t.value for t in EdgeType]}"
        )

    from app.services.lineage.tracker import LineageEdge

    edge_id = LineageEdge.generate_id(
        request.source_node_id,
        request.target_node_id,
        edge_type
    )

    edge = LineageEdge(
        edge_id=edge_id,
        source_node_id=request.source_node_id,
        target_node_id=request.target_node_id,
        edge_type=edge_type,
        transformation_logic=request.transformation_logic,
        expression=request.expression
    )

    tracker.add_edge(edge)

    return {
        "success": True,
        "edge_id": edge_id,
        "edge": edge.to_dict()
    }


@router.get("/nodes/{node_id}/upstream")
async def get_upstream_lineage(
    node_id: str,
    depth: int = Query(10, ge=1, le=50, description="최대 탐색 깊이"),
    tracker: LineageTracker = Depends(get_tracker),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    업스트림 (상위) 리니지 조회

    이 노드가 의존하는 상위 노드들을 조회합니다.
    """
    nodes, edges = tracker.get_upstream(node_id, depth=depth)

    # 그래프 시각화 데이터
    graph = LineageGraph()
    for node in nodes:
        graph.add_node(node)
    for edge in edges:
        graph.add_edge(edge)

    return {
        "node_id": node_id,
        "direction": "upstream",
        "depth": depth,
        "total_nodes": len(nodes),
        "total_edges": len(edges),
        "nodes": [n.to_dict() for n in nodes],
        "edges": [e.to_dict() for e in edges],
        "visualization": graph.to_visualization_format()
    }


@router.get("/nodes/{node_id}/downstream")
async def get_downstream_lineage(
    node_id: str,
    depth: int = Query(10, ge=1, le=50, description="최대 탐색 깊이"),
    tracker: LineageTracker = Depends(get_tracker),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    다운스트림 (하위) 리니지 조회

    이 노드에 의존하는 하위 노드들을 조회합니다.
    """
    nodes, edges = tracker.get_downstream(node_id, depth=depth)

    # 그래프 시각화 데이터
    graph = LineageGraph()
    for node in nodes:
        graph.add_node(node)
    for edge in edges:
        graph.add_edge(edge)

    return {
        "node_id": node_id,
        "direction": "downstream",
        "depth": depth,
        "total_nodes": len(nodes),
        "total_edges": len(edges),
        "nodes": [n.to_dict() for n in nodes],
        "edges": [e.to_dict() for e in edges],
        "visualization": graph.to_visualization_format()
    }


@router.get("/nodes/{node_id}/full")
async def get_full_lineage(
    node_id: str,
    upstream_depth: int = Query(10, ge=1, le=50),
    downstream_depth: int = Query(10, ge=1, le=50),
    tracker: LineageTracker = Depends(get_tracker),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    전체 리니지 조회 (업스트림 + 다운스트림)
    """
    return tracker.get_full_lineage(
        node_id,
        upstream_depth=upstream_depth,
        downstream_depth=downstream_depth
    )


@router.get("/sources/{source_id}/lineage")
async def get_source_lineage(
    source_id: str,
    tracker: LineageTracker = Depends(get_tracker),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    소스별 리니지 조회
    """
    return tracker.get_source_lineage(source_id)


@router.get("/runs/{run_id}/lineage")
async def get_run_lineage(
    run_id: str,
    tracker: LineageTracker = Depends(get_tracker),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    실행별 리니지 조회
    """
    result = tracker.get_run_lineage(run_id)
    if not result:
        raise HTTPException(status_code=404, detail="Run lineage not found")
    return result


# ============== Impact Analysis Endpoints ==============

@router.post("/impact/analyze")
async def analyze_impact(
    request: ImpactAnalysisRequest,
    analyzer: ImpactAnalyzer = Depends(get_analyzer),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    영향 분석 실행

    노드 삭제, 스키마 변경, 소스 불가용 등의 영향을 분석합니다.
    """
    if request.change_type == "delete":
        result = analyzer.analyze_deletion_impact(request.node_id)

    elif request.change_type == "schema_change":
        if not request.changed_fields:
            raise HTTPException(
                status_code=400,
                detail="changed_fields required for schema_change analysis"
            )
        result = analyzer.analyze_schema_change_impact(
            request.node_id,
            request.changed_fields
        )

    elif request.change_type == "unavailable":
        result = analyzer.analyze_source_unavailability(request.node_id)

    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid change_type. Must be: delete, schema_change, unavailable"
        )

    return result.to_dict()


@router.get("/nodes/{node_id}/dependencies")
async def get_dependencies(
    node_id: str,
    analyzer: ImpactAnalyzer = Depends(get_analyzer),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    노드 의존성 보고서
    """
    return analyzer.get_dependency_report(node_id)


@router.get("/sources/{source_id}/data-flow")
async def get_data_flow(
    source_id: str,
    analyzer: ImpactAnalyzer = Depends(get_analyzer),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    소스별 데이터 흐름 보고서
    """
    return analyzer.get_data_flow_report(source_id)


# ============== Statistics Endpoints ==============

@router.get("/statistics")
async def get_lineage_statistics(
    tracker: LineageTracker = Depends(get_tracker),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth)
) -> Dict[str, Any]:
    """
    리니지 통계
    """
    # 노드 통계
    node_stats = {}
    for node_type in NodeType:
        count = mongo.db[tracker.NODES_COLLECTION].count_documents({
            "node_type": node_type.value
        })
        node_stats[node_type.value] = count

    # 엣지 통계
    edge_stats = {}
    for edge_type in EdgeType:
        count = mongo.db[tracker.EDGES_COLLECTION].count_documents({
            "edge_type": edge_type.value
        })
        edge_stats[edge_type.value] = count

    # 최근 실행
    recent_runs = list(
        mongo.db[tracker.RUNS_COLLECTION]
        .find()
        .sort("started_at", -1)
        .limit(10)
    )

    return {
        "total_nodes": sum(node_stats.values()),
        "total_edges": sum(edge_stats.values()),
        "nodes_by_type": node_stats,
        "edges_by_type": edge_stats,
        "recent_runs": [
            {
                "run_id": r["run_id"],
                "dag_id": r.get("dag_id"),
                "source_id": r.get("source_id"),
                "status": r.get("status"),
                "started_at": r.get("started_at").isoformat() if r.get("started_at") else None,
                "nodes_count": len(r.get("nodes", [])),
                "edges_count": len(r.get("edges", []))
            }
            for r in recent_runs
        ]
    }
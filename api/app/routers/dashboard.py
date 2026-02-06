"""
Dashboard Router.

Endpoints for dashboard data and system overview.
"""

import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends

from app.models.schemas import DashboardResponse
from app.services.mongo_service import MongoService
from app.services.airflow_trigger import AirflowTrigger
from app.auth.dependencies import require_auth, AuthContext

logger = logging.getLogger(__name__)
router = APIRouter()


def get_mongo():
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


@router.get("", response_model=DashboardResponse)
async def get_dashboard(
    optimized: bool = Query(True, description="Use optimized aggregation queries"),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
):
    """
    Get dashboard overview data.

    N+1 최적화: optimized=True (기본값) 시 $facet을 사용하여
    여러 count 쿼리를 단일 aggregation으로 병합 처리
    - 기존: 6개 개별 count 쿼리 + 1개 find 쿼리
    - 최적화 후: 3개 aggregation 쿼리 (sources, crawlers, results 각각 $facet)
    """
    if optimized:
        stats = mongo.get_dashboard_stats_optimized()
    else:
        stats = mongo.get_dashboard_stats()
    return stats


@router.get("/recent-activity")
async def get_recent_activity(
    hours: int = 24,
    optimized: bool = Query(True, description="Use optimized aggregation with $lookup"),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
):
    """
    Get recent system activity.

    N+1 최적화: optimized=True (기본값) 시 $lookup을 사용하여
    각 결과/에러/변경 레코드에 대해 소스 이름을 개별 조회하지 않고
    aggregation pipeline에서 조인하여 처리

    - 기존: 3개 find 쿼리 (소스 이름 없음, 또는 N개 추가 쿼리 필요)
    - 최적화 후: 3개 aggregation 쿼리 ($lookup으로 소스 이름 포함)
    """
    if optimized:
        return mongo.get_recent_activity_optimized(hours=hours)

    # 기존 방식 (하위 호환성)
    cutoff = datetime.utcnow() - timedelta(hours=hours)

    # Recent crawl results
    recent_results = list(mongo.db.crawl_results.find(
        {"executed_at": {"$gte": cutoff}},
        {"source_id": 1, "status": 1, "executed_at": 1, "record_count": 1, "execution_time_ms": 1}
    ).sort("executed_at", -1).limit(50))

    # Recent errors
    recent_errors = list(mongo.db.error_logs.find(
        {"created_at": {"$gte": cutoff}},
        {"source_id": 1, "error_code": 1, "message": 1, "resolved": 1, "created_at": 1}
    ).sort("created_at", -1).limit(20))

    # Recent code changes
    recent_changes = list(mongo.db.crawler_history.find(
        {"changed_at": {"$gte": cutoff}},
        {"crawler_id": 1, "version": 1, "change_reason": 1, "changed_at": 1, "changed_by": 1}
    ).sort("changed_at", -1).limit(20))

    # Serialize ObjectIds
    for item in recent_results + recent_errors + recent_changes:
        for key in list(item.keys()):
            if hasattr(item[key], '__str__') and key == '_id':
                item[key] = str(item[key])
            elif key in ['source_id', 'crawler_id'] and item[key]:
                item[key] = str(item[key])

    return {
        "period_hours": hours,
        "crawl_results": recent_results,
        "errors": recent_errors,
        "code_changes": recent_changes
    }


@router.get("/sources-status")
async def get_sources_status(mongo: MongoService = Depends(get_mongo), auth: AuthContext = Depends(require_auth)):
    """
    Get status overview of all sources.

    N+1 최적화: aggregation pipeline을 사용하여
    - 상태별 카운트를 $group으로 계산 (개별 count 쿼리 제거)
    - 문제 있는 소스를 $match로 필터링 (메모리 내 반복 제거)
    """
    # 최적화: 상태별 카운트를 단일 aggregation으로 계산
    status_pipeline = [
        {'$group': {
            '_id': '$status',
            'count': {'$sum': 1}
        }}
    ]
    status_results = list(mongo.db.sources.aggregate(status_pipeline))

    status_summary = {
        "active": 0,
        "inactive": 0,
        "error": 0
    }
    total_sources = 0
    for r in status_results:
        status_key = r['_id'] or 'inactive'
        status_summary[status_key] = status_summary.get(status_key, 0) + r['count']
        total_sources += r['count']

    # 최적화: 문제 있는 소스만 직접 쿼리 (전체 목록 조회 후 필터링 대신)
    issues_pipeline = [
        {'$match': {'error_count': {'$gt': 3}}},
        {'$project': {
            '_id': 1,
            'name': 1,
            'error_count': 1,
            'last_run': 1,
            'last_success': 1
        }},
        {'$sort': {'error_count': -1}},
        {'$limit': 50}  # 문제 소스 수 제한
    ]
    sources_with_issues_raw = list(mongo.db.sources.aggregate(issues_pipeline))

    sources_with_issues = [
        {
            "id": str(s['_id']),
            "name": s['name'],
            "error_count": s['error_count'],
            "last_run": s.get('last_run'),
            "last_success": s.get('last_success')
        }
        for s in sources_with_issues_raw
    ]

    return {
        "status_summary": status_summary,
        "total_sources": total_sources,
        "sources_with_issues": sources_with_issues
    }


@router.get("/execution-trends")
async def get_execution_trends(
    days: int = 7,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth),
):
    """Get execution trends over time."""
    cutoff = datetime.utcnow() - timedelta(days=days)

    # Aggregate by day
    pipeline = [
        {"$match": {"executed_at": {"$gte": cutoff}}},
        {
            "$group": {
                "_id": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$executed_at"}
                },
                "total": {"$sum": 1},
                "success": {
                    "$sum": {"$cond": [{"$eq": ["$status", "success"]}, 1, 0]}
                },
                "failed": {
                    "$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}
                },
                "avg_time_ms": {"$avg": "$execution_time_ms"},
                "total_records": {"$sum": "$record_count"}
            }
        },
        {"$sort": {"_id": 1}}
    ]

    trends = list(mongo.db.crawl_results.aggregate(pipeline))

    return {
        "period_days": days,
        "daily_stats": [
            {
                "date": t["_id"],
                "total": t["total"],
                "success": t["success"],
                "failed": t["failed"],
                "success_rate": round(t["success"] / t["total"] * 100, 2) if t["total"] > 0 else 0,
                "avg_time_ms": round(t["avg_time_ms"], 2) if t["avg_time_ms"] else 0,
                "total_records": t["total_records"]
            }
            for t in trends
        ]
    }


@router.get("/system-health")
async def get_system_health(mongo: MongoService = Depends(get_mongo), auth: AuthContext = Depends(require_auth)):
    """Get overall system health indicators."""
    # MongoDB health
    try:
        mongo.db.command('ping')
        mongo_status = "healthy"
    except Exception as e:
        mongo_status = f"error: {e}"

    # Airflow health
    airflow = AirflowTrigger()
    try:
        result = await airflow.get_dag_runs("source_manager", limit=1)
        airflow_status = "healthy" if "error" not in result else f"error: {result.get('error')}"
    except Exception as e:
        airflow_status = f"error: {e}"

    # Calculate system metrics
    stats = mongo.get_dashboard_stats()

    health_score = 100
    issues = []

    # Check for high error rate
    if stats['recent_executions']['total'] > 0:
        error_rate = stats['recent_executions']['failed'] / stats['recent_executions']['total']
        if error_rate > 0.3:
            health_score -= 30
            issues.append("High error rate (>30%)")
        elif error_rate > 0.1:
            health_score -= 10
            issues.append("Elevated error rate (>10%)")

    # Check for unresolved errors
    if stats['unresolved_errors'] > 10:
        health_score -= 20
        issues.append(f"{stats['unresolved_errors']} unresolved errors")
    elif stats['unresolved_errors'] > 5:
        health_score -= 10
        issues.append(f"{stats['unresolved_errors']} unresolved errors")

    # Check for error sources
    if stats['sources']['error'] > 0:
        health_score -= 10 * min(stats['sources']['error'], 3)
        issues.append(f"{stats['sources']['error']} sources in error state")

    return {
        "health_score": max(0, health_score),
        "status": "healthy" if health_score >= 80 else "degraded" if health_score >= 50 else "critical",
        "components": {
            "mongodb": mongo_status,
            "airflow": airflow_status
        },
        "issues": issues,
        "metrics": stats,
        "timestamp": datetime.utcnow().isoformat()
    }

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

logger = logging.getLogger(__name__)
router = APIRouter()


def get_mongo():
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


@router.get("", response_model=DashboardResponse)
async def get_dashboard(mongo: MongoService = Depends(get_mongo)):
    """Get dashboard overview data."""
    stats = mongo.get_dashboard_stats()
    return stats


@router.get("/recent-activity")
async def get_recent_activity(
    hours: int = 24,
    mongo: MongoService = Depends(get_mongo)
):
    """Get recent system activity."""
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
async def get_sources_status(mongo: MongoService = Depends(get_mongo)):
    """Get status overview of all sources."""
    sources = mongo.list_sources(limit=500)

    status_summary = {
        "active": 0,
        "inactive": 0,
        "error": 0
    }

    sources_with_issues = []

    for source in sources:
        status = source.get('status', 'inactive')
        status_summary[status] = status_summary.get(status, 0) + 1

        # Flag sources with issues
        if source.get('error_count', 0) > 3:
            sources_with_issues.append({
                "id": source['_id'],
                "name": source['name'],
                "error_count": source['error_count'],
                "last_run": source.get('last_run'),
                "last_success": source.get('last_success')
            })

    return {
        "status_summary": status_summary,
        "total_sources": len(sources),
        "sources_with_issues": sources_with_issues
    }


@router.get("/execution-trends")
async def get_execution_trends(
    days: int = 7,
    mongo: MongoService = Depends(get_mongo)
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
async def get_system_health(mongo: MongoService = Depends(get_mongo)):
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

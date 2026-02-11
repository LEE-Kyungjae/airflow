"""
Observability Dashboard - Aggregated metrics and KPIs.

Provides comprehensive dashboard data for ETL observability including:
- Executive summary KPIs
- Real-time health status
- Trend analysis
- Source performance rankings
- Alert and SLA summaries
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict

try:
    from app.core import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
from .metrics import MetricsCollector
from .alerts import AlertRuleEngine
from .sla import SLAMonitor
from .freshness import FreshnessTracker


@dataclass
class HealthStatus:
    """Overall system health status."""
    status: str              # healthy, degraded, unhealthy
    score: float             # 0-100 health score
    components: Dict[str, str]  # Component-level health
    issues: List[str]        # Active issues
    last_checked: datetime


@dataclass
class DashboardSummary:
    """Executive summary for dashboard."""
    # Overall metrics
    total_sources: int
    active_sources: int
    total_runs_24h: int
    success_rate_24h: float
    records_processed_24h: int
    errors_24h: int

    # Health indicators
    health_status: str
    health_score: float

    # Quality metrics
    avg_quality_score: float
    quality_trend: str  # improving, stable, declining

    # Freshness
    fresh_sources: int
    stale_sources: int
    critical_freshness: int

    # Alerts & SLA
    active_alerts: int
    sla_compliance_rate: float
    pending_reviews: int

    # Timestamp
    generated_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data["generated_at"] = self.generated_at.isoformat()
        return data


class ObservabilityDashboard:
    """
    Comprehensive observability dashboard service.

    Aggregates data from all observability components to provide
    a unified view of ETL pipeline health and performance.
    """

    def __init__(
        self,
        mongo_service=None,
        metrics_collector: Optional[MetricsCollector] = None,
        alert_engine: Optional[AlertRuleEngine] = None,
        sla_monitor: Optional[SLAMonitor] = None,
        freshness_tracker: Optional[FreshnessTracker] = None
    ):
        """
        Initialize dashboard service.

        Args:
            mongo_service: MongoDB service instance
            metrics_collector: MetricsCollector instance
            alert_engine: AlertRuleEngine instance
            sla_monitor: SLAMonitor instance
            freshness_tracker: FreshnessTracker instance
        """
        self.mongo = mongo_service
        self.metrics = metrics_collector or MetricsCollector(mongo_service)
        self.alerts = alert_engine or AlertRuleEngine(mongo_service)
        self.sla = sla_monitor or SLAMonitor(mongo_service)
        self.freshness = freshness_tracker or FreshnessTracker(mongo_service)

    # ==================== Executive Summary ====================

    async def get_executive_summary(self) -> DashboardSummary:
        """
        Get executive summary for dashboard.

        Returns:
            DashboardSummary with key metrics
        """
        now = datetime.utcnow()

        # Get metrics stats
        metrics_stats = await self.metrics.get_aggregate_stats(hours=24)

        # Get source counts
        source_counts = await self._get_source_counts()

        # Get freshness summary
        freshness_summary = await self.freshness.get_freshness_summary()

        # Get alert counts
        alert_counts = await self.alerts.get_active_alerts_count()

        # Get SLA compliance
        sla_compliance = await self.sla.get_compliance_summary(hours=24)

        # Get pending reviews
        pending_reviews = await self.metrics.get_pending_review_count()

        # Calculate quality trend
        quality_trend = await self._calculate_quality_trend()

        # Calculate health
        health = await self._calculate_health_status()

        return DashboardSummary(
            total_sources=source_counts.get("total", 0),
            active_sources=source_counts.get("active", 0),
            total_runs_24h=metrics_stats.get("totals", {}).get("runs", 0),
            success_rate_24h=metrics_stats.get("totals", {}).get("success_rate", 0),
            records_processed_24h=metrics_stats.get("totals", {}).get("records", 0),
            errors_24h=metrics_stats.get("totals", {}).get("errors", 0),
            health_status=health.status,
            health_score=health.score,
            avg_quality_score=await self._get_avg_quality_score(),
            quality_trend=quality_trend,
            fresh_sources=freshness_summary.get("fresh", 0),
            stale_sources=freshness_summary.get("stale", 0),
            critical_freshness=freshness_summary.get("critical", 0),
            active_alerts=sum(alert_counts.values()),
            sla_compliance_rate=sla_compliance.get("compliance_rate", 100),
            pending_reviews=pending_reviews,
            generated_at=now
        )

    async def _get_source_counts(self) -> Dict[str, int]:
        """Get source counts by status."""
        if not self.mongo:
            return {"total": 0, "active": 0}

        try:
            total = self.mongo.db.sources.count_documents({})
            active = self.mongo.db.sources.count_documents({"status": "active"})
            error = self.mongo.db.sources.count_documents({"status": "error"})

            return {
                "total": total,
                "active": active,
                "error": error,
                "inactive": total - active - error
            }
        except Exception as e:
            logger.error("Failed to get source counts", error=str(e))
            return {"total": 0, "active": 0}

    async def _get_avg_quality_score(self) -> float:
        """Get average quality score for last 24 hours."""
        if not self.mongo:
            return 0.0

        since = datetime.utcnow() - timedelta(hours=24)

        try:
            pipeline = [
                {
                    "$match": {
                        "started_at": {"$gte": since},
                        "quality_score": {"$ne": None}
                    }
                },
                {"$group": {"_id": None, "avg": {"$avg": "$quality_score"}}}
            ]
            results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
            return round(results[0]["avg"], 2) if results else 0.0
        except Exception as e:
            logger.error("Failed to get avg quality score", error=str(e))
            return 0.0

    async def _calculate_quality_trend(self) -> str:
        """Calculate quality score trend."""
        if not self.mongo:
            return "stable"

        try:
            # Compare last 24h vs previous 24h
            now = datetime.utcnow()
            recent_start = now - timedelta(hours=24)
            previous_start = now - timedelta(hours=48)

            def get_avg(start: datetime, end: datetime) -> float:
                pipeline = [
                    {
                        "$match": {
                            "started_at": {"$gte": start, "$lt": end},
                            "quality_score": {"$ne": None}
                        }
                    },
                    {"$group": {"_id": None, "avg": {"$avg": "$quality_score"}}}
                ]
                results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
                return results[0]["avg"] if results else 0

            recent_avg = get_avg(recent_start, now)
            previous_avg = get_avg(previous_start, recent_start)

            if previous_avg == 0:
                return "stable"

            change = ((recent_avg - previous_avg) / previous_avg) * 100

            if change > 5:
                return "improving"
            elif change < -5:
                return "declining"
            else:
                return "stable"

        except Exception as e:
            logger.error("Failed to calculate quality trend", error=str(e))
            return "stable"

    async def _calculate_health_status(self) -> HealthStatus:
        """Calculate overall system health."""
        now = datetime.utcnow()
        issues = []
        component_health = {}

        # Check metrics health
        metrics_stats = await self.metrics.get_aggregate_stats(hours=1)
        success_rate = metrics_stats.get("totals", {}).get("success_rate", 100)
        if success_rate < 80:
            issues.append(f"Low success rate: {success_rate}%")
            component_health["metrics"] = "unhealthy"
        elif success_rate < 95:
            component_health["metrics"] = "degraded"
        else:
            component_health["metrics"] = "healthy"

        # Check freshness health
        freshness_summary = await self.freshness.get_freshness_summary()
        total_sources = freshness_summary.get("total_sources", 1) or 1
        critical = freshness_summary.get("critical", 0)
        stale = freshness_summary.get("stale", 0)

        if critical > 0:
            issues.append(f"{critical} sources with critical freshness")
            component_health["freshness"] = "unhealthy"
        elif stale > 0:
            component_health["freshness"] = "degraded"
        else:
            component_health["freshness"] = "healthy"

        # Check SLA health
        sla_compliance = await self.sla.get_compliance_summary(hours=24)
        compliance_rate = sla_compliance.get("compliance_rate", 100)
        if compliance_rate < 90:
            issues.append(f"SLA compliance at {compliance_rate}%")
            component_health["sla"] = "unhealthy"
        elif compliance_rate < 95:
            component_health["sla"] = "degraded"
        else:
            component_health["sla"] = "healthy"

        # Check alerts health
        alert_counts = await self.alerts.get_active_alerts_count()
        critical_alerts = alert_counts.get("critical", 0)
        if critical_alerts > 0:
            issues.append(f"{critical_alerts} critical alerts")
            component_health["alerts"] = "unhealthy"
        elif sum(alert_counts.values()) > 5:
            component_health["alerts"] = "degraded"
        else:
            component_health["alerts"] = "healthy"

        # Calculate overall score
        health_scores = {
            "healthy": 100,
            "degraded": 70,
            "unhealthy": 30
        }
        scores = [health_scores.get(h, 50) for h in component_health.values()]
        overall_score = sum(scores) / len(scores) if scores else 100

        # Determine overall status
        if "unhealthy" in component_health.values():
            overall_status = "unhealthy"
        elif "degraded" in component_health.values():
            overall_status = "degraded"
        else:
            overall_status = "healthy"

        return HealthStatus(
            status=overall_status,
            score=round(overall_score, 1),
            components=component_health,
            issues=issues,
            last_checked=now
        )

    # ==================== Detailed Analytics ====================

    async def get_source_performance(
        self,
        hours: int = 24,
        limit: int = 20
    ) -> List[Dict]:
        """
        Get source performance rankings.

        Args:
            hours: Time window
            limit: Maximum sources to return

        Returns:
            List of source performance metrics
        """
        source_stats = await self.metrics.get_source_stats(hours=hours, limit=limit)

        # Enrich with freshness data
        for stats in source_stats:
            source_id = stats.get("source_id")
            if source_id:
                state = await self.freshness.check_freshness(source_id)
                stats["freshness_status"] = state.status.value
                stats["data_age_hours"] = state.data_age_hours

        # Calculate performance score for each source
        for stats in source_stats:
            score = self._calculate_source_score(stats)
            stats["performance_score"] = score

        # Sort by performance score
        source_stats.sort(key=lambda x: x.get("performance_score", 0), reverse=True)

        return source_stats

    def _calculate_source_score(self, stats: Dict) -> float:
        """Calculate performance score for a source."""
        weights = {
            "success_rate": 0.3,
            "avg_quality_score": 0.25,
            "freshness": 0.25,
            "throughput": 0.2
        }

        score = 0.0

        # Success rate score (0-100)
        success_rate = stats.get("success_rate", 0)
        score += weights["success_rate"] * success_rate

        # Quality score (0-100)
        quality = stats.get("avg_quality_score", 0)
        score += weights["avg_quality_score"] * quality

        # Freshness score (100 if fresh, 50 if stale, 0 if critical)
        freshness_status = stats.get("freshness_status", "unknown")
        freshness_scores = {
            "fresh": 100,
            "stale": 50,
            "critical": 0,
            "unknown": 50,
            "disabled": 75
        }
        score += weights["freshness"] * freshness_scores.get(freshness_status, 50)

        # Throughput score (normalized)
        # Higher throughput is generally better, but cap at 100
        records = stats.get("total_records", 0)
        runs = stats.get("total_runs", 1) or 1
        avg_records = records / runs
        throughput_score = min(100, (avg_records / 100) * 100)
        score += weights["throughput"] * throughput_score

        return round(score, 1)

    async def get_category_analytics(
        self,
        hours: int = 24
    ) -> List[Dict]:
        """
        Get analytics grouped by category.

        Args:
            hours: Time window

        Returns:
            List of category statistics
        """
        return await self.metrics.get_category_stats(hours=hours)

    async def get_error_analytics(
        self,
        source_id: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Get comprehensive error analytics.

        Args:
            source_id: Optional source filter
            hours: Time window

        Returns:
            Error analytics
        """
        distribution = await self.metrics.get_error_distribution(
            source_id=source_id, hours=hours
        )

        # Get error trend
        trend = await self._get_error_trend(source_id, hours)

        # Get top error sources
        top_sources = await self._get_top_error_sources(hours)

        return {
            "distribution": distribution,
            "trend": trend,
            "top_sources": top_sources
        }

    async def _get_error_trend(
        self,
        source_id: Optional[str],
        hours: int
    ) -> List[Dict]:
        """Get hourly error trend."""
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)
        match_stage = {
            "started_at": {"$gte": since},
            "error_count": {"$gt": 0}
        }
        if source_id:
            match_stage["source_id"] = source_id

        pipeline = [
            {"$match": match_stage},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:00:00Z",
                            "date": "$started_at"
                        }
                    },
                    "error_count": {"$sum": "$error_count"},
                    "runs_with_errors": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]

        try:
            results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
            return [
                {
                    "hour": r["_id"],
                    "errors": r["error_count"],
                    "runs_with_errors": r["runs_with_errors"]
                }
                for r in results
            ]
        except Exception as e:
            logger.error("Failed to get error trend", error=str(e))
            return []

    async def _get_top_error_sources(
        self,
        hours: int,
        limit: int = 10
    ) -> List[Dict]:
        """Get sources with most errors."""
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)

        pipeline = [
            {"$match": {"started_at": {"$gte": since}, "error_count": {"$gt": 0}}},
            {
                "$group": {
                    "_id": "$source_id",
                    "total_errors": {"$sum": "$error_count"},
                    "runs_with_errors": {"$sum": 1},
                    "last_error": {"$max": "$last_error"}
                }
            },
            {"$sort": {"total_errors": -1}},
            {"$limit": limit}
        ]

        try:
            results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
            return [
                {
                    "source_id": r["_id"],
                    "total_errors": r["total_errors"],
                    "runs_with_errors": r["runs_with_errors"],
                    "last_error": r["last_error"]
                }
                for r in results
            ]
        except Exception as e:
            logger.error("Failed to get top error sources", error=str(e))
            return []

    # ==================== Time-Series Data ====================

    async def get_execution_timeline(
        self,
        source_id: Optional[str] = None,
        hours: int = 24
    ) -> List[Dict]:
        """
        Get execution timeline for visualization.

        Args:
            source_id: Optional source filter
            hours: Time window

        Returns:
            Hourly execution data
        """
        return await self.metrics.get_hourly_trend(
            source_id=source_id, hours=hours
        )

    async def get_quality_timeline(
        self,
        source_id: Optional[str] = None,
        hours: int = 24
    ) -> List[Dict]:
        """
        Get quality score timeline.

        Args:
            source_id: Optional source filter
            hours: Time window

        Returns:
            Hourly quality data
        """
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)
        match_stage = {
            "started_at": {"$gte": since},
            "quality_score": {"$ne": None}
        }
        if source_id:
            match_stage["source_id"] = source_id

        pipeline = [
            {"$match": match_stage},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:00:00Z",
                            "date": "$started_at"
                        }
                    },
                    "avg_quality": {"$avg": "$quality_score"},
                    "min_quality": {"$min": "$quality_score"},
                    "max_quality": {"$max": "$quality_score"},
                    "samples": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]

        try:
            results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
            return [
                {
                    "hour": r["_id"],
                    "avg_quality": round(r["avg_quality"], 2),
                    "min_quality": round(r["min_quality"], 2),
                    "max_quality": round(r["max_quality"], 2),
                    "samples": r["samples"]
                }
                for r in results
            ]
        except Exception as e:
            logger.error("Failed to get quality timeline", error=str(e))
            return []

    # ==================== Combined Dashboard Data ====================

    async def get_full_dashboard(self) -> Dict[str, Any]:
        """
        Get all dashboard data in a single call.

        Returns:
            Complete dashboard data
        """
        summary = await self.get_executive_summary()

        return {
            "summary": summary.to_dict(),
            "health": (await self._calculate_health_status()).__dict__,
            "source_performance": await self.get_source_performance(
                hours=24, limit=10
            ),
            "category_analytics": await self.get_category_analytics(hours=24),
            "error_analytics": await self.get_error_analytics(hours=24),
            "execution_timeline": await self.get_execution_timeline(hours=24),
            "quality_timeline": await self.get_quality_timeline(hours=24),
            "freshness": await self.freshness.get_freshness_summary(),
            "sla_compliance": await self.sla.get_compliance_summary(hours=24),
            "recent_alerts": await self.alerts.get_alert_history(hours=24, limit=10),
            "recent_breaches": await self.sla.get_recent_breaches(hours=24, limit=10),
        }

    async def get_source_dashboard(
        self,
        source_id: str,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Get dashboard data for a specific source.

        Args:
            source_id: Source identifier
            hours: Time window

        Returns:
            Source-specific dashboard data
        """
        # Get metrics
        metrics = await self.metrics.get_metrics_by_source(
            source_id=source_id, hours=hours, limit=100
        )

        # Get aggregate stats
        stats = await self.metrics.get_aggregate_stats(
            source_id=source_id, hours=hours
        )

        # Get freshness
        freshness = await self.freshness.check_freshness(source_id)

        # Get error distribution
        errors = await self.metrics.get_error_distribution(
            source_id=source_id, hours=hours
        )

        # Get execution timeline
        timeline = await self.metrics.get_hourly_trend(
            source_id=source_id, hours=hours
        )

        # Get alerts
        alerts = await self.alerts.get_alert_history(
            source_id=source_id, hours=hours, limit=20
        )

        # Get SLA breaches
        breaches = await self.sla.get_recent_breaches(
            source_id=source_id, hours=hours
        )

        return {
            "source_id": source_id,
            "period_hours": hours,
            "metrics_summary": stats,
            "freshness": freshness.to_dict(),
            "error_distribution": errors,
            "execution_timeline": timeline,
            "recent_alerts": alerts,
            "sla_breaches": breaches,
            "recent_runs": metrics[:20],
        }

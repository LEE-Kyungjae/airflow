"""
Pipeline Metrics Collection and Storage.

Provides comprehensive metrics collection for ETL pipeline executions including:
- Execution time tracking
- Record counts and throughput
- Error rates and failure tracking
- Quality scores
- Resource utilization
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from bson import ObjectId

from app.core import get_logger

logger = get_logger(__name__)


class MetricType(str, Enum):
    """Types of metrics that can be collected."""
    EXECUTION = "execution"       # Pipeline run metrics
    THROUGHPUT = "throughput"     # Records processed per time
    QUALITY = "quality"           # Data quality scores
    ERROR = "error"               # Error counts and rates
    LATENCY = "latency"           # Processing latency
    RESOURCE = "resource"         # Resource utilization


@dataclass
class PipelineMetric:
    """
    Pipeline execution metric record.

    Captures comprehensive metrics for a single pipeline run.
    """
    # Identifiers
    source_id: str
    run_id: str
    crawler_id: Optional[str] = None
    dag_id: Optional[str] = None

    # Timing
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    execution_time_ms: int = 0

    # Record counts
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_skipped: int = 0
    records_failed: int = 0

    # Quality metrics
    quality_score: Optional[float] = None
    validation_passed: int = 0
    validation_failed: int = 0

    # Error tracking
    error_count: int = 0
    warning_count: int = 0
    error_types: Dict[str, int] = field(default_factory=dict)
    last_error: Optional[str] = None

    # Status
    status: str = "running"  # running, success, partial, failed

    # Resource metrics
    memory_peak_mb: Optional[float] = None
    cpu_time_ms: Optional[int] = None
    network_bytes: Optional[int] = None

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    category: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        data = asdict(self)
        data["created_at"] = datetime.utcnow()
        return data

    def mark_completed(self, status: str = "success") -> None:
        """Mark the metric as completed."""
        self.completed_at = datetime.utcnow()
        self.status = status
        if self.started_at and self.completed_at:
            self.execution_time_ms = int(
                (self.completed_at - self.started_at).total_seconds() * 1000
            )

    def add_error(self, error_type: str, message: str) -> None:
        """Record an error."""
        self.error_count += 1
        self.error_types[error_type] = self.error_types.get(error_type, 0) + 1
        self.last_error = message

    def calculate_error_rate(self) -> float:
        """Calculate error rate as percentage."""
        total = self.records_extracted or 1
        return round((self.records_failed / total) * 100, 2)

    def calculate_throughput(self) -> float:
        """Calculate records per second throughput."""
        if not self.execution_time_ms or self.execution_time_ms == 0:
            return 0.0
        seconds = self.execution_time_ms / 1000
        return round(self.records_loaded / seconds, 2) if seconds > 0 else 0.0


class MetricsCollector:
    """
    Metrics collection and storage service.

    Features:
    - Real-time metric recording
    - Aggregation queries
    - Trend analysis
    - Alert integration
    """

    COLLECTION_NAME = "pipeline_metrics"

    def __init__(self, mongo_service=None):
        """
        Initialize metrics collector.

        Args:
            mongo_service: MongoDB service instance
        """
        self.mongo = mongo_service
        self._active_metrics: Dict[str, PipelineMetric] = {}

    async def start_metric(
        self,
        source_id: str,
        run_id: str,
        crawler_id: Optional[str] = None,
        dag_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> PipelineMetric:
        """
        Start tracking metrics for a pipeline run.

        Args:
            source_id: Source identifier
            run_id: Unique run identifier
            crawler_id: Crawler identifier
            dag_id: Airflow DAG identifier
            metadata: Additional metadata

        Returns:
            PipelineMetric instance
        """
        metric = PipelineMetric(
            source_id=source_id,
            run_id=run_id,
            crawler_id=crawler_id,
            dag_id=dag_id,
            metadata=metadata or {}
        )

        self._active_metrics[run_id] = metric

        logger.info(
            "Metric tracking started",
            source_id=source_id,
            run_id=run_id,
        )

        return metric

    async def update_metric(
        self,
        run_id: str,
        **updates
    ) -> Optional[PipelineMetric]:
        """
        Update an active metric.

        Args:
            run_id: Run identifier
            **updates: Fields to update

        Returns:
            Updated PipelineMetric or None
        """
        metric = self._active_metrics.get(run_id)
        if not metric:
            logger.warning("Metric not found for update", run_id=run_id)
            return None

        for key, value in updates.items():
            if hasattr(metric, key):
                setattr(metric, key, value)

        return metric

    async def complete_metric(
        self,
        run_id: str,
        status: str = "success"
    ) -> Optional[str]:
        """
        Complete and store a metric.

        Args:
            run_id: Run identifier
            status: Final status (success, partial, failed)

        Returns:
            Stored metric ID or None
        """
        metric = self._active_metrics.pop(run_id, None)
        if not metric:
            logger.warning("Metric not found for completion", run_id=run_id)
            return None

        metric.mark_completed(status)

        # Store in MongoDB
        metric_id = await self._store_metric(metric)

        logger.info(
            "Metric completed",
            run_id=run_id,
            status=status,
            execution_time_ms=metric.execution_time_ms,
            records_loaded=metric.records_loaded,
            error_count=metric.error_count,
        )

        return metric_id

    async def _store_metric(self, metric: PipelineMetric) -> Optional[str]:
        """Store metric in MongoDB."""
        if not self.mongo:
            logger.warning("MongoDB not configured, skipping metric storage")
            return None

        try:
            result = self.mongo.db[self.COLLECTION_NAME].insert_one(metric.to_dict())
            return str(result.inserted_id)
        except Exception as e:
            logger.error("Failed to store metric", error=str(e))
            return None

    async def record_metric(self, metric: PipelineMetric) -> Optional[str]:
        """
        Record a complete metric directly.

        Use this for metrics that don't need start/update/complete lifecycle.

        Args:
            metric: Complete PipelineMetric

        Returns:
            Stored metric ID or None
        """
        if metric.status == "running":
            metric.mark_completed("success")
        return await self._store_metric(metric)

    # ==================== Query Methods ====================

    async def get_metric(self, metric_id: str) -> Optional[Dict]:
        """Get a single metric by ID."""
        if not self.mongo:
            return None

        try:
            doc = self.mongo.db[self.COLLECTION_NAME].find_one(
                {"_id": ObjectId(metric_id)}
            )
            return self._serialize_doc(doc) if doc else None
        except Exception as e:
            logger.error("Failed to get metric", error=str(e))
            return None

    async def get_metrics_by_run(self, run_id: str) -> List[Dict]:
        """Get all metrics for a run."""
        if not self.mongo:
            return []

        try:
            cursor = self.mongo.db[self.COLLECTION_NAME].find(
                {"run_id": run_id}
            ).sort("started_at", -1)
            return [self._serialize_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error("Failed to get metrics by run", error=str(e))
            return []

    async def get_metrics_by_source(
        self,
        source_id: str,
        hours: int = 24,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get recent metrics for a source."""
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)
        query = {
            "source_id": source_id,
            "started_at": {"$gte": since}
        }
        if status:
            query["status"] = status

        try:
            cursor = (
                self.mongo.db[self.COLLECTION_NAME]
                .find(query)
                .sort("started_at", -1)
                .limit(limit)
            )
            return [self._serialize_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error("Failed to get metrics by source", error=str(e))
            return []

    async def get_aggregate_stats(
        self,
        source_id: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Get aggregated statistics for metrics.

        Args:
            source_id: Optional source filter
            hours: Time window in hours

        Returns:
            Aggregated statistics
        """
        if not self.mongo:
            return {}

        since = datetime.utcnow() - timedelta(hours=hours)
        match_stage = {"started_at": {"$gte": since}}
        if source_id:
            match_stage["source_id"] = source_id

        pipeline = [
            {"$match": match_stage},
            {
                "$group": {
                    "_id": "$status",
                    "count": {"$sum": 1},
                    "total_records": {"$sum": "$records_loaded"},
                    "total_errors": {"$sum": "$error_count"},
                    "avg_execution_time": {"$avg": "$execution_time_ms"},
                    "avg_quality_score": {"$avg": "$quality_score"},
                    "total_execution_time": {"$sum": "$execution_time_ms"},
                }
            }
        ]

        try:
            results = list(self.mongo.db[self.COLLECTION_NAME].aggregate(pipeline))

            # Restructure results
            stats = {
                "period_hours": hours,
                "by_status": {},
                "totals": {
                    "runs": 0,
                    "records": 0,
                    "errors": 0,
                    "execution_time_ms": 0
                }
            }

            for result in results:
                status = result["_id"]
                stats["by_status"][status] = {
                    "count": result["count"],
                    "records": result["total_records"],
                    "errors": result["total_errors"],
                    "avg_execution_time_ms": round(result["avg_execution_time"] or 0, 2),
                    "avg_quality_score": round(result["avg_quality_score"] or 0, 2),
                }
                stats["totals"]["runs"] += result["count"]
                stats["totals"]["records"] += result["total_records"]
                stats["totals"]["errors"] += result["total_errors"]
                stats["totals"]["execution_time_ms"] += result["total_execution_time"]

            # Calculate overall rates
            if stats["totals"]["runs"] > 0:
                success_count = stats["by_status"].get("success", {}).get("count", 0)
                stats["totals"]["success_rate"] = round(
                    (success_count / stats["totals"]["runs"]) * 100, 2
                )
                if stats["totals"]["records"] > 0:
                    stats["totals"]["error_rate"] = round(
                        (stats["totals"]["errors"] / stats["totals"]["records"]) * 100, 2
                    )
                else:
                    stats["totals"]["error_rate"] = 0

            return stats

        except Exception as e:
            logger.error("Failed to get aggregate stats", error=str(e))
            return {}

    async def get_source_stats(
        self,
        hours: int = 24,
        limit: int = 50
    ) -> List[Dict]:
        """
        Get statistics grouped by source.

        Args:
            hours: Time window in hours
            limit: Maximum sources to return

        Returns:
            List of source statistics
        """
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)

        pipeline = [
            {"$match": {"started_at": {"$gte": since}}},
            {
                "$group": {
                    "_id": "$source_id",
                    "total_runs": {"$sum": 1},
                    "success_count": {
                        "$sum": {"$cond": [{"$eq": ["$status", "success"]}, 1, 0]}
                    },
                    "failed_count": {
                        "$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}
                    },
                    "total_records": {"$sum": "$records_loaded"},
                    "total_errors": {"$sum": "$error_count"},
                    "avg_execution_time": {"$avg": "$execution_time_ms"},
                    "avg_quality_score": {"$avg": "$quality_score"},
                    "last_run": {"$max": "$started_at"},
                }
            },
            {"$sort": {"total_runs": -1}},
            {"$limit": limit}
        ]

        try:
            results = list(self.mongo.db[self.COLLECTION_NAME].aggregate(pipeline))

            return [
                {
                    "source_id": r["_id"],
                    "total_runs": r["total_runs"],
                    "success_count": r["success_count"],
                    "failed_count": r["failed_count"],
                    "success_rate": round(
                        (r["success_count"] / r["total_runs"]) * 100, 2
                    ) if r["total_runs"] > 0 else 0,
                    "total_records": r["total_records"],
                    "total_errors": r["total_errors"],
                    "avg_execution_time_ms": round(r["avg_execution_time"] or 0, 2),
                    "avg_quality_score": round(r["avg_quality_score"] or 0, 2),
                    "last_run": r["last_run"].isoformat() if r["last_run"] else None,
                }
                for r in results
            ]
        except Exception as e:
            logger.error("Failed to get source stats", error=str(e))
            return []

    async def get_category_stats(
        self,
        hours: int = 24
    ) -> List[Dict]:
        """
        Get statistics grouped by category.

        Args:
            hours: Time window in hours

        Returns:
            List of category statistics
        """
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)

        pipeline = [
            {"$match": {"started_at": {"$gte": since}, "category": {"$ne": None}}},
            {
                "$group": {
                    "_id": "$category",
                    "total_runs": {"$sum": 1},
                    "success_count": {
                        "$sum": {"$cond": [{"$eq": ["$status", "success"]}, 1, 0]}
                    },
                    "total_records": {"$sum": "$records_loaded"},
                    "total_errors": {"$sum": "$error_count"},
                    "avg_quality_score": {"$avg": "$quality_score"},
                }
            },
            {"$sort": {"total_runs": -1}}
        ]

        try:
            results = list(self.mongo.db[self.COLLECTION_NAME].aggregate(pipeline))

            return [
                {
                    "category": r["_id"],
                    "total_runs": r["total_runs"],
                    "success_rate": round(
                        (r["success_count"] / r["total_runs"]) * 100, 2
                    ) if r["total_runs"] > 0 else 0,
                    "total_records": r["total_records"],
                    "total_errors": r["total_errors"],
                    "avg_quality_score": round(r["avg_quality_score"] or 0, 2),
                }
                for r in results
            ]
        except Exception as e:
            logger.error("Failed to get category stats", error=str(e))
            return []

    async def get_error_distribution(
        self,
        source_id: Optional[str] = None,
        hours: int = 24
    ) -> List[Dict]:
        """
        Get error type distribution.

        Args:
            source_id: Optional source filter
            hours: Time window in hours

        Returns:
            List of error type counts
        """
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
            {"$project": {"error_types": {"$objectToArray": "$error_types"}}},
            {"$unwind": "$error_types"},
            {
                "$group": {
                    "_id": "$error_types.k",
                    "count": {"$sum": "$error_types.v"}
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]

        try:
            results = list(self.mongo.db[self.COLLECTION_NAME].aggregate(pipeline))
            return [
                {"error_type": r["_id"], "count": r["count"]}
                for r in results
            ]
        except Exception as e:
            logger.error("Failed to get error distribution", error=str(e))
            return []

    async def get_hourly_trend(
        self,
        source_id: Optional[str] = None,
        hours: int = 24
    ) -> List[Dict]:
        """
        Get hourly execution trend.

        Args:
            source_id: Optional source filter
            hours: Time window in hours

        Returns:
            List of hourly statistics
        """
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)
        match_stage = {"started_at": {"$gte": since}}
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
                    "runs": {"$sum": 1},
                    "success": {
                        "$sum": {"$cond": [{"$eq": ["$status", "success"]}, 1, 0]}
                    },
                    "records": {"$sum": "$records_loaded"},
                    "errors": {"$sum": "$error_count"},
                    "avg_execution_time": {"$avg": "$execution_time_ms"},
                }
            },
            {"$sort": {"_id": 1}}
        ]

        try:
            results = list(self.mongo.db[self.COLLECTION_NAME].aggregate(pipeline))
            return [
                {
                    "hour": r["_id"],
                    "runs": r["runs"],
                    "success": r["success"],
                    "records": r["records"],
                    "errors": r["errors"],
                    "success_rate": round(
                        (r["success"] / r["runs"]) * 100, 2
                    ) if r["runs"] > 0 else 0,
                    "avg_execution_time_ms": round(r["avg_execution_time"] or 0, 2),
                }
                for r in results
            ]
        except Exception as e:
            logger.error("Failed to get hourly trend", error=str(e))
            return []

    async def get_pending_review_count(self) -> int:
        """Get count of records pending quality review."""
        if not self.mongo:
            return 0

        try:
            return self.mongo.db.data_reviews.count_documents({
                "review_status": "pending"
            })
        except Exception as e:
            logger.error("Failed to get pending review count", error=str(e))
            return 0

    def _serialize_doc(self, doc: Dict) -> Dict:
        """Serialize MongoDB document for API response."""
        if not doc:
            return {}

        result = {}
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                result[key] = str(value)
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result

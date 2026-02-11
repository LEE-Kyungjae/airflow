"""
SLA (Service Level Agreement) Monitoring for ETL Pipelines.

Provides comprehensive SLA definition and monitoring including:
- Availability SLAs (uptime percentage)
- Latency SLAs (execution time limits)
- Throughput SLAs (minimum records per period)
- Quality SLAs (minimum quality scores)
- Freshness SLAs (maximum data age)
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from bson import ObjectId

try:
    from app.core import get_logger
    from app.services.alerts import AlertDispatcher, AlertSeverity
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    AlertDispatcher = None
    AlertSeverity = None


class SLAType(str, Enum):
    """Types of SLA definitions."""
    AVAILABILITY = "availability"       # Uptime percentage
    LATENCY = "latency"                # Max execution time
    THROUGHPUT = "throughput"          # Min records per period
    QUALITY = "quality"                # Min quality score
    FRESHNESS = "freshness"            # Max data age
    SUCCESS_RATE = "success_rate"      # Min success percentage
    ERROR_RATE = "error_rate"          # Max error percentage


class SLAStatus(str, Enum):
    """SLA compliance status."""
    COMPLIANT = "compliant"            # Within SLA
    AT_RISK = "at_risk"                # Close to breach
    BREACHED = "breached"              # SLA violated
    UNKNOWN = "unknown"                # Insufficient data


@dataclass
class SLADefinition:
    """
    Definition of a Service Level Agreement.

    Specifies the expected performance levels for a source or category.
    """
    # Identification
    name: str
    description: str

    # Scope
    source_id: Optional[str] = None      # None = global SLA
    category: Optional[str] = None       # Apply to category
    sla_type: SLAType = SLAType.AVAILABILITY

    # Thresholds
    target_value: float = 99.0           # Target SLA value
    warning_threshold: float = 95.0      # Warning when below this
    critical_threshold: float = 90.0     # Critical when below this

    # Time window
    window_hours: int = 24               # Evaluation window
    evaluation_schedule: str = "hourly"  # hourly, daily, weekly

    # Metadata
    enabled: bool = True
    priority: int = 1                    # 1=highest
    owner: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        data = asdict(self)
        data["sla_type"] = self.sla_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SLADefinition":
        """Create from dictionary."""
        data = data.copy()
        data.pop("_id", None)
        data["sla_type"] = SLAType(data.get("sla_type", "availability"))
        return cls(**data)


@dataclass
class SLABreach:
    """
    Record of an SLA breach or near-breach.
    """
    sla_id: str
    sla_name: str
    sla_type: str
    source_id: Optional[str]
    category: Optional[str]

    # Breach details
    status: SLAStatus
    target_value: float
    actual_value: float
    variance_percent: float

    # Timing
    detected_at: datetime
    evaluation_window_start: datetime
    evaluation_window_end: datetime

    # Resolution
    acknowledged: bool = False
    acknowledged_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        data = asdict(self)
        data["status"] = self.status.value if isinstance(self.status, SLAStatus) else self.status
        return data


@dataclass
class SLAReport:
    """
    SLA compliance report for a period.
    """
    period_start: datetime
    period_end: datetime
    total_slas: int
    compliant_count: int
    at_risk_count: int
    breached_count: int
    compliance_rate: float
    by_type: Dict[str, Dict[str, int]]
    by_source: List[Dict[str, Any]]
    breaches: List[Dict[str, Any]]


class SLAMonitor:
    """
    SLA definition and monitoring service.

    Features:
    - SLA definition CRUD
    - Real-time SLA evaluation
    - Breach detection and alerting
    - Compliance reporting
    """

    DEFINITIONS_COLLECTION = "sla_definitions"
    BREACHES_COLLECTION = "sla_breaches"
    EVALUATIONS_COLLECTION = "sla_evaluations"

    def __init__(
        self,
        mongo_service=None,
        alert_dispatcher: Optional[AlertDispatcher] = None
    ):
        """
        Initialize SLA monitor.

        Args:
            mongo_service: MongoDB service instance
            alert_dispatcher: Alert dispatcher for notifications
        """
        self.mongo = mongo_service
        self.dispatcher = alert_dispatcher

    # ==================== SLA Definition Management ====================

    async def create_sla(self, sla: SLADefinition) -> str:
        """
        Create a new SLA definition.

        Args:
            sla: SLA definition

        Returns:
            Created SLA ID
        """
        if not self.mongo:
            logger.warning("MongoDB not configured")
            return ""

        try:
            result = self.mongo.db[self.DEFINITIONS_COLLECTION].insert_one(
                sla.to_dict()
            )
            sla_id = str(result.inserted_id)

            logger.info(
                "SLA definition created",
                sla_id=sla_id,
                name=sla.name,
                type=sla.sla_type.value,
            )

            return sla_id

        except Exception as e:
            logger.error("Failed to create SLA", error=str(e))
            raise

    async def get_sla(self, sla_id: str) -> Optional[SLADefinition]:
        """Get an SLA by ID."""
        if not self.mongo:
            return None

        try:
            doc = self.mongo.db[self.DEFINITIONS_COLLECTION].find_one(
                {"_id": ObjectId(sla_id)}
            )
            return SLADefinition.from_dict(doc) if doc else None
        except Exception as e:
            logger.error("Failed to get SLA", error=str(e))
            return None

    async def update_sla(self, sla_id: str, updates: Dict[str, Any]) -> bool:
        """Update an SLA definition."""
        if not self.mongo:
            return False

        try:
            updates["updated_at"] = datetime.utcnow()
            result = self.mongo.db[self.DEFINITIONS_COLLECTION].update_one(
                {"_id": ObjectId(sla_id)},
                {"$set": updates}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error("Failed to update SLA", error=str(e))
            return False

    async def delete_sla(self, sla_id: str) -> bool:
        """Delete an SLA definition."""
        if not self.mongo:
            return False

        try:
            result = self.mongo.db[self.DEFINITIONS_COLLECTION].delete_one(
                {"_id": ObjectId(sla_id)}
            )
            return result.deleted_count > 0
        except Exception as e:
            logger.error("Failed to delete SLA", error=str(e))
            return False

    async def list_slas(
        self,
        source_id: Optional[str] = None,
        sla_type: Optional[SLAType] = None,
        enabled_only: bool = True
    ) -> List[Dict]:
        """
        List SLA definitions with filtering.

        Args:
            source_id: Filter by source
            sla_type: Filter by type
            enabled_only: Only return enabled SLAs

        Returns:
            List of SLA definitions
        """
        if not self.mongo:
            return []

        query = {}
        if enabled_only:
            query["enabled"] = True
        if source_id:
            query["$or"] = [
                {"source_id": source_id},
                {"source_id": None}
            ]
        if sla_type:
            query["sla_type"] = sla_type.value

        try:
            cursor = (
                self.mongo.db[self.DEFINITIONS_COLLECTION]
                .find(query)
                .sort("priority", 1)
            )
            return [self._serialize_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error("Failed to list SLAs", error=str(e))
            return []

    # ==================== SLA Evaluation ====================

    async def evaluate_all_slas(self) -> List[SLABreach]:
        """
        Evaluate all enabled SLAs and detect breaches.

        Returns:
            List of detected breaches
        """
        slas = await self.list_slas(enabled_only=True)
        all_breaches = []

        for sla_doc in slas:
            sla = SLADefinition.from_dict(sla_doc)
            sla_id = sla_doc.get("_id", "")

            breach = await self.evaluate_sla(str(sla_id), sla)
            if breach:
                all_breaches.append(breach)

        return all_breaches

    async def evaluate_sla(
        self,
        sla_id: str,
        sla: Optional[SLADefinition] = None
    ) -> Optional[SLABreach]:
        """
        Evaluate a single SLA.

        Args:
            sla_id: SLA identifier
            sla: Optional pre-loaded SLA definition

        Returns:
            SLABreach if breached or at risk, None otherwise
        """
        if sla is None:
            sla = await self.get_sla(sla_id)
            if not sla:
                return None

        # Calculate actual value based on SLA type
        actual_value = await self._calculate_sla_value(sla)
        if actual_value is None:
            logger.warning("Could not calculate SLA value", sla_id=sla_id)
            return None

        # Determine status
        status = self._determine_status(sla, actual_value)

        # Store evaluation
        await self._store_evaluation(sla_id, sla, actual_value, status)

        # Create breach record if not compliant
        if status != SLAStatus.COMPLIANT:
            breach = await self._create_breach(sla_id, sla, actual_value, status)
            return breach

        return None

    async def _calculate_sla_value(self, sla: SLADefinition) -> Optional[float]:
        """Calculate actual SLA value based on type."""
        if not self.mongo:
            return None

        since = datetime.utcnow() - timedelta(hours=sla.window_hours)
        query = {"started_at": {"$gte": since}}

        if sla.source_id:
            query["source_id"] = sla.source_id
        if sla.category:
            query["category"] = sla.category

        try:
            if sla.sla_type == SLAType.AVAILABILITY:
                return await self._calc_availability(query)

            elif sla.sla_type == SLAType.SUCCESS_RATE:
                return await self._calc_success_rate(query)

            elif sla.sla_type == SLAType.ERROR_RATE:
                return await self._calc_error_rate(query)

            elif sla.sla_type == SLAType.LATENCY:
                return await self._calc_avg_latency(query)

            elif sla.sla_type == SLAType.THROUGHPUT:
                return await self._calc_throughput(query, sla.window_hours)

            elif sla.sla_type == SLAType.QUALITY:
                return await self._calc_avg_quality(query)

            elif sla.sla_type == SLAType.FRESHNESS:
                return await self._calc_freshness(sla.source_id)

            return None

        except Exception as e:
            logger.error("Failed to calculate SLA value", error=str(e))
            return None

    async def _calc_availability(self, query: Dict) -> float:
        """Calculate availability percentage."""
        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": None,
                    "total": {"$sum": 1},
                    "success": {
                        "$sum": {
                            "$cond": [
                                {"$in": ["$status", ["success", "partial"]]},
                                1, 0
                            ]
                        }
                    }
                }
            }
        ]

        results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
        if results and results[0]["total"] > 0:
            return round((results[0]["success"] / results[0]["total"]) * 100, 2)
        return 100.0  # No data = assume available

    async def _calc_success_rate(self, query: Dict) -> float:
        """Calculate success rate percentage."""
        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": None,
                    "total": {"$sum": 1},
                    "success": {
                        "$sum": {"$cond": [{"$eq": ["$status", "success"]}, 1, 0]}
                    }
                }
            }
        ]

        results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
        if results and results[0]["total"] > 0:
            return round((results[0]["success"] / results[0]["total"]) * 100, 2)
        return 100.0

    async def _calc_error_rate(self, query: Dict) -> float:
        """Calculate error rate percentage."""
        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": None,
                    "total_records": {"$sum": "$records_loaded"},
                    "total_errors": {"$sum": "$error_count"}
                }
            }
        ]

        results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
        if results and results[0]["total_records"] > 0:
            return round(
                (results[0]["total_errors"] / results[0]["total_records"]) * 100, 2
            )
        return 0.0

    async def _calc_avg_latency(self, query: Dict) -> float:
        """Calculate average execution time in ms."""
        pipeline = [
            {"$match": query},
            {"$group": {"_id": None, "avg": {"$avg": "$execution_time_ms"}}}
        ]

        results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
        if results:
            return round(results[0]["avg"] or 0, 2)
        return 0.0

    async def _calc_throughput(self, query: Dict, hours: int) -> float:
        """Calculate records per hour throughput."""
        pipeline = [
            {"$match": query},
            {"$group": {"_id": None, "total": {"$sum": "$records_loaded"}}}
        ]

        results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
        if results:
            return round(results[0]["total"] / max(hours, 1), 2)
        return 0.0

    async def _calc_avg_quality(self, query: Dict) -> float:
        """Calculate average quality score."""
        query["quality_score"] = {"$ne": None}
        pipeline = [
            {"$match": query},
            {"$group": {"_id": None, "avg": {"$avg": "$quality_score"}}}
        ]

        results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
        if results:
            return round(results[0]["avg"] or 0, 2)
        return 100.0

    async def _calc_freshness(self, source_id: Optional[str]) -> float:
        """Calculate data freshness in hours."""
        if not source_id:
            return 0.0

        # Get last successful run
        doc = self.mongo.db.pipeline_metrics.find_one(
            {"source_id": source_id, "status": "success"},
            sort=[("completed_at", -1)]
        )

        if doc and doc.get("completed_at"):
            age = datetime.utcnow() - doc["completed_at"]
            return round(age.total_seconds() / 3600, 2)  # Hours

        return float('inf')  # No successful run

    def _determine_status(
        self,
        sla: SLADefinition,
        actual_value: float
    ) -> SLAStatus:
        """Determine SLA compliance status."""
        # For latency and error rate, lower is better
        if sla.sla_type in [SLAType.LATENCY, SLAType.ERROR_RATE, SLAType.FRESHNESS]:
            if actual_value <= sla.target_value:
                return SLAStatus.COMPLIANT
            elif actual_value <= sla.warning_threshold:
                return SLAStatus.AT_RISK
            else:
                return SLAStatus.BREACHED

        # For others, higher is better
        else:
            if actual_value >= sla.target_value:
                return SLAStatus.COMPLIANT
            elif actual_value >= sla.warning_threshold:
                return SLAStatus.AT_RISK
            elif actual_value >= sla.critical_threshold:
                return SLAStatus.BREACHED
            else:
                return SLAStatus.BREACHED

    async def _store_evaluation(
        self,
        sla_id: str,
        sla: SLADefinition,
        actual_value: float,
        status: SLAStatus
    ) -> None:
        """Store SLA evaluation result."""
        if not self.mongo:
            return

        try:
            self.mongo.db[self.EVALUATIONS_COLLECTION].insert_one({
                "sla_id": sla_id,
                "sla_name": sla.name,
                "sla_type": sla.sla_type.value,
                "source_id": sla.source_id,
                "target_value": sla.target_value,
                "actual_value": actual_value,
                "status": status.value,
                "evaluated_at": datetime.utcnow(),
            })
        except Exception as e:
            logger.error("Failed to store evaluation", error=str(e))

    async def _create_breach(
        self,
        sla_id: str,
        sla: SLADefinition,
        actual_value: float,
        status: SLAStatus
    ) -> SLABreach:
        """Create and store an SLA breach record."""
        now = datetime.utcnow()
        window_start = now - timedelta(hours=sla.window_hours)

        variance = abs(actual_value - sla.target_value)
        variance_percent = (variance / max(sla.target_value, 1)) * 100

        breach = SLABreach(
            sla_id=sla_id,
            sla_name=sla.name,
            sla_type=sla.sla_type.value,
            source_id=sla.source_id,
            category=sla.category,
            status=status,
            target_value=sla.target_value,
            actual_value=actual_value,
            variance_percent=round(variance_percent, 2),
            detected_at=now,
            evaluation_window_start=window_start,
            evaluation_window_end=now,
        )

        # Store breach
        if self.mongo:
            try:
                self.mongo.db[self.BREACHES_COLLECTION].insert_one(breach.to_dict())
            except Exception as e:
                logger.error("Failed to store breach", error=str(e))

        # Send alert
        await self._send_breach_alert(sla, breach)

        logger.warning(
            "SLA breach detected",
            sla_name=sla.name,
            sla_type=sla.sla_type.value,
            status=status.value,
            target=sla.target_value,
            actual=actual_value,
        )

        return breach

    async def _send_breach_alert(
        self,
        sla: SLADefinition,
        breach: SLABreach
    ) -> None:
        """Send alert for SLA breach."""
        if not self.dispatcher:
            return

        severity = (
            AlertSeverity.CRITICAL if breach.status == SLAStatus.BREACHED
            else AlertSeverity.WARNING
        )

        status_emoji = "BREACHED" if breach.status == SLAStatus.BREACHED else "AT RISK"

        message = f"""
SLA {status_emoji}: {sla.name}

Type: {sla.sla_type.value}
Source: {sla.source_id or 'Global'}
Category: {sla.category or 'N/A'}

Target: {sla.target_value}
Actual: {breach.actual_value}
Variance: {breach.variance_percent}%

Evaluation Window: {sla.window_hours} hours
        """.strip()

        await self.dispatcher.send_alert(
            title=f"SLA {status_emoji}: {sla.name}",
            message=message,
            severity=severity,
            source_id=sla.source_id,
            metadata={
                "sla_id": breach.sla_id,
                "sla_type": sla.sla_type.value,
                "target_value": sla.target_value,
                "actual_value": breach.actual_value,
            }
        )

    # ==================== Breach Management ====================

    async def get_recent_breaches(
        self,
        source_id: Optional[str] = None,
        sla_type: Optional[str] = None,
        status: Optional[str] = None,
        hours: int = 24,
        limit: int = 100
    ) -> List[Dict]:
        """
        Get recent SLA breaches.

        Args:
            source_id: Filter by source
            sla_type: Filter by SLA type
            status: Filter by status (breached, at_risk)
            hours: Time window
            limit: Maximum records

        Returns:
            List of breaches
        """
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)
        query = {"detected_at": {"$gte": since}}

        if source_id:
            query["source_id"] = source_id
        if sla_type:
            query["sla_type"] = sla_type
        if status:
            query["status"] = status

        try:
            cursor = (
                self.mongo.db[self.BREACHES_COLLECTION]
                .find(query)
                .sort("detected_at", -1)
                .limit(limit)
            )
            return [self._serialize_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error("Failed to get breaches", error=str(e))
            return []

    async def acknowledge_breach(
        self,
        breach_id: str,
        acknowledged_by: Optional[str] = None
    ) -> bool:
        """Acknowledge an SLA breach."""
        if not self.mongo:
            return False

        try:
            result = self.mongo.db[self.BREACHES_COLLECTION].update_one(
                {"_id": ObjectId(breach_id)},
                {
                    "$set": {
                        "acknowledged": True,
                        "acknowledged_at": datetime.utcnow(),
                        "acknowledged_by": acknowledged_by
                    }
                }
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error("Failed to acknowledge breach", error=str(e))
            return False

    async def resolve_breach(
        self,
        breach_id: str,
        resolution_notes: Optional[str] = None
    ) -> bool:
        """Mark an SLA breach as resolved."""
        if not self.mongo:
            return False

        try:
            result = self.mongo.db[self.BREACHES_COLLECTION].update_one(
                {"_id": ObjectId(breach_id)},
                {
                    "$set": {
                        "resolved": True,
                        "resolved_at": datetime.utcnow(),
                        "resolution_notes": resolution_notes
                    }
                }
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error("Failed to resolve breach", error=str(e))
            return False

    # ==================== Reporting ====================

    async def get_compliance_summary(
        self,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Get SLA compliance summary.

        Args:
            hours: Time window

        Returns:
            Compliance summary
        """
        if not self.mongo:
            return {}

        since = datetime.utcnow() - timedelta(hours=hours)

        try:
            # Get latest evaluation for each SLA
            pipeline = [
                {"$match": {"evaluated_at": {"$gte": since}}},
                {"$sort": {"evaluated_at": -1}},
                {
                    "$group": {
                        "_id": "$sla_id",
                        "sla_name": {"$first": "$sla_name"},
                        "sla_type": {"$first": "$sla_type"},
                        "status": {"$first": "$status"},
                        "target_value": {"$first": "$target_value"},
                        "actual_value": {"$first": "$actual_value"},
                        "evaluated_at": {"$first": "$evaluated_at"}
                    }
                }
            ]

            results = list(
                self.mongo.db[self.EVALUATIONS_COLLECTION].aggregate(pipeline)
            )

            # Count by status
            status_counts = {"compliant": 0, "at_risk": 0, "breached": 0}
            type_status = {}

            for r in results:
                status = r.get("status", "unknown")
                if status in status_counts:
                    status_counts[status] += 1

                sla_type = r.get("sla_type", "unknown")
                if sla_type not in type_status:
                    type_status[sla_type] = {"compliant": 0, "at_risk": 0, "breached": 0}
                if status in type_status[sla_type]:
                    type_status[sla_type][status] += 1

            total = sum(status_counts.values())
            compliance_rate = (
                (status_counts["compliant"] / total) * 100 if total > 0 else 100
            )

            return {
                "period_hours": hours,
                "total_slas": total,
                "compliant": status_counts["compliant"],
                "at_risk": status_counts["at_risk"],
                "breached": status_counts["breached"],
                "compliance_rate": round(compliance_rate, 2),
                "by_type": type_status,
                "evaluations": [self._serialize_doc(r) for r in results]
            }

        except Exception as e:
            logger.error("Failed to get compliance summary", error=str(e))
            return {}

    async def get_sla_history(
        self,
        sla_id: str,
        days: int = 7
    ) -> List[Dict]:
        """
        Get historical evaluations for an SLA.

        Args:
            sla_id: SLA identifier
            days: Number of days

        Returns:
            List of historical evaluations
        """
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(days=days)

        try:
            cursor = (
                self.mongo.db[self.EVALUATIONS_COLLECTION]
                .find({
                    "sla_id": sla_id,
                    "evaluated_at": {"$gte": since}
                })
                .sort("evaluated_at", 1)
            )
            return [self._serialize_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error("Failed to get SLA history", error=str(e))
            return []

    def _serialize_doc(self, doc: Dict) -> Dict:
        """Serialize MongoDB document."""
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


# ==================== Pre-defined SLA Templates ====================

def create_default_slas() -> List[SLADefinition]:
    """Create a set of default SLA templates."""
    return [
        SLADefinition(
            name="Pipeline Availability",
            description="Ensure pipelines are running successfully",
            sla_type=SLAType.AVAILABILITY,
            target_value=99.0,
            warning_threshold=97.0,
            critical_threshold=95.0,
            window_hours=24,
            priority=1,
            tags=["availability", "default"]
        ),
        SLADefinition(
            name="Success Rate",
            description="Minimum successful execution rate",
            sla_type=SLAType.SUCCESS_RATE,
            target_value=95.0,
            warning_threshold=90.0,
            critical_threshold=85.0,
            window_hours=24,
            priority=1,
            tags=["reliability", "default"]
        ),
        SLADefinition(
            name="Error Rate",
            description="Maximum acceptable error rate",
            sla_type=SLAType.ERROR_RATE,
            target_value=1.0,
            warning_threshold=3.0,
            critical_threshold=5.0,
            window_hours=24,
            priority=2,
            tags=["quality", "default"]
        ),
        SLADefinition(
            name="Execution Latency",
            description="Maximum average execution time",
            sla_type=SLAType.LATENCY,
            target_value=60000,  # 60 seconds
            warning_threshold=120000,
            critical_threshold=300000,
            window_hours=24,
            priority=2,
            tags=["performance", "default"]
        ),
        SLADefinition(
            name="Data Quality",
            description="Minimum data quality score",
            sla_type=SLAType.QUALITY,
            target_value=90.0,
            warning_threshold=85.0,
            critical_threshold=80.0,
            window_hours=24,
            priority=1,
            tags=["quality", "default"]
        ),
    ]

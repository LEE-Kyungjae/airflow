"""
Data Freshness Tracking for ETL Pipelines.

Monitors data freshness (staleness) across all sources and provides:
- Per-source freshness configuration
- Real-time freshness status
- Staleness alerts
- Historical freshness trends
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from bson import ObjectId

from app.core import get_logger
from app.services.alerts import AlertDispatcher, AlertSeverity

logger = get_logger(__name__)


class FreshnessStatus(str, Enum):
    """Data freshness status."""
    FRESH = "fresh"                 # Within expected freshness
    STALE = "stale"                 # Exceeds warning threshold
    CRITICAL = "critical"           # Exceeds critical threshold
    UNKNOWN = "unknown"             # No data or unable to determine
    DISABLED = "disabled"           # Freshness tracking disabled


@dataclass
class FreshnessConfig:
    """
    Freshness configuration for a source.

    Defines expected freshness requirements and alert thresholds.
    """
    # Identification
    source_id: str
    source_name: Optional[str] = None

    # Freshness thresholds (in hours)
    expected_frequency_hours: float = 24.0      # Expected update frequency
    warning_threshold_hours: float = 36.0       # Warning when data is this old
    critical_threshold_hours: float = 48.0      # Critical when data is this old

    # Schedule context
    schedule_cron: Optional[str] = None         # Cron expression if scheduled
    business_hours_only: bool = False           # Only count business hours
    timezone: str = "UTC"

    # Alert configuration
    alert_on_stale: bool = True
    alert_on_critical: bool = True
    alert_cooldown_hours: float = 4.0

    # State
    enabled: bool = True
    last_alert_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FreshnessConfig":
        """Create from dictionary."""
        data = data.copy()
        data.pop("_id", None)
        return cls(**data)


@dataclass
class FreshnessState:
    """
    Current freshness state for a source.
    """
    source_id: str
    source_name: Optional[str]
    status: FreshnessStatus
    last_successful_run: Optional[datetime]
    last_data_update: Optional[datetime]
    data_age_hours: float
    expected_frequency_hours: float
    warning_threshold_hours: float
    critical_threshold_hours: float
    next_expected_update: Optional[datetime]
    records_in_last_run: int
    evaluated_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data["status"] = self.status.value if isinstance(self.status, FreshnessStatus) else self.status
        return data


class FreshnessTracker:
    """
    Data freshness tracking service.

    Features:
    - Source freshness configuration
    - Real-time freshness evaluation
    - Staleness detection and alerting
    - Historical freshness tracking
    """

    CONFIG_COLLECTION = "freshness_config"
    HISTORY_COLLECTION = "freshness_history"

    def __init__(
        self,
        mongo_service=None,
        alert_dispatcher: Optional[AlertDispatcher] = None
    ):
        """
        Initialize freshness tracker.

        Args:
            mongo_service: MongoDB service instance
            alert_dispatcher: Alert dispatcher for notifications
        """
        self.mongo = mongo_service
        self.dispatcher = alert_dispatcher

    # ==================== Configuration Management ====================

    async def set_freshness_config(self, config: FreshnessConfig) -> str:
        """
        Set or update freshness configuration for a source.

        Args:
            config: Freshness configuration

        Returns:
            Config ID
        """
        if not self.mongo:
            logger.warning("MongoDB not configured")
            return ""

        try:
            # Upsert configuration
            result = self.mongo.db[self.CONFIG_COLLECTION].update_one(
                {"source_id": config.source_id},
                {"$set": config.to_dict()},
                upsert=True
            )

            config_id = str(result.upserted_id) if result.upserted_id else config.source_id

            logger.info(
                "Freshness config set",
                source_id=config.source_id,
                expected_frequency=config.expected_frequency_hours,
            )

            return config_id

        except Exception as e:
            logger.error("Failed to set freshness config", error=str(e))
            raise

    async def get_freshness_config(
        self,
        source_id: str
    ) -> Optional[FreshnessConfig]:
        """Get freshness configuration for a source."""
        if not self.mongo:
            return None

        try:
            doc = self.mongo.db[self.CONFIG_COLLECTION].find_one(
                {"source_id": source_id}
            )
            return FreshnessConfig.from_dict(doc) if doc else None
        except Exception as e:
            logger.error("Failed to get freshness config", error=str(e))
            return None

    async def delete_freshness_config(self, source_id: str) -> bool:
        """Delete freshness configuration for a source."""
        if not self.mongo:
            return False

        try:
            result = self.mongo.db[self.CONFIG_COLLECTION].delete_one(
                {"source_id": source_id}
            )
            return result.deleted_count > 0
        except Exception as e:
            logger.error("Failed to delete freshness config", error=str(e))
            return False

    async def list_freshness_configs(
        self,
        enabled_only: bool = True
    ) -> List[Dict]:
        """List all freshness configurations."""
        if not self.mongo:
            return []

        query = {}
        if enabled_only:
            query["enabled"] = True

        try:
            cursor = self.mongo.db[self.CONFIG_COLLECTION].find(query)
            return [self._serialize_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error("Failed to list freshness configs", error=str(e))
            return []

    # ==================== Freshness Evaluation ====================

    async def check_freshness(
        self,
        source_id: str,
        config: Optional[FreshnessConfig] = None
    ) -> FreshnessState:
        """
        Check freshness status for a source.

        Args:
            source_id: Source identifier
            config: Optional pre-loaded config

        Returns:
            FreshnessState
        """
        now = datetime.utcnow()

        # Get configuration
        if config is None:
            config = await self.get_freshness_config(source_id)

        # Handle missing config with defaults
        if config is None:
            config = FreshnessConfig(
                source_id=source_id,
                expected_frequency_hours=24.0,
                warning_threshold_hours=36.0,
                critical_threshold_hours=48.0
            )

        # Get last successful run
        last_run_info = await self._get_last_successful_run(source_id)

        if not last_run_info:
            return FreshnessState(
                source_id=source_id,
                source_name=config.source_name,
                status=FreshnessStatus.UNKNOWN,
                last_successful_run=None,
                last_data_update=None,
                data_age_hours=float('inf'),
                expected_frequency_hours=config.expected_frequency_hours,
                warning_threshold_hours=config.warning_threshold_hours,
                critical_threshold_hours=config.critical_threshold_hours,
                next_expected_update=None,
                records_in_last_run=0,
                evaluated_at=now
            )

        # Calculate data age
        last_run_time = last_run_info.get("completed_at") or last_run_info.get("started_at")
        data_age = now - last_run_time
        data_age_hours = round(data_age.total_seconds() / 3600, 2)

        # Determine status
        status = self._determine_freshness_status(config, data_age_hours)

        # Calculate next expected update
        next_expected = last_run_time + timedelta(hours=config.expected_frequency_hours)

        state = FreshnessState(
            source_id=source_id,
            source_name=config.source_name,
            status=status,
            last_successful_run=last_run_time,
            last_data_update=last_run_time,
            data_age_hours=data_age_hours,
            expected_frequency_hours=config.expected_frequency_hours,
            warning_threshold_hours=config.warning_threshold_hours,
            critical_threshold_hours=config.critical_threshold_hours,
            next_expected_update=next_expected,
            records_in_last_run=last_run_info.get("records_loaded", 0),
            evaluated_at=now
        )

        # Store evaluation
        await self._store_freshness_evaluation(state)

        # Send alert if needed
        if status != FreshnessStatus.FRESH:
            await self._handle_staleness_alert(config, state)

        return state

    async def check_all_freshness(self) -> List[FreshnessState]:
        """
        Check freshness for all configured sources.

        Returns:
            List of freshness states
        """
        configs = await self.list_freshness_configs(enabled_only=True)
        states = []

        for config_doc in configs:
            config = FreshnessConfig.from_dict(config_doc)
            state = await self.check_freshness(config.source_id, config)
            states.append(state)

        return states

    async def _get_last_successful_run(
        self,
        source_id: str
    ) -> Optional[Dict]:
        """Get information about the last successful run."""
        if not self.mongo:
            return None

        try:
            doc = self.mongo.db.pipeline_metrics.find_one(
                {
                    "source_id": source_id,
                    "status": {"$in": ["success", "partial"]}
                },
                sort=[("completed_at", -1)]
            )
            return doc
        except Exception as e:
            logger.error("Failed to get last run", error=str(e))
            return None

    def _determine_freshness_status(
        self,
        config: FreshnessConfig,
        data_age_hours: float
    ) -> FreshnessStatus:
        """Determine freshness status based on age and thresholds."""
        if not config.enabled:
            return FreshnessStatus.DISABLED

        if data_age_hours >= config.critical_threshold_hours:
            return FreshnessStatus.CRITICAL
        elif data_age_hours >= config.warning_threshold_hours:
            return FreshnessStatus.STALE
        else:
            return FreshnessStatus.FRESH

    async def _store_freshness_evaluation(
        self,
        state: FreshnessState
    ) -> None:
        """Store freshness evaluation in history."""
        if not self.mongo:
            return

        try:
            self.mongo.db[self.HISTORY_COLLECTION].insert_one(state.to_dict())
        except Exception as e:
            logger.error("Failed to store freshness evaluation", error=str(e))

    async def _handle_staleness_alert(
        self,
        config: FreshnessConfig,
        state: FreshnessState
    ) -> None:
        """Handle staleness alert based on configuration."""
        now = datetime.utcnow()

        # Check alert eligibility
        should_alert = False
        if state.status == FreshnessStatus.CRITICAL and config.alert_on_critical:
            should_alert = True
        elif state.status == FreshnessStatus.STALE and config.alert_on_stale:
            should_alert = True

        if not should_alert:
            return

        # Check cooldown
        if config.last_alert_at:
            cooldown_end = config.last_alert_at + timedelta(
                hours=config.alert_cooldown_hours
            )
            if now < cooldown_end:
                return

        # Send alert
        await self._send_staleness_alert(config, state)

        # Update last alert time
        if self.mongo:
            try:
                self.mongo.db[self.CONFIG_COLLECTION].update_one(
                    {"source_id": config.source_id},
                    {"$set": {"last_alert_at": now}}
                )
            except Exception as e:
                logger.error("Failed to update last alert time", error=str(e))

    async def _send_staleness_alert(
        self,
        config: FreshnessConfig,
        state: FreshnessState
    ) -> None:
        """Send staleness alert notification."""
        if not self.dispatcher:
            return

        severity = (
            AlertSeverity.CRITICAL if state.status == FreshnessStatus.CRITICAL
            else AlertSeverity.WARNING
        )

        status_text = "CRITICAL" if state.status == FreshnessStatus.CRITICAL else "STALE"

        last_update = (
            state.last_successful_run.strftime("%Y-%m-%d %H:%M:%S UTC")
            if state.last_successful_run
            else "Never"
        )

        message = f"""
Data Freshness Alert: {status_text}

Source: {config.source_name or config.source_id}
Source ID: {config.source_id}

Data Age: {state.data_age_hours:.1f} hours
Expected Frequency: {config.expected_frequency_hours} hours
Warning Threshold: {config.warning_threshold_hours} hours
Critical Threshold: {config.critical_threshold_hours} hours

Last Successful Update: {last_update}
Records in Last Run: {state.records_in_last_run}

Please investigate why data has not been refreshed.
        """.strip()

        await self.dispatcher.send_alert(
            title=f"Data Freshness {status_text}: {config.source_name or config.source_id}",
            message=message,
            severity=severity,
            source_id=config.source_id,
            metadata={
                "data_age_hours": state.data_age_hours,
                "expected_frequency_hours": config.expected_frequency_hours,
                "status": state.status.value,
            }
        )

    # ==================== Query Methods ====================

    async def get_freshness_summary(self) -> Dict[str, Any]:
        """
        Get summary of freshness across all sources.

        Returns:
            Summary statistics
        """
        states = await self.check_all_freshness()

        status_counts = {
            FreshnessStatus.FRESH.value: 0,
            FreshnessStatus.STALE.value: 0,
            FreshnessStatus.CRITICAL.value: 0,
            FreshnessStatus.UNKNOWN.value: 0,
            FreshnessStatus.DISABLED.value: 0,
        }

        total_age = 0.0
        sources_with_age = 0

        for state in states:
            status_counts[state.status.value] += 1
            if state.data_age_hours < float('inf'):
                total_age += state.data_age_hours
                sources_with_age += 1

        avg_age = round(total_age / sources_with_age, 2) if sources_with_age > 0 else 0

        return {
            "total_sources": len(states),
            "fresh": status_counts[FreshnessStatus.FRESH.value],
            "stale": status_counts[FreshnessStatus.STALE.value],
            "critical": status_counts[FreshnessStatus.CRITICAL.value],
            "unknown": status_counts[FreshnessStatus.UNKNOWN.value],
            "disabled": status_counts[FreshnessStatus.DISABLED.value],
            "average_data_age_hours": avg_age,
            "health_score": round(
                (status_counts[FreshnessStatus.FRESH.value] / max(len(states), 1)) * 100, 2
            ),
            "states": [s.to_dict() for s in states]
        }

    async def get_stale_sources(self) -> List[Dict]:
        """Get list of stale and critical sources."""
        states = await self.check_all_freshness()

        stale_sources = [
            s.to_dict() for s in states
            if s.status in [FreshnessStatus.STALE, FreshnessStatus.CRITICAL]
        ]

        # Sort by age (most stale first)
        stale_sources.sort(key=lambda x: x.get("data_age_hours", 0), reverse=True)

        return stale_sources

    async def get_freshness_history(
        self,
        source_id: str,
        hours: int = 24
    ) -> List[Dict]:
        """
        Get freshness history for a source.

        Args:
            source_id: Source identifier
            hours: Time window

        Returns:
            List of historical evaluations
        """
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)

        try:
            cursor = (
                self.mongo.db[self.HISTORY_COLLECTION]
                .find({
                    "source_id": source_id,
                    "evaluated_at": {"$gte": since}
                })
                .sort("evaluated_at", 1)
            )
            return [self._serialize_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error("Failed to get freshness history", error=str(e))
            return []

    async def get_freshness_trend(
        self,
        source_id: Optional[str] = None,
        days: int = 7
    ) -> List[Dict]:
        """
        Get daily freshness trend.

        Args:
            source_id: Optional source filter
            days: Number of days

        Returns:
            Daily freshness statistics
        """
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(days=days)
        match_stage = {"evaluated_at": {"$gte": since}}
        if source_id:
            match_stage["source_id"] = source_id

        pipeline = [
            {"$match": match_stage},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$evaluated_at"
                        }
                    },
                    "fresh_count": {
                        "$sum": {"$cond": [{"$eq": ["$status", "fresh"]}, 1, 0]}
                    },
                    "stale_count": {
                        "$sum": {"$cond": [{"$eq": ["$status", "stale"]}, 1, 0]}
                    },
                    "critical_count": {
                        "$sum": {"$cond": [{"$eq": ["$status", "critical"]}, 1, 0]}
                    },
                    "avg_age": {"$avg": "$data_age_hours"},
                    "total": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]

        try:
            results = list(
                self.mongo.db[self.HISTORY_COLLECTION].aggregate(pipeline)
            )
            return [
                {
                    "date": r["_id"],
                    "fresh": r["fresh_count"],
                    "stale": r["stale_count"],
                    "critical": r["critical_count"],
                    "avg_age_hours": round(r["avg_age"] or 0, 2),
                    "health_rate": round(
                        (r["fresh_count"] / max(r["total"], 1)) * 100, 2
                    )
                }
                for r in results
            ]
        except Exception as e:
            logger.error("Failed to get freshness trend", error=str(e))
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


# ==================== Auto-Configuration ====================

async def auto_configure_freshness(
    tracker: FreshnessTracker,
    mongo_service
) -> int:
    """
    Auto-configure freshness tracking for all sources.

    Analyzes historical run patterns to determine appropriate
    freshness thresholds for each source.

    Args:
        tracker: FreshnessTracker instance
        mongo_service: MongoDB service

    Returns:
        Number of sources configured
    """
    if not mongo_service:
        return 0

    try:
        # Get all active sources
        sources = list(mongo_service.db.sources.find({"status": "active"}))
        configured = 0

        for source in sources:
            source_id = str(source["_id"])
            source_name = source.get("name", "")
            schedule = source.get("schedule", "")

            # Analyze historical run frequency
            pipeline = [
                {"$match": {"source_id": source_id, "status": "success"}},
                {"$sort": {"completed_at": -1}},
                {"$limit": 10},
                {
                    "$group": {
                        "_id": None,
                        "runs": {"$push": "$completed_at"}
                    }
                }
            ]

            results = list(mongo_service.db.pipeline_metrics.aggregate(pipeline))

            # Calculate average interval
            avg_interval_hours = 24.0  # Default
            if results and len(results[0].get("runs", [])) >= 2:
                runs = sorted(results[0]["runs"])
                intervals = []
                for i in range(1, len(runs)):
                    interval = (runs[i] - runs[i-1]).total_seconds() / 3600
                    intervals.append(interval)
                avg_interval_hours = sum(intervals) / len(intervals)

            # Set thresholds based on frequency
            warning_multiplier = 1.5
            critical_multiplier = 2.0

            config = FreshnessConfig(
                source_id=source_id,
                source_name=source_name,
                expected_frequency_hours=round(avg_interval_hours, 1),
                warning_threshold_hours=round(avg_interval_hours * warning_multiplier, 1),
                critical_threshold_hours=round(avg_interval_hours * critical_multiplier, 1),
                schedule_cron=schedule,
                enabled=True
            )

            await tracker.set_freshness_config(config)
            configured += 1

        logger.info(
            "Auto-configured freshness tracking",
            sources_configured=configured
        )

        return configured

    except Exception as e:
        logger.error("Failed to auto-configure freshness", error=str(e))
        return 0

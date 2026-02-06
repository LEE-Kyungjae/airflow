"""
Alert Rule Engine for ETL Observability.

Provides configurable alert rules and automated triggers for:
- Threshold-based alerts (error rates, execution times)
- Pattern-based alerts (consecutive failures, anomalies)
- Scheduled alerts (SLA breaches, freshness violations)
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field, asdict
from bson import ObjectId

from app.core import get_logger
from app.services.alerts import AlertDispatcher, AlertSeverity

logger = get_logger(__name__)


class AlertCondition(str, Enum):
    """Types of conditions for alert rules."""
    THRESHOLD_ABOVE = "threshold_above"     # Value > threshold
    THRESHOLD_BELOW = "threshold_below"     # Value < threshold
    EQUALS = "equals"                       # Value == expected
    NOT_EQUALS = "not_equals"               # Value != expected
    CONSECUTIVE_FAILURES = "consecutive_failures"  # N failures in a row
    RATE_ABOVE = "rate_above"               # Rate exceeds threshold
    RATE_BELOW = "rate_below"               # Rate below threshold
    PATTERN_MATCH = "pattern_match"         # Regex pattern match
    MISSING_DATA = "missing_data"           # Expected data not present


class AlertAction(str, Enum):
    """Actions to take when alert is triggered."""
    NOTIFY = "notify"           # Send notification
    LOG = "log"                 # Log only
    DISABLE_SOURCE = "disable_source"   # Disable the source
    TRIGGER_RETRY = "trigger_retry"     # Trigger a retry
    ESCALATE = "escalate"       # Escalate to higher severity
    WEBHOOK = "webhook"         # Call custom webhook


@dataclass
class AlertRule:
    """
    Definition of an alert rule.

    Specifies when and how to trigger alerts based on metric conditions.
    """
    # Identification
    name: str
    description: str

    # Scope
    source_id: Optional[str] = None  # None = applies to all sources
    metric_type: str = "any"         # execution, quality, error, etc.

    # Condition
    condition: AlertCondition = AlertCondition.THRESHOLD_ABOVE
    metric_field: str = "error_count"       # Field to check
    threshold: float = 0             # Threshold value
    window_minutes: int = 60         # Time window for rate calculations
    consecutive_count: int = 3       # For consecutive failures

    # Response
    severity: AlertSeverity = AlertSeverity.WARNING
    actions: List[AlertAction] = field(default_factory=lambda: [AlertAction.NOTIFY])
    cooldown_minutes: int = 30       # Minimum time between alerts

    # State
    enabled: bool = True
    last_triggered: Optional[datetime] = None
    trigger_count: int = 0

    # Metadata
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        data = asdict(self)
        data["condition"] = self.condition.value
        data["severity"] = self.severity.value
        data["actions"] = [a.value if isinstance(a, AlertAction) else a for a in self.actions]
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AlertRule":
        """Create from dictionary."""
        data = data.copy()
        data.pop("_id", None)
        data["condition"] = AlertCondition(data.get("condition", "threshold_above"))
        data["severity"] = AlertSeverity(data.get("severity", "warning"))
        data["actions"] = [
            AlertAction(a) if isinstance(a, str) else a
            for a in data.get("actions", ["notify"])
        ]
        return cls(**data)


@dataclass
class AlertTrigger:
    """Record of a triggered alert."""
    rule_id: str
    rule_name: str
    source_id: Optional[str]
    triggered_at: datetime
    severity: str
    condition_details: Dict[str, Any]
    actions_taken: List[str]
    notification_sent: bool
    notification_result: Optional[Dict[str, Any]] = None
    acknowledged: bool = False
    acknowledged_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None
    resolved: bool = False
    resolved_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        return asdict(self)


class AlertRuleEngine:
    """
    Alert rule evaluation and trigger engine.

    Features:
    - Rule CRUD operations
    - Metric evaluation against rules
    - Cooldown management
    - Alert history tracking
    - Integration with AlertDispatcher
    """

    RULES_COLLECTION = "alert_rules"
    HISTORY_COLLECTION = "alert_history"

    def __init__(
        self,
        mongo_service=None,
        alert_dispatcher: Optional[AlertDispatcher] = None
    ):
        """
        Initialize alert rule engine.

        Args:
            mongo_service: MongoDB service instance
            alert_dispatcher: Alert dispatcher for notifications
        """
        self.mongo = mongo_service
        self.dispatcher = alert_dispatcher
        self._rules_cache: Dict[str, AlertRule] = {}
        self._last_cache_refresh: Optional[datetime] = None
        self._cache_ttl_seconds = 300  # 5 minutes

    # ==================== Rule Management ====================

    async def create_rule(self, rule: AlertRule) -> str:
        """
        Create a new alert rule.

        Args:
            rule: AlertRule to create

        Returns:
            Created rule ID
        """
        if not self.mongo:
            logger.warning("MongoDB not configured")
            return ""

        try:
            result = self.mongo.db[self.RULES_COLLECTION].insert_one(rule.to_dict())
            rule_id = str(result.inserted_id)

            # Invalidate cache
            self._rules_cache.clear()

            logger.info(
                "Alert rule created",
                rule_id=rule_id,
                name=rule.name,
            )

            return rule_id

        except Exception as e:
            logger.error("Failed to create alert rule", error=str(e))
            raise

    async def get_rule(self, rule_id: str) -> Optional[AlertRule]:
        """Get a rule by ID."""
        if not self.mongo:
            return None

        try:
            doc = self.mongo.db[self.RULES_COLLECTION].find_one(
                {"_id": ObjectId(rule_id)}
            )
            return AlertRule.from_dict(doc) if doc else None
        except Exception as e:
            logger.error("Failed to get alert rule", error=str(e))
            return None

    async def update_rule(self, rule_id: str, updates: Dict[str, Any]) -> bool:
        """
        Update an alert rule.

        Args:
            rule_id: Rule ID
            updates: Fields to update

        Returns:
            True if updated
        """
        if not self.mongo:
            return False

        try:
            updates["updated_at"] = datetime.utcnow()
            result = self.mongo.db[self.RULES_COLLECTION].update_one(
                {"_id": ObjectId(rule_id)},
                {"$set": updates}
            )

            # Invalidate cache
            self._rules_cache.clear()

            return result.modified_count > 0

        except Exception as e:
            logger.error("Failed to update alert rule", error=str(e))
            return False

    async def delete_rule(self, rule_id: str) -> bool:
        """Delete an alert rule."""
        if not self.mongo:
            return False

        try:
            result = self.mongo.db[self.RULES_COLLECTION].delete_one(
                {"_id": ObjectId(rule_id)}
            )

            # Invalidate cache
            self._rules_cache.clear()

            return result.deleted_count > 0

        except Exception as e:
            logger.error("Failed to delete alert rule", error=str(e))
            return False

    async def list_rules(
        self,
        source_id: Optional[str] = None,
        enabled_only: bool = True,
        tags: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        List alert rules with filtering.

        Args:
            source_id: Filter by source
            enabled_only: Only return enabled rules
            tags: Filter by tags

        Returns:
            List of rules
        """
        if not self.mongo:
            return []

        query = {}
        if enabled_only:
            query["enabled"] = True
        if source_id:
            query["$or"] = [
                {"source_id": source_id},
                {"source_id": None}  # Global rules
            ]
        if tags:
            query["tags"] = {"$in": tags}

        try:
            cursor = self.mongo.db[self.RULES_COLLECTION].find(query)
            return [self._serialize_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error("Failed to list alert rules", error=str(e))
            return []

    async def toggle_rule(self, rule_id: str, enabled: bool) -> bool:
        """Enable or disable a rule."""
        return await self.update_rule(rule_id, {"enabled": enabled})

    # ==================== Rule Evaluation ====================

    async def evaluate_metric(
        self,
        metric_data: Dict[str, Any],
        source_id: Optional[str] = None
    ) -> List[AlertTrigger]:
        """
        Evaluate a metric against all applicable rules.

        Args:
            metric_data: Metric data to evaluate
            source_id: Source identifier

        Returns:
            List of triggered alerts
        """
        rules = await self._get_applicable_rules(source_id)
        triggers = []

        for rule in rules:
            if self._should_skip_rule(rule):
                continue

            if await self._evaluate_condition(rule, metric_data):
                trigger = await self._trigger_alert(rule, metric_data, source_id)
                if trigger:
                    triggers.append(trigger)

        return triggers

    async def evaluate_consecutive_failures(
        self,
        source_id: str,
        current_status: str
    ) -> List[AlertTrigger]:
        """
        Check for consecutive failure patterns.

        Args:
            source_id: Source identifier
            current_status: Current run status

        Returns:
            List of triggered alerts
        """
        if current_status == "success":
            return []

        rules = await self._get_applicable_rules(source_id)
        triggers = []

        for rule in rules:
            if rule.condition != AlertCondition.CONSECUTIVE_FAILURES:
                continue

            if self._should_skip_rule(rule):
                continue

            # Check recent failures
            failure_count = await self._count_recent_failures(
                source_id, rule.consecutive_count
            )

            if failure_count >= rule.consecutive_count:
                metric_data = {
                    "consecutive_failures": failure_count,
                    "threshold": rule.consecutive_count
                }
                trigger = await self._trigger_alert(rule, metric_data, source_id)
                if trigger:
                    triggers.append(trigger)

        return triggers

    async def _get_applicable_rules(self, source_id: Optional[str]) -> List[AlertRule]:
        """Get rules applicable to a source."""
        await self._refresh_cache_if_needed()

        applicable = []
        for rule in self._rules_cache.values():
            if not rule.enabled:
                continue
            if rule.source_id is None or rule.source_id == source_id:
                applicable.append(rule)

        return applicable

    async def _refresh_cache_if_needed(self):
        """Refresh rules cache if expired."""
        now = datetime.utcnow()

        if (
            self._last_cache_refresh is None or
            (now - self._last_cache_refresh).total_seconds() > self._cache_ttl_seconds
        ):
            await self._refresh_cache()

    async def _refresh_cache(self):
        """Refresh the rules cache from database."""
        if not self.mongo:
            return

        try:
            cursor = self.mongo.db[self.RULES_COLLECTION].find({"enabled": True})
            self._rules_cache = {}

            for doc in cursor:
                rule = AlertRule.from_dict(doc)
                self._rules_cache[str(doc["_id"])] = rule

            self._last_cache_refresh = datetime.utcnow()

            logger.debug(
                "Alert rules cache refreshed",
                rule_count=len(self._rules_cache)
            )

        except Exception as e:
            logger.error("Failed to refresh rules cache", error=str(e))

    def _should_skip_rule(self, rule: AlertRule) -> bool:
        """Check if rule should be skipped due to cooldown."""
        if rule.last_triggered is None:
            return False

        cooldown_end = rule.last_triggered + timedelta(minutes=rule.cooldown_minutes)
        return datetime.utcnow() < cooldown_end

    async def _evaluate_condition(
        self,
        rule: AlertRule,
        metric_data: Dict[str, Any]
    ) -> bool:
        """
        Evaluate if a rule's condition is met.

        Args:
            rule: Alert rule
            metric_data: Metric data

        Returns:
            True if condition is met
        """
        field_value = metric_data.get(rule.metric_field)
        if field_value is None:
            return rule.condition == AlertCondition.MISSING_DATA

        try:
            if rule.condition == AlertCondition.THRESHOLD_ABOVE:
                return float(field_value) > rule.threshold

            elif rule.condition == AlertCondition.THRESHOLD_BELOW:
                return float(field_value) < rule.threshold

            elif rule.condition == AlertCondition.EQUALS:
                return field_value == rule.threshold

            elif rule.condition == AlertCondition.NOT_EQUALS:
                return field_value != rule.threshold

            elif rule.condition == AlertCondition.RATE_ABOVE:
                rate = await self._calculate_rate(
                    rule.metric_field,
                    metric_data.get("source_id"),
                    rule.window_minutes
                )
                return rate > rule.threshold

            elif rule.condition == AlertCondition.RATE_BELOW:
                rate = await self._calculate_rate(
                    rule.metric_field,
                    metric_data.get("source_id"),
                    rule.window_minutes
                )
                return rate < rule.threshold

            return False

        except (ValueError, TypeError) as e:
            logger.warning(
                "Error evaluating condition",
                rule=rule.name,
                field=rule.metric_field,
                value=field_value,
                error=str(e)
            )
            return False

    async def _calculate_rate(
        self,
        field: str,
        source_id: Optional[str],
        window_minutes: int
    ) -> float:
        """Calculate rate for a field over a time window."""
        if not self.mongo:
            return 0.0

        since = datetime.utcnow() - timedelta(minutes=window_minutes)
        query = {"started_at": {"$gte": since}}
        if source_id:
            query["source_id"] = source_id

        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": None,
                    "total": {"$sum": f"${field}"},
                    "count": {"$sum": 1}
                }
            }
        ]

        try:
            results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))
            if results and results[0]["count"] > 0:
                return results[0]["total"] / results[0]["count"]
            return 0.0
        except Exception as e:
            logger.error("Failed to calculate rate", error=str(e))
            return 0.0

    async def _count_recent_failures(
        self,
        source_id: str,
        count: int
    ) -> int:
        """Count recent consecutive failures."""
        if not self.mongo:
            return 0

        try:
            # Get recent metrics sorted by time
            cursor = (
                self.mongo.db.pipeline_metrics
                .find({"source_id": source_id})
                .sort("started_at", -1)
                .limit(count)
            )

            failures = 0
            for doc in cursor:
                if doc.get("status") == "failed":
                    failures += 1
                else:
                    break  # Stop counting at first success

            return failures

        except Exception as e:
            logger.error("Failed to count failures", error=str(e))
            return 0

    async def _trigger_alert(
        self,
        rule: AlertRule,
        metric_data: Dict[str, Any],
        source_id: Optional[str]
    ) -> Optional[AlertTrigger]:
        """
        Trigger an alert for a rule.

        Args:
            rule: Triggered rule
            metric_data: Metric data that triggered alert
            source_id: Source identifier

        Returns:
            AlertTrigger record
        """
        now = datetime.utcnow()

        # Create trigger record
        trigger = AlertTrigger(
            rule_id=str(getattr(rule, "_id", "")),
            rule_name=rule.name,
            source_id=source_id,
            triggered_at=now,
            severity=rule.severity.value,
            condition_details={
                "condition": rule.condition.value,
                "field": rule.metric_field,
                "threshold": rule.threshold,
                "actual_value": metric_data.get(rule.metric_field),
            },
            actions_taken=[],
            notification_sent=False
        )

        # Execute actions
        for action in rule.actions:
            action_result = await self._execute_action(
                action, rule, metric_data, source_id
            )
            if action_result:
                trigger.actions_taken.append(action.value)

                if action == AlertAction.NOTIFY:
                    trigger.notification_sent = True
                    trigger.notification_result = action_result

        # Update rule state
        rule.last_triggered = now
        rule.trigger_count += 1

        # Store trigger in history
        await self._store_trigger(trigger)

        # Update rule in database
        if self.mongo and hasattr(rule, "_id"):
            await self.update_rule(
                str(rule._id),
                {
                    "last_triggered": now,
                    "trigger_count": rule.trigger_count
                }
            )

        logger.info(
            "Alert triggered",
            rule=rule.name,
            source_id=source_id,
            severity=rule.severity.value,
        )

        return trigger

    async def _execute_action(
        self,
        action: AlertAction,
        rule: AlertRule,
        metric_data: Dict[str, Any],
        source_id: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Execute an alert action."""
        try:
            if action == AlertAction.NOTIFY:
                return await self._send_notification(rule, metric_data, source_id)

            elif action == AlertAction.LOG:
                logger.warning(
                    "Alert action: LOG",
                    rule=rule.name,
                    source_id=source_id,
                    metric_data=metric_data
                )
                return {"logged": True}

            elif action == AlertAction.DISABLE_SOURCE:
                return await self._disable_source(source_id)

            elif action == AlertAction.ESCALATE:
                # Escalate to higher severity
                escalated_severity = self._escalate_severity(rule.severity)
                return await self._send_notification(
                    rule, metric_data, source_id,
                    severity_override=escalated_severity
                )

            return None

        except Exception as e:
            logger.error(
                "Failed to execute alert action",
                action=action.value,
                error=str(e)
            )
            return None

    async def _send_notification(
        self,
        rule: AlertRule,
        metric_data: Dict[str, Any],
        source_id: Optional[str],
        severity_override: Optional[AlertSeverity] = None
    ) -> Optional[Dict[str, Any]]:
        """Send alert notification via dispatcher."""
        if not self.dispatcher:
            logger.warning("Alert dispatcher not configured")
            return None

        severity = severity_override or rule.severity

        title = f"Alert: {rule.name}"
        message = self._format_alert_message(rule, metric_data, source_id)

        result = await self.dispatcher.send_alert(
            title=title,
            message=message,
            severity=severity,
            source_id=source_id,
            metadata={
                "rule_name": rule.name,
                "condition": rule.condition.value,
                "field": rule.metric_field,
                "threshold": rule.threshold,
                "actual_value": metric_data.get(rule.metric_field),
            }
        )

        return result

    def _format_alert_message(
        self,
        rule: AlertRule,
        metric_data: Dict[str, Any],
        source_id: Optional[str]
    ) -> str:
        """Format alert message for notification."""
        actual_value = metric_data.get(rule.metric_field, "N/A")

        return f"""
Alert Rule Triggered: {rule.name}

Description: {rule.description}

Source: {source_id or 'All Sources'}
Condition: {rule.condition.value}
Field: {rule.metric_field}
Threshold: {rule.threshold}
Actual Value: {actual_value}

Please investigate and take appropriate action.
        """.strip()

    def _escalate_severity(self, current: AlertSeverity) -> AlertSeverity:
        """Escalate to next severity level."""
        escalation = {
            AlertSeverity.INFO: AlertSeverity.WARNING,
            AlertSeverity.WARNING: AlertSeverity.ERROR,
            AlertSeverity.ERROR: AlertSeverity.CRITICAL,
            AlertSeverity.CRITICAL: AlertSeverity.CRITICAL,
        }
        return escalation.get(current, AlertSeverity.ERROR)

    async def _disable_source(self, source_id: Optional[str]) -> Optional[Dict]:
        """Disable a source (placeholder for integration)."""
        if not source_id or not self.mongo:
            return None

        try:
            result = self.mongo.db.sources.update_one(
                {"_id": ObjectId(source_id)},
                {"$set": {"status": "disabled", "disabled_at": datetime.utcnow()}}
            )
            return {"disabled": result.modified_count > 0}
        except Exception as e:
            logger.error("Failed to disable source", error=str(e))
            return None

    async def _store_trigger(self, trigger: AlertTrigger) -> Optional[str]:
        """Store trigger in history."""
        if not self.mongo:
            return None

        try:
            result = self.mongo.db[self.HISTORY_COLLECTION].insert_one(
                trigger.to_dict()
            )
            return str(result.inserted_id)
        except Exception as e:
            logger.error("Failed to store trigger", error=str(e))
            return None

    # ==================== Alert History ====================

    async def get_alert_history(
        self,
        source_id: Optional[str] = None,
        severity: Optional[str] = None,
        acknowledged: Optional[bool] = None,
        hours: int = 24,
        limit: int = 100
    ) -> List[Dict]:
        """
        Get alert trigger history.

        Args:
            source_id: Filter by source
            severity: Filter by severity
            acknowledged: Filter by acknowledgement status
            hours: Time window
            limit: Maximum records

        Returns:
            List of alert triggers
        """
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)
        query = {"triggered_at": {"$gte": since}}

        if source_id:
            query["source_id"] = source_id
        if severity:
            query["severity"] = severity
        if acknowledged is not None:
            query["acknowledged"] = acknowledged

        try:
            cursor = (
                self.mongo.db[self.HISTORY_COLLECTION]
                .find(query)
                .sort("triggered_at", -1)
                .limit(limit)
            )
            return [self._serialize_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error("Failed to get alert history", error=str(e))
            return []

    async def acknowledge_alert(
        self,
        trigger_id: str,
        acknowledged_by: Optional[str] = None
    ) -> bool:
        """Acknowledge an alert trigger."""
        if not self.mongo:
            return False

        try:
            result = self.mongo.db[self.HISTORY_COLLECTION].update_one(
                {"_id": ObjectId(trigger_id)},
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
            logger.error("Failed to acknowledge alert", error=str(e))
            return False

    async def resolve_alert(
        self,
        trigger_id: str,
        resolution_note: Optional[str] = None
    ) -> bool:
        """Mark an alert as resolved."""
        if not self.mongo:
            return False

        try:
            updates = {
                "resolved": True,
                "resolved_at": datetime.utcnow()
            }
            if resolution_note:
                updates["resolution_note"] = resolution_note

            result = self.mongo.db[self.HISTORY_COLLECTION].update_one(
                {"_id": ObjectId(trigger_id)},
                {"$set": updates}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error("Failed to resolve alert", error=str(e))
            return False

    async def get_active_alerts_count(self) -> Dict[str, int]:
        """Get count of active (unacknowledged) alerts by severity."""
        if not self.mongo:
            return {}

        try:
            pipeline = [
                {"$match": {"acknowledged": False}},
                {"$group": {"_id": "$severity", "count": {"$sum": 1}}}
            ]
            results = list(
                self.mongo.db[self.HISTORY_COLLECTION].aggregate(pipeline)
            )
            return {r["_id"]: r["count"] for r in results}
        except Exception as e:
            logger.error("Failed to get active alerts count", error=str(e))
            return {}

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


# ==================== Pre-defined Alert Rules ====================

def create_default_rules() -> List[AlertRule]:
    """Create a set of default alert rules."""
    return [
        AlertRule(
            name="High Error Rate",
            description="Error count exceeds threshold during pipeline execution",
            condition=AlertCondition.THRESHOLD_ABOVE,
            metric_field="error_count",
            threshold=10,
            severity=AlertSeverity.ERROR,
            actions=[AlertAction.NOTIFY],
            cooldown_minutes=30,
            tags=["error", "default"]
        ),
        AlertRule(
            name="Low Quality Score",
            description="Data quality score falls below acceptable threshold",
            condition=AlertCondition.THRESHOLD_BELOW,
            metric_field="quality_score",
            threshold=70.0,
            severity=AlertSeverity.WARNING,
            actions=[AlertAction.NOTIFY],
            cooldown_minutes=60,
            tags=["quality", "default"]
        ),
        AlertRule(
            name="Consecutive Failures",
            description="Multiple consecutive pipeline failures detected",
            condition=AlertCondition.CONSECUTIVE_FAILURES,
            metric_field="status",
            consecutive_count=3,
            severity=AlertSeverity.CRITICAL,
            actions=[AlertAction.NOTIFY, AlertAction.ESCALATE],
            cooldown_minutes=15,
            tags=["failure", "default"]
        ),
        AlertRule(
            name="Long Execution Time",
            description="Pipeline execution time exceeds expected duration",
            condition=AlertCondition.THRESHOLD_ABOVE,
            metric_field="execution_time_ms",
            threshold=300000,  # 5 minutes
            severity=AlertSeverity.WARNING,
            actions=[AlertAction.NOTIFY],
            cooldown_minutes=60,
            tags=["performance", "default"]
        ),
        AlertRule(
            name="Zero Records Loaded",
            description="Pipeline completed but loaded no records",
            condition=AlertCondition.EQUALS,
            metric_field="records_loaded",
            threshold=0,
            severity=AlertSeverity.WARNING,
            actions=[AlertAction.NOTIFY],
            cooldown_minutes=30,
            tags=["data", "default"]
        ),
    ]

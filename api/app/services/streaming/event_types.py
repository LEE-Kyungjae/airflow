"""
Event Types and Models for Streaming Pipeline.

This module defines the event types and data models used throughout
the streaming architecture. Designed for future Kafka integration.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field, asdict
from uuid import uuid4
import json


class EventType(str, Enum):
    """
    Core event types in the streaming pipeline.

    Naming Convention:
    - ENTITY_ACTION format for clarity
    - Past tense for completed events
    - Present tense for commands/requests
    """
    # Data Lifecycle Events
    DATA_CREATED = "data.created"
    DATA_UPDATED = "data.updated"
    DATA_DELETED = "data.deleted"
    DATA_VALIDATED = "data.validated"
    DATA_VALIDATION_FAILED = "data.validation_failed"

    # Review Workflow Events
    REVIEW_REQUESTED = "review.requested"
    REVIEW_STARTED = "review.started"
    REVIEW_COMPLETED = "review.completed"
    REVIEW_REJECTED = "review.rejected"
    REVIEW_CORRECTION_APPLIED = "review.correction_applied"

    # Promotion Events (staging -> production)
    PROMOTION_REQUESTED = "promotion.requested"
    PROMOTED_TO_PRODUCTION = "promotion.completed"
    PROMOTION_FAILED = "promotion.failed"
    PROMOTION_ROLLED_BACK = "promotion.rolled_back"

    # Crawler Events
    CRAWL_STARTED = "crawl.started"
    CRAWL_COMPLETED = "crawl.completed"
    CRAWL_FAILED = "crawl.failed"

    # Schema Events
    SCHEMA_DETECTED = "schema.detected"
    SCHEMA_EVOLVED = "schema.evolved"
    SCHEMA_CONFLICT = "schema.conflict"

    # System Events
    SYSTEM_HEALTH_CHECK = "system.health_check"
    SYSTEM_ALERT = "system.alert"

    # Batch Processing Events (Lambda Architecture - Batch Layer)
    BATCH_JOB_STARTED = "batch.job_started"
    BATCH_JOB_COMPLETED = "batch.job_completed"
    BATCH_REPROCESSING = "batch.reprocessing"


class EventPriority(int, Enum):
    """Event priority levels for processing order."""
    CRITICAL = 0    # System alerts, failures
    HIGH = 1        # Data validation failures, promotions
    NORMAL = 2      # Regular data events
    LOW = 3         # Metrics, logging events
    BACKGROUND = 4  # Cleanup, maintenance


class EventSource(str, Enum):
    """Sources that can emit events."""
    MONGODB_CHANGE_STREAM = "mongodb.change_stream"
    API_ENDPOINT = "api.endpoint"
    CRAWLER_ENGINE = "crawler.engine"
    AIRFLOW_DAG = "airflow.dag"
    REVIEW_SYSTEM = "review.system"
    SCHEDULER = "scheduler"
    MANUAL = "manual"
    KAFKA_CONSUMER = "kafka.consumer"  # Future


@dataclass
class EventMetadata:
    """
    Common metadata for all events.

    Follows CloudEvents specification for interoperability.
    """
    event_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    source: EventSource = EventSource.API_ENDPOINT
    correlation_id: Optional[str] = None  # For tracing related events
    causation_id: Optional[str] = None    # ID of event that caused this one
    version: str = "1.0"
    retry_count: int = 0
    max_retries: int = 3

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source.value,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "version": self.version,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries
        }


@dataclass
class BaseEvent:
    """
    Base class for all events in the streaming pipeline.

    Designed to be serializable to JSON for Kafka compatibility.
    """
    event_type: EventType
    priority: EventPriority = EventPriority.NORMAL
    metadata: EventMetadata = field(default_factory=EventMetadata)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "priority": self.priority.value,
            "metadata": self.metadata.to_dict()
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)

    @property
    def event_id(self) -> str:
        return self.metadata.event_id

    @property
    def timestamp(self) -> datetime:
        return self.metadata.timestamp


@dataclass
class DataEvent(BaseEvent):
    """
    Event for data operations (create, update, delete).

    Attributes:
        source_id: MongoDB ObjectId of the source
        collection: Target collection name
        document_id: MongoDB ObjectId of the document
        operation: MongoDB operation type (insert, update, replace, delete)
        data: The actual data payload
        previous_data: Previous state (for updates)
        change_fields: List of changed field names
    """
    source_id: str = ""
    collection: str = ""
    document_id: str = ""
    operation: str = ""  # insert, update, replace, delete
    data: Dict[str, Any] = field(default_factory=dict)
    previous_data: Optional[Dict[str, Any]] = None
    change_fields: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "source_id": self.source_id,
            "collection": self.collection,
            "document_id": self.document_id,
            "operation": self.operation,
            "data": self.data,
            "previous_data": self.previous_data,
            "change_fields": self.change_fields
        })
        return base


@dataclass
class ReviewEvent(BaseEvent):
    """Event for review workflow operations."""
    review_id: str = ""
    staging_id: str = ""
    source_id: str = ""
    reviewer_id: Optional[str] = None
    status: str = ""
    corrections: List[Dict[str, Any]] = field(default_factory=list)
    review_duration_ms: Optional[int] = None
    confidence_score: Optional[float] = None
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "review_id": self.review_id,
            "staging_id": self.staging_id,
            "source_id": self.source_id,
            "reviewer_id": self.reviewer_id,
            "status": self.status,
            "corrections": self.corrections,
            "review_duration_ms": self.review_duration_ms,
            "confidence_score": self.confidence_score,
            "notes": self.notes
        })
        return base


@dataclass
class PromotionEvent(BaseEvent):
    """Event for data promotion (staging -> production)."""
    staging_id: str = ""
    production_id: Optional[str] = None
    staging_collection: str = ""
    production_collection: str = ""
    source_id: str = ""
    promoted_by: str = ""
    has_corrections: bool = False
    rollback_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "staging_id": self.staging_id,
            "production_id": self.production_id,
            "staging_collection": self.staging_collection,
            "production_collection": self.production_collection,
            "source_id": self.source_id,
            "promoted_by": self.promoted_by,
            "has_corrections": self.has_corrections,
            "rollback_reason": self.rollback_reason
        })
        return base


@dataclass
class ValidationEvent(BaseEvent):
    """Event for data validation results."""
    document_id: str = ""
    collection: str = ""
    source_id: str = ""
    validation_passed: bool = True
    validation_rules: List[str] = field(default_factory=list)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    quality_score: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "document_id": self.document_id,
            "collection": self.collection,
            "source_id": self.source_id,
            "validation_passed": self.validation_passed,
            "validation_rules": self.validation_rules,
            "errors": self.errors,
            "warnings": self.warnings,
            "quality_score": self.quality_score
        })
        return base


@dataclass
class CrawlEvent(BaseEvent):
    """Event for crawl operations."""
    source_id: str = ""
    crawler_id: str = ""
    run_id: str = ""
    status: str = ""
    record_count: int = 0
    error_message: Optional[str] = None
    execution_time_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "source_id": self.source_id,
            "crawler_id": self.crawler_id,
            "run_id": self.run_id,
            "status": self.status,
            "record_count": self.record_count,
            "error_message": self.error_message,
            "execution_time_ms": self.execution_time_ms
        })
        return base


@dataclass
class BatchEvent(BaseEvent):
    """
    Event for batch processing (Lambda Architecture - Batch Layer).

    Batch events track large-scale reprocessing jobs that reconcile
    with the real-time streaming layer.
    """
    job_id: str = ""
    job_type: str = ""  # reprocess, aggregate, reconcile
    source_ids: List[str] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    records_processed: int = 0
    records_failed: int = 0
    status: str = ""

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "job_id": self.job_id,
            "job_type": self.job_type,
            "source_ids": self.source_ids,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "status": self.status
        })
        return base


@dataclass
class SystemAlertEvent(BaseEvent):
    """Event for system alerts and monitoring."""
    alert_type: str = ""
    severity: str = "warning"  # info, warning, error, critical
    message: str = ""
    component: str = ""
    details: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Auto-set priority based on severity
        severity_priority_map = {
            "critical": EventPriority.CRITICAL,
            "error": EventPriority.HIGH,
            "warning": EventPriority.NORMAL,
            "info": EventPriority.LOW
        }
        self.priority = severity_priority_map.get(self.severity, EventPriority.NORMAL)

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "alert_type": self.alert_type,
            "severity": self.severity,
            "message": self.message,
            "component": self.component,
            "details": self.details
        })
        return base


# Type alias for any event
Event = Union[
    DataEvent,
    ReviewEvent,
    PromotionEvent,
    ValidationEvent,
    CrawlEvent,
    BatchEvent,
    SystemAlertEvent
]


def event_from_dict(data: Dict[str, Any]) -> BaseEvent:
    """
    Factory function to create event from dictionary.

    Useful for deserializing events from Kafka or other message brokers.
    """
    event_type_str = data.get("event_type", "")

    # Determine event class based on event type prefix
    if event_type_str.startswith("data."):
        cls = DataEvent
    elif event_type_str.startswith("review."):
        cls = ReviewEvent
    elif event_type_str.startswith("promotion."):
        cls = PromotionEvent
    elif event_type_str.startswith("crawl."):
        cls = CrawlEvent
    elif event_type_str.startswith("batch."):
        cls = BatchEvent
    elif event_type_str.startswith("system."):
        cls = SystemAlertEvent
    else:
        # Default to base validation event
        cls = ValidationEvent

    # Parse metadata
    metadata_dict = data.get("metadata", {})
    metadata = EventMetadata(
        event_id=metadata_dict.get("event_id", str(uuid4())),
        timestamp=datetime.fromisoformat(metadata_dict.get("timestamp", datetime.utcnow().isoformat())),
        source=EventSource(metadata_dict.get("source", EventSource.API_ENDPOINT.value)),
        correlation_id=metadata_dict.get("correlation_id"),
        causation_id=metadata_dict.get("causation_id"),
        version=metadata_dict.get("version", "1.0"),
        retry_count=metadata_dict.get("retry_count", 0),
        max_retries=metadata_dict.get("max_retries", 3)
    )

    # Build event with common fields
    event_data = {
        "event_type": EventType(event_type_str),
        "priority": EventPriority(data.get("priority", EventPriority.NORMAL.value)),
        "metadata": metadata
    }

    # Add class-specific fields
    for key, value in data.items():
        if key not in ("event_type", "priority", "metadata"):
            event_data[key] = value

    return cls(**event_data)


# Topic/Channel mapping for future Kafka integration
TOPIC_MAPPING = {
    # Data events -> high-throughput topic
    EventType.DATA_CREATED: "crawler.data.events",
    EventType.DATA_UPDATED: "crawler.data.events",
    EventType.DATA_DELETED: "crawler.data.events",

    # Validation events
    EventType.DATA_VALIDATED: "crawler.validation.events",
    EventType.DATA_VALIDATION_FAILED: "crawler.validation.events",

    # Review workflow -> separate topic for ordering guarantees
    EventType.REVIEW_REQUESTED: "crawler.review.events",
    EventType.REVIEW_STARTED: "crawler.review.events",
    EventType.REVIEW_COMPLETED: "crawler.review.events",
    EventType.REVIEW_REJECTED: "crawler.review.events",
    EventType.REVIEW_CORRECTION_APPLIED: "crawler.review.events",

    # Promotion events -> critical, separate topic
    EventType.PROMOTION_REQUESTED: "crawler.promotion.events",
    EventType.PROMOTED_TO_PRODUCTION: "crawler.promotion.events",
    EventType.PROMOTION_FAILED: "crawler.promotion.events",
    EventType.PROMOTION_ROLLED_BACK: "crawler.promotion.events",

    # Crawl events
    EventType.CRAWL_STARTED: "crawler.crawl.events",
    EventType.CRAWL_COMPLETED: "crawler.crawl.events",
    EventType.CRAWL_FAILED: "crawler.crawl.events",

    # System events -> monitoring topic
    EventType.SYSTEM_HEALTH_CHECK: "crawler.system.events",
    EventType.SYSTEM_ALERT: "crawler.system.events",

    # Batch events
    EventType.BATCH_JOB_STARTED: "crawler.batch.events",
    EventType.BATCH_JOB_COMPLETED: "crawler.batch.events",
    EventType.BATCH_REPROCESSING: "crawler.batch.events",
}


def get_topic_for_event(event: BaseEvent) -> str:
    """Get the appropriate topic/channel for an event."""
    return TOPIC_MAPPING.get(event.event_type, "crawler.default.events")

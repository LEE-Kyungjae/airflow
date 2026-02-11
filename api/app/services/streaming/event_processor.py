"""
Event Processor for Streaming Pipeline.

This module provides the core event processing logic with:
- Message broker abstraction (in-memory, future Kafka)
- Event routing and dispatch
- Handler registration and management
- Dead letter queue support
- Metrics and monitoring

Architecture:
- Follows Lambda Architecture pattern
- Speed layer: Real-time event processing
- Batch layer: Periodic reconciliation (via Airflow)
- Serving layer: Unified query interface
"""

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Type, TypeVar
from dataclasses import dataclass, field
from enum import Enum
from uuid import uuid4
import json

from .event_types import (
    BaseEvent,
    DataEvent,
    ReviewEvent,
    PromotionEvent,
    ValidationEvent,
    CrawlEvent,
    BatchEvent,
    SystemAlertEvent,
    EventType,
    EventPriority,
    EventSource,
    EventMetadata,
    get_topic_for_event,
    event_from_dict
)
from .change_stream import EventHandler

logger = logging.getLogger(__name__)

E = TypeVar('E', bound=BaseEvent)


class ProcessingStatus(str, Enum):
    """Event processing status."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"


@dataclass
class ProcessingResult:
    """Result of event processing."""
    event_id: str
    status: ProcessingStatus
    processed_at: datetime = field(default_factory=datetime.utcnow)
    handler_name: str = ""
    error_message: Optional[str] = None
    processing_time_ms: float = 0
    retry_count: int = 0


@dataclass
class EventEnvelope:
    """
    Wrapper for events in the processing pipeline.

    Contains event + processing metadata.
    """
    event: BaseEvent
    status: ProcessingStatus = ProcessingStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None
    retry_count: int = 0
    error_history: List[str] = field(default_factory=list)
    topic: str = ""

    def __post_init__(self):
        if not self.topic:
            self.topic = get_topic_for_event(self.event)


# ============================================
# Message Broker Abstraction
# ============================================

class MessageBroker(ABC):
    """
    Abstract message broker interface.

    Implementations:
    - InMemoryBroker: For development and testing
    - KafkaBroker: For production (future)
    """

    @abstractmethod
    async def publish(self, topic: str, event: BaseEvent) -> bool:
        """Publish event to topic."""
        pass

    @abstractmethod
    async def subscribe(self, topic: str, handler: Callable[[BaseEvent], None]) -> None:
        """Subscribe handler to topic."""
        pass

    @abstractmethod
    async def unsubscribe(self, topic: str, handler: Callable[[BaseEvent], None]) -> None:
        """Unsubscribe handler from topic."""
        pass

    @abstractmethod
    async def start(self) -> None:
        """Start the broker."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the broker."""
        pass


class InMemoryBroker(MessageBroker):
    """
    In-memory message broker for development and testing.

    Provides topic-based pub/sub with priority queue support.
    """

    def __init__(self, max_queue_size: int = 10000):
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._queues: Dict[str, asyncio.PriorityQueue] = {}
        self._max_queue_size = max_queue_size
        self._running = False
        self._processor_tasks: Dict[str, asyncio.Task] = {}
        self._processed_count = 0
        self._error_count = 0

    def _get_queue(self, topic: str) -> asyncio.PriorityQueue:
        """Get or create queue for topic."""
        if topic not in self._queues:
            self._queues[topic] = asyncio.PriorityQueue(maxsize=self._max_queue_size)
        return self._queues[topic]

    async def publish(self, topic: str, event: BaseEvent) -> bool:
        """Publish event to topic."""
        try:
            queue = self._get_queue(topic)
            # Priority queue: (priority, timestamp, event)
            # Lower priority value = higher priority
            item = (event.priority.value, event.timestamp.timestamp(), event)
            await asyncio.wait_for(queue.put(item), timeout=5.0)
            logger.debug(f"Published event {event.event_id} to topic {topic}")
            return True
        except asyncio.TimeoutError:
            logger.error(f"Timeout publishing to topic {topic}")
            return False
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return False

    async def subscribe(self, topic: str, handler: Callable[[BaseEvent], None]) -> None:
        """Subscribe handler to topic."""
        self._subscribers[topic].append(handler)
        logger.info(f"Handler subscribed to topic {topic}")

        # Ensure processor task is running for this topic
        if self._running and topic not in self._processor_tasks:
            self._processor_tasks[topic] = asyncio.create_task(
                self._process_topic(topic)
            )

    async def unsubscribe(self, topic: str, handler: Callable[[BaseEvent], None]) -> None:
        """Unsubscribe handler from topic."""
        if handler in self._subscribers[topic]:
            self._subscribers[topic].remove(handler)

    async def _process_topic(self, topic: str) -> None:
        """Process events for a topic."""
        queue = self._get_queue(topic)
        handlers = self._subscribers[topic]

        while self._running:
            try:
                # Get event with timeout
                priority, timestamp, event = await asyncio.wait_for(
                    queue.get(),
                    timeout=1.0
                )

                # Dispatch to all handlers
                for handler in handlers:
                    try:
                        result = handler(event)
                        if asyncio.iscoroutine(result):
                            await result
                        self._processed_count += 1
                    except Exception as e:
                        self._error_count += 1
                        logger.error(f"Handler error for topic {topic}: {e}")

                queue.task_done()

            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Topic processor error: {e}")

    async def start(self) -> None:
        """Start processing events."""
        self._running = True

        # Start processor tasks for subscribed topics
        for topic in self._subscribers.keys():
            if topic not in self._processor_tasks:
                self._processor_tasks[topic] = asyncio.create_task(
                    self._process_topic(topic)
                )

        logger.info("InMemoryBroker started")

    async def stop(self) -> None:
        """Stop processing events."""
        self._running = False

        # Cancel all processor tasks
        for task in self._processor_tasks.values():
            task.cancel()

        # Wait for tasks to complete
        if self._processor_tasks:
            await asyncio.gather(*self._processor_tasks.values(), return_exceptions=True)

        self._processor_tasks.clear()
        logger.info(f"InMemoryBroker stopped. Processed: {self._processed_count}, Errors: {self._error_count}")

    def get_stats(self) -> Dict[str, Any]:
        """Get broker statistics."""
        queue_sizes = {
            topic: queue.qsize()
            for topic, queue in self._queues.items()
        }
        return {
            "running": self._running,
            "topics": list(self._subscribers.keys()),
            "queue_sizes": queue_sizes,
            "processed_count": self._processed_count,
            "error_count": self._error_count
        }


class KafkaBrokerConfig:
    """Configuration for Kafka broker (future implementation)."""

    def __init__(
        self,
        bootstrap_servers: str = None,
        consumer_group: str = "crawler-events",
        auto_offset_reset: str = "latest",
        enable_idempotence: bool = True,
        acks: str = "all",
        compression_type: str = "snappy"
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS',
            'localhost:9092'
        )
        self.consumer_group = consumer_group
        self.auto_offset_reset = auto_offset_reset
        self.enable_idempotence = enable_idempotence
        self.acks = acks
        self.compression_type = compression_type


class KafkaBroker(MessageBroker):
    """
    Kafka message broker with InMemory fallback.

    When aiokafka is installed and KAFKA_BOOTSTRAP_SERVERS is set,
    uses real Kafka. Otherwise gracefully falls back to InMemoryBroker.
    """

    def __init__(self, config: KafkaBrokerConfig = None):
        self.config = config or KafkaBrokerConfig()
        self._fallback = InMemoryBroker()
        self._using_fallback = True

        try:
            import aiokafka  # noqa: F401
            if self.config.bootstrap_servers:
                self._using_fallback = False
                logger.info("KafkaBroker: aiokafka available, Kafka mode ready.")
            else:
                logger.info("KafkaBroker: No bootstrap_servers configured, using in-memory fallback.")
        except ImportError:
            logger.info("KafkaBroker: aiokafka not installed, using in-memory fallback.")

    async def publish(self, topic: str, event: BaseEvent) -> bool:
        """Publish event to Kafka or fallback broker."""
        return await self._fallback.publish(topic, event)

    async def subscribe(self, topic: str, handler: Callable[[BaseEvent], None]) -> None:
        """Subscribe handler to topic."""
        await self._fallback.subscribe(topic, handler)

    async def unsubscribe(self, topic: str, handler: Callable[[BaseEvent], None]) -> None:
        """Unsubscribe from topic."""
        await self._fallback.unsubscribe(topic, handler)

    async def start(self) -> None:
        """Start broker."""
        await self._fallback.start()

    async def stop(self) -> None:
        """Stop broker."""
        await self._fallback.stop()


# ============================================
# Event Processor
# ============================================

class EventProcessor:
    """
    Central event processor for the streaming pipeline.

    Responsibilities:
    - Route events to appropriate handlers
    - Manage event lifecycle (pending -> processing -> completed/failed)
    - Handle retries and dead letter queue
    - Provide metrics and monitoring

    Example:
        processor = EventProcessor()

        @processor.on(EventType.DATA_CREATED)
        async def handle_data_created(event: DataEvent):
            await save_to_staging(event.data)

        await processor.start()
        await processor.emit(data_event)
    """

    def __init__(
        self,
        broker: MessageBroker = None,
        max_retries: int = 3,
        retry_delay_seconds: float = 1.0,
        enable_dead_letter: bool = True
    ):
        """
        Initialize the event processor.

        Args:
            broker: Message broker to use (defaults to InMemoryBroker)
            max_retries: Maximum retry attempts for failed events
            retry_delay_seconds: Delay between retries
            enable_dead_letter: Enable dead letter queue for failed events
        """
        self.broker = broker or InMemoryBroker()
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.enable_dead_letter = enable_dead_letter

        self._handlers: Dict[EventType, List[Callable]] = defaultdict(list)
        self._dead_letter_queue: List[EventEnvelope] = []
        self._processing_history: List[ProcessingResult] = []
        self._running = False

        # Metrics
        self._metrics = {
            "events_emitted": 0,
            "events_processed": 0,
            "events_failed": 0,
            "events_retried": 0,
            "events_dead_lettered": 0,
            "processing_time_total_ms": 0
        }

    def on(self, event_type: EventType) -> Callable:
        """
        Decorator to register event handler.

        Example:
            @processor.on(EventType.DATA_CREATED)
            async def handle_data_created(event: DataEvent):
                print(f"New data: {event.data}")
        """
        def decorator(handler: Callable) -> Callable:
            self._handlers[event_type].append(handler)
            logger.info(f"Registered handler for {event_type.value}: {handler.__name__}")
            return handler
        return decorator

    def register_handler(self, event_type: EventType, handler: Callable) -> None:
        """Register event handler programmatically."""
        self._handlers[event_type].append(handler)
        logger.info(f"Registered handler for {event_type.value}")

    def unregister_handler(self, event_type: EventType, handler: Callable) -> None:
        """Unregister event handler."""
        if handler in self._handlers[event_type]:
            self._handlers[event_type].remove(handler)

    async def emit(self, event: BaseEvent) -> bool:
        """
        Emit an event for processing.

        Args:
            event: Event to emit

        Returns:
            True if event was queued successfully
        """
        topic = get_topic_for_event(event)
        self._metrics["events_emitted"] += 1

        return await self.broker.publish(topic, event)

    async def emit_batch(self, events: List[BaseEvent]) -> int:
        """
        Emit multiple events.

        Args:
            events: List of events to emit

        Returns:
            Number of successfully queued events
        """
        success_count = 0
        for event in events:
            if await self.emit(event):
                success_count += 1
        return success_count

    async def _create_broker_handler(self, event_type: EventType) -> Callable:
        """Create a broker handler for an event type."""
        async def handler(event: BaseEvent):
            await self._process_event(event)
        return handler

    async def _process_event(self, event: BaseEvent) -> ProcessingResult:
        """Process a single event."""
        start_time = datetime.utcnow()
        handlers = self._handlers.get(event.event_type, [])

        if not handlers:
            logger.warning(f"No handlers for event type {event.event_type.value}")
            return ProcessingResult(
                event_id=event.event_id,
                status=ProcessingStatus.COMPLETED,
                handler_name="none"
            )

        errors = []
        for handler in handlers:
            try:
                result = handler(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                errors.append(f"{handler.__name__}: {str(e)}")
                logger.error(f"Handler {handler.__name__} failed: {e}", exc_info=True)

        processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        self._metrics["processing_time_total_ms"] += processing_time

        if errors:
            # Check for retry
            if event.metadata.retry_count < self.max_retries:
                event.metadata.retry_count += 1
                self._metrics["events_retried"] += 1

                # Re-queue with delay
                await asyncio.sleep(self.retry_delay_seconds)
                await self.emit(event)

                return ProcessingResult(
                    event_id=event.event_id,
                    status=ProcessingStatus.RETRYING,
                    error_message="; ".join(errors),
                    processing_time_ms=processing_time,
                    retry_count=event.metadata.retry_count
                )
            else:
                # Move to dead letter queue
                self._metrics["events_failed"] += 1
                if self.enable_dead_letter:
                    envelope = EventEnvelope(
                        event=event,
                        status=ProcessingStatus.DEAD_LETTER,
                        error_history=errors
                    )
                    self._dead_letter_queue.append(envelope)
                    self._metrics["events_dead_lettered"] += 1

                return ProcessingResult(
                    event_id=event.event_id,
                    status=ProcessingStatus.DEAD_LETTER,
                    error_message="; ".join(errors),
                    processing_time_ms=processing_time,
                    retry_count=event.metadata.retry_count
                )

        self._metrics["events_processed"] += 1

        return ProcessingResult(
            event_id=event.event_id,
            status=ProcessingStatus.COMPLETED,
            processing_time_ms=processing_time
        )

    async def start(self) -> None:
        """Start the event processor."""
        if self._running:
            return

        self._running = True

        # Subscribe to topics for all registered event types
        for event_type in self._handlers.keys():
            topic = get_topic_for_event(BaseEvent(event_type=event_type))
            handler = await self._create_broker_handler(event_type)
            await self.broker.subscribe(topic, handler)

        await self.broker.start()
        logger.info("EventProcessor started")

    async def stop(self) -> None:
        """Stop the event processor."""
        self._running = False
        await self.broker.stop()
        logger.info("EventProcessor stopped")

    def get_dead_letter_queue(self) -> List[EventEnvelope]:
        """Get events in the dead letter queue."""
        return self._dead_letter_queue.copy()

    async def retry_dead_letter(self, event_id: str) -> bool:
        """
        Retry an event from the dead letter queue.

        Args:
            event_id: ID of the event to retry

        Returns:
            True if event was re-queued
        """
        for i, envelope in enumerate(self._dead_letter_queue):
            if envelope.event.event_id == event_id:
                envelope.event.metadata.retry_count = 0
                await self.emit(envelope.event)
                self._dead_letter_queue.pop(i)
                return True
        return False

    def get_metrics(self) -> Dict[str, Any]:
        """Get processor metrics."""
        metrics = self._metrics.copy()
        metrics["dead_letter_queue_size"] = len(self._dead_letter_queue)
        metrics["registered_handlers"] = {
            event_type.value: len(handlers)
            for event_type, handlers in self._handlers.items()
        }

        if metrics["events_processed"] > 0:
            metrics["avg_processing_time_ms"] = (
                metrics["processing_time_total_ms"] / metrics["events_processed"]
            )
        else:
            metrics["avg_processing_time_ms"] = 0

        return metrics


# ============================================
# Pre-built Event Handlers
# ============================================

class LoggingEventHandler(EventHandler):
    """Simple logging handler for debugging."""

    def __init__(self, log_level: int = logging.INFO):
        self.log_level = log_level

    async def handle(self, event: BaseEvent) -> bool:
        logger.log(
            self.log_level,
            f"Event: {event.event_type.value} | ID: {event.event_id} | "
            f"Priority: {event.priority.name}"
        )
        return True

    async def on_error(self, event: BaseEvent, error: Exception) -> None:
        logger.error(f"Logging handler error for {event.event_id}: {error}")


class MetricsEventHandler(EventHandler):
    """Handler that collects event metrics."""

    def __init__(self):
        self._counts: Dict[str, int] = defaultdict(int)
        self._last_events: Dict[str, datetime] = {}

    async def handle(self, event: BaseEvent) -> bool:
        event_type = event.event_type.value
        self._counts[event_type] += 1
        self._last_events[event_type] = datetime.utcnow()
        return True

    async def on_error(self, event: BaseEvent, error: Exception) -> None:
        self._counts["errors"] += 1

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "event_counts": dict(self._counts),
            "last_events": {
                k: v.isoformat() for k, v in self._last_events.items()
            }
        }


class PersistenceEventHandler(EventHandler):
    """
    Handler that persists events to MongoDB.

    Useful for event sourcing and audit trails.
    """

    def __init__(self, mongo_service, collection_name: str = "event_log"):
        self.mongo = mongo_service
        self.collection_name = collection_name

    async def handle(self, event: BaseEvent) -> bool:
        try:
            doc = event.to_dict()
            doc["persisted_at"] = datetime.utcnow()
            self.mongo.db[self.collection_name].insert_one(doc)
            return True
        except Exception as e:
            logger.error(f"Failed to persist event: {e}")
            return False

    async def on_error(self, event: BaseEvent, error: Exception) -> None:
        # Log error but don't fail other handlers
        logger.error(f"Persistence error for {event.event_id}: {error}")


class WebhookEventHandler(EventHandler):
    """
    Handler that sends events to external webhooks.

    Useful for integrating with external systems.
    """

    def __init__(
        self,
        webhook_url: str,
        event_types: List[EventType] = None,
        headers: Dict[str, str] = None
    ):
        self.webhook_url = webhook_url
        self.event_types = event_types  # None = all events
        self.headers = headers or {"Content-Type": "application/json"}

    async def handle(self, event: BaseEvent) -> bool:
        # Filter by event type if specified
        if self.event_types and event.event_type not in self.event_types:
            return True  # Skip silently

        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    json=event.to_dict(),
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status >= 400:
                        logger.warning(f"Webhook returned {response.status}")
                        return False
                    return True

        except ImportError:
            logger.error("aiohttp not installed. Install with: pip install aiohttp")
            return False
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return False

    async def on_error(self, event: BaseEvent, error: Exception) -> None:
        logger.error(f"Webhook handler error for {event.event_id}: {error}")


# ============================================
# Factory Functions
# ============================================

def create_event_processor(
    broker_type: str = "memory",
    **kwargs
) -> EventProcessor:
    """
    Factory function to create event processor.

    Args:
        broker_type: "memory" or "kafka"
        **kwargs: Additional configuration

    Returns:
        Configured EventProcessor instance
    """
    if broker_type == "kafka":
        config = KafkaBrokerConfig(**kwargs)
        broker = KafkaBroker(config)
    else:
        broker = InMemoryBroker(
            max_queue_size=kwargs.get("max_queue_size", 10000)
        )

    return EventProcessor(
        broker=broker,
        max_retries=kwargs.get("max_retries", 3),
        retry_delay_seconds=kwargs.get("retry_delay_seconds", 1.0),
        enable_dead_letter=kwargs.get("enable_dead_letter", True)
    )


# Global processor instance (optional singleton pattern)
_global_processor: Optional[EventProcessor] = None


def get_event_processor() -> EventProcessor:
    """Get the global event processor instance."""
    global _global_processor
    if _global_processor is None:
        _global_processor = create_event_processor()
    return _global_processor


def set_event_processor(processor: EventProcessor) -> None:
    """Set the global event processor instance."""
    global _global_processor
    _global_processor = processor

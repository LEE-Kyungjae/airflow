"""
Real-time Streaming Processing Module.

This module provides the streaming layer for the Lambda Architecture,
enabling real-time data processing alongside batch processing (Airflow).

Architecture Overview:
----------------------

    +------------------+     +-----------------+     +------------------+
    |   Data Sources   | --> | MongoDB Change  | --> |  Event Processor |
    | (Crawlers, API)  |     |    Streams      |     |   (Speed Layer)  |
    +------------------+     +-----------------+     +------------------+
                                                              |
                                                              v
    +------------------+     +-----------------+     +------------------+
    |  Batch Layer     | <-- |   MongoDB       | <-- |  Real-time       |
    |  (Airflow DAGs)  |     |   Collections   |     |  Validation      |
    +------------------+     +-----------------+     +------------------+
           |                                                  |
           v                                                  v
    +------------------+     +-----------------+     +------------------+
    |  Batch Views     | --> |  Serving Layer  | <-- |  Real-time Views |
    |  (Aggregations)  |     |  (Unified API)  |     |  (Latest State)  |
    +------------------+     +-----------------+     +------------------+

Components:
-----------

1. **Event Types** (`event_types.py`)
   - Domain event definitions following CloudEvents spec
   - Event types: DATA_CREATED, DATA_UPDATED, REVIEW_COMPLETED, PROMOTED_TO_PRODUCTION
   - Future Kafka topic mapping

2. **Change Stream Listener** (`change_stream.py`)
   - MongoDB Change Streams for CDC (Change Data Capture)
   - Resumable watching with token persistence
   - Event transformation to domain events

3. **Event Processor** (`event_processor.py`)
   - Message broker abstraction (InMemory, future Kafka)
   - Event routing and dispatch
   - Dead letter queue support
   - Retry mechanisms

4. **Real-time Validator** (`realtime_validator.py`)
   - Stream-based data validation
   - Integration with data_quality framework
   - Validation result event emission

Usage Examples:
---------------

Basic Event Processing:

    from api.app.services.streaming import (
        EventProcessor,
        EventType,
        DataEvent,
        create_event_processor
    )

    # Create processor
    processor = create_event_processor()

    # Register handler
    @processor.on(EventType.DATA_CREATED)
    async def handle_data_created(event: DataEvent):
        print(f"New data in {event.collection}: {event.document_id}")

    # Start processing
    await processor.start()

    # Emit events
    event = DataEvent(
        event_type=EventType.DATA_CREATED,
        source_id="source_123",
        collection="staging_data",
        document_id="doc_456",
        data={"title": "Test", "content": "Hello"}
    )
    await processor.emit(event)


MongoDB Change Stream Listening:

    from api.app.services.streaming import (
        ChangeStreamListener,
        ChangeStreamConfig,
        RealtimeValidator
    )

    # Configure change stream
    config = ChangeStreamConfig(
        collections=["staging_data", "staging_news"],
        full_document="updateLookup"
    )

    # Create listener
    listener = ChangeStreamListener(config=config)

    # Add validator handler
    validator = RealtimeValidator(mongo_service)
    listener.add_handler(validator)

    # Start listening
    await listener.start()


Integrated Pipeline:

    from api.app.services.streaming import StreamingPipeline

    # Create integrated pipeline
    pipeline = StreamingPipeline(mongo_service)

    # Register custom handlers
    pipeline.on_data_created(my_handler)
    pipeline.on_promotion(my_promotion_handler)

    # Start all components
    await pipeline.start()


Future Kafka Integration:
-------------------------

When ready to integrate Kafka:

1. Install aiokafka: `pip install aiokafka`
2. Configure KafkaBroker:

    from api.app.services.streaming import (
        KafkaBrokerConfig,
        KafkaBroker,
        EventProcessor
    )

    config = KafkaBrokerConfig(
        bootstrap_servers="kafka:9092",
        consumer_group="crawler-events"
    )
    broker = KafkaBroker(config)
    processor = EventProcessor(broker=broker)

3. Topics are pre-defined in TOPIC_MAPPING for proper event routing
"""

# Event Types
from .event_types import (
    # Enums
    EventType,
    EventPriority,
    EventSource,

    # Base Classes
    EventMetadata,
    BaseEvent,

    # Event Classes
    DataEvent,
    ReviewEvent,
    PromotionEvent,
    ValidationEvent,
    CrawlEvent,
    BatchEvent,
    SystemAlertEvent,

    # Type alias
    Event,

    # Utilities
    event_from_dict,
    get_topic_for_event,
    TOPIC_MAPPING
)

# Change Stream
from .change_stream import (
    ChangeStreamListener,
    ChangeStreamConfig,
    ChangeOperation,
    CollectionChangeStream,
    EventHandler,
    ResumeToken,
    change_stream_context
)

# Event Processor
from .event_processor import (
    EventProcessor,
    MessageBroker,
    InMemoryBroker,
    KafkaBroker,
    KafkaBrokerConfig,
    ProcessingStatus,
    ProcessingResult,
    EventEnvelope,

    # Pre-built handlers
    LoggingEventHandler,
    MetricsEventHandler,
    PersistenceEventHandler,
    WebhookEventHandler,

    # Factory functions
    create_event_processor,
    get_event_processor,
    set_event_processor
)

# Real-time Validator
from .realtime_validator import (
    RealtimeValidator,
    RealtimeValidationResult,
    ValidationStats,
    ValidatorCache,
    QuickValidator,
    ValidationPipeline,

    # Factory functions
    create_realtime_validator,
    create_validation_pipeline
)


# ============================================
# High-level Integration Classes
# ============================================

class StreamingPipeline:
    """
    Integrated streaming pipeline that combines all components.

    Provides a high-level API for setting up the complete
    real-time processing infrastructure.
    """

    def __init__(
        self,
        mongo_service=None,
        mongo_uri: str = None,
        database_name: str = None,
        collections: list = None
    ):
        """
        Initialize the streaming pipeline.

        Args:
            mongo_service: MongoDB service instance
            mongo_uri: MongoDB connection URI
            database_name: Database name
            collections: Collections to watch (None = all)
        """
        self.mongo_service = mongo_service

        # Default collections to watch
        default_collections = [
            "staging_news",
            "staging_financial",
            "staging_data",
            "data_reviews",
            "data_lineage"
        ]

        # Create components
        self.config = ChangeStreamConfig(
            collections=collections or default_collections,
            full_document="updateLookup"
        )

        self.listener = ChangeStreamListener(
            mongo_uri=mongo_uri,
            database_name=database_name,
            config=self.config
        )

        self.processor = create_event_processor()
        self.validator = create_realtime_validator(mongo_service)

        # Wire up validator to emit events through processor
        self.validator.set_event_emitter(
            lambda event: self.processor.emit(event)
        )

        # Add validator as handler
        self.listener.add_handler(self.validator)

        # Add logging handler for debugging
        self.listener.add_handler(LoggingEventHandler())

        self._running = False

    def on(self, event_type: EventType):
        """
        Decorator to register event handler.

        Example:
            @pipeline.on(EventType.DATA_CREATED)
            async def handle_created(event):
                pass
        """
        return self.processor.on(event_type)

    def on_data_created(self, handler):
        """Register handler for DATA_CREATED events."""
        self.processor.register_handler(EventType.DATA_CREATED, handler)

    def on_data_updated(self, handler):
        """Register handler for DATA_UPDATED events."""
        self.processor.register_handler(EventType.DATA_UPDATED, handler)

    def on_review_completed(self, handler):
        """Register handler for REVIEW_COMPLETED events."""
        self.processor.register_handler(EventType.REVIEW_COMPLETED, handler)

    def on_promotion(self, handler):
        """Register handler for PROMOTED_TO_PRODUCTION events."""
        self.processor.register_handler(EventType.PROMOTED_TO_PRODUCTION, handler)

    def on_validation_failed(self, handler):
        """Register handler for DATA_VALIDATION_FAILED events."""
        self.processor.register_handler(EventType.DATA_VALIDATION_FAILED, handler)

    async def emit(self, event: BaseEvent) -> bool:
        """Emit an event."""
        return await self.processor.emit(event)

    async def start(self) -> None:
        """Start the streaming pipeline."""
        if self._running:
            return

        self._running = True
        await self.processor.start()

        # Start listener in background task
        import asyncio
        asyncio.create_task(self.listener.start())

    async def stop(self) -> None:
        """Stop the streaming pipeline."""
        self._running = False
        await self.listener.stop()
        await self.processor.stop()

    def get_stats(self) -> dict:
        """Get pipeline statistics."""
        return {
            "listener": self.listener.get_stats(),
            "processor": self.processor.get_metrics(),
            "validator": self.validator.get_stats(),
            "running": self._running
        }


# Module-level convenience functions

async def start_streaming(
    mongo_service=None,
    collections: list = None
) -> StreamingPipeline:
    """
    Quick start function for streaming pipeline.

    Example:
        pipeline = await start_streaming(mongo_service)
    """
    pipeline = StreamingPipeline(
        mongo_service=mongo_service,
        collections=collections
    )
    await pipeline.start()
    return pipeline


__all__ = [
    # Event Types
    "EventType",
    "EventPriority",
    "EventSource",
    "EventMetadata",
    "BaseEvent",
    "DataEvent",
    "ReviewEvent",
    "PromotionEvent",
    "ValidationEvent",
    "CrawlEvent",
    "BatchEvent",
    "SystemAlertEvent",
    "Event",
    "event_from_dict",
    "get_topic_for_event",
    "TOPIC_MAPPING",

    # Change Stream
    "ChangeStreamListener",
    "ChangeStreamConfig",
    "ChangeOperation",
    "CollectionChangeStream",
    "EventHandler",
    "ResumeToken",
    "change_stream_context",

    # Event Processor
    "EventProcessor",
    "MessageBroker",
    "InMemoryBroker",
    "KafkaBroker",
    "KafkaBrokerConfig",
    "ProcessingStatus",
    "ProcessingResult",
    "EventEnvelope",
    "LoggingEventHandler",
    "MetricsEventHandler",
    "PersistenceEventHandler",
    "WebhookEventHandler",
    "create_event_processor",
    "get_event_processor",
    "set_event_processor",

    # Real-time Validator
    "RealtimeValidator",
    "RealtimeValidationResult",
    "ValidationStats",
    "ValidatorCache",
    "QuickValidator",
    "ValidationPipeline",
    "create_realtime_validator",
    "create_validation_pipeline",

    # High-level Integration
    "StreamingPipeline",
    "start_streaming"
]

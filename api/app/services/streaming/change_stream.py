"""
MongoDB Change Stream Listener.

This module provides real-time change data capture (CDC) from MongoDB
using Change Streams. Supports resumable watching and graceful shutdown.

Architecture:
- Uses MongoDB Change Streams (requires replica set)
- Transforms changes to domain events
- Routes events to the event processor
- Maintains resume tokens for crash recovery
"""

import asyncio
import logging
import os
import signal
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError, OperationFailure

from .event_types import (
    DataEvent,
    EventType,
    EventSource,
    EventMetadata,
    EventPriority,
    BaseEvent
)

logger = logging.getLogger(__name__)


class ChangeOperation(str, Enum):
    """MongoDB change stream operation types."""
    INSERT = "insert"
    UPDATE = "update"
    REPLACE = "replace"
    DELETE = "delete"
    INVALIDATE = "invalidate"
    DROP = "drop"
    DROP_DATABASE = "dropDatabase"
    RENAME = "rename"


@dataclass
class ChangeStreamConfig:
    """Configuration for change stream watching."""
    # Collections to watch (empty = watch all)
    collections: List[str] = field(default_factory=list)

    # Operation types to capture
    operations: List[ChangeOperation] = field(default_factory=lambda: [
        ChangeOperation.INSERT,
        ChangeOperation.UPDATE,
        ChangeOperation.REPLACE,
        ChangeOperation.DELETE
    ])

    # Full document options: 'default', 'updateLookup', 'whenAvailable', 'required'
    full_document: str = "updateLookup"

    # Full document before change (MongoDB 6.0+)
    full_document_before_change: Optional[str] = None

    # Maximum await time for changes (milliseconds)
    max_await_time_ms: int = 1000

    # Batch size for change events
    batch_size: int = 100

    # Resume token storage collection
    resume_token_collection: str = "change_stream_resume_tokens"

    # Pipeline for filtering (aggregation stages)
    pipeline: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ResumeToken:
    """Resume token for crash recovery."""
    stream_id: str
    token: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    last_event_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stream_id": self.stream_id,
            "token": self.token,
            "timestamp": self.timestamp,
            "last_event_id": self.last_event_id
        }


class EventHandler(ABC):
    """Abstract base class for change stream event handlers."""

    @abstractmethod
    async def handle(self, event: BaseEvent) -> bool:
        """
        Handle an event.

        Args:
            event: The event to handle

        Returns:
            True if handled successfully, False otherwise
        """
        pass

    @abstractmethod
    async def on_error(self, event: BaseEvent, error: Exception) -> None:
        """Handle processing errors."""
        pass


class ChangeStreamListener:
    """
    MongoDB Change Stream Listener.

    Provides real-time CDC from MongoDB with:
    - Resumable watching with token persistence
    - Graceful shutdown handling
    - Event transformation to domain events
    - Pluggable event handlers
    - Collection filtering

    Example:
        listener = ChangeStreamListener(mongo_uri, database_name)
        listener.add_handler(my_event_handler)
        await listener.start()
    """

    def __init__(
        self,
        mongo_uri: str = None,
        database_name: str = None,
        config: ChangeStreamConfig = None,
        stream_id: str = "default"
    ):
        """
        Initialize the change stream listener.

        Args:
            mongo_uri: MongoDB connection URI
            database_name: Database to watch
            config: Change stream configuration
            stream_id: Unique identifier for this stream (for resume tokens)
        """
        self.mongo_uri = mongo_uri or os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        self.database_name = database_name or os.getenv('MONGODB_DATABASE', 'crawler_system')
        self.config = config or ChangeStreamConfig()
        self.stream_id = stream_id

        self._client: Optional[MongoClient] = None
        self._handlers: List[EventHandler] = []
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._resume_token: Optional[ResumeToken] = None
        self._processed_count = 0
        self._error_count = 0
        self._last_activity: Optional[datetime] = None

        # Collection name to event type mapping
        self._collection_event_map = self._build_collection_event_map()

    def _build_collection_event_map(self) -> Dict[str, EventType]:
        """Map collection names to primary event types."""
        return {
            # Staging collections -> DATA events
            "staging_news": EventType.DATA_CREATED,
            "staging_financial": EventType.DATA_CREATED,
            "staging_data": EventType.DATA_CREATED,

            # Production collections -> DATA events
            "news_articles": EventType.DATA_CREATED,
            "financial_data": EventType.DATA_CREATED,
            "crawl_data": EventType.DATA_CREATED,

            # Review workflow collections
            "data_reviews": EventType.REVIEW_STARTED,

            # Lineage tracking
            "data_lineage": EventType.PROMOTED_TO_PRODUCTION,

            # Crawl results
            "crawl_results": EventType.CRAWL_COMPLETED,

            # Sources and crawlers
            "sources": EventType.DATA_UPDATED,
            "crawlers": EventType.DATA_UPDATED,
        }

    @property
    def client(self) -> MongoClient:
        """Get or create MongoDB client."""
        if self._client is None:
            self._client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=5000,
                directConnection=False
            )
        return self._client

    @property
    def db(self):
        """Get database instance."""
        return self.client[self.database_name]

    def add_handler(self, handler: EventHandler) -> None:
        """Add an event handler."""
        self._handlers.append(handler)
        logger.info(f"Added event handler: {type(handler).__name__}")

    def remove_handler(self, handler: EventHandler) -> None:
        """Remove an event handler."""
        if handler in self._handlers:
            self._handlers.remove(handler)

    async def _load_resume_token(self) -> Optional[Dict[str, Any]]:
        """Load resume token from storage."""
        try:
            doc = self.db[self.config.resume_token_collection].find_one(
                {"stream_id": self.stream_id}
            )
            if doc and doc.get("token"):
                self._resume_token = ResumeToken(
                    stream_id=self.stream_id,
                    token=doc["token"],
                    timestamp=doc.get("timestamp", datetime.utcnow()),
                    last_event_id=doc.get("last_event_id")
                )
                logger.info(f"Loaded resume token for stream '{self.stream_id}'")
                return doc["token"]
        except Exception as e:
            logger.warning(f"Failed to load resume token: {e}")
        return None

    async def _save_resume_token(self, token: Dict[str, Any], event_id: str = None) -> None:
        """Persist resume token for crash recovery."""
        try:
            self.db[self.config.resume_token_collection].update_one(
                {"stream_id": self.stream_id},
                {
                    "$set": {
                        "token": token,
                        "timestamp": datetime.utcnow(),
                        "last_event_id": event_id
                    }
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Failed to save resume token: {e}")

    def _build_pipeline(self) -> List[Dict[str, Any]]:
        """Build the aggregation pipeline for change stream."""
        pipeline = []

        # Filter by collections if specified
        if self.config.collections:
            pipeline.append({
                "$match": {
                    "ns.coll": {"$in": self.config.collections}
                }
            })

        # Filter by operation types
        if self.config.operations:
            op_values = [op.value for op in self.config.operations]
            pipeline.append({
                "$match": {
                    "operationType": {"$in": op_values}
                }
            })

        # Add custom pipeline stages
        pipeline.extend(self.config.pipeline)

        return pipeline

    def _transform_change_to_event(self, change: Dict[str, Any]) -> Optional[DataEvent]:
        """
        Transform a MongoDB change document to a domain event.

        Args:
            change: MongoDB change stream document

        Returns:
            DataEvent or None if transformation fails
        """
        try:
            operation_type = change.get("operationType", "")
            namespace = change.get("ns", {})
            collection = namespace.get("coll", "")
            document_key = change.get("documentKey", {})
            document_id = str(document_key.get("_id", ""))

            # Map operation to event type
            operation_event_map = {
                "insert": EventType.DATA_CREATED,
                "update": EventType.DATA_UPDATED,
                "replace": EventType.DATA_UPDATED,
                "delete": EventType.DATA_DELETED
            }

            event_type = operation_event_map.get(
                operation_type,
                self._collection_event_map.get(collection, EventType.DATA_UPDATED)
            )

            # Get document data
            full_document = change.get("fullDocument", {})
            update_description = change.get("updateDescription", {})

            # Extract changed fields for updates
            changed_fields = []
            if update_description:
                changed_fields = list(update_description.get("updatedFields", {}).keys())
                changed_fields.extend(update_description.get("removedFields", []))

            # Get source_id if available
            source_id = ""
            if full_document:
                source_id = str(full_document.get("source_id", full_document.get("_source_id", "")))
            elif document_key:
                # For deletes, source_id might be in document key
                source_id = str(document_key.get("source_id", ""))

            # Determine priority based on collection
            priority = EventPriority.NORMAL
            if collection.startswith("staging_"):
                priority = EventPriority.NORMAL
            elif collection in ["data_reviews", "data_lineage"]:
                priority = EventPriority.HIGH
            elif collection in ["error_logs"]:
                priority = EventPriority.HIGH

            # Create the event
            event = DataEvent(
                event_type=event_type,
                priority=priority,
                metadata=EventMetadata(
                    source=EventSource.MONGODB_CHANGE_STREAM,
                    correlation_id=str(change.get("_id", {}).get("_data", ""))
                ),
                source_id=source_id,
                collection=collection,
                document_id=document_id,
                operation=operation_type,
                data=self._serialize_document(full_document) if full_document else {},
                previous_data=self._serialize_document(
                    change.get("fullDocumentBeforeChange", {})
                ) if change.get("fullDocumentBeforeChange") else None,
                change_fields=changed_fields
            )

            return event

        except Exception as e:
            logger.error(f"Failed to transform change to event: {e}", exc_info=True)
            return None

    def _serialize_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize MongoDB document to JSON-compatible format."""
        if not doc:
            return {}

        result = {}
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                result[key] = str(value)
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, dict):
                result[key] = self._serialize_document(value)
            elif isinstance(value, list):
                result[key] = [
                    self._serialize_document(item) if isinstance(item, dict)
                    else str(item) if isinstance(item, ObjectId)
                    else item
                    for item in value
                ]
            else:
                result[key] = value
        return result

    async def _dispatch_event(self, event: DataEvent) -> None:
        """Dispatch event to all registered handlers."""
        for handler in self._handlers:
            try:
                success = await handler.handle(event)
                if not success:
                    logger.warning(f"Handler {type(handler).__name__} returned False for event {event.event_id}")
            except Exception as e:
                self._error_count += 1
                logger.error(f"Handler {type(handler).__name__} failed: {e}", exc_info=True)
                try:
                    await handler.on_error(event, e)
                except Exception as handler_error:
                    logger.error(f"Error handler failed: {handler_error}")

    async def _watch_loop(self) -> None:
        """Main watching loop."""
        resume_token = await self._load_resume_token()
        pipeline = self._build_pipeline()

        watch_options = {
            "full_document": self.config.full_document,
            "max_await_time_ms": self.config.max_await_time_ms,
            "batch_size": self.config.batch_size
        }

        if resume_token:
            watch_options["resume_after"] = resume_token

        if self.config.full_document_before_change:
            watch_options["full_document_before_change"] = self.config.full_document_before_change

        logger.info(f"Starting change stream watch on database '{self.database_name}'")
        logger.info(f"Watching collections: {self.config.collections or 'all'}")

        try:
            with self.db.watch(pipeline, **watch_options) as stream:
                while self._running:
                    # Check for shutdown signal
                    if self._shutdown_event.is_set():
                        logger.info("Shutdown signal received, stopping watch")
                        break

                    # Get next change with timeout
                    try:
                        change = stream.try_next()

                        if change is None:
                            # No changes available, yield control
                            await asyncio.sleep(0.1)
                            continue

                        self._last_activity = datetime.utcnow()

                        # Transform and dispatch
                        event = self._transform_change_to_event(change)
                        if event:
                            await self._dispatch_event(event)
                            self._processed_count += 1

                            # Persist resume token periodically
                            if self._processed_count % 100 == 0:
                                await self._save_resume_token(
                                    stream.resume_token,
                                    event.event_id
                                )

                        # Always save token for last processed change
                        await self._save_resume_token(
                            stream.resume_token,
                            event.event_id if event else None
                        )

                    except StopIteration:
                        # Stream exhausted (shouldn't happen with watch)
                        break

        except OperationFailure as e:
            if "ChangeStreamHistoryLost" in str(e):
                logger.warning("Change stream history lost, clearing resume token")
                self.db[self.config.resume_token_collection].delete_one(
                    {"stream_id": self.stream_id}
                )
            raise

    async def start(self) -> None:
        """Start the change stream listener."""
        if self._running:
            logger.warning("Change stream listener already running")
            return

        self._running = True
        self._shutdown_event.clear()

        logger.info(f"Change stream listener starting (stream_id={self.stream_id})")

        try:
            await self._watch_loop()
        except Exception as e:
            logger.error(f"Change stream error: {e}", exc_info=True)
            raise
        finally:
            self._running = False
            logger.info(f"Change stream listener stopped. Processed: {self._processed_count}, Errors: {self._error_count}")

    async def stop(self) -> None:
        """Stop the change stream listener gracefully."""
        logger.info("Stopping change stream listener...")
        self._shutdown_event.set()
        self._running = False

    def close(self) -> None:
        """Close the MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None

    def get_stats(self) -> Dict[str, Any]:
        """Get listener statistics."""
        return {
            "stream_id": self.stream_id,
            "running": self._running,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "last_activity": self._last_activity.isoformat() if self._last_activity else None,
            "handlers_count": len(self._handlers),
            "collections_watched": self.config.collections or "all"
        }


class CollectionChangeStream:
    """
    Simplified change stream for a single collection.

    For cases where you only need to watch one collection.
    """

    def __init__(
        self,
        collection_name: str,
        mongo_uri: str = None,
        database_name: str = None,
        callback: Callable[[DataEvent], None] = None
    ):
        self.collection_name = collection_name
        self.callback = callback
        self._listener = ChangeStreamListener(
            mongo_uri=mongo_uri,
            database_name=database_name,
            config=ChangeStreamConfig(collections=[collection_name]),
            stream_id=f"collection_{collection_name}"
        )

        if callback:
            self._listener.add_handler(_CallbackHandler(callback))

    async def start(self):
        await self._listener.start()

    async def stop(self):
        await self._listener.stop()


class _CallbackHandler(EventHandler):
    """Simple callback wrapper for EventHandler interface."""

    def __init__(self, callback: Callable[[DataEvent], None]):
        self._callback = callback

    async def handle(self, event: BaseEvent) -> bool:
        try:
            result = self._callback(event)
            # Support both sync and async callbacks
            if asyncio.iscoroutine(result):
                await result
            return True
        except Exception:
            return False

    async def on_error(self, event: BaseEvent, error: Exception) -> None:
        logger.error(f"Callback error for event {event.event_id}: {error}")


@asynccontextmanager
async def change_stream_context(
    collections: List[str] = None,
    mongo_uri: str = None,
    database_name: str = None
):
    """
    Context manager for change stream listener.

    Example:
        async with change_stream_context(['staging_data']) as listener:
            listener.add_handler(my_handler)
            await listener.start()
    """
    config = ChangeStreamConfig(collections=collections or [])
    listener = ChangeStreamListener(
        mongo_uri=mongo_uri,
        database_name=database_name,
        config=config
    )

    try:
        yield listener
    finally:
        await listener.stop()
        listener.close()

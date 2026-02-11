"""
Tests for Streaming Event System.

Tests cover:
- InMemoryBroker publish/subscribe
- KafkaBroker graceful fallback to InMemory
- EventProcessor event routing
- Event type creation and serialization
"""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from api.app.services.streaming.event_processor import (
    EventProcessor,
    InMemoryBroker,
    KafkaBroker,
)
from api.app.services.streaming.event_types import (
    BaseEvent,
    CrawlEvent,
    DataEvent,
    EventPriority,
    EventType,
)


# ============================================
# Event Type Tests
# ============================================


class TestEventTypes:
    def test_data_event_creation(self):
        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="news",
            data={"title": "Test"},
        )
        assert event.event_type == EventType.DATA_CREATED
        assert event.source_id == "src_1"
        assert event.collection == "news"

    def test_crawl_event_creation(self):
        event = CrawlEvent(
            event_type=EventType.CRAWL_STARTED,
            source_id="src_1",
            crawler_id="cr_1",
            status="running",
        )
        assert event.event_type == EventType.CRAWL_STARTED
        assert event.crawler_id == "cr_1"

    def test_event_has_id_and_timestamp(self):
        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={},
        )
        assert event.event_id is not None
        assert event.timestamp is not None
        assert isinstance(event.timestamp, datetime)

    def test_event_default_priority(self):
        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={},
        )
        assert event.priority == EventPriority.NORMAL

    def test_event_to_dict(self):
        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={"key": "value"},
        )
        d = event.to_dict()
        assert "event_type" in d
        assert d["source_id"] == "src_1"


# ============================================
# InMemoryBroker Tests
# ============================================


class TestInMemoryBroker:
    @pytest.fixture
    def broker(self):
        return InMemoryBroker(max_queue_size=100)

    @pytest.mark.asyncio
    async def test_publish_returns_true(self, broker):
        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={},
        )
        result = await broker.publish("test.topic", event)
        assert result is True

    @pytest.mark.asyncio
    async def test_subscribe_registers_handler(self, broker):
        handler = AsyncMock()
        await broker.subscribe("test.topic", handler)
        assert "test.topic" in broker._subscribers
        assert handler in broker._subscribers["test.topic"]

    @pytest.mark.asyncio
    async def test_unsubscribe_removes_handler(self, broker):
        handler = AsyncMock()
        await broker.subscribe("test.topic", handler)
        await broker.unsubscribe("test.topic", handler)
        assert handler not in broker._subscribers["test.topic"]

    @pytest.mark.asyncio
    async def test_publish_and_consume(self, broker):
        received = []

        async def handler(event):
            received.append(event)

        await broker.subscribe("test.topic", handler)
        await broker.start()

        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={"key": "value"},
        )
        await broker.publish("test.topic", event)

        # Give processor time to consume
        await asyncio.sleep(0.2)
        await broker.stop()

        assert len(received) == 1
        assert received[0].source_id == "src_1"

    @pytest.mark.asyncio
    async def test_multiple_subscribers(self, broker):
        received_a = []
        received_b = []

        async def handler_a(event):
            received_a.append(event)

        async def handler_b(event):
            received_b.append(event)

        await broker.subscribe("test.topic", handler_a)
        await broker.subscribe("test.topic", handler_b)
        await broker.start()

        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={},
        )
        await broker.publish("test.topic", event)

        await asyncio.sleep(0.2)
        await broker.stop()

        assert len(received_a) == 1
        assert len(received_b) == 1

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self, broker):
        await broker.start()
        assert broker._running is True
        await broker.stop()
        assert broker._running is False

    @pytest.mark.asyncio
    async def test_separate_topics(self, broker):
        received_a = []
        received_b = []

        async def handler_a(event):
            received_a.append(event)

        async def handler_b(event):
            received_b.append(event)

        await broker.subscribe("topic.a", handler_a)
        await broker.subscribe("topic.b", handler_b)
        await broker.start()

        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={},
        )
        await broker.publish("topic.a", event)

        await asyncio.sleep(0.2)
        await broker.stop()

        assert len(received_a) == 1
        assert len(received_b) == 0


# ============================================
# KafkaBroker Fallback Tests
# ============================================


class TestKafkaBrokerFallback:
    def test_kafka_broker_creates_without_error(self):
        broker = KafkaBroker()
        assert broker is not None
        assert broker._using_fallback is True

    @pytest.mark.asyncio
    async def test_kafka_broker_publish_uses_fallback(self):
        broker = KafkaBroker()
        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={},
        )
        result = await broker.publish("test.topic", event)
        assert result is True

    @pytest.mark.asyncio
    async def test_kafka_broker_subscribe_uses_fallback(self):
        broker = KafkaBroker()
        handler = AsyncMock()
        await broker.subscribe("test.topic", handler)
        assert "test.topic" in broker._fallback._subscribers

    @pytest.mark.asyncio
    async def test_kafka_broker_full_lifecycle(self):
        broker = KafkaBroker()
        received = []

        async def handler(event):
            received.append(event)

        await broker.subscribe("test.topic", handler)
        await broker.start()

        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={"key": "value"},
        )
        await broker.publish("test.topic", event)

        await asyncio.sleep(0.2)
        await broker.stop()

        assert len(received) == 1


# ============================================
# EventProcessor Tests
# ============================================


class TestEventProcessor:
    @pytest.mark.asyncio
    async def test_processor_creation(self):
        broker = InMemoryBroker()
        processor = EventProcessor(broker=broker)
        assert processor is not None

    @pytest.mark.asyncio
    async def test_processor_on_decorator(self):
        broker = InMemoryBroker()
        processor = EventProcessor(broker=broker)

        received = []

        @processor.on(EventType.DATA_CREATED)
        async def handle_data(event):
            received.append(event)

        await processor.start()

        event = DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={},
        )
        await processor.emit(event)

        await asyncio.sleep(0.2)
        await processor.stop()

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_processor_multiple_event_types(self):
        broker = InMemoryBroker()
        processor = EventProcessor(broker=broker)

        created_events = []
        crawl_events = []

        @processor.on(EventType.DATA_CREATED)
        async def handle_data(event):
            created_events.append(event)

        @processor.on(EventType.CRAWL_STARTED)
        async def handle_crawl(event):
            crawl_events.append(event)

        await processor.start()

        await processor.emit(DataEvent(
            event_type=EventType.DATA_CREATED,
            source_id="src_1",
            collection="test",
            data={},
        ))
        await processor.emit(CrawlEvent(
            event_type=EventType.CRAWL_STARTED,
            source_id="src_1",
            crawler_id="cr_1",
            status="running",
        ))

        await asyncio.sleep(0.3)
        await processor.stop()

        assert len(created_events) == 1
        assert len(crawl_events) == 1

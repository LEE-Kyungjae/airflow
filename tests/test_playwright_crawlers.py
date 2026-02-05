"""
Tests for Playwright-based crawlers.

These tests verify the functionality of PlaywrightCrawler, SPACrawler,
and DynamicTableCrawler classes.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import asdict

# Test imports
from crawlers.playwright_crawler import (
    PlaywrightCrawler,
    PlaywrightConfig,
    ElementInfo,
)
from crawlers.spa_crawler import (
    SPACrawler,
    SPAConfig,
    SPAFramework,
    SPAState,
)
from crawlers.dynamic_table_crawler import (
    DynamicTableCrawler,
    DynamicTableConfig,
    TableLibrary,
    TableMetadata,
)
from crawlers.base_crawler import CrawlResult


class TestPlaywrightConfig:
    """Tests for PlaywrightConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = PlaywrightConfig()

        assert config.headless is True
        assert config.browser_type == "chromium"
        assert config.timeout == 30000
        assert config.wait_until == "networkidle"
        assert config.javascript_enabled is True
        assert config.viewport == {"width": 1920, "height": 1080}
        assert config.user_agent is not None

    def test_custom_config(self):
        """Test custom configuration values."""
        config = PlaywrightConfig(
            headless=False,
            browser_type="firefox",
            timeout=60000,
            viewport={"width": 1280, "height": 720},
            block_resources=["image", "font"]
        )

        assert config.headless is False
        assert config.browser_type == "firefox"
        assert config.timeout == 60000
        assert config.viewport == {"width": 1280, "height": 720}
        assert "image" in config.block_resources

    def test_proxy_config(self):
        """Test proxy configuration."""
        config = PlaywrightConfig(
            proxy={"server": "http://proxy:8080", "username": "user", "password": "pass"}
        )

        assert config.proxy["server"] == "http://proxy:8080"


class TestSPAConfig:
    """Tests for SPAConfig dataclass."""

    def test_default_spa_config(self):
        """Test default SPA configuration."""
        config = SPAConfig()

        assert config.auto_detect_framework is True
        assert config.wait_for_hydration is True
        assert config.extract_state is False
        assert config.wait_for_router is True

    def test_custom_spa_config(self):
        """Test custom SPA configuration."""
        config = SPAConfig(
            framework=SPAFramework.REACT,
            wait_for_hydration=True,
            hydration_timeout=15000,
            extract_state=True
        )

        assert config.framework == SPAFramework.REACT
        assert config.hydration_timeout == 15000
        assert config.extract_state is True


class TestDynamicTableConfig:
    """Tests for DynamicTableConfig dataclass."""

    def test_default_table_config(self):
        """Test default table configuration."""
        config = DynamicTableConfig()

        assert config.auto_detect_library is True
        assert config.pagination_enabled is True
        assert config.max_pages == 50
        assert config.wait_for_rows is True

    def test_custom_table_config(self):
        """Test custom table configuration."""
        config = DynamicTableConfig(
            table_library=TableLibrary.AG_GRID,
            max_pages=100,
            preferred_page_size=50
        )

        assert config.table_library == TableLibrary.AG_GRID
        assert config.max_pages == 100


class TestPlaywrightCrawler:
    """Tests for PlaywrightCrawler class."""

    def test_initialization(self):
        """Test crawler initialization."""
        crawler = PlaywrightCrawler()

        assert crawler.config is not None
        assert crawler._is_started is False
        assert crawler.page is None
        assert crawler.browser is None

    def test_initialization_with_config(self):
        """Test crawler initialization with custom config."""
        config = PlaywrightConfig(headless=False, timeout=60000)
        crawler = PlaywrightCrawler(config)

        assert crawler.config.headless is False
        assert crawler.config.timeout == 60000

    def test_ensure_page_raises_without_start(self):
        """Test that operations raise without starting browser."""
        crawler = PlaywrightCrawler()

        with pytest.raises(RuntimeError, match="Page not initialized"):
            crawler._ensure_page()

    def test_get_elapsed_ms(self):
        """Test elapsed time calculation."""
        crawler = PlaywrightCrawler()

        # Without start time
        assert crawler._get_elapsed_ms() == 0

        # With start time
        import time
        crawler._start_time = time.time() - 1  # 1 second ago
        elapsed = crawler._get_elapsed_ms()
        assert 900 <= elapsed <= 1100  # Approximately 1000ms

    def test_parse_number(self):
        """Test number parsing."""
        crawler = PlaywrightCrawler()

        assert crawler._parse_number("1,234.56") == 1234.56
        assert crawler._parse_number("$99.99") == 99.99
        assert crawler._parse_number("50%") == 50.0
        assert crawler._parse_number("-123") == -123.0
        assert crawler._parse_number("invalid") is None
        assert crawler._parse_number(None) is None

    def test_convert_value(self):
        """Test value conversion by type."""
        crawler = PlaywrightCrawler()

        # String type
        assert crawler._convert_value("  hello  ", "string") == "hello"

        # Number type
        assert crawler._convert_value("1,234", "number") == 1234.0

        # None handling
        assert crawler._convert_value(None, "string") is None
        assert crawler._convert_value("", "string") is None

    def test_get_item_id(self):
        """Test item ID generation for deduplication."""
        crawler = PlaywrightCrawler()

        item1 = {"text": "Hello World"}
        item2 = {"text": "Hello World"}
        item3 = {"text": "Different"}

        # Same content should produce same ID
        assert crawler._get_item_id(item1) == crawler._get_item_id(item2)

        # Different content should produce different ID
        assert crawler._get_item_id(item1) != crawler._get_item_id(item3)


class TestSPACrawler:
    """Tests for SPACrawler class."""

    def test_initialization(self):
        """Test SPA crawler initialization."""
        crawler = SPACrawler()

        assert isinstance(crawler.spa_config, SPAConfig)
        assert crawler.framework == SPAFramework.UNKNOWN
        assert crawler._is_hydrated is False
        assert crawler._is_ssr is False

    def test_initialization_with_framework(self):
        """Test SPA crawler with specified framework."""
        config = SPAConfig(framework=SPAFramework.VUE)
        crawler = SPACrawler(config)

        assert crawler.framework == SPAFramework.VUE


class TestDynamicTableCrawler:
    """Tests for DynamicTableCrawler class."""

    def test_initialization(self):
        """Test table crawler initialization."""
        crawler = DynamicTableCrawler()

        assert isinstance(crawler.table_config, DynamicTableConfig)
        assert crawler.library == TableLibrary.UNKNOWN
        assert crawler._table_metadata is None

    def test_initialization_with_library(self):
        """Test table crawler with specified library."""
        config = DynamicTableConfig(table_library=TableLibrary.DATATABLES)
        crawler = DynamicTableCrawler(config)

        assert crawler.library == TableLibrary.DATATABLES


class TestCrawlResult:
    """Tests for CrawlResult dataclass."""

    def test_successful_result(self):
        """Test successful crawl result."""
        result = CrawlResult(
            success=True,
            data=[{"title": "Test"}],
            record_count=1
        )

        assert result.success is True
        assert len(result.data) == 1
        assert result.error_code is None

    def test_failed_result(self):
        """Test failed crawl result."""
        result = CrawlResult(
            success=False,
            error_code="E001",
            error_message="Timeout"
        )

        assert result.success is False
        assert result.error_code == "E001"
        assert result.record_count == 0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = CrawlResult(
            success=True,
            data=[{"a": 1}],
            record_count=1,
            execution_time_ms=1000
        )

        d = result.to_dict()

        assert d["status"] == "success"
        assert d["record_count"] == 1
        assert d["execution_time_ms"] == 1000


class TestTableMetadata:
    """Tests for TableMetadata dataclass."""

    def test_table_metadata(self):
        """Test table metadata creation."""
        metadata = TableMetadata(
            library=TableLibrary.AG_GRID,
            total_rows=100,
            visible_rows=25,
            total_pages=4,
            current_page=1,
            page_size=25,
            columns=["Name", "Value", "Date"],
            has_pagination=True,
            has_sorting=True,
            has_filtering=False,
            is_virtual_scroll=True
        )

        assert metadata.library == TableLibrary.AG_GRID
        assert metadata.total_rows == 100
        assert len(metadata.columns) == 3
        assert metadata.is_virtual_scroll is True


class TestSPAState:
    """Tests for SPAState dataclass."""

    def test_spa_state(self):
        """Test SPA state creation."""
        state = SPAState(
            framework=SPAFramework.REACT,
            is_hydrated=True,
            is_ssr=True,
            state_data={"user": {"name": "Test"}},
            api_responses=[],
            route_info={"pathname": "/test"}
        )

        assert state.framework == SPAFramework.REACT
        assert state.is_hydrated is True
        assert "user" in state.state_data


class TestElementInfo:
    """Tests for ElementInfo dataclass."""

    def test_element_info(self):
        """Test element info creation."""
        info = ElementInfo(
            selector=".test",
            text="Hello",
            html="<div>Hello</div>",
            attributes={"class": "test", "id": "el1"},
            bounding_box={"x": 0, "y": 0, "width": 100, "height": 50},
            is_visible=True
        )

        assert info.selector == ".test"
        assert info.text == "Hello"
        assert info.attributes["class"] == "test"
        assert info.is_visible is True


# Integration tests (require actual browser - marked as slow)
@pytest.mark.slow
@pytest.mark.integration
class TestPlaywrightCrawlerIntegration:
    """Integration tests for PlaywrightCrawler (requires browser)."""

    @pytest.mark.asyncio
    async def test_browser_lifecycle(self):
        """Test browser start and close."""
        crawler = PlaywrightCrawler()

        await crawler.start()
        assert crawler._is_started is True
        assert crawler.browser is not None
        assert crawler.page is not None

        await crawler.close()
        assert crawler._is_started is False
        assert crawler.browser is None

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager."""
        async with PlaywrightCrawler() as crawler:
            assert crawler._is_started is True
            assert crawler.page is not None

        # After exit
        assert crawler._is_started is False

    @pytest.mark.asyncio
    async def test_navigation(self):
        """Test page navigation."""
        async with PlaywrightCrawler() as crawler:
            response = await crawler.navigate("https://example.com")

            assert response is not None
            assert response.status == 200

            url = await crawler.get_url()
            assert "example.com" in url

    @pytest.mark.asyncio
    async def test_get_html(self):
        """Test HTML extraction."""
        async with PlaywrightCrawler() as crawler:
            await crawler.navigate("https://example.com")
            html = await crawler.get_html()

            assert "<!DOCTYPE html>" in html.lower() or "<html" in html.lower()
            assert "Example Domain" in html

    @pytest.mark.asyncio
    async def test_get_text(self):
        """Test text extraction."""
        async with PlaywrightCrawler() as crawler:
            await crawler.navigate("https://example.com")
            title = await crawler.get_text("h1")

            assert "Example Domain" in title

    @pytest.mark.asyncio
    async def test_screenshot(self):
        """Test screenshot capture."""
        async with PlaywrightCrawler() as crawler:
            await crawler.navigate("https://example.com")
            screenshot = await crawler.screenshot()

            assert isinstance(screenshot, bytes)
            assert len(screenshot) > 0

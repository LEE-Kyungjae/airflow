"""
Playwright Executor for Airflow Integration.

Provides a unified interface for executing Playwright-based crawlers
within Airflow DAGs, handling configuration, error management, and
result processing.
"""

import asyncio
import logging
import os
import sys
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional, Type, Union
from enum import Enum
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from crawlers.base_crawler import CrawlResult
from crawlers.playwright_crawler import PlaywrightCrawler, PlaywrightConfig
from crawlers.spa_crawler import SPACrawler, SPAConfig, SPAFramework
from crawlers.dynamic_table_crawler import DynamicTableCrawler, DynamicTableConfig, TableLibrary

logger = logging.getLogger(__name__)


class PageType(str, Enum):
    """Types of pages that can be crawled."""
    STATIC = "static"           # Simple HTML pages
    DYNAMIC = "dynamic"         # JS-rendered pages
    SPA = "spa"                 # Single Page Applications
    SPA_REACT = "spa_react"     # React SPA
    SPA_VUE = "spa_vue"         # Vue SPA
    SPA_ANGULAR = "spa_angular" # Angular SPA
    TABLE = "table"             # Dynamic tables
    TABLE_DATATABLES = "table_datatables"
    TABLE_AG_GRID = "table_ag_grid"
    TABLE_ANT = "table_ant"
    INFINITE_SCROLL = "infinite_scroll"
    PAGINATION = "pagination"


class CrawlerType(str, Enum):
    """Available crawler types."""
    PLAYWRIGHT = "playwright"
    SPA = "spa"
    DYNAMIC_TABLE = "dynamic_table"


@dataclass
class ExecutorConfig:
    """Configuration for PlaywrightExecutor."""

    # Browser settings
    headless: bool = True
    browser_type: str = "chromium"
    timeout: int = 30000

    # Resource blocking for performance
    block_images: bool = True
    block_stylesheets: bool = False
    block_fonts: bool = True

    # Retry settings
    max_retries: int = 3
    retry_delay: int = 2000

    # Screenshot on error
    screenshot_on_error: bool = True
    screenshot_dir: str = "/tmp/crawl_errors"

    # Proxy settings
    use_proxy: bool = False
    proxy_url: Optional[str] = None

    # Docker/Container settings
    container_mode: bool = True

    # Logging
    verbose: bool = False


@dataclass
class SourceConfig:
    """Configuration for a crawl source."""

    # Basic info
    source_id: str
    url: str
    name: str

    # Page type
    page_type: PageType = PageType.DYNAMIC
    crawler_type: Optional[CrawlerType] = None

    # Selectors
    wait_selector: Optional[str] = None
    container_selector: Optional[str] = None

    # Fields to extract
    fields: List[Dict[str, str]] = field(default_factory=list)

    # Actions
    pre_actions: List[Dict[str, Any]] = field(default_factory=list)
    post_actions: List[Dict[str, Any]] = field(default_factory=list)

    # Pagination
    pagination_enabled: bool = False
    next_button_selector: Optional[str] = None
    max_pages: int = 10

    # Infinite scroll
    infinite_scroll: bool = False
    max_items: int = 100
    scroll_delay: int = 1000

    # Table settings
    table_selector: str = "table"
    extract_all_pages: bool = True

    # SPA settings
    wait_for_hydration: bool = True
    extract_state: bool = False

    # Authentication
    requires_auth: bool = False
    auth_config: Optional[Dict[str, Any]] = None

    # Custom settings
    custom_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SourceConfig':
        """Create SourceConfig from dictionary."""
        # Handle page_type conversion
        if 'page_type' in data and isinstance(data['page_type'], str):
            data['page_type'] = PageType(data['page_type'])

        # Handle crawler_type conversion
        if 'crawler_type' in data and isinstance(data['crawler_type'], str):
            data['crawler_type'] = CrawlerType(data['crawler_type'])

        # Filter known fields
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in known_fields}

        return cls(**filtered_data)


@dataclass
class ExecutionResult:
    """Result of a crawl execution."""
    success: bool
    source_id: str
    url: str
    crawler_type: str
    data: List[Dict[str, Any]]
    record_count: int
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    execution_time_ms: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    screenshot_path: Optional[str] = None
    html_snapshot: Optional[str] = None

    def to_crawl_result(self) -> CrawlResult:
        """Convert to CrawlResult."""
        return CrawlResult(
            success=self.success,
            data=self.data,
            record_count=self.record_count,
            error_code=self.error_code,
            error_message=self.error_message,
            execution_time_ms=self.execution_time_ms,
            html_snapshot=self.html_snapshot,
            metadata=self.metadata
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'success': self.success,
            'source_id': self.source_id,
            'url': self.url,
            'crawler_type': self.crawler_type,
            'data': self.data,
            'record_count': self.record_count,
            'error_code': self.error_code,
            'error_message': self.error_message,
            'execution_time_ms': self.execution_time_ms,
            'metadata': self.metadata,
            'screenshot_path': self.screenshot_path
        }


class PlaywrightExecutor:
    """
    Executor for Playwright-based crawlers in Airflow.

    Provides automatic crawler selection, configuration,
    execution, and error handling.
    """

    # Mapping of page types to crawler types
    PAGE_TYPE_TO_CRAWLER = {
        PageType.STATIC: CrawlerType.PLAYWRIGHT,
        PageType.DYNAMIC: CrawlerType.PLAYWRIGHT,
        PageType.SPA: CrawlerType.SPA,
        PageType.SPA_REACT: CrawlerType.SPA,
        PageType.SPA_VUE: CrawlerType.SPA,
        PageType.SPA_ANGULAR: CrawlerType.SPA,
        PageType.TABLE: CrawlerType.DYNAMIC_TABLE,
        PageType.TABLE_DATATABLES: CrawlerType.DYNAMIC_TABLE,
        PageType.TABLE_AG_GRID: CrawlerType.DYNAMIC_TABLE,
        PageType.TABLE_ANT: CrawlerType.DYNAMIC_TABLE,
        PageType.INFINITE_SCROLL: CrawlerType.PLAYWRIGHT,
        PageType.PAGINATION: CrawlerType.PLAYWRIGHT,
    }

    # Mapping of page types to SPA frameworks
    PAGE_TYPE_TO_SPA_FRAMEWORK = {
        PageType.SPA_REACT: SPAFramework.REACT,
        PageType.SPA_VUE: SPAFramework.VUE,
        PageType.SPA_ANGULAR: SPAFramework.ANGULAR,
    }

    # Mapping of page types to table libraries
    PAGE_TYPE_TO_TABLE_LIBRARY = {
        PageType.TABLE_DATATABLES: TableLibrary.DATATABLES,
        PageType.TABLE_AG_GRID: TableLibrary.AG_GRID,
        PageType.TABLE_ANT: TableLibrary.ANT_TABLE,
    }

    def __init__(self, config: Optional[ExecutorConfig] = None):
        """
        Initialize executor.

        Args:
            config: Executor configuration
        """
        self.config = config or ExecutorConfig()
        self._ensure_screenshot_dir()

    def _ensure_screenshot_dir(self):
        """Ensure screenshot directory exists."""
        if self.config.screenshot_on_error:
            os.makedirs(self.config.screenshot_dir, exist_ok=True)

    def get_crawler_type(self, source_config: SourceConfig) -> CrawlerType:
        """
        Determine the appropriate crawler type for a source.

        Args:
            source_config: Source configuration

        Returns:
            CrawlerType to use
        """
        # Use explicit crawler type if specified
        if source_config.crawler_type:
            return source_config.crawler_type

        # Map page type to crawler type
        return self.PAGE_TYPE_TO_CRAWLER.get(
            source_config.page_type,
            CrawlerType.PLAYWRIGHT
        )

    def get_crawler_class(self, crawler_type: CrawlerType) -> Type[PlaywrightCrawler]:
        """
        Get the crawler class for a crawler type.

        Args:
            crawler_type: Type of crawler

        Returns:
            Crawler class
        """
        mapping = {
            CrawlerType.PLAYWRIGHT: PlaywrightCrawler,
            CrawlerType.SPA: SPACrawler,
            CrawlerType.DYNAMIC_TABLE: DynamicTableCrawler,
        }
        return mapping.get(crawler_type, PlaywrightCrawler)

    def _build_playwright_config(
        self,
        source_config: SourceConfig
    ) -> PlaywrightConfig:
        """Build PlaywrightConfig from source and executor config."""
        # Determine blocked resources
        block_resources = []
        if self.config.block_images:
            block_resources.append("image")
        if self.config.block_stylesheets:
            block_resources.append("stylesheet")
        if self.config.block_fonts:
            block_resources.append("font")

        # Build proxy config
        proxy = None
        if self.config.use_proxy and self.config.proxy_url:
            proxy = {"server": self.config.proxy_url}

        return PlaywrightConfig(
            headless=self.config.headless,
            browser_type=self.config.browser_type,
            timeout=self.config.timeout,
            block_resources=block_resources,
            proxy=proxy,
            retry_count=self.config.max_retries,
            retry_delay=self.config.retry_delay,
            screenshot_on_error=self.config.screenshot_on_error,
        )

    def _build_spa_config(self, source_config: SourceConfig) -> SPAConfig:
        """Build SPAConfig from source config."""
        base_config = self._build_playwright_config(source_config)

        # Determine framework
        framework = None
        if source_config.page_type in self.PAGE_TYPE_TO_SPA_FRAMEWORK:
            framework = self.PAGE_TYPE_TO_SPA_FRAMEWORK[source_config.page_type]

        return SPAConfig(
            headless=base_config.headless,
            browser_type=base_config.browser_type,
            timeout=base_config.timeout,
            block_resources=base_config.block_resources,
            proxy=base_config.proxy,
            framework=framework,
            auto_detect_framework=framework is None,
            wait_for_hydration=source_config.wait_for_hydration,
            extract_state=source_config.extract_state,
        )

    def _build_table_config(self, source_config: SourceConfig) -> DynamicTableConfig:
        """Build DynamicTableConfig from source config."""
        base_config = self._build_playwright_config(source_config)

        # Determine table library
        table_library = None
        if source_config.page_type in self.PAGE_TYPE_TO_TABLE_LIBRARY:
            table_library = self.PAGE_TYPE_TO_TABLE_LIBRARY[source_config.page_type]

        return DynamicTableConfig(
            headless=base_config.headless,
            browser_type=base_config.browser_type,
            timeout=base_config.timeout,
            block_resources=base_config.block_resources,
            proxy=base_config.proxy,
            table_library=table_library,
            auto_detect_library=table_library is None,
            pagination_enabled=source_config.pagination_enabled,
            max_pages=source_config.max_pages,
        )

    async def execute(self, source_config: SourceConfig) -> ExecutionResult:
        """
        Execute a crawl for the given source configuration.

        Args:
            source_config: Source configuration

        Returns:
            ExecutionResult with crawl results
        """
        crawler_type = self.get_crawler_type(source_config)
        crawler_class = self.get_crawler_class(crawler_type)

        logger.info(
            f"Executing crawl for {source_config.source_id} "
            f"using {crawler_type.value} crawler"
        )

        # Build appropriate config
        if crawler_type == CrawlerType.SPA:
            config = self._build_spa_config(source_config)
        elif crawler_type == CrawlerType.DYNAMIC_TABLE:
            config = self._build_table_config(source_config)
        else:
            config = self._build_playwright_config(source_config)

        # Execute with retry
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                result = await self._execute_crawl(
                    crawler_class,
                    config,
                    source_config,
                    crawler_type
                )

                if result.success:
                    return result

                # On failure, store error and retry
                last_error = result.error_message
                logger.warning(
                    f"Crawl attempt {attempt + 1} failed for {source_config.source_id}: "
                    f"{result.error_message}"
                )

                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay / 1000)

            except Exception as e:
                last_error = str(e)
                logger.error(
                    f"Crawl attempt {attempt + 1} exception for {source_config.source_id}: {e}"
                )

                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay / 1000)

        # All retries failed
        return ExecutionResult(
            success=False,
            source_id=source_config.source_id,
            url=source_config.url,
            crawler_type=crawler_type.value,
            data=[],
            record_count=0,
            error_code='E010',
            error_message=f"All {self.config.max_retries} attempts failed. Last error: {last_error}",
            metadata={
                "retries": self.config.max_retries,
                "last_error": last_error
            }
        )

    async def _execute_crawl(
        self,
        crawler_class: Type[PlaywrightCrawler],
        config: PlaywrightConfig,
        source_config: SourceConfig,
        crawler_type: CrawlerType
    ) -> ExecutionResult:
        """Execute the actual crawl."""
        screenshot_path = None
        html_snapshot = None

        async with crawler_class(config) as crawler:
            try:
                # Handle different crawler types
                if crawler_type == CrawlerType.DYNAMIC_TABLE:
                    result = await self._execute_table_crawl(crawler, source_config)
                elif crawler_type == CrawlerType.SPA:
                    result = await self._execute_spa_crawl(crawler, source_config)
                else:
                    result = await self._execute_basic_crawl(crawler, source_config)

                return ExecutionResult(
                    success=result.success,
                    source_id=source_config.source_id,
                    url=source_config.url,
                    crawler_type=crawler_type.value,
                    data=result.data,
                    record_count=result.record_count,
                    error_code=result.error_code,
                    error_message=result.error_message,
                    execution_time_ms=result.execution_time_ms,
                    metadata=result.metadata,
                    html_snapshot=result.html_snapshot
                )

            except Exception as e:
                logger.error(f"Crawl execution error: {e}")

                # Take error screenshot
                if self.config.screenshot_on_error:
                    try:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        screenshot_path = os.path.join(
                            self.config.screenshot_dir,
                            f"error_{source_config.source_id}_{timestamp}.png"
                        )
                        await crawler.screenshot(path=screenshot_path, full_page=True)
                    except Exception:
                        pass

                # Get HTML snapshot
                try:
                    html_snapshot = await crawler.get_html()
                    html_snapshot = html_snapshot[:5000]
                except Exception:
                    pass

                return ExecutionResult(
                    success=False,
                    source_id=source_config.source_id,
                    url=source_config.url,
                    crawler_type=crawler_type.value,
                    data=[],
                    record_count=0,
                    error_code='E010',
                    error_message=str(e),
                    screenshot_path=screenshot_path,
                    html_snapshot=html_snapshot
                )

    async def _execute_basic_crawl(
        self,
        crawler: PlaywrightCrawler,
        source_config: SourceConfig
    ) -> CrawlResult:
        """Execute basic Playwright crawl."""
        # Handle pagination
        if source_config.pagination_enabled and source_config.next_button_selector:
            return await self._execute_pagination_crawl(crawler, source_config)

        # Handle infinite scroll
        if source_config.infinite_scroll:
            return await self._execute_infinite_scroll_crawl(crawler, source_config)

        # Standard crawl
        return await crawler.crawl(
            url=source_config.url,
            fields=source_config.fields,
            wait_selector=source_config.wait_selector,
            pre_actions=source_config.pre_actions,
            post_actions=source_config.post_actions
        )

    async def _execute_pagination_crawl(
        self,
        crawler: PlaywrightCrawler,
        source_config: SourceConfig
    ) -> CrawlResult:
        """Execute crawl with pagination handling."""
        import time
        start_time = time.time()

        # Navigate to initial page
        await crawler.navigate(source_config.url)

        if source_config.wait_selector:
            await crawler.wait_for_selector(source_config.wait_selector)

        # Execute pre-actions
        if source_config.pre_actions:
            await crawler._execute_actions(source_config.pre_actions)

        # Handle pagination
        item_selector = source_config.container_selector or source_config.wait_selector
        if not item_selector:
            item_selector = "body"

        data = await crawler.handle_pagination(
            next_button_selector=source_config.next_button_selector,
            item_selector=item_selector,
            max_pages=source_config.max_pages
        )

        execution_time_ms = int((time.time() - start_time) * 1000)

        return CrawlResult(
            success=True,
            data=data,
            record_count=len(data),
            execution_time_ms=execution_time_ms,
            metadata={
                "url": source_config.url,
                "crawler_type": "playwright_pagination"
            }
        )

    async def _execute_infinite_scroll_crawl(
        self,
        crawler: PlaywrightCrawler,
        source_config: SourceConfig
    ) -> CrawlResult:
        """Execute crawl with infinite scroll handling."""
        import time
        start_time = time.time()

        # Navigate to initial page
        await crawler.navigate(source_config.url)

        if source_config.wait_selector:
            await crawler.wait_for_selector(source_config.wait_selector)

        # Execute pre-actions
        if source_config.pre_actions:
            await crawler._execute_actions(source_config.pre_actions)

        # Handle infinite scroll
        item_selector = source_config.container_selector or source_config.wait_selector
        if not item_selector:
            item_selector = "body"

        data = await crawler.handle_infinite_scroll(
            item_selector=item_selector,
            max_items=source_config.max_items,
            scroll_delay=source_config.scroll_delay
        )

        execution_time_ms = int((time.time() - start_time) * 1000)

        return CrawlResult(
            success=True,
            data=data,
            record_count=len(data),
            execution_time_ms=execution_time_ms,
            metadata={
                "url": source_config.url,
                "crawler_type": "playwright_infinite_scroll"
            }
        )

    async def _execute_spa_crawl(
        self,
        crawler: SPACrawler,
        source_config: SourceConfig
    ) -> CrawlResult:
        """Execute SPA-specific crawl."""
        return await crawler.crawl(
            url=source_config.url,
            fields=source_config.fields,
            wait_selector=source_config.wait_selector,
            pre_actions=source_config.pre_actions,
            post_actions=source_config.post_actions
        )

    async def _execute_table_crawl(
        self,
        crawler: DynamicTableCrawler,
        source_config: SourceConfig
    ) -> CrawlResult:
        """Execute dynamic table crawl."""
        return await crawler.crawl_table(
            url=source_config.url,
            table_selector=source_config.table_selector,
            extract_all=source_config.extract_all_pages,
            wait_selector=source_config.wait_selector
        )

    def execute_sync(self, source_config: SourceConfig) -> ExecutionResult:
        """
        Synchronous wrapper for execute.

        Args:
            source_config: Source configuration

        Returns:
            ExecutionResult
        """
        return asyncio.run(self.execute(source_config))

    async def execute_batch(
        self,
        source_configs: List[SourceConfig],
        concurrency: int = 3
    ) -> List[ExecutionResult]:
        """
        Execute multiple crawls with concurrency control.

        Args:
            source_configs: List of source configurations
            concurrency: Maximum concurrent executions

        Returns:
            List of ExecutionResults
        """
        semaphore = asyncio.Semaphore(concurrency)

        async def execute_with_semaphore(config: SourceConfig) -> ExecutionResult:
            async with semaphore:
                return await self.execute(config)

        tasks = [execute_with_semaphore(config) for config in source_configs]
        return await asyncio.gather(*tasks)

    def execute_batch_sync(
        self,
        source_configs: List[SourceConfig],
        concurrency: int = 3
    ) -> List[ExecutionResult]:
        """
        Synchronous wrapper for execute_batch.

        Args:
            source_configs: List of source configurations
            concurrency: Maximum concurrent executions

        Returns:
            List of ExecutionResults
        """
        return asyncio.run(self.execute_batch(source_configs, concurrency))


# Airflow task-compatible functions
def run_playwright_crawl(
    source_config: Dict[str, Any],
    executor_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Airflow-compatible function for running Playwright crawls.

    Args:
        source_config: Source configuration dictionary
        executor_config: Executor configuration dictionary

    Returns:
        Execution result dictionary
    """
    exec_config = ExecutorConfig(**executor_config) if executor_config else ExecutorConfig()
    src_config = SourceConfig.from_dict(source_config)

    executor = PlaywrightExecutor(exec_config)
    result = executor.execute_sync(src_config)

    return result.to_dict()


def run_playwright_batch(
    source_configs: List[Dict[str, Any]],
    executor_config: Optional[Dict[str, Any]] = None,
    concurrency: int = 3
) -> List[Dict[str, Any]]:
    """
    Airflow-compatible function for batch Playwright crawls.

    Args:
        source_configs: List of source configuration dictionaries
        executor_config: Executor configuration dictionary
        concurrency: Maximum concurrent executions

    Returns:
        List of execution result dictionaries
    """
    exec_config = ExecutorConfig(**executor_config) if executor_config else ExecutorConfig()
    src_configs = [SourceConfig.from_dict(cfg) for cfg in source_configs]

    executor = PlaywrightExecutor(exec_config)
    results = executor.execute_batch_sync(src_configs, concurrency)

    return [result.to_dict() for result in results]


# Example usage for Airflow PythonOperator
"""
from airflow.decorators import task
from airflow.dags.utils.playwright_executor import run_playwright_crawl, SourceConfig, PageType

@task
def crawl_spa_page():
    config = {
        "source_id": "example_spa",
        "url": "https://example.com/spa",
        "name": "Example SPA",
        "page_type": "spa_react",
        "wait_selector": ".content-loaded",
        "fields": [
            {"name": "title", "selector": "h1"},
            {"name": "items", "selector": ".item", "is_list": True}
        ]
    }
    return run_playwright_crawl(config)

@task
def crawl_data_table():
    config = {
        "source_id": "example_table",
        "url": "https://example.com/data",
        "name": "Example Table",
        "page_type": "table_datatables",
        "table_selector": "#data-table",
        "extract_all_pages": True
    }
    return run_playwright_crawl(config)
"""

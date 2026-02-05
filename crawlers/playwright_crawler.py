"""
Playwright-based crawler for JavaScript-rendered websites.

This module provides a high-performance async crawler using Playwright
for sites that require JavaScript execution, dynamic content loading,
infinite scroll handling, and complex user interactions.
"""

import asyncio
import logging
import time
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Callable, TypeVar, Union
from contextlib import asynccontextmanager

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    Playwright,
    Response,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError
)

from .base_crawler import CrawlResult

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class PlaywrightConfig:
    """Configuration for Playwright crawler."""

    # Browser settings
    headless: bool = True
    browser_type: str = "chromium"  # chromium, firefox, webkit
    slow_mo: int = 0  # Slow down operations by ms

    # Timeouts (in milliseconds)
    timeout: int = 30000  # Default timeout for operations
    navigation_timeout: int = 30000  # Page navigation timeout

    # Wait strategy
    wait_until: str = "networkidle"  # load, domcontentloaded, networkidle, commit

    # Viewport
    viewport: Optional[Dict[str, int]] = None  # {"width": 1920, "height": 1080}

    # User agent
    user_agent: Optional[str] = None

    # Proxy configuration
    proxy: Optional[Dict[str, str]] = None  # {"server": "...", "username": "...", "password": "..."}

    # JavaScript
    javascript_enabled: bool = True

    # Resource blocking for performance
    block_resources: List[str] = field(default_factory=list)  # ["image", "stylesheet", "font", "media"]

    # Additional browser args
    browser_args: List[str] = field(default_factory=list)

    # Retry settings
    retry_count: int = 3
    retry_delay: int = 1000  # ms

    # Screenshot settings
    screenshot_on_error: bool = True

    # Cookies and storage
    cookies: List[Dict[str, Any]] = field(default_factory=list)
    storage_state: Optional[str] = None  # Path to storage state file

    # Headers
    extra_headers: Dict[str, str] = field(default_factory=dict)

    # Geolocation
    geolocation: Optional[Dict[str, float]] = None  # {"latitude": 37.5665, "longitude": 126.9780}

    # Locale and timezone
    locale: str = "ko-KR"
    timezone_id: str = "Asia/Seoul"

    def __post_init__(self):
        """Set default viewport if not provided."""
        if self.viewport is None:
            self.viewport = {"width": 1920, "height": 1080}

        if self.user_agent is None:
            self.user_agent = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )


@dataclass
class ElementInfo:
    """Information about a DOM element."""
    selector: str
    text: str
    html: str
    attributes: Dict[str, str]
    bounding_box: Optional[Dict[str, float]] = None
    is_visible: bool = True


class PlaywrightCrawler:
    """
    JavaScript rendering crawler using Playwright.

    Supports async context manager pattern for proper resource cleanup.
    Handles dynamic content, infinite scroll, pagination, and complex interactions.
    """

    def __init__(self, config: Optional[PlaywrightConfig] = None):
        """
        Initialize Playwright crawler.

        Args:
            config: Playwright configuration options
        """
        self.config = config or PlaywrightConfig()
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._start_time: Optional[float] = None
        self._responses: List[Response] = []
        self._is_started = False

    @property
    def page(self) -> Optional[Page]:
        """Get current page instance."""
        return self._page

    @property
    def browser(self) -> Optional[Browser]:
        """Get browser instance."""
        return self._browser

    @property
    def context(self) -> Optional[BrowserContext]:
        """Get browser context."""
        return self._context

    async def __aenter__(self) -> 'PlaywrightCrawler':
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def start(self) -> None:
        """
        Start browser and create page.

        Initializes Playwright, launches browser, creates context and page.
        """
        if self._is_started:
            return

        self._start_time = time.time()

        try:
            # Start Playwright
            self._playwright = await async_playwright().start()

            # Get browser type
            browser_type = getattr(self._playwright, self.config.browser_type)

            # Launch options
            launch_options = {
                "headless": self.config.headless,
                "slow_mo": self.config.slow_mo,
            }

            if self.config.browser_args:
                launch_options["args"] = self.config.browser_args

            if self.config.proxy:
                launch_options["proxy"] = self.config.proxy

            # Launch browser
            self._browser = await browser_type.launch(**launch_options)

            # Context options
            context_options = {
                "viewport": self.config.viewport,
                "user_agent": self.config.user_agent,
                "locale": self.config.locale,
                "timezone_id": self.config.timezone_id,
                "java_script_enabled": self.config.javascript_enabled,
            }

            if self.config.geolocation:
                context_options["geolocation"] = self.config.geolocation
                context_options["permissions"] = ["geolocation"]

            if self.config.extra_headers:
                context_options["extra_http_headers"] = self.config.extra_headers

            if self.config.storage_state:
                context_options["storage_state"] = self.config.storage_state

            # Create context
            self._context = await self._browser.new_context(**context_options)

            # Set default timeouts
            self._context.set_default_timeout(self.config.timeout)
            self._context.set_default_navigation_timeout(self.config.navigation_timeout)

            # Add cookies if specified
            if self.config.cookies:
                await self._context.add_cookies(self.config.cookies)

            # Create page
            self._page = await self._context.new_page()

            # Setup resource blocking
            if self.config.block_resources:
                await self._setup_resource_blocking()

            # Track responses
            self._page.on("response", self._on_response)

            self._is_started = True
            logger.info(f"Playwright browser started: {self.config.browser_type}")

        except Exception as e:
            logger.error(f"Failed to start Playwright: {e}")
            await self.close()
            raise

    async def close(self) -> None:
        """
        Close browser and cleanup resources.

        Properly closes page, context, browser, and Playwright instance.
        """
        if self._page:
            try:
                await self._page.close()
            except Exception:
                pass
            self._page = None

        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass
            self._context = None

        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None

        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

        self._is_started = False
        self._responses.clear()
        logger.info("Playwright browser closed")

    async def _setup_resource_blocking(self) -> None:
        """Setup resource blocking for performance optimization."""
        resource_types = set(self.config.block_resources)

        async def route_handler(route):
            if route.request.resource_type in resource_types:
                await route.abort()
            else:
                await route.continue_()

        await self._page.route("**/*", route_handler)

    def _on_response(self, response: Response) -> None:
        """Track responses for debugging."""
        self._responses.append(response)

    def _get_elapsed_ms(self) -> int:
        """Get elapsed time since start in milliseconds."""
        if self._start_time is None:
            return 0
        return int((time.time() - self._start_time) * 1000)

    # ==================== Navigation Methods ====================

    async def navigate(
        self,
        url: str,
        wait_until: Optional[str] = None,
        timeout: Optional[int] = None
    ) -> Response:
        """
        Navigate to URL.

        Args:
            url: Target URL
            wait_until: Wait condition override
            timeout: Timeout override in ms

        Returns:
            Response object
        """
        self._ensure_page()

        options = {
            "wait_until": wait_until or self.config.wait_until,
        }
        if timeout:
            options["timeout"] = timeout

        response = await self._page.goto(url, **options)
        logger.info(f"Navigated to {url}, status: {response.status if response else 'N/A'}")
        return response

    async def reload(self, wait_until: Optional[str] = None) -> Response:
        """Reload current page."""
        self._ensure_page()
        return await self._page.reload(wait_until=wait_until or self.config.wait_until)

    async def go_back(self) -> Optional[Response]:
        """Navigate back in history."""
        self._ensure_page()
        return await self._page.go_back()

    async def go_forward(self) -> Optional[Response]:
        """Navigate forward in history."""
        self._ensure_page()
        return await self._page.go_forward()

    # ==================== Wait Methods ====================

    async def wait_for_selector(
        self,
        selector: str,
        state: str = "visible",
        timeout: Optional[int] = None
    ) -> None:
        """
        Wait for selector to appear.

        Args:
            selector: CSS or XPath selector
            state: visible, attached, detached, hidden
            timeout: Timeout in ms
        """
        self._ensure_page()
        await self._page.wait_for_selector(
            selector,
            state=state,
            timeout=timeout or self.config.timeout
        )

    async def wait_for_load_state(
        self,
        state: str = "networkidle",
        timeout: Optional[int] = None
    ) -> None:
        """
        Wait for page load state.

        Args:
            state: load, domcontentloaded, networkidle
            timeout: Timeout in ms
        """
        self._ensure_page()
        await self._page.wait_for_load_state(
            state,
            timeout=timeout or self.config.navigation_timeout
        )

    async def wait_for_network_idle(
        self,
        timeout: Optional[int] = None
    ) -> None:
        """Wait for network to be idle."""
        await self.wait_for_load_state("networkidle", timeout)

    async def wait_for_function(
        self,
        expression: str,
        timeout: Optional[int] = None
    ) -> Any:
        """
        Wait for JavaScript function to return truthy value.

        Args:
            expression: JavaScript expression to evaluate
            timeout: Timeout in ms

        Returns:
            Evaluation result
        """
        self._ensure_page()
        return await self._page.wait_for_function(
            expression,
            timeout=timeout or self.config.timeout
        )

    async def wait_for_response(
        self,
        url_pattern: str,
        timeout: Optional[int] = None
    ) -> Response:
        """
        Wait for a specific network response.

        Args:
            url_pattern: URL pattern to match
            timeout: Timeout in ms

        Returns:
            Response object
        """
        self._ensure_page()
        return await self._page.wait_for_response(
            url_pattern,
            timeout=timeout or self.config.timeout
        )

    async def wait(self, ms: int) -> None:
        """
        Wait for specified milliseconds.

        Args:
            ms: Milliseconds to wait
        """
        await asyncio.sleep(ms / 1000)

    # ==================== Interaction Methods ====================

    async def click(
        self,
        selector: str,
        button: str = "left",
        click_count: int = 1,
        delay: int = 0,
        force: bool = False,
        timeout: Optional[int] = None
    ) -> None:
        """
        Click on element.

        Args:
            selector: Element selector
            button: left, right, middle
            click_count: Number of clicks
            delay: Delay between mouse down and up
            force: Force click even if not visible
            timeout: Timeout in ms
        """
        self._ensure_page()
        await self._page.click(
            selector,
            button=button,
            click_count=click_count,
            delay=delay,
            force=force,
            timeout=timeout or self.config.timeout
        )

    async def double_click(self, selector: str, **kwargs) -> None:
        """Double click on element."""
        self._ensure_page()
        await self._page.dblclick(selector, **kwargs)

    async def fill(
        self,
        selector: str,
        value: str,
        timeout: Optional[int] = None
    ) -> None:
        """
        Fill input field with value.

        Args:
            selector: Input selector
            value: Value to fill
            timeout: Timeout in ms
        """
        self._ensure_page()
        await self._page.fill(
            selector,
            value,
            timeout=timeout or self.config.timeout
        )

    async def type_text(
        self,
        selector: str,
        text: str,
        delay: int = 50
    ) -> None:
        """
        Type text character by character.

        Args:
            selector: Input selector
            text: Text to type
            delay: Delay between keystrokes in ms
        """
        self._ensure_page()
        await self._page.type(selector, text, delay=delay)

    async def clear(self, selector: str) -> None:
        """Clear input field."""
        self._ensure_page()
        await self._page.fill(selector, "")

    async def select(
        self,
        selector: str,
        value: Optional[str] = None,
        label: Optional[str] = None,
        index: Optional[int] = None
    ) -> List[str]:
        """
        Select option from dropdown.

        Args:
            selector: Select element selector
            value: Option value
            label: Option label text
            index: Option index

        Returns:
            List of selected option values
        """
        self._ensure_page()
        options = {}
        if value is not None:
            options["value"] = value
        if label is not None:
            options["label"] = label
        if index is not None:
            options["index"] = index

        return await self._page.select_option(selector, **options)

    async def check(self, selector: str) -> None:
        """Check checkbox."""
        self._ensure_page()
        await self._page.check(selector)

    async def uncheck(self, selector: str) -> None:
        """Uncheck checkbox."""
        self._ensure_page()
        await self._page.uncheck(selector)

    async def hover(self, selector: str) -> None:
        """Hover over element."""
        self._ensure_page()
        await self._page.hover(selector)

    async def press(self, selector: str, key: str) -> None:
        """
        Press keyboard key on element.

        Args:
            selector: Element selector
            key: Key to press (e.g., 'Enter', 'Tab', 'ArrowDown')
        """
        self._ensure_page()
        await self._page.press(selector, key)

    async def keyboard_press(self, key: str) -> None:
        """Press keyboard key globally."""
        self._ensure_page()
        await self._page.keyboard.press(key)

    async def focus(self, selector: str) -> None:
        """Focus on element."""
        self._ensure_page()
        await self._page.focus(selector)

    async def drag_and_drop(
        self,
        source_selector: str,
        target_selector: str
    ) -> None:
        """
        Drag element to target.

        Args:
            source_selector: Source element selector
            target_selector: Target element selector
        """
        self._ensure_page()
        await self._page.drag_and_drop(source_selector, target_selector)

    # ==================== Content Extraction Methods ====================

    async def get_html(self) -> str:
        """
        Get current page HTML.

        Returns:
            Full page HTML content
        """
        self._ensure_page()
        return await self._page.content()

    async def get_inner_html(self, selector: str) -> str:
        """
        Get inner HTML of element.

        Args:
            selector: Element selector

        Returns:
            Inner HTML content
        """
        self._ensure_page()
        return await self._page.inner_html(selector)

    async def get_text(self, selector: str) -> str:
        """
        Get text content of element.

        Args:
            selector: Element selector

        Returns:
            Text content
        """
        self._ensure_page()
        return await self._page.inner_text(selector)

    async def get_texts(self, selector: str) -> List[str]:
        """
        Get text content of all matching elements.

        Args:
            selector: Elements selector

        Returns:
            List of text contents
        """
        self._ensure_page()
        elements = await self._page.query_selector_all(selector)
        texts = []
        for element in elements:
            text = await element.inner_text()
            texts.append(text.strip())
        return texts

    async def get_attribute(
        self,
        selector: str,
        attribute: str
    ) -> Optional[str]:
        """
        Get attribute value of element.

        Args:
            selector: Element selector
            attribute: Attribute name

        Returns:
            Attribute value or None
        """
        self._ensure_page()
        return await self._page.get_attribute(selector, attribute)

    async def get_attributes(
        self,
        selector: str,
        attribute: str
    ) -> List[str]:
        """
        Get attribute values of all matching elements.

        Args:
            selector: Elements selector
            attribute: Attribute name

        Returns:
            List of attribute values
        """
        self._ensure_page()
        elements = await self._page.query_selector_all(selector)
        attrs = []
        for element in elements:
            attr = await element.get_attribute(attribute)
            if attr:
                attrs.append(attr)
        return attrs

    async def get_input_value(self, selector: str) -> str:
        """
        Get value of input element.

        Args:
            selector: Input selector

        Returns:
            Input value
        """
        self._ensure_page()
        return await self._page.input_value(selector)

    async def get_element_info(self, selector: str) -> Optional[ElementInfo]:
        """
        Get comprehensive information about an element.

        Args:
            selector: Element selector

        Returns:
            ElementInfo or None if not found
        """
        self._ensure_page()
        element = await self._page.query_selector(selector)
        if not element:
            return None

        text = await element.inner_text()
        html = await element.inner_html()
        is_visible = await element.is_visible()
        bounding_box = await element.bounding_box()

        # Get all attributes
        attributes = await element.evaluate("""
            el => {
                const attrs = {};
                for (const attr of el.attributes) {
                    attrs[attr.name] = attr.value;
                }
                return attrs;
            }
        """)

        return ElementInfo(
            selector=selector,
            text=text.strip(),
            html=html,
            attributes=attributes,
            bounding_box=bounding_box,
            is_visible=is_visible
        )

    async def count_elements(self, selector: str) -> int:
        """
        Count matching elements.

        Args:
            selector: Elements selector

        Returns:
            Number of matching elements
        """
        self._ensure_page()
        elements = await self._page.query_selector_all(selector)
        return len(elements)

    async def is_visible(self, selector: str) -> bool:
        """Check if element is visible."""
        self._ensure_page()
        return await self._page.is_visible(selector)

    async def is_enabled(self, selector: str) -> bool:
        """Check if element is enabled."""
        self._ensure_page()
        return await self._page.is_enabled(selector)

    async def is_checked(self, selector: str) -> bool:
        """Check if checkbox is checked."""
        self._ensure_page()
        return await self._page.is_checked(selector)

    # ==================== JavaScript Execution ====================

    async def evaluate(self, script: str, arg: Any = None) -> Any:
        """
        Execute JavaScript in page context.

        Args:
            script: JavaScript code to execute
            arg: Optional argument to pass to script

        Returns:
            Evaluation result
        """
        self._ensure_page()
        if arg is not None:
            return await self._page.evaluate(script, arg)
        return await self._page.evaluate(script)

    async def evaluate_handle(self, script: str, arg: Any = None):
        """
        Execute JavaScript and return handle.

        Args:
            script: JavaScript code to execute
            arg: Optional argument

        Returns:
            JSHandle to the result
        """
        self._ensure_page()
        if arg is not None:
            return await self._page.evaluate_handle(script, arg)
        return await self._page.evaluate_handle(script)

    async def add_script_tag(
        self,
        url: Optional[str] = None,
        content: Optional[str] = None
    ) -> None:
        """
        Add script tag to page.

        Args:
            url: Script URL
            content: Script content
        """
        self._ensure_page()
        options = {}
        if url:
            options["url"] = url
        if content:
            options["content"] = content
        await self._page.add_script_tag(**options)

    # ==================== Screenshot Methods ====================

    async def screenshot(
        self,
        path: Optional[str] = None,
        full_page: bool = False,
        clip: Optional[Dict[str, int]] = None,
        quality: int = 80,
        image_type: str = "png"
    ) -> bytes:
        """
        Take screenshot.

        Args:
            path: Save path (optional)
            full_page: Capture full page
            clip: Clip area {"x": 0, "y": 0, "width": 100, "height": 100}
            quality: JPEG quality (0-100)
            image_type: png or jpeg

        Returns:
            Screenshot bytes
        """
        self._ensure_page()
        options = {
            "full_page": full_page,
            "type": image_type,
        }

        if path:
            options["path"] = path

        if clip:
            options["clip"] = clip

        if image_type == "jpeg":
            options["quality"] = quality

        return await self._page.screenshot(**options)

    async def screenshot_element(
        self,
        selector: str,
        path: Optional[str] = None
    ) -> bytes:
        """
        Take screenshot of specific element.

        Args:
            selector: Element selector
            path: Save path (optional)

        Returns:
            Screenshot bytes
        """
        self._ensure_page()
        element = await self._page.query_selector(selector)
        if not element:
            raise ValueError(f"Element not found: {selector}")

        options = {}
        if path:
            options["path"] = path

        return await element.screenshot(**options)

    # ==================== Table Extraction ====================

    async def extract_table(
        self,
        table_selector: str,
        header_selector: str = "thead th, thead td",
        row_selector: str = "tbody tr",
        cell_selector: str = "td"
    ) -> List[Dict[str, str]]:
        """
        Extract data from HTML table.

        Args:
            table_selector: Table element selector
            header_selector: Header cells selector (relative to table)
            row_selector: Data rows selector (relative to table)
            cell_selector: Data cells selector (relative to row)

        Returns:
            List of dictionaries with header keys and cell values
        """
        self._ensure_page()

        # Get headers
        headers = await self.evaluate(f"""
            () => {{
                const table = document.querySelector('{table_selector}');
                if (!table) return [];
                const headerCells = table.querySelectorAll('{header_selector}');
                return Array.from(headerCells).map(cell => cell.innerText.trim());
            }}
        """)

        if not headers:
            logger.warning(f"No headers found in table: {table_selector}")
            headers = []

        # Get rows
        rows_data = await self.evaluate(f"""
            () => {{
                const table = document.querySelector('{table_selector}');
                if (!table) return [];
                const rows = table.querySelectorAll('{row_selector}');
                return Array.from(rows).map(row => {{
                    const cells = row.querySelectorAll('{cell_selector}');
                    return Array.from(cells).map(cell => cell.innerText.trim());
                }});
            }}
        """)

        # Convert to list of dicts
        result = []
        for row in rows_data:
            if headers:
                # Use headers as keys
                record = {}
                for i, value in enumerate(row):
                    key = headers[i] if i < len(headers) else f"column_{i}"
                    record[key] = value
                result.append(record)
            else:
                # Use index keys
                result.append({f"column_{i}": v for i, v in enumerate(row)})

        return result

    async def extract_tables_all(self, table_selector: str = "table") -> List[List[Dict[str, str]]]:
        """
        Extract all tables from page.

        Args:
            table_selector: Table elements selector

        Returns:
            List of table data (each table is a list of row dicts)
        """
        self._ensure_page()

        table_count = await self.count_elements(table_selector)
        tables = []

        for i in range(table_count):
            nth_selector = f"{table_selector}:nth-of-type({i + 1})"
            table_data = await self.extract_table(nth_selector)
            tables.append(table_data)

        return tables

    # ==================== Scroll Methods ====================

    async def scroll_to_bottom(
        self,
        delay: int = 500,
        max_scrolls: int = 50,
        scroll_step: int = 500
    ) -> None:
        """
        Scroll to bottom of page (for infinite scroll).

        Args:
            delay: Delay between scrolls in ms
            max_scrolls: Maximum number of scroll attempts
            scroll_step: Pixels to scroll each step
        """
        self._ensure_page()

        previous_height = 0
        scroll_count = 0

        while scroll_count < max_scrolls:
            # Get current scroll height
            current_height = await self.evaluate("document.body.scrollHeight")

            if current_height == previous_height:
                # No more content to load
                break

            # Scroll down
            await self.evaluate(f"window.scrollBy(0, {scroll_step})")
            await self.wait(delay)

            previous_height = current_height
            scroll_count += 1

        logger.info(f"Scrolled to bottom in {scroll_count} steps")

    async def scroll_to_element(self, selector: str) -> None:
        """
        Scroll element into view.

        Args:
            selector: Element selector
        """
        self._ensure_page()
        await self.evaluate(f"""
            document.querySelector('{selector}')?.scrollIntoView({{
                behavior: 'smooth',
                block: 'center'
            }})
        """)
        await self.wait(300)

    async def scroll_by(self, x: int = 0, y: int = 0) -> None:
        """
        Scroll by specified amount.

        Args:
            x: Horizontal scroll amount
            y: Vertical scroll amount
        """
        self._ensure_page()
        await self.evaluate(f"window.scrollBy({x}, {y})")

    async def scroll_to(self, x: int = 0, y: int = 0) -> None:
        """
        Scroll to absolute position.

        Args:
            x: Horizontal position
            y: Vertical position
        """
        self._ensure_page()
        await self.evaluate(f"window.scrollTo({x}, {y})")

    # ==================== Pagination Methods ====================

    async def handle_pagination(
        self,
        next_button_selector: str,
        item_selector: str,
        max_pages: int = 10,
        extract_fn: Optional[Callable[[], Any]] = None,
        wait_after_click: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Handle click-based pagination.

        Args:
            next_button_selector: Next page button selector
            item_selector: Items to extract selector
            max_pages: Maximum pages to crawl
            extract_fn: Custom extraction function (optional)
            wait_after_click: Wait time after clicking next in ms

        Returns:
            List of all extracted items
        """
        self._ensure_page()
        all_items = []
        page_num = 1

        while page_num <= max_pages:
            logger.info(f"Processing page {page_num}")

            # Wait for items
            try:
                await self.wait_for_selector(item_selector, timeout=5000)
            except PlaywrightTimeoutError:
                logger.warning(f"No items found on page {page_num}")
                break

            # Extract items
            if extract_fn:
                items = await extract_fn()
            else:
                items = await self._extract_items_default(item_selector)

            all_items.extend(items)
            logger.info(f"Extracted {len(items)} items from page {page_num}")

            # Check for next button
            next_button_visible = await self.is_visible(next_button_selector)
            next_button_enabled = True

            try:
                next_button_enabled = await self.is_enabled(next_button_selector)
            except Exception:
                pass

            if not next_button_visible or not next_button_enabled:
                logger.info(f"No more pages available after page {page_num}")
                break

            # Click next
            try:
                await self.click(next_button_selector)
                await self.wait(wait_after_click)
                await self.wait_for_network_idle(timeout=5000)
            except Exception as e:
                logger.warning(f"Failed to click next button: {e}")
                break

            page_num += 1

        return all_items

    async def handle_infinite_scroll(
        self,
        item_selector: str,
        max_items: int = 100,
        scroll_delay: int = 1000,
        extract_fn: Optional[Callable[[], Any]] = None,
        no_new_items_threshold: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Handle infinite scroll pagination.

        Args:
            item_selector: Items to extract selector
            max_items: Maximum items to extract
            scroll_delay: Delay between scrolls in ms
            extract_fn: Custom extraction function (optional)
            no_new_items_threshold: Stop after this many scrolls without new items

        Returns:
            List of all extracted items
        """
        self._ensure_page()
        all_items = []
        seen_ids = set()
        no_new_items_count = 0

        while len(all_items) < max_items:
            # Extract current items
            if extract_fn:
                items = await extract_fn()
            else:
                items = await self._extract_items_default(item_selector)

            # Track new items
            new_items_count = 0
            for item in items:
                item_id = self._get_item_id(item)
                if item_id not in seen_ids:
                    seen_ids.add(item_id)
                    all_items.append(item)
                    new_items_count += 1

                    if len(all_items) >= max_items:
                        break

            logger.info(f"Found {new_items_count} new items, total: {len(all_items)}")

            # Check if we got new items
            if new_items_count == 0:
                no_new_items_count += 1
                if no_new_items_count >= no_new_items_threshold:
                    logger.info("No new items found, stopping infinite scroll")
                    break
            else:
                no_new_items_count = 0

            # Scroll down
            await self.scroll_to_bottom(delay=scroll_delay, max_scrolls=1)
            await self.wait(scroll_delay)

        return all_items[:max_items]

    async def _extract_items_default(self, selector: str) -> List[Dict[str, Any]]:
        """Default item extraction."""
        elements = await self._page.query_selector_all(selector)
        items = []

        for element in elements:
            text = await element.inner_text()
            html = await element.inner_html()
            items.append({
                "text": text.strip(),
                "html": html
            })

        return items

    def _get_item_id(self, item: Dict[str, Any]) -> str:
        """Generate unique ID for item deduplication."""
        import hashlib
        content = str(item.get("text", "") or item.get("html", ""))
        return hashlib.md5(content.encode()).hexdigest()

    # ==================== Frame Methods ====================

    async def switch_to_frame(self, frame_selector: str) -> None:
        """
        Switch to iframe.

        Args:
            frame_selector: Frame element selector
        """
        self._ensure_page()
        frame = await self._page.query_selector(frame_selector)
        if frame:
            content_frame = await frame.content_frame()
            if content_frame:
                self._page = content_frame

    async def switch_to_main_frame(self) -> None:
        """Switch back to main frame."""
        if self._context:
            pages = self._context.pages
            if pages:
                self._page = pages[0]

    # ==================== Dialog Handling ====================

    async def accept_dialog(self, prompt_text: Optional[str] = None) -> None:
        """
        Setup handler to accept dialogs.

        Args:
            prompt_text: Text to enter for prompt dialogs
        """
        self._ensure_page()

        async def handle_dialog(dialog):
            if prompt_text and dialog.type == "prompt":
                await dialog.accept(prompt_text)
            else:
                await dialog.accept()

        self._page.on("dialog", handle_dialog)

    async def dismiss_dialog(self) -> None:
        """Setup handler to dismiss dialogs."""
        self._ensure_page()

        async def handle_dialog(dialog):
            await dialog.dismiss()

        self._page.on("dialog", handle_dialog)

    # ==================== Storage Methods ====================

    async def get_cookies(self) -> List[Dict[str, Any]]:
        """Get all cookies."""
        self._ensure_page()
        return await self._context.cookies()

    async def set_cookie(self, cookie: Dict[str, Any]) -> None:
        """
        Set a cookie.

        Args:
            cookie: Cookie dictionary with name, value, url/domain
        """
        self._ensure_page()
        await self._context.add_cookies([cookie])

    async def clear_cookies(self) -> None:
        """Clear all cookies."""
        self._ensure_page()
        await self._context.clear_cookies()

    async def get_local_storage(self, key: str) -> Optional[str]:
        """Get local storage item."""
        self._ensure_page()
        return await self.evaluate(f"localStorage.getItem('{key}')")

    async def set_local_storage(self, key: str, value: str) -> None:
        """Set local storage item."""
        self._ensure_page()
        await self.evaluate(f"localStorage.setItem('{key}', '{value}')")

    async def get_session_storage(self, key: str) -> Optional[str]:
        """Get session storage item."""
        self._ensure_page()
        return await self.evaluate(f"sessionStorage.getItem('{key}')")

    async def set_session_storage(self, key: str, value: str) -> None:
        """Set session storage item."""
        self._ensure_page()
        await self.evaluate(f"sessionStorage.setItem('{key}', '{value}')")

    # ==================== Utility Methods ====================

    def _ensure_page(self) -> None:
        """Ensure page is available."""
        if self._page is None:
            raise RuntimeError("Page not initialized. Call start() first or use async context manager.")

    async def get_page_info(self) -> Dict[str, Any]:
        """
        Get current page information.

        Returns:
            Dictionary with URL, title, and viewport info
        """
        self._ensure_page()
        return {
            "url": self._page.url,
            "title": await self._page.title(),
            "viewport": self._page.viewport_size,
        }

    async def get_url(self) -> str:
        """Get current page URL."""
        self._ensure_page()
        return self._page.url

    async def get_title(self) -> str:
        """Get current page title."""
        self._ensure_page()
        return await self._page.title()

    async def pdf(self, path: str, **kwargs) -> bytes:
        """
        Generate PDF of current page.

        Args:
            path: Save path
            **kwargs: Additional PDF options

        Returns:
            PDF bytes
        """
        self._ensure_page()
        return await self._page.pdf(path=path, **kwargs)

    # ==================== Crawl Execution ====================

    async def crawl(
        self,
        url: str,
        fields: List[Dict[str, str]],
        wait_selector: Optional[str] = None,
        pre_actions: Optional[List[Dict[str, Any]]] = None,
        post_actions: Optional[List[Dict[str, Any]]] = None
    ) -> CrawlResult:
        """
        Execute a complete crawl operation.

        Args:
            url: Target URL
            fields: List of field definitions with 'name', 'selector', 'data_type'
            wait_selector: Selector to wait for before extraction
            pre_actions: Actions to perform before extraction
            post_actions: Actions to perform after extraction

        Returns:
            CrawlResult with extracted data
        """
        start_time = time.time()
        html_snapshot = None

        try:
            # Navigate
            await self.navigate(url)

            # Wait for specific element
            if wait_selector:
                await self.wait_for_selector(wait_selector)

            # Execute pre-actions
            if pre_actions:
                await self._execute_actions(pre_actions)

            # Extract data
            extracted_data = await self._extract_fields(fields)

            # Execute post-actions
            if post_actions:
                await self._execute_actions(post_actions)

            execution_time_ms = int((time.time() - start_time) * 1000)

            return CrawlResult(
                success=True,
                data=extracted_data,
                record_count=len(extracted_data),
                execution_time_ms=execution_time_ms,
                metadata={
                    "url": url,
                    "page_title": await self.get_title(),
                    "crawler_type": "playwright"
                }
            )

        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout crawling {url}: {e}")

            if self.config.screenshot_on_error:
                try:
                    await self.screenshot(path=f"error_{int(time.time())}.png")
                except Exception:
                    pass

            try:
                html_snapshot = await self.get_html()
                html_snapshot = html_snapshot[:5000]
            except Exception:
                pass

            return CrawlResult(
                success=False,
                error_code='E001',
                error_message=str(e),
                execution_time_ms=int((time.time() - start_time) * 1000),
                html_snapshot=html_snapshot
            )

        except PlaywrightError as e:
            logger.error(f"Playwright error crawling {url}: {e}")

            try:
                html_snapshot = await self.get_html()
                html_snapshot = html_snapshot[:5000]
            except Exception:
                pass

            return CrawlResult(
                success=False,
                error_code='E002',
                error_message=str(e),
                execution_time_ms=int((time.time() - start_time) * 1000),
                html_snapshot=html_snapshot
            )

        except Exception as e:
            logger.error(f"Unexpected error crawling {url}: {e}")

            try:
                html_snapshot = await self.get_html()
                html_snapshot = html_snapshot[:5000]
            except Exception:
                pass

            return CrawlResult(
                success=False,
                error_code='E010',
                error_message=str(e),
                execution_time_ms=int((time.time() - start_time) * 1000),
                html_snapshot=html_snapshot
            )

    async def _extract_fields(self, fields: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Extract data based on field definitions."""
        # Check for list vs single record extraction
        is_list = any(f.get('is_list', False) for f in fields)

        if is_list:
            # Find container selector
            container_selector = None
            for field in fields:
                if field.get('is_container'):
                    container_selector = field.get('selector')
                    break

            if container_selector:
                return await self._extract_list_items(container_selector, fields)
            else:
                return await self._extract_parallel_lists(fields)
        else:
            # Single record extraction
            record = await self._extract_single_record(fields)
            return [record] if record else []

    async def _extract_single_record(self, fields: List[Dict[str, str]]) -> Dict[str, Any]:
        """Extract a single record from page."""
        record = {}

        for field in fields:
            name = field['name']
            selector = field.get('selector', '')
            data_type = field.get('data_type', 'string')
            attribute = field.get('attribute')

            if not selector:
                continue

            try:
                if attribute:
                    value = await self.get_attribute(selector, attribute)
                else:
                    value = await self.get_text(selector)

                record[name] = self._convert_value(value, data_type)

            except Exception as e:
                logger.warning(f"Error extracting field {name}: {e}")
                record[name] = None

        return record

    async def _extract_list_items(
        self,
        container_selector: str,
        fields: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """Extract multiple items from containers."""
        containers = await self._page.query_selector_all(container_selector)
        items = []

        for container in containers:
            record = {}

            for field in fields:
                if field.get('is_container'):
                    continue

                name = field['name']
                selector = field.get('selector', '')
                data_type = field.get('data_type', 'string')
                attribute = field.get('attribute')

                if not selector:
                    continue

                try:
                    # Make selector relative to container
                    relative_selector = selector
                    if selector.startswith(container_selector):
                        relative_selector = selector[len(container_selector):].strip()

                    element = await container.query_selector(relative_selector)

                    if element:
                        if attribute:
                            value = await element.get_attribute(attribute)
                        else:
                            value = await element.inner_text()

                        record[name] = self._convert_value(value, data_type)
                    else:
                        record[name] = None

                except Exception as e:
                    logger.warning(f"Error extracting field {name}: {e}")
                    record[name] = None

            if any(v is not None for v in record.values()):
                items.append(record)

        return items

    async def _extract_parallel_lists(self, fields: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Extract data from parallel lists."""
        field_values = {}
        max_length = 0

        for field in fields:
            name = field['name']
            selector = field.get('selector', '')
            data_type = field.get('data_type', 'string')
            attribute = field.get('attribute')

            if not selector:
                continue

            if attribute:
                values = await self.get_attributes(selector, attribute)
            else:
                values = await self.get_texts(selector)

            values = [self._convert_value(v, data_type) for v in values]
            field_values[name] = values
            max_length = max(max_length, len(values))

        # Combine into records
        records = []
        for i in range(max_length):
            record = {}
            for name, values in field_values.items():
                record[name] = values[i] if i < len(values) else None
            records.append(record)

        return records

    def _convert_value(self, value: Any, data_type: str) -> Any:
        """Convert value to specified data type."""
        if value is None:
            return None

        value = str(value).strip()

        if not value:
            return None

        if data_type == 'number':
            return self._parse_number(value)
        elif data_type == 'date':
            return self._parse_date(value)
        else:
            return value

    def _parse_number(self, value: str) -> Optional[float]:
        """Parse number from string."""
        import re

        # Remove common symbols
        cleaned = re.sub(r'[,\s%$]', '', value)

        try:
            return float(cleaned)
        except ValueError:
            # Try extracting just digits and decimal
            match = re.search(r'-?[\d.]+', cleaned)
            if match:
                try:
                    return float(match.group())
                except ValueError:
                    pass
            return None

    def _parse_date(self, value: str) -> str:
        """Parse and normalize date string."""
        # Return as-is for now, can be enhanced with dateutil
        return value

    async def _execute_actions(self, actions: List[Dict[str, Any]]) -> None:
        """Execute a sequence of actions."""
        for action in actions:
            action_type = action.get('type')
            selector = action.get('selector')
            value = action.get('value')

            if action_type == 'click':
                await self.click(selector)
            elif action_type == 'fill':
                await self.fill(selector, value)
            elif action_type == 'select':
                await self.select(selector, value=value)
            elif action_type == 'wait':
                await self.wait(int(value))
            elif action_type == 'wait_selector':
                await self.wait_for_selector(selector)
            elif action_type == 'scroll_bottom':
                await self.scroll_to_bottom()
            elif action_type == 'scroll_to':
                await self.scroll_to_element(selector)
            elif action_type == 'press':
                await self.press(selector, value)
            elif action_type == 'evaluate':
                await self.evaluate(value)
            else:
                logger.warning(f"Unknown action type: {action_type}")


# Convenience function
@asynccontextmanager
async def create_playwright_crawler(
    config: Optional[PlaywrightConfig] = None
) -> PlaywrightCrawler:
    """
    Create a Playwright crawler with automatic cleanup.

    Usage:
        async with create_playwright_crawler() as crawler:
            await crawler.navigate("https://example.com")
            html = await crawler.get_html()
    """
    crawler = PlaywrightCrawler(config)
    try:
        await crawler.start()
        yield crawler
    finally:
        await crawler.close()

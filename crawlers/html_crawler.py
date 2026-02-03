"""
HTML Crawler for web page scraping.

This module provides crawlers for HTML content using
BeautifulSoup and optionally Selenium for dynamic pages.
"""

import logging
import os
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup

from .base_crawler import BaseCrawler, CrawlResult

logger = logging.getLogger(__name__)


class HTMLCrawler(BaseCrawler):
    """Crawler for HTML web pages."""

    def __init__(
        self,
        url: str,
        use_selenium: bool = False,
        wait_for_selector: Optional[str] = None,
        selenium_timeout: int = 10,
        **kwargs
    ):
        """
        Initialize HTML crawler.

        Args:
            url: Target URL
            use_selenium: Use Selenium for dynamic content
            wait_for_selector: CSS selector to wait for (Selenium only)
            selenium_timeout: Timeout for Selenium operations
            **kwargs: Additional arguments for BaseCrawler
        """
        super().__init__(url, **kwargs)
        self.use_selenium = use_selenium
        self.wait_for_selector = wait_for_selector
        self.selenium_timeout = selenium_timeout
        self._driver = None

    def _get_selenium_driver(self):
        """Get or create Selenium WebDriver."""
        if self._driver is None:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service

            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument(f'user-agent={self.headers.get("User-Agent", "")}')

            # Check for Selenium Grid
            selenium_hub = os.getenv('SELENIUM_HUB_URL', 'http://selenium-hub:4444/wd/hub')

            try:
                # Try remote WebDriver first (Docker environment)
                self._driver = webdriver.Remote(
                    command_executor=selenium_hub,
                    options=options
                )
            except Exception:
                # Fall back to local WebDriver
                self._driver = webdriver.Chrome(options=options)

            self._driver.set_page_load_timeout(self.timeout)

        return self._driver

    def _close_selenium(self):
        """Close Selenium WebDriver."""
        if self._driver:
            try:
                self._driver.quit()
            except Exception:
                pass
            self._driver = None

    def close(self):
        """Close all resources."""
        self._close_selenium()
        super().close()

    def _fetch_with_selenium(self) -> str:
        """
        Fetch page content using Selenium.

        Returns:
            Page HTML content
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        driver = self._get_selenium_driver()
        driver.get(self.url)

        # Wait for specific element if specified
        if self.wait_for_selector:
            WebDriverWait(driver, self.selenium_timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.wait_for_selector))
            )

        return driver.page_source

    def crawl(self, fields: List[Dict[str, str]]) -> CrawlResult:
        """
        Crawl HTML page and extract data.

        Args:
            fields: List of field definitions with 'name', 'selector', 'data_type'

        Returns:
            CrawlResult with extracted data
        """
        try:
            # Fetch page content
            if self.use_selenium:
                html_content = self._fetch_with_selenium()
            else:
                response = self.fetch_url()
                encoding = self.detect_encoding(response)
                response.encoding = encoding
                html_content = response.text

            # Parse HTML
            soup = BeautifulSoup(html_content, 'lxml')

            # Extract data based on fields
            extracted_data = []

            # Check if we're extracting a list or single record
            is_list = any(f.get('is_list', False) for f in fields)

            if is_list:
                # Find common parent container
                container_selector = self._find_container_selector(fields)
                if container_selector:
                    containers = soup.select(container_selector)
                    for container in containers:
                        record = self._extract_record(container, fields)
                        if record:
                            extracted_data.append(record)
                else:
                    # Extract as parallel lists
                    extracted_data = self._extract_parallel_lists(soup, fields)
            else:
                # Single record extraction
                record = self._extract_record(soup, fields)
                if record:
                    extracted_data.append(record)

            return CrawlResult(
                success=True,
                data=extracted_data,
                record_count=len(extracted_data),
                html_snapshot=html_content[:5000] if not extracted_data else None
            )

        except Exception as e:
            logger.error(f"Error crawling {self.url}: {e}")

            # Get HTML snapshot for debugging
            html_snapshot = None
            try:
                if self.use_selenium and self._driver:
                    html_snapshot = self._driver.page_source[:5000]
            except Exception:
                pass

            return CrawlResult(
                success=False,
                error_code='E002',  # Selector fail by default, will be reclassified
                error_message=str(e),
                html_snapshot=html_snapshot
            )

    def _extract_record(
        self,
        element: BeautifulSoup,
        fields: List[Dict[str, str]]
    ) -> Optional[Dict[str, Any]]:
        """
        Extract a single record from an element.

        Args:
            element: BeautifulSoup element
            fields: Field definitions

        Returns:
            Extracted record or None
        """
        record = {}

        for field in fields:
            name = field['name']
            selector = field.get('selector', '')
            data_type = field.get('data_type', 'string')
            attr = field.get('attribute')  # For extracting attributes like href

            if not selector:
                continue

            try:
                if field.get('is_list'):
                    elements = element.select(selector)
                    values = []
                    for el in elements:
                        value = self._extract_value(el, attr, data_type)
                        if value is not None:
                            values.append(value)
                    record[name] = values
                else:
                    target = element.select_one(selector)
                    if target:
                        record[name] = self._extract_value(target, attr, data_type)
                    else:
                        record[name] = None
            except Exception as e:
                logger.warning(f"Error extracting field {name}: {e}")
                record[name] = None

        # Return None if all values are None/empty
        if all(v is None or v == '' or v == [] for v in record.values()):
            return None

        return record

    def _extract_value(
        self,
        element: BeautifulSoup,
        attribute: Optional[str],
        data_type: str
    ) -> Any:
        """
        Extract value from element.

        Args:
            element: BeautifulSoup element
            attribute: Attribute to extract (None for text content)
            data_type: Data type for conversion

        Returns:
            Extracted and converted value
        """
        if attribute:
            raw_value = element.get(attribute, '')
        else:
            raw_value = element.get_text(strip=True)

        raw_value = self.clean_text(raw_value)

        if data_type == 'number':
            return self.parse_number(raw_value)
        elif data_type == 'date':
            parsed = self.parse_date(raw_value)
            return parsed.isoformat() if parsed else raw_value
        else:
            return raw_value

    def _extract_parallel_lists(
        self,
        soup: BeautifulSoup,
        fields: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """
        Extract data when fields are parallel lists.

        Args:
            soup: BeautifulSoup object
            fields: Field definitions

        Returns:
            List of records
        """
        # Extract all values for each field
        field_values = {}
        max_length = 0

        for field in fields:
            name = field['name']
            selector = field.get('selector', '')
            data_type = field.get('data_type', 'string')
            attr = field.get('attribute')

            if selector:
                elements = soup.select(selector)
                values = []
                for el in elements:
                    values.append(self._extract_value(el, attr, data_type))
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

    def _find_container_selector(
        self,
        fields: List[Dict[str, str]]
    ) -> Optional[str]:
        """
        Find common parent container selector.

        Args:
            fields: Field definitions

        Returns:
            Container selector or None
        """
        # Look for explicit container definition
        for field in fields:
            if field.get('is_container'):
                return field.get('selector')

        # Try to infer from field selectors
        # This is a simplified heuristic
        selectors = [f.get('selector', '') for f in fields if f.get('selector')]

        if not selectors:
            return None

        # Find common parent class patterns
        # e.g., if selectors are ".item .title", ".item .price", container is ".item"
        common_parts = []
        first_parts = selectors[0].split()

        for part in first_parts:
            if all(part in s for s in selectors):
                common_parts.append(part)
            else:
                break

        if common_parts:
            return ' '.join(common_parts)

        return None


class SeleniumHTMLCrawler(HTMLCrawler):
    """Convenience class for Selenium-based crawling."""

    def __init__(self, url: str, **kwargs):
        """Initialize with Selenium enabled by default."""
        kwargs['use_selenium'] = True
        super().__init__(url, **kwargs)

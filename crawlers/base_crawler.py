"""
Base Crawler class providing common functionality.

This module defines the abstract base class for all crawlers
with shared methods for HTTP requests, error handling, and result formatting.
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class CrawlResult:
    """Result of a crawl operation."""
    success: bool
    data: List[Dict[str, Any]] = field(default_factory=list)
    record_count: int = 0
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    execution_time_ms: int = 0
    html_snapshot: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        return {
            'status': 'success' if self.success else 'failed',
            'data': self.data,
            'record_count': self.record_count,
            'error_code': self.error_code,
            'error_message': self.error_message,
            'execution_time_ms': self.execution_time_ms,
            'metadata': self.metadata
        }


class BaseCrawler(ABC):
    """Abstract base class for all crawlers."""

    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache'
    }

    def __init__(
        self,
        url: str,
        timeout: int = 30,
        headers: Optional[Dict[str, str]] = None,
        proxies: Optional[Dict[str, str]] = None,
        retry_count: int = 3,
        retry_backoff: float = 0.5
    ):
        """
        Initialize base crawler.

        Args:
            url: Target URL to crawl
            timeout: Request timeout in seconds
            headers: Custom HTTP headers
            proxies: Proxy configuration
            retry_count: Number of retries for failed requests
            retry_backoff: Backoff factor for retries
        """
        self.url = url
        self.timeout = timeout
        self.headers = {**self.DEFAULT_HEADERS, **(headers or {})}
        self.proxies = proxies
        self.retry_count = retry_count
        self.retry_backoff = retry_backoff

        self._session: Optional[requests.Session] = None
        self._start_time: Optional[float] = None

    @property
    def session(self) -> requests.Session:
        """Get or create requests session with retry configuration."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(self.headers)

            # Configure retry strategy
            retry_strategy = Retry(
                total=self.retry_count,
                backoff_factor=self.retry_backoff,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._session.mount("http://", adapter)
            self._session.mount("https://", adapter)

            if self.proxies:
                self._session.proxies.update(self.proxies)

        return self._session

    def close(self):
        """Close the session."""
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def _start_timer(self):
        """Start execution timer."""
        self._start_time = time.time()

    def _get_elapsed_ms(self) -> int:
        """Get elapsed time in milliseconds."""
        if self._start_time is None:
            return 0
        return int((time.time() - self._start_time) * 1000)

    def fetch_url(
        self,
        url: Optional[str] = None,
        method: str = 'GET',
        **kwargs
    ) -> requests.Response:
        """
        Fetch URL with configured session.

        Args:
            url: URL to fetch (defaults to self.url)
            method: HTTP method
            **kwargs: Additional arguments for requests

        Returns:
            Response object

        Raises:
            requests.RequestException: On request failure
        """
        target_url = url or self.url

        kwargs.setdefault('timeout', self.timeout)

        response = self.session.request(method, target_url, **kwargs)
        response.raise_for_status()

        return response

    def detect_encoding(self, response: requests.Response) -> str:
        """
        Detect response encoding.

        Args:
            response: Response object

        Returns:
            Detected encoding
        """
        # Try apparent encoding first
        if response.apparent_encoding:
            return response.apparent_encoding

        # Check content-type header
        content_type = response.headers.get('content-type', '')
        if 'charset=' in content_type.lower():
            import re
            match = re.search(r'charset=([^\s;]+)', content_type, re.IGNORECASE)
            if match:
                return match.group(1)

        # Default to UTF-8
        return 'utf-8'

    @abstractmethod
    def crawl(self, fields: List[Dict[str, str]]) -> CrawlResult:
        """
        Execute the crawl operation.

        Args:
            fields: List of fields to extract

        Returns:
            CrawlResult with extracted data
        """
        pass

    def execute(self, fields: List[Dict[str, str]]) -> CrawlResult:
        """
        Execute crawl with timing and error handling.

        Args:
            fields: List of fields to extract

        Returns:
            CrawlResult with execution details
        """
        self._start_timer()

        try:
            result = self.crawl(fields)
            result.execution_time_ms = self._get_elapsed_ms()
            return result

        except requests.Timeout as e:
            logger.error(f"Timeout crawling {self.url}: {e}")
            return CrawlResult(
                success=False,
                error_code='E001',
                error_message=str(e),
                execution_time_ms=self._get_elapsed_ms()
            )

        except requests.ConnectionError as e:
            logger.error(f"Connection error crawling {self.url}: {e}")
            return CrawlResult(
                success=False,
                error_code='E007',
                error_message=str(e),
                execution_time_ms=self._get_elapsed_ms()
            )

        except requests.HTTPError as e:
            logger.error(f"HTTP error crawling {self.url}: {e}")
            status_code = e.response.status_code if e.response else None

            if status_code in [401, 403]:
                error_code = 'E003'
            elif status_code == 429:
                error_code = 'E005'
            elif status_code and status_code >= 500:
                error_code = 'E008'
            else:
                error_code = 'E010'

            return CrawlResult(
                success=False,
                error_code=error_code,
                error_message=str(e),
                execution_time_ms=self._get_elapsed_ms()
            )

        except Exception as e:
            logger.error(f"Unexpected error crawling {self.url}: {e}")
            return CrawlResult(
                success=False,
                error_code='E010',
                error_message=str(e),
                execution_time_ms=self._get_elapsed_ms()
            )

        finally:
            self.close()

    @staticmethod
    def clean_text(text: Optional[str]) -> str:
        """
        Clean extracted text.

        Args:
            text: Text to clean

        Returns:
            Cleaned text
        """
        if not text:
            return ''

        # Remove extra whitespace
        import re
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    @staticmethod
    def parse_number(value: str) -> Optional[float]:
        """
        Parse number from string.

        Args:
            value: String value to parse

        Returns:
            Parsed number or None
        """
        if not value:
            return None

        import re
        # Remove currency symbols, commas, spaces
        cleaned = re.sub(r'[^\d.-]', '', value)

        try:
            return float(cleaned)
        except ValueError:
            return None

    @staticmethod
    def parse_date(value: str, formats: Optional[List[str]] = None) -> Optional[datetime]:
        """
        Parse date from string.

        Args:
            value: String value to parse
            formats: List of date formats to try

        Returns:
            Parsed datetime or None
        """
        if not value:
            return None

        if formats is None:
            formats = [
                '%Y-%m-%d',
                '%Y/%m/%d',
                '%Y.%m.%d',
                '%d-%m-%Y',
                '%d/%m/%Y',
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d %H:%M:%S',
                '%Y년 %m월 %d일',
                '%Y년%m월%d일'
            ]

        value = value.strip()

        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

        return None

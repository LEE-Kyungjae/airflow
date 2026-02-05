"""
Crawler modules for different data types.

This package contains base and specialized crawler classes
for HTML, PDF, Excel, CSV, OCR-based image data sources,
and JavaScript-rendered pages using Playwright.
"""

from .base_crawler import BaseCrawler, CrawlResult
from .html_crawler import HTMLCrawler
from .pdf_crawler import PDFCrawler
from .excel_crawler import ExcelCrawler
from .csv_crawler import CSVCrawler
from .ocr_crawler import OCRCrawler, NewsImageCrawler, TableImageCrawler

# Playwright-based crawlers (async)
from .playwright_crawler import (
    PlaywrightCrawler,
    PlaywrightConfig,
    ElementInfo,
    create_playwright_crawler
)
from .spa_crawler import (
    SPACrawler,
    SPAConfig,
    SPAFramework,
    SPAState
)
from .dynamic_table_crawler import (
    DynamicTableCrawler,
    DynamicTableConfig,
    TableLibrary,
    TableMetadata
)

# Authentication module (optional - requires cryptography)
try:
    from .auth import (
        AuthCredentials,
        SessionState,
        SessionManager,
        AuthType,
        AuthenticatedCrawler,
        PlaywrightConfig as AuthPlaywrightConfig
    )
    _AUTH_AVAILABLE = True
except ImportError:
    _AUTH_AVAILABLE = False
    AuthCredentials = None
    SessionState = None
    SessionManager = None
    AuthType = None
    AuthenticatedCrawler = None
    AuthPlaywrightConfig = None

__all__ = [
    # Base
    'BaseCrawler',
    'CrawlResult',

    # Traditional crawlers
    'HTMLCrawler',
    'PDFCrawler',
    'ExcelCrawler',
    'CSVCrawler',
    'OCRCrawler',
    'NewsImageCrawler',
    'TableImageCrawler',

    # Playwright crawlers
    'PlaywrightCrawler',
    'PlaywrightConfig',
    'ElementInfo',
    'create_playwright_crawler',

    # SPA crawler
    'SPACrawler',
    'SPAConfig',
    'SPAFramework',
    'SPAState',

    # Dynamic table crawler
    'DynamicTableCrawler',
    'DynamicTableConfig',
    'TableLibrary',
    'TableMetadata',

    # Authentication (when available)
    'AuthCredentials',
    'SessionState',
    'SessionManager',
    'AuthType',
    'AuthenticatedCrawler',
    'AuthPlaywrightConfig',
]

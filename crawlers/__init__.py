"""
Crawler modules for different data types.

This package contains base and specialized crawler classes
for HTML, PDF, Excel, and CSV data sources.
"""

from .base_crawler import BaseCrawler, CrawlResult
from .html_crawler import HTMLCrawler
from .pdf_crawler import PDFCrawler
from .excel_crawler import ExcelCrawler
from .csv_crawler import CSVCrawler

__all__ = [
    'BaseCrawler',
    'CrawlResult',
    'HTMLCrawler',
    'PDFCrawler',
    'ExcelCrawler',
    'CSVCrawler'
]

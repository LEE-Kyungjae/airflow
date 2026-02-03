"""
Crawler modules for different data types.

This package contains base and specialized crawler classes
for HTML, PDF, Excel, CSV, and OCR-based image data sources.
"""

from .base_crawler import BaseCrawler, CrawlResult
from .html_crawler import HTMLCrawler
from .pdf_crawler import PDFCrawler
from .excel_crawler import ExcelCrawler
from .csv_crawler import CSVCrawler
from .ocr_crawler import OCRCrawler, NewsImageCrawler, TableImageCrawler

__all__ = [
    'BaseCrawler',
    'CrawlResult',
    'HTMLCrawler',
    'PDFCrawler',
    'ExcelCrawler',
    'CSVCrawler',
    'OCRCrawler',
    'NewsImageCrawler',
    'TableImageCrawler'
]

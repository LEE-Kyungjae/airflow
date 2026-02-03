"""
PDF Crawler for extracting data from PDF documents.

This module provides functionality to download and extract
text and tables from PDF files using pdfplumber.
"""

import io
import logging
import tempfile
from typing import Dict, Any, List, Optional

import pdfplumber

from .base_crawler import BaseCrawler, CrawlResult

logger = logging.getLogger(__name__)


class PDFCrawler(BaseCrawler):
    """Crawler for PDF documents."""

    def __init__(
        self,
        url: str,
        pages: Optional[List[int]] = None,
        extract_tables: bool = True,
        extract_text: bool = True,
        table_settings: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        """
        Initialize PDF crawler.

        Args:
            url: URL of PDF file or page with PDF links
            pages: Specific pages to extract (0-indexed), None for all
            extract_tables: Whether to extract tables
            extract_text: Whether to extract text
            table_settings: Custom settings for table extraction
            **kwargs: Additional arguments for BaseCrawler
        """
        super().__init__(url, **kwargs)
        self.pages = pages
        self.extract_tables = extract_tables
        self.extract_text = extract_text
        self.table_settings = table_settings or {}

    def crawl(self, fields: List[Dict[str, str]]) -> CrawlResult:
        """
        Crawl PDF and extract data.

        Args:
            fields: Field definitions (used for data mapping)

        Returns:
            CrawlResult with extracted data
        """
        try:
            # Download PDF
            response = self.fetch_url()
            pdf_content = io.BytesIO(response.content)

            # Extract data
            extracted_data = []

            with pdfplumber.open(pdf_content) as pdf:
                pages_to_process = self.pages or range(len(pdf.pages))

                for page_num in pages_to_process:
                    if page_num >= len(pdf.pages):
                        logger.warning(f"Page {page_num} out of range, skipping")
                        continue

                    page = pdf.pages[page_num]
                    page_data = {'page_number': page_num + 1}

                    # Extract tables
                    if self.extract_tables:
                        tables = page.extract_tables(self.table_settings)
                        if tables:
                            page_data['tables'] = self._process_tables(tables, fields)

                    # Extract text
                    if self.extract_text:
                        text = page.extract_text()
                        if text:
                            page_data['text'] = text.strip()

                            # Try to extract field values from text
                            field_values = self._extract_from_text(text, fields)
                            page_data.update(field_values)

                    extracted_data.append(page_data)

            # Flatten if single page and no pagination needed
            if len(extracted_data) == 1:
                extracted_data = extracted_data[0].get('tables', [extracted_data[0]])

            return CrawlResult(
                success=True,
                data=extracted_data if isinstance(extracted_data, list) else [extracted_data],
                record_count=len(extracted_data) if isinstance(extracted_data, list) else 1
            )

        except Exception as e:
            logger.error(f"Error crawling PDF {self.url}: {e}")
            return CrawlResult(
                success=False,
                error_code='E009',
                error_message=str(e)
            )

    def _process_tables(
        self,
        tables: List[List[List[str]]],
        fields: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """
        Process extracted tables into structured data.

        Args:
            tables: Raw table data from pdfplumber
            fields: Field definitions for mapping

        Returns:
            List of records
        """
        all_records = []

        for table in tables:
            if not table or len(table) < 2:
                continue

            # First row as header
            headers = [self._clean_header(h) for h in table[0] if h]

            if not headers:
                continue

            # Map headers to field names
            header_mapping = self._map_headers_to_fields(headers, fields)

            # Process data rows
            for row in table[1:]:
                if not row or all(not cell for cell in row):
                    continue

                record = {}
                for i, cell in enumerate(row):
                    if i < len(headers):
                        header = headers[i]
                        field_name = header_mapping.get(header, header)
                        field_def = next(
                            (f for f in fields if f['name'] == field_name),
                            {'data_type': 'string'}
                        )
                        record[field_name] = self._convert_value(
                            cell,
                            field_def.get('data_type', 'string')
                        )

                if record:
                    all_records.append(record)

        return all_records

    def _clean_header(self, header: Optional[str]) -> str:
        """Clean header text."""
        if not header:
            return ''
        return self.clean_text(header).lower().replace(' ', '_')

    def _map_headers_to_fields(
        self,
        headers: List[str],
        fields: List[Dict[str, str]]
    ) -> Dict[str, str]:
        """
        Map table headers to field names.

        Args:
            headers: Table headers
            fields: Field definitions

        Returns:
            Mapping of header to field name
        """
        mapping = {}

        for header in headers:
            # Direct match
            for field in fields:
                field_name = field['name'].lower().replace(' ', '_')
                if header == field_name:
                    mapping[header] = field['name']
                    break

                # Partial match
                if field_name in header or header in field_name:
                    mapping[header] = field['name']
                    break

            # Default to header as field name
            if header not in mapping:
                mapping[header] = header

        return mapping

    def _convert_value(self, value: Optional[str], data_type: str) -> Any:
        """
        Convert value to specified type.

        Args:
            value: Raw value
            data_type: Target data type

        Returns:
            Converted value
        """
        if not value:
            return None

        value = self.clean_text(value)

        if data_type == 'number':
            return self.parse_number(value)
        elif data_type == 'date':
            parsed = self.parse_date(value)
            return parsed.isoformat() if parsed else value
        else:
            return value

    def _extract_from_text(
        self,
        text: str,
        fields: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Extract field values from text using patterns.

        Args:
            text: Page text content
            fields: Field definitions

        Returns:
            Extracted values
        """
        import re

        extracted = {}

        for field in fields:
            pattern = field.get('pattern')
            if not pattern:
                continue

            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                if match.groups():
                    value = match.group(1)
                else:
                    value = match.group(0)

                extracted[field['name']] = self._convert_value(
                    value,
                    field.get('data_type', 'string')
                )

        return extracted


class PDFTableCrawler(PDFCrawler):
    """Convenience class for table-focused PDF extraction."""

    def __init__(self, url: str, **kwargs):
        """Initialize with table extraction enabled."""
        kwargs['extract_tables'] = True
        kwargs['extract_text'] = False
        super().__init__(url, **kwargs)


class PDFTextCrawler(PDFCrawler):
    """Convenience class for text-focused PDF extraction."""

    def __init__(self, url: str, **kwargs):
        """Initialize with text extraction enabled."""
        kwargs['extract_tables'] = False
        kwargs['extract_text'] = True
        super().__init__(url, **kwargs)

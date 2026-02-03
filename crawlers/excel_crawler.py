"""
Excel Crawler for extracting data from Excel files.

This module provides functionality to download and extract
data from Excel files (.xlsx, .xls) using pandas and openpyxl.
"""

import io
import logging
from typing import Dict, Any, List, Optional, Union

import pandas as pd

from .base_crawler import BaseCrawler, CrawlResult

logger = logging.getLogger(__name__)


class ExcelCrawler(BaseCrawler):
    """Crawler for Excel files."""

    def __init__(
        self,
        url: str,
        sheet_name: Optional[Union[str, int, List[Union[str, int]]]] = 0,
        header_row: Optional[int] = 0,
        skip_rows: Optional[int] = None,
        use_cols: Optional[List[Union[str, int]]] = None,
        dtype: Optional[Dict[str, str]] = None,
        **kwargs
    ):
        """
        Initialize Excel crawler.

        Args:
            url: URL of Excel file
            sheet_name: Sheet(s) to read (name, index, or list)
            header_row: Row number for column labels (0-indexed)
            skip_rows: Number of rows to skip at the start
            use_cols: Specific columns to read
            dtype: Column data types
            **kwargs: Additional arguments for BaseCrawler
        """
        super().__init__(url, **kwargs)
        self.sheet_name = sheet_name
        self.header_row = header_row
        self.skip_rows = skip_rows
        self.use_cols = use_cols
        self.dtype = dtype

    def crawl(self, fields: List[Dict[str, str]]) -> CrawlResult:
        """
        Crawl Excel file and extract data.

        Args:
            fields: Field definitions for column mapping

        Returns:
            CrawlResult with extracted data
        """
        try:
            # Download Excel file
            response = self.fetch_url()
            excel_content = io.BytesIO(response.content)

            # Determine engine based on file extension
            engine = 'openpyxl'
            if self.url.lower().endswith('.xls'):
                engine = 'xlrd'

            # Read Excel file
            df = pd.read_excel(
                excel_content,
                sheet_name=self.sheet_name,
                header=self.header_row,
                skiprows=self.skip_rows,
                usecols=self.use_cols,
                dtype=self.dtype,
                engine=engine
            )

            # Handle multiple sheets
            if isinstance(df, dict):
                # Multiple sheets returned as dict
                all_data = []
                for sheet_name, sheet_df in df.items():
                    records = self._process_dataframe(sheet_df, fields)
                    for record in records:
                        record['_sheet'] = sheet_name
                    all_data.extend(records)
                extracted_data = all_data
            else:
                extracted_data = self._process_dataframe(df, fields)

            return CrawlResult(
                success=True,
                data=extracted_data,
                record_count=len(extracted_data),
                metadata={
                    'columns': list(df.columns) if not isinstance(df, dict) else None,
                    'sheet_name': str(self.sheet_name)
                }
            )

        except Exception as e:
            logger.error(f"Error crawling Excel {self.url}: {e}")
            return CrawlResult(
                success=False,
                error_code='E009',
                error_message=str(e)
            )

    def _process_dataframe(
        self,
        df: pd.DataFrame,
        fields: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """
        Process DataFrame into list of records.

        Args:
            df: pandas DataFrame
            fields: Field definitions

        Returns:
            List of records
        """
        # Clean column names
        df.columns = [self._clean_column_name(col) for col in df.columns]

        # Map columns to fields
        column_mapping = self._map_columns_to_fields(df.columns.tolist(), fields)

        # Rename columns
        df = df.rename(columns=column_mapping)

        # Filter to only requested fields if specified
        if fields:
            field_names = [f['name'] for f in fields]
            existing_cols = [col for col in field_names if col in df.columns]
            if existing_cols:
                df = df[existing_cols]

        # Convert data types based on field definitions
        df = self._convert_data_types(df, fields)

        # Drop rows with all NaN values
        df = df.dropna(how='all')

        # Convert to records
        records = df.to_dict(orient='records')

        # Clean up NaN values
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None

        return records

    def _clean_column_name(self, col: Any) -> str:
        """Clean column name."""
        if pd.isna(col):
            return 'unnamed'
        return str(col).strip().lower().replace(' ', '_').replace('\n', '_')

    def _map_columns_to_fields(
        self,
        columns: List[str],
        fields: List[Dict[str, str]]
    ) -> Dict[str, str]:
        """
        Map DataFrame columns to field names.

        Args:
            columns: DataFrame column names
            fields: Field definitions

        Returns:
            Column to field name mapping
        """
        mapping = {}

        for col in columns:
            # Look for matching field
            for field in fields:
                field_name = field['name'].lower().replace(' ', '_')
                col_clean = col.lower().replace(' ', '_')

                # Direct match
                if col_clean == field_name:
                    mapping[col] = field['name']
                    break

                # Check selector (might contain column name/pattern)
                selector = field.get('selector', '').lower()
                if selector and (selector in col_clean or col_clean in selector):
                    mapping[col] = field['name']
                    break

                # Partial match
                if field_name in col_clean or col_clean in field_name:
                    mapping[col] = field['name']
                    break

        return mapping

    def _convert_data_types(
        self,
        df: pd.DataFrame,
        fields: List[Dict[str, str]]
    ) -> pd.DataFrame:
        """
        Convert column data types based on field definitions.

        Args:
            df: DataFrame to convert
            fields: Field definitions

        Returns:
            DataFrame with converted types
        """
        for field in fields:
            name = field['name']
            data_type = field.get('data_type', 'string')

            if name not in df.columns:
                continue

            try:
                if data_type == 'number':
                    df[name] = pd.to_numeric(df[name], errors='coerce')
                elif data_type == 'date':
                    df[name] = pd.to_datetime(df[name], errors='coerce')
                elif data_type == 'string':
                    df[name] = df[name].astype(str).replace('nan', None)
            except Exception as e:
                logger.warning(f"Could not convert column {name} to {data_type}: {e}")

        return df


class ExcelMultiSheetCrawler(ExcelCrawler):
    """Convenience class for reading all sheets."""

    def __init__(self, url: str, **kwargs):
        """Initialize to read all sheets."""
        kwargs['sheet_name'] = None  # Read all sheets
        super().__init__(url, **kwargs)

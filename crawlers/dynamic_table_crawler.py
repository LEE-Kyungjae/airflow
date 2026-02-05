"""
Dynamic Table Crawler for JavaScript-rendered tables.

Specialized crawler for dynamic tables like DataTables, AG Grid, Handsontable,
and other JavaScript table libraries that render data dynamically.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Callable, Tuple
from enum import Enum

from .playwright_crawler import PlaywrightCrawler, PlaywrightConfig, CrawlResult

logger = logging.getLogger(__name__)


class TableLibrary(str, Enum):
    """Supported table libraries."""
    DATATABLES = "datatables"
    AG_GRID = "ag_grid"
    HANDSONTABLE = "handsontable"
    TABULATOR = "tabulator"
    TANSTACK = "tanstack"  # TanStack Table (React Table v8)
    KENDO_GRID = "kendo_grid"
    MATERIAL_TABLE = "material_table"
    ANT_TABLE = "ant_table"  # Ant Design Table
    ELEMENT_TABLE = "element_table"  # Element Plus Table
    UNKNOWN = "unknown"


@dataclass
class DynamicTableConfig(PlaywrightConfig):
    """Configuration for dynamic table crawler."""

    # Table library detection
    table_library: Optional[TableLibrary] = None
    auto_detect_library: bool = True

    # Pagination settings
    pagination_enabled: bool = True
    max_pages: int = 50
    page_load_delay: int = 500  # ms

    # Row loading settings
    wait_for_rows: bool = True
    row_load_timeout: int = 10000  # ms
    min_rows_expected: int = 1

    # Page size settings
    change_page_size: bool = False
    preferred_page_size: int = 100

    # Sorting
    sort_before_extract: bool = False
    sort_column: Optional[str] = None
    sort_direction: str = "asc"

    # Filtering
    apply_filters: bool = False
    filters: Dict[str, Any] = field(default_factory=dict)

    # Virtual scrolling
    handle_virtual_scroll: bool = True
    virtual_scroll_buffer: int = 500  # pixels


@dataclass
class TableMetadata:
    """Metadata about detected table."""
    library: TableLibrary
    total_rows: int
    visible_rows: int
    total_pages: int
    current_page: int
    page_size: int
    columns: List[str]
    has_pagination: bool
    has_sorting: bool
    has_filtering: bool
    is_virtual_scroll: bool


class DynamicTableCrawler(PlaywrightCrawler):
    """
    Crawler specialized for dynamic JavaScript tables.

    Features:
    - Automatic table library detection
    - Multi-page data extraction
    - Page size manipulation
    - Sort and filter handling
    - Virtual/infinite scroll support
    - Column header detection
    - Cell data type inference
    """

    def __init__(self, config: Optional[DynamicTableConfig] = None):
        """
        Initialize dynamic table crawler.

        Args:
            config: Table-specific configuration
        """
        self.table_config = config or DynamicTableConfig()
        super().__init__(self.table_config)

        self._detected_library: Optional[TableLibrary] = None
        self._table_metadata: Optional[TableMetadata] = None

    @property
    def library(self) -> TableLibrary:
        """Get detected or configured table library."""
        return self._detected_library or self.table_config.table_library or TableLibrary.UNKNOWN

    async def detect_table_library(self, table_selector: str = "table") -> TableLibrary:
        """
        Detect the table library used on the page.

        Args:
            table_selector: Table element selector

        Returns:
            Detected TableLibrary enum value
        """
        self._ensure_page()

        detection_result = await self.evaluate(f"""
            () => {{
                // DataTables detection
                if (window.jQuery && window.jQuery.fn.DataTable) {{
                    return 'datatables';
                }}
                if (document.querySelector('.dataTables_wrapper')) {{
                    return 'datatables';
                }}

                // AG Grid detection
                if (window.agGrid || document.querySelector('.ag-root-wrapper')) {{
                    return 'ag_grid';
                }}

                // Handsontable detection
                if (window.Handsontable || document.querySelector('.handsontable')) {{
                    return 'handsontable';
                }}

                // Tabulator detection
                if (window.Tabulator || document.querySelector('.tabulator')) {{
                    return 'tabulator';
                }}

                // TanStack Table detection
                if (document.querySelector('[data-tanstack-table]')) {{
                    return 'tanstack';
                }}

                // Kendo Grid detection
                if (window.kendo && window.kendo.ui.Grid) {{
                    return 'kendo_grid';
                }}
                if (document.querySelector('.k-grid')) {{
                    return 'kendo_grid';
                }}

                // Material UI Table detection
                if (document.querySelector('.MuiTable-root') ||
                    document.querySelector('.MuiDataGrid-root')) {{
                    return 'material_table';
                }}

                // Ant Design Table detection
                if (document.querySelector('.ant-table')) {{
                    return 'ant_table';
                }}

                // Element Plus Table detection
                if (document.querySelector('.el-table')) {{
                    return 'element_table';
                }}

                return 'unknown';
            }}
        """)

        self._detected_library = TableLibrary(detection_result)
        logger.info(f"Detected table library: {self._detected_library}")
        return self._detected_library

    async def get_table_metadata(self, table_selector: str = "table") -> TableMetadata:
        """
        Get metadata about the table.

        Args:
            table_selector: Table element selector

        Returns:
            TableMetadata object
        """
        self._ensure_page()
        library = self.library

        if library == TableLibrary.DATATABLES:
            metadata = await self._get_datatables_metadata(table_selector)
        elif library == TableLibrary.AG_GRID:
            metadata = await self._get_ag_grid_metadata()
        elif library == TableLibrary.ANT_TABLE:
            metadata = await self._get_ant_table_metadata()
        elif library == TableLibrary.MATERIAL_TABLE:
            metadata = await self._get_material_table_metadata()
        else:
            metadata = await self._get_generic_table_metadata(table_selector)

        self._table_metadata = metadata
        return metadata

    async def _get_datatables_metadata(self, table_selector: str) -> TableMetadata:
        """Get DataTables metadata."""
        data = await self.evaluate(f"""
            () => {{
                const table = jQuery('{table_selector}').DataTable();
                if (!table) return null;

                const info = table.page.info();
                const columns = table.columns().header().toArray().map(h => h.innerText.trim());

                return {{
                    totalRows: info.recordsTotal,
                    visibleRows: info.recordsDisplay,
                    totalPages: info.pages,
                    currentPage: info.page + 1,
                    pageSize: info.length,
                    columns: columns,
                    hasPagination: info.pages > 1,
                    hasSorting: table.order().length > 0,
                    hasFiltering: table.search() !== ''
                }};
            }}
        """)

        if not data:
            return await self._get_generic_table_metadata(table_selector)

        return TableMetadata(
            library=TableLibrary.DATATABLES,
            total_rows=data["totalRows"],
            visible_rows=data["visibleRows"],
            total_pages=data["totalPages"],
            current_page=data["currentPage"],
            page_size=data["pageSize"],
            columns=data["columns"],
            has_pagination=data["hasPagination"],
            has_sorting=data["hasSorting"],
            has_filtering=data["hasFiltering"],
            is_virtual_scroll=False
        )

    async def _get_ag_grid_metadata(self) -> TableMetadata:
        """Get AG Grid metadata."""
        data = await self.evaluate("""
            () => {
                const gridElements = document.querySelectorAll('.ag-root-wrapper');
                if (gridElements.length === 0) return null;

                const gridElement = gridElements[0];

                // Try to get grid API
                let api = null;
                if (gridElement.__agComponent) {
                    api = gridElement.__agComponent.gridOptions?.api;
                }

                if (!api) {
                    // Fallback to DOM inspection
                    const headerCells = gridElement.querySelectorAll('.ag-header-cell-text');
                    const columns = Array.from(headerCells).map(h => h.innerText.trim());
                    const rowElements = gridElement.querySelectorAll('.ag-row');

                    return {
                        totalRows: rowElements.length,
                        visibleRows: rowElements.length,
                        totalPages: 1,
                        currentPage: 1,
                        pageSize: rowElements.length,
                        columns: columns,
                        hasPagination: !!gridElement.querySelector('.ag-paging-panel'),
                        hasSorting: !!gridElement.querySelector('.ag-sort-indicator-icon'),
                        hasFiltering: !!gridElement.querySelector('.ag-floating-filter'),
                        isVirtualScroll: !!gridElement.querySelector('.ag-body-viewport')
                    };
                }

                const rowCount = api.getDisplayedRowCount();
                const columns = api.getColumnDefs()?.map(c => c.headerName || c.field) || [];

                // Check pagination
                let totalPages = 1;
                let currentPage = 1;
                let pageSize = rowCount;

                const paginationInfo = api.paginationGetTotalPages?.();
                if (paginationInfo !== undefined) {
                    totalPages = paginationInfo;
                    currentPage = api.paginationGetCurrentPage() + 1;
                    pageSize = api.paginationGetPageSize();
                }

                return {
                    totalRows: api.getModel().getRowCount(),
                    visibleRows: rowCount,
                    totalPages: totalPages,
                    currentPage: currentPage,
                    pageSize: pageSize,
                    columns: columns,
                    hasPagination: totalPages > 1,
                    hasSorting: api.getSortModel()?.length > 0,
                    hasFiltering: api.getFilterModel() && Object.keys(api.getFilterModel()).length > 0,
                    isVirtualScroll: true
                };
            }
        """)

        if not data:
            return await self._get_generic_table_metadata(".ag-root-wrapper table")

        return TableMetadata(
            library=TableLibrary.AG_GRID,
            total_rows=data["totalRows"],
            visible_rows=data["visibleRows"],
            total_pages=data["totalPages"],
            current_page=data["currentPage"],
            page_size=data["pageSize"],
            columns=data["columns"],
            has_pagination=data["hasPagination"],
            has_sorting=data["hasSorting"],
            has_filtering=data["hasFiltering"],
            is_virtual_scroll=data["isVirtualScroll"]
        )

    async def _get_ant_table_metadata(self) -> TableMetadata:
        """Get Ant Design Table metadata."""
        data = await self.evaluate("""
            () => {
                const table = document.querySelector('.ant-table');
                if (!table) return null;

                const headerCells = table.querySelectorAll('.ant-table-thead th');
                const columns = Array.from(headerCells).map(h => h.innerText.trim());

                const rows = table.querySelectorAll('.ant-table-tbody tr.ant-table-row');
                const visibleRows = rows.length;

                // Check pagination
                const pagination = document.querySelector('.ant-pagination');
                let totalRows = visibleRows;
                let totalPages = 1;
                let currentPage = 1;
                let pageSize = visibleRows;

                if (pagination) {
                    const totalEl = pagination.querySelector('.ant-pagination-total-text');
                    if (totalEl) {
                        const match = totalEl.innerText.match(/\\d+/g);
                        if (match) totalRows = parseInt(match[match.length - 1]);
                    }

                    const activeEl = pagination.querySelector('.ant-pagination-item-active');
                    if (activeEl) currentPage = parseInt(activeEl.innerText);

                    const pageItems = pagination.querySelectorAll('.ant-pagination-item');
                    if (pageItems.length > 0) {
                        totalPages = Math.max(...Array.from(pageItems).map(p => parseInt(p.innerText) || 0));
                    }

                    const sizeChanger = pagination.querySelector('.ant-pagination-options-size-changer');
                    if (sizeChanger) {
                        const selected = sizeChanger.querySelector('.ant-select-selection-item');
                        if (selected) pageSize = parseInt(selected.innerText) || visibleRows;
                    }
                }

                return {
                    totalRows: totalRows,
                    visibleRows: visibleRows,
                    totalPages: totalPages,
                    currentPage: currentPage,
                    pageSize: pageSize,
                    columns: columns,
                    hasPagination: !!pagination,
                    hasSorting: !!table.querySelector('.ant-table-column-sort'),
                    hasFiltering: !!table.querySelector('.ant-table-filter-trigger'),
                    isVirtualScroll: !!table.querySelector('.ant-table-body-virtual')
                };
            }
        """)

        if not data:
            return await self._get_generic_table_metadata(".ant-table table")

        return TableMetadata(
            library=TableLibrary.ANT_TABLE,
            total_rows=data["totalRows"],
            visible_rows=data["visibleRows"],
            total_pages=data["totalPages"],
            current_page=data["currentPage"],
            page_size=data["pageSize"],
            columns=data["columns"],
            has_pagination=data["hasPagination"],
            has_sorting=data["hasSorting"],
            has_filtering=data["hasFiltering"],
            is_virtual_scroll=data["isVirtualScroll"]
        )

    async def _get_material_table_metadata(self) -> TableMetadata:
        """Get Material UI Table metadata."""
        data = await self.evaluate("""
            () => {
                // Check for MUI DataGrid first
                let table = document.querySelector('.MuiDataGrid-root');
                if (table) {
                    const headerCells = table.querySelectorAll('.MuiDataGrid-columnHeaderTitle');
                    const columns = Array.from(headerCells).map(h => h.innerText.trim());

                    const rows = table.querySelectorAll('.MuiDataGrid-row');
                    const visibleRows = rows.length;

                    // Check for pagination
                    const paginationEl = table.querySelector('.MuiTablePagination-displayedRows');
                    let totalRows = visibleRows;

                    if (paginationEl) {
                        const match = paginationEl.innerText.match(/of\\s+(\\d+)/);
                        if (match) totalRows = parseInt(match[1]);
                    }

                    return {
                        totalRows: totalRows,
                        visibleRows: visibleRows,
                        totalPages: Math.ceil(totalRows / visibleRows),
                        currentPage: 1,
                        pageSize: visibleRows,
                        columns: columns,
                        hasPagination: !!paginationEl,
                        hasSorting: !!table.querySelector('.MuiDataGrid-sortIcon'),
                        hasFiltering: !!table.querySelector('.MuiDataGrid-filterIcon'),
                        isVirtualScroll: !!table.querySelector('.MuiDataGrid-virtualScroller')
                    };
                }

                // Regular MUI Table
                table = document.querySelector('.MuiTable-root');
                if (!table) return null;

                const headerCells = table.querySelectorAll('thead th');
                const columns = Array.from(headerCells).map(h => h.innerText.trim());

                const rows = table.querySelectorAll('tbody tr');
                const visibleRows = rows.length;

                return {
                    totalRows: visibleRows,
                    visibleRows: visibleRows,
                    totalPages: 1,
                    currentPage: 1,
                    pageSize: visibleRows,
                    columns: columns,
                    hasPagination: !!document.querySelector('.MuiTablePagination-root'),
                    hasSorting: !!table.querySelector('.MuiTableSortLabel-root'),
                    hasFiltering: false,
                    isVirtualScroll: false
                };
            }
        """)

        if not data:
            return await self._get_generic_table_metadata(".MuiTable-root, .MuiDataGrid-root")

        return TableMetadata(
            library=TableLibrary.MATERIAL_TABLE,
            total_rows=data["totalRows"],
            visible_rows=data["visibleRows"],
            total_pages=data["totalPages"],
            current_page=data["currentPage"],
            page_size=data["pageSize"],
            columns=data["columns"],
            has_pagination=data["hasPagination"],
            has_sorting=data["hasSorting"],
            has_filtering=data["hasFiltering"],
            is_virtual_scroll=data["isVirtualScroll"]
        )

    async def _get_generic_table_metadata(self, table_selector: str) -> TableMetadata:
        """Get metadata for generic tables."""
        data = await self.evaluate(f"""
            () => {{
                const table = document.querySelector('{table_selector}');
                if (!table) return null;

                const headerCells = table.querySelectorAll('thead th, thead td');
                const columns = Array.from(headerCells).map(h => h.innerText.trim());

                const rows = table.querySelectorAll('tbody tr');
                const visibleRows = rows.length;

                return {{
                    totalRows: visibleRows,
                    visibleRows: visibleRows,
                    totalPages: 1,
                    currentPage: 1,
                    pageSize: visibleRows,
                    columns: columns.filter(c => c.length > 0),
                    hasPagination: false,
                    hasSorting: false,
                    hasFiltering: false,
                    isVirtualScroll: false
                }};
            }}
        """)

        if not data:
            return TableMetadata(
                library=TableLibrary.UNKNOWN,
                total_rows=0,
                visible_rows=0,
                total_pages=1,
                current_page=1,
                page_size=0,
                columns=[],
                has_pagination=False,
                has_sorting=False,
                has_filtering=False,
                is_virtual_scroll=False
            )

        return TableMetadata(
            library=TableLibrary.UNKNOWN,
            total_rows=data["totalRows"],
            visible_rows=data["visibleRows"],
            total_pages=data["totalPages"],
            current_page=data["currentPage"],
            page_size=data["pageSize"],
            columns=data["columns"],
            has_pagination=data["hasPagination"],
            has_sorting=data["hasSorting"],
            has_filtering=data["hasFiltering"],
            is_virtual_scroll=data["isVirtualScroll"]
        )

    async def wait_for_table_load(
        self,
        table_selector: str = "table",
        timeout: Optional[int] = None
    ) -> bool:
        """
        Wait for table to finish loading data.

        Args:
            table_selector: Table element selector
            timeout: Timeout in ms

        Returns:
            True if table loaded successfully
        """
        self._ensure_page()
        timeout = timeout or self.table_config.row_load_timeout
        library = self.library

        try:
            if library == TableLibrary.DATATABLES:
                await self._wait_for_datatables_load(table_selector, timeout)
            elif library == TableLibrary.AG_GRID:
                await self._wait_for_ag_grid_load(timeout)
            elif library == TableLibrary.ANT_TABLE:
                await self._wait_for_ant_table_load(timeout)
            else:
                await self._wait_for_generic_table_load(table_selector, timeout)

            return True

        except Exception as e:
            logger.warning(f"Table load wait failed: {e}")
            return False

    async def _wait_for_datatables_load(self, table_selector: str, timeout: int) -> None:
        """Wait for DataTables to finish loading."""
        await self.wait_for_function(
            f"""
            () => {{
                const table = document.querySelector('{table_selector}');
                if (!table) return false;

                // Check for processing indicator
                const processing = document.querySelector('.dataTables_processing');
                if (processing && processing.style.display !== 'none') return false;

                // Check for loading class
                if (table.classList.contains('dataTable') &&
                    !document.querySelector('.dataTables_empty')) {{
                    const rows = table.querySelectorAll('tbody tr');
                    return rows.length >= {self.table_config.min_rows_expected};
                }}

                return false;
            }}
            """,
            timeout=timeout
        )

    async def _wait_for_ag_grid_load(self, timeout: int) -> None:
        """Wait for AG Grid to finish loading."""
        await self.wait_for_function(
            f"""
            () => {{
                const grid = document.querySelector('.ag-root-wrapper');
                if (!grid) return false;

                // Check for loading overlay
                const loading = grid.querySelector('.ag-overlay-loading-center');
                if (loading) return false;

                // Check for no-rows overlay
                const noRows = grid.querySelector('.ag-overlay-no-rows-center');
                if (noRows) return true;  // No data is also "loaded"

                // Check for rows
                const rows = grid.querySelectorAll('.ag-row');
                return rows.length >= {self.table_config.min_rows_expected};
            }}
            """,
            timeout=timeout
        )

    async def _wait_for_ant_table_load(self, timeout: int) -> None:
        """Wait for Ant Design Table to finish loading."""
        await self.wait_for_function(
            f"""
            () => {{
                const table = document.querySelector('.ant-table');
                if (!table) return false;

                // Check for loading spinner
                const loading = document.querySelector('.ant-spin-spinning, .ant-table-loading');
                if (loading) return false;

                // Check for rows
                const rows = table.querySelectorAll('.ant-table-tbody tr.ant-table-row');
                return rows.length >= {self.table_config.min_rows_expected};
            }}
            """,
            timeout=timeout
        )

    async def _wait_for_generic_table_load(self, table_selector: str, timeout: int) -> None:
        """Wait for generic table to load."""
        await self.wait_for_function(
            f"""
            () => {{
                const table = document.querySelector('{table_selector}');
                if (!table) return false;

                const rows = table.querySelectorAll('tbody tr');
                return rows.length >= {self.table_config.min_rows_expected};
            }}
            """,
            timeout=timeout
        )

    async def change_page_size(
        self,
        size: int,
        table_selector: str = "table"
    ) -> bool:
        """
        Change the number of rows displayed per page.

        Args:
            size: New page size
            table_selector: Table element selector

        Returns:
            True if page size changed successfully
        """
        self._ensure_page()
        library = self.library

        try:
            if library == TableLibrary.DATATABLES:
                await self._change_datatables_page_size(table_selector, size)
            elif library == TableLibrary.AG_GRID:
                await self._change_ag_grid_page_size(size)
            elif library == TableLibrary.ANT_TABLE:
                await self._change_ant_table_page_size(size)
            else:
                # Try generic select approach
                await self._change_generic_page_size(size)

            # Wait for table to reload
            await self.wait(self.table_config.page_load_delay)
            await self.wait_for_table_load(table_selector)

            return True

        except Exception as e:
            logger.warning(f"Failed to change page size: {e}")
            return False

    async def _change_datatables_page_size(self, table_selector: str, size: int) -> None:
        """Change DataTables page size."""
        await self.evaluate(f"""
            () => {{
                const table = jQuery('{table_selector}').DataTable();
                table.page.len({size}).draw();
            }}
        """)

    async def _change_ag_grid_page_size(self, size: int) -> None:
        """Change AG Grid page size."""
        # Try page size selector first
        page_size_selector = ".ag-paging-page-size select, .ag-page-size select"

        if await self.is_visible(page_size_selector):
            await self.select(page_size_selector, value=str(size))
        else:
            # Try API approach
            await self.evaluate(f"""
                () => {{
                    const gridElements = document.querySelectorAll('.ag-root-wrapper');
                    for (const el of gridElements) {{
                        if (el.__agComponent && el.__agComponent.gridOptions?.api) {{
                            el.__agComponent.gridOptions.api.paginationSetPageSize({size});
                        }}
                    }}
                }}
            """)

    async def _change_ant_table_page_size(self, size: int) -> None:
        """Change Ant Design Table page size."""
        # Click page size changer
        changer_selector = ".ant-pagination-options-size-changer"

        if await self.is_visible(changer_selector):
            await self.click(changer_selector)
            await self.wait(200)

            # Select the size option
            option_selector = f".ant-select-item-option[title*='{size}']"
            if await self.is_visible(option_selector):
                await self.click(option_selector)

    async def _change_generic_page_size(self, size: int) -> None:
        """Change page size using generic approach."""
        # Common page size selectors
        selectors = [
            "select[name='length']",
            "select.page-size",
            "select[data-page-size]",
            ".page-size-selector select",
            "[data-testid='page-size'] select"
        ]

        for selector in selectors:
            if await self.is_visible(selector):
                await self.select(selector, value=str(size))
                return

        logger.warning("Could not find page size selector")

    async def extract_current_page(
        self,
        table_selector: str = "table"
    ) -> List[Dict[str, Any]]:
        """
        Extract data from the current page of the table.

        Args:
            table_selector: Table element selector

        Returns:
            List of row data dictionaries
        """
        self._ensure_page()
        library = self.library

        if library == TableLibrary.DATATABLES:
            return await self._extract_datatables_page(table_selector)
        elif library == TableLibrary.AG_GRID:
            return await self._extract_ag_grid_page()
        elif library == TableLibrary.ANT_TABLE:
            return await self._extract_ant_table_page()
        elif library == TableLibrary.MATERIAL_TABLE:
            return await self._extract_material_table_page()
        else:
            return await self.extract_table(table_selector)

    async def _extract_datatables_page(self, table_selector: str) -> List[Dict[str, Any]]:
        """Extract data from DataTables."""
        return await self.evaluate(f"""
            () => {{
                const table = jQuery('{table_selector}').DataTable();
                const headers = table.columns().header().toArray().map(h => h.innerText.trim());
                const data = table.rows({{page: 'current'}}).data().toArray();

                return data.map(row => {{
                    const record = {{}};
                    for (let i = 0; i < headers.length; i++) {{
                        // Handle array or object row data
                        const value = Array.isArray(row) ? row[i] : row[headers[i]];
                        record[headers[i]] = typeof value === 'string' ? value.trim() : value;
                    }}
                    return record;
                }});
            }}
        """)

    async def _extract_ag_grid_page(self) -> List[Dict[str, Any]]:
        """Extract data from AG Grid."""
        return await self.evaluate("""
            () => {
                const grid = document.querySelector('.ag-root-wrapper');
                if (!grid) return [];

                // Get headers
                const headerCells = grid.querySelectorAll('.ag-header-cell');
                const columns = [];
                const colIds = [];

                headerCells.forEach(cell => {
                    const text = cell.querySelector('.ag-header-cell-text');
                    if (text) {
                        columns.push(text.innerText.trim());
                        colIds.push(cell.getAttribute('col-id') || text.innerText.trim());
                    }
                });

                // Get rows
                const rows = grid.querySelectorAll('.ag-row');
                const data = [];

                rows.forEach(row => {
                    const record = {};
                    const cells = row.querySelectorAll('.ag-cell');

                    cells.forEach((cell, idx) => {
                        const colId = cell.getAttribute('col-id') || colIds[idx] || `col_${idx}`;
                        const header = columns[colIds.indexOf(colId)] || colId;

                        // Try to get cell value
                        const cellValue = cell.querySelector('.ag-cell-value');
                        record[header] = cellValue ? cellValue.innerText.trim() : cell.innerText.trim();
                    });

                    if (Object.keys(record).length > 0) {
                        data.push(record);
                    }
                });

                return data;
            }
        """)

    async def _extract_ant_table_page(self) -> List[Dict[str, Any]]:
        """Extract data from Ant Design Table."""
        return await self.evaluate("""
            () => {
                const table = document.querySelector('.ant-table');
                if (!table) return [];

                // Get headers
                const headerCells = table.querySelectorAll('.ant-table-thead th');
                const headers = Array.from(headerCells).map(h => h.innerText.trim());

                // Get rows
                const rows = table.querySelectorAll('.ant-table-tbody tr.ant-table-row');
                const data = [];

                rows.forEach(row => {
                    const cells = row.querySelectorAll('td');
                    const record = {};

                    cells.forEach((cell, idx) => {
                        if (idx < headers.length) {
                            record[headers[idx]] = cell.innerText.trim();
                        }
                    });

                    if (Object.keys(record).length > 0) {
                        data.push(record);
                    }
                });

                return data;
            }
        """)

    async def _extract_material_table_page(self) -> List[Dict[str, Any]]:
        """Extract data from Material UI Table."""
        return await self.evaluate("""
            () => {
                // Check for DataGrid first
                let grid = document.querySelector('.MuiDataGrid-root');
                if (grid) {
                    const headerCells = grid.querySelectorAll('.MuiDataGrid-columnHeaderTitle');
                    const headers = Array.from(headerCells).map(h => h.innerText.trim());

                    const rows = grid.querySelectorAll('.MuiDataGrid-row');
                    const data = [];

                    rows.forEach(row => {
                        const cells = row.querySelectorAll('.MuiDataGrid-cell');
                        const record = {};

                        cells.forEach((cell, idx) => {
                            if (idx < headers.length) {
                                record[headers[idx]] = cell.innerText.trim();
                            }
                        });

                        if (Object.keys(record).length > 0) {
                            data.push(record);
                        }
                    });

                    return data;
                }

                // Regular MUI Table
                const table = document.querySelector('.MuiTable-root');
                if (!table) return [];

                const headerCells = table.querySelectorAll('thead th');
                const headers = Array.from(headerCells).map(h => h.innerText.trim());

                const rows = table.querySelectorAll('tbody tr');
                const data = [];

                rows.forEach(row => {
                    const cells = row.querySelectorAll('td');
                    const record = {};

                    cells.forEach((cell, idx) => {
                        if (idx < headers.length) {
                            record[headers[idx]] = cell.innerText.trim();
                        }
                    });

                    if (Object.keys(record).length > 0) {
                        data.push(record);
                    }
                });

                return data;
            }
        """)

    async def go_to_next_page(self, table_selector: str = "table") -> bool:
        """
        Navigate to the next page.

        Args:
            table_selector: Table element selector

        Returns:
            True if navigation successful
        """
        self._ensure_page()
        library = self.library

        try:
            if library == TableLibrary.DATATABLES:
                result = await self._datatables_next_page(table_selector)
            elif library == TableLibrary.AG_GRID:
                result = await self._ag_grid_next_page()
            elif library == TableLibrary.ANT_TABLE:
                result = await self._ant_table_next_page()
            else:
                result = await self._generic_next_page()

            if result:
                await self.wait(self.table_config.page_load_delay)
                await self.wait_for_table_load(table_selector)

            return result

        except Exception as e:
            logger.warning(f"Failed to go to next page: {e}")
            return False

    async def _datatables_next_page(self, table_selector: str) -> bool:
        """Go to next page in DataTables."""
        return await self.evaluate(f"""
            () => {{
                const table = jQuery('{table_selector}').DataTable();
                const info = table.page.info();

                if (info.page < info.pages - 1) {{
                    table.page('next').draw('page');
                    return true;
                }}
                return false;
            }}
        """)

    async def _ag_grid_next_page(self) -> bool:
        """Go to next page in AG Grid."""
        next_btn = ".ag-paging-button[ref='btNext'], .ag-icon-next"

        if await self.is_visible(next_btn):
            is_disabled = await self.evaluate(f"""
                () => {{
                    const btn = document.querySelector("{next_btn}");
                    return btn && (btn.disabled || btn.classList.contains('ag-disabled'));
                }}
            """)

            if not is_disabled:
                await self.click(next_btn)
                return True

        return False

    async def _ant_table_next_page(self) -> bool:
        """Go to next page in Ant Design Table."""
        next_btn = ".ant-pagination-next:not(.ant-pagination-disabled)"

        if await self.is_visible(next_btn):
            await self.click(next_btn)
            return True

        return False

    async def _generic_next_page(self) -> bool:
        """Go to next page using generic approach."""
        # Common next button selectors
        selectors = [
            "button.next:not([disabled])",
            "a.next:not(.disabled)",
            ".pagination-next:not(.disabled)",
            "[data-testid='next-page']:not([disabled])",
            ".page-item.next:not(.disabled) a",
            "button[aria-label='Next page']:not([disabled])",
            "[aria-label='Go to next page']:not([disabled])"
        ]

        for selector in selectors:
            if await self.is_visible(selector):
                await self.click(selector)
                return True

        return False

    async def extract_all_pages(
        self,
        table_selector: str = "table",
        max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract data from all pages of the table.

        Args:
            table_selector: Table element selector
            max_pages: Maximum number of pages to extract

        Returns:
            List of all row data dictionaries
        """
        self._ensure_page()
        all_data = []
        max_pages = max_pages or self.table_config.max_pages
        page_num = 1

        # Optionally change page size first
        if self.table_config.change_page_size:
            await self.change_page_size(
                self.table_config.preferred_page_size,
                table_selector
            )

        while page_num <= max_pages:
            logger.info(f"Extracting page {page_num}")

            # Wait for table to load
            await self.wait_for_table_load(table_selector)

            # Extract current page
            page_data = await self.extract_current_page(table_selector)
            all_data.extend(page_data)

            logger.info(f"Extracted {len(page_data)} rows from page {page_num}, total: {len(all_data)}")

            # Try to go to next page
            if not await self.go_to_next_page(table_selector):
                logger.info(f"No more pages after page {page_num}")
                break

            page_num += 1

        return all_data

    async def extract_with_virtual_scroll(
        self,
        table_selector: str = "table",
        max_rows: int = 1000,
        scroll_container_selector: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract data from a table with virtual scrolling.

        Args:
            table_selector: Table element selector
            max_rows: Maximum rows to extract
            scroll_container_selector: Scroll container selector

        Returns:
            List of extracted row data
        """
        self._ensure_page()
        library = self.library

        # Determine scroll container
        if not scroll_container_selector:
            if library == TableLibrary.AG_GRID:
                scroll_container_selector = ".ag-body-viewport"
            elif library == TableLibrary.MATERIAL_TABLE:
                scroll_container_selector = ".MuiDataGrid-virtualScroller"
            else:
                scroll_container_selector = f"{table_selector} tbody"

        seen_rows = {}
        all_data = []

        while len(all_data) < max_rows:
            # Extract visible rows
            current_data = await self.extract_current_page(table_selector)

            new_count = 0
            for row in current_data:
                # Create unique key for deduplication
                row_key = hash(str(sorted(row.items())))

                if row_key not in seen_rows:
                    seen_rows[row_key] = True
                    all_data.append(row)
                    new_count += 1

                    if len(all_data) >= max_rows:
                        break

            logger.info(f"Found {new_count} new rows, total: {len(all_data)}")

            if new_count == 0:
                # No new data, we've reached the end
                break

            # Scroll down
            await self.evaluate(f"""
                () => {{
                    const container = document.querySelector('{scroll_container_selector}');
                    if (container) {{
                        container.scrollTop += {self.table_config.virtual_scroll_buffer};
                    }}
                }}
            """)

            await self.wait(500)

        return all_data[:max_rows]

    async def sort_by_column(
        self,
        column_name: str,
        direction: str = "asc",
        table_selector: str = "table"
    ) -> bool:
        """
        Sort table by column.

        Args:
            column_name: Column header text
            direction: asc or desc
            table_selector: Table element selector

        Returns:
            True if sorting successful
        """
        self._ensure_page()
        library = self.library

        try:
            if library == TableLibrary.DATATABLES:
                await self._datatables_sort(table_selector, column_name, direction)
            elif library == TableLibrary.AG_GRID:
                await self._ag_grid_sort(column_name, direction)
            else:
                await self._generic_sort(column_name, direction)

            await self.wait(self.table_config.page_load_delay)
            await self.wait_for_table_load(table_selector)

            return True

        except Exception as e:
            logger.warning(f"Failed to sort by column: {e}")
            return False

    async def _datatables_sort(self, table_selector: str, column_name: str, direction: str) -> None:
        """Sort DataTables by column."""
        await self.evaluate(f"""
            () => {{
                const table = jQuery('{table_selector}').DataTable();
                const headers = table.columns().header().toArray();
                const colIdx = headers.findIndex(h => h.innerText.trim() === '{column_name}');

                if (colIdx >= 0) {{
                    table.order([colIdx, '{direction}']).draw();
                }}
            }}
        """)

    async def _ag_grid_sort(self, column_name: str, direction: str) -> None:
        """Sort AG Grid by column."""
        # Click on column header
        header_selector = f".ag-header-cell-text:has-text('{column_name}')"

        # Determine number of clicks needed
        clicks = 1 if direction == "asc" else 2

        for _ in range(clicks):
            await self.click(f".ag-header-cell:has({header_selector})")
            await self.wait(200)

    async def _generic_sort(self, column_name: str, direction: str) -> None:
        """Sort using generic approach."""
        # Click on sortable header
        header_selector = f"th:has-text('{column_name}')"

        if await self.is_visible(header_selector):
            # Determine number of clicks
            clicks = 1 if direction == "asc" else 2

            for _ in range(clicks):
                await self.click(header_selector)
                await self.wait(300)

    async def crawl_table(
        self,
        url: str,
        table_selector: str = "table",
        extract_all: bool = True,
        wait_selector: Optional[str] = None
    ) -> CrawlResult:
        """
        Execute a complete table crawl operation.

        Args:
            url: Target URL
            table_selector: Table element selector
            extract_all: Extract all pages or just first page
            wait_selector: Selector to wait for

        Returns:
            CrawlResult with extracted data
        """
        import time
        start_time = time.time()

        try:
            # Navigate
            await self.navigate(url)

            # Detect library
            if self.table_config.auto_detect_library:
                await self.detect_table_library(table_selector)

            # Wait for table
            wait_sel = wait_selector or table_selector
            await self.wait_for_selector(wait_sel)
            await self.wait_for_table_load(table_selector)

            # Get metadata
            metadata = await self.get_table_metadata(table_selector)

            # Sort if configured
            if self.table_config.sort_before_extract and self.table_config.sort_column:
                await self.sort_by_column(
                    self.table_config.sort_column,
                    self.table_config.sort_direction,
                    table_selector
                )

            # Extract data
            if extract_all and metadata.has_pagination:
                data = await self.extract_all_pages(table_selector)
            elif extract_all and metadata.is_virtual_scroll:
                data = await self.extract_with_virtual_scroll(table_selector)
            else:
                data = await self.extract_current_page(table_selector)

            execution_time_ms = int((time.time() - start_time) * 1000)

            return CrawlResult(
                success=True,
                data=data,
                record_count=len(data),
                execution_time_ms=execution_time_ms,
                metadata={
                    "url": url,
                    "table_library": metadata.library.value,
                    "total_rows": metadata.total_rows,
                    "columns": metadata.columns,
                    "pages_extracted": metadata.total_pages if extract_all else 1,
                    "crawler_type": "dynamic_table"
                }
            )

        except Exception as e:
            logger.error(f"Table crawl failed: {e}")

            execution_time_ms = int((time.time() - start_time) * 1000)
            html_snapshot = None

            try:
                html_snapshot = await self.get_html()
                html_snapshot = html_snapshot[:5000]
            except Exception:
                pass

            return CrawlResult(
                success=False,
                error_code='E002',
                error_message=str(e),
                execution_time_ms=execution_time_ms,
                html_snapshot=html_snapshot
            )

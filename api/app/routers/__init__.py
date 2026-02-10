"""API Routers."""

from . import (
    sources, crawlers, errors, dashboard, quick_add, monitoring,
    reviews, data_quality, metrics, lineage, export, backup,
    contracts, schemas, catalog, versions,
)

try:
    from . import auth
except ImportError:
    auth = None

try:
    from . import auth_config
except ImportError:
    auth_config = None

__all__ = [
    'sources',
    'crawlers',
    'errors',
    'dashboard',
    'quick_add',
    'monitoring',
    'reviews',
    'data_quality',
    'metrics',
    'lineage',
    'export',
    'backup',
    'contracts',
    'schemas',
    'catalog',
    'versions',
    'auth',
    'auth_config',
]

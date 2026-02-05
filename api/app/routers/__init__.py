"""API Routers."""

from . import sources, crawlers, errors, dashboard, quick_add, monitoring, reviews

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
    'auth',
    'auth_config'
]

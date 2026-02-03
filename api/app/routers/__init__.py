"""API Routers."""

from . import sources, crawlers, errors, dashboard, quick_add, monitoring, reviews

try:
    from . import auth
except ImportError:
    auth = None

__all__ = ['sources', 'crawlers', 'errors', 'dashboard', 'quick_add', 'monitoring', 'reviews', 'auth']

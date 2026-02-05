"""
Authentication module for crawlers.

This package provides session management, credential storage,
and authenticated crawling capabilities for sites requiring login.
"""

from .session_manager import (
    AuthCredentials,
    SessionState,
    SessionManager,
    AuthType
)
from .auth_crawler import AuthenticatedCrawler, PlaywrightConfig

__all__ = [
    'AuthCredentials',
    'SessionState',
    'SessionManager',
    'AuthType',
    'AuthenticatedCrawler',
    'PlaywrightConfig'
]

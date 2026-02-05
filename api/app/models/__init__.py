"""Pydantic models for API schemas."""

from .schemas import (
    FieldDefinition,
    SourceCreate,
    SourceUpdate,
    SourceResponse,
    CrawlerResponse,
    CrawlerHistoryResponse,
    CrawlResultResponse,
    ErrorLogResponse,
    DashboardResponse,
    TriggerResponse,
    PaginatedResponse
)

from .auth_schemas import (
    AuthType,
    OAuthProvider,
    FormLoginSelectors,
    PreLoginAction,
    FormAuthConfig,
    OAuthConfig,
    ApiKeyConfig,
    BasicAuthConfig,
    CredentialsCreate,
    AuthConfigCreate,
    AuthConfigResponse,
    SessionStatusResponse,
    SessionRefreshRequest,
    SessionRefreshResponse,
    LoginTestRequest,
    LoginTestResponse,
    BulkSessionCleanupResponse
)

__all__ = [
    # Core schemas
    'FieldDefinition',
    'SourceCreate',
    'SourceUpdate',
    'SourceResponse',
    'CrawlerResponse',
    'CrawlerHistoryResponse',
    'CrawlResultResponse',
    'ErrorLogResponse',
    'DashboardResponse',
    'TriggerResponse',
    'PaginatedResponse',
    # Auth schemas
    'AuthType',
    'OAuthProvider',
    'FormLoginSelectors',
    'PreLoginAction',
    'FormAuthConfig',
    'OAuthConfig',
    'ApiKeyConfig',
    'BasicAuthConfig',
    'CredentialsCreate',
    'AuthConfigCreate',
    'AuthConfigResponse',
    'SessionStatusResponse',
    'SessionRefreshRequest',
    'SessionRefreshResponse',
    'LoginTestRequest',
    'LoginTestResponse',
    'BulkSessionCleanupResponse'
]

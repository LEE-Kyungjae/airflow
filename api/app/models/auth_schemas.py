"""
Pydantic schemas for authentication configuration API.

These models define the structure for auth credentials, session states,
and related API request/response payloads.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from enum import Enum


class AuthType(str, Enum):
    """Supported authentication types."""
    FORM = "form"
    OAUTH = "oauth"
    API_KEY = "api_key"
    COOKIE = "cookie"
    BASIC = "basic"
    BEARER = "bearer"
    CUSTOM = "custom"


class OAuthProvider(str, Enum):
    """Supported OAuth providers."""
    GOOGLE = "google"
    GITHUB = "github"
    KAKAO = "kakao"
    NAVER = "naver"


# ============================================================
# Form Login Configuration
# ============================================================

class FormLoginSelectors(BaseModel):
    """
    CSS selectors for form-based login.

    Defines the DOM elements needed for automated form login.
    """
    username: str = Field(
        ...,
        description="CSS selector for username/email input field",
        examples=["#email", "input[name='username']", ".login-email"]
    )
    password: str = Field(
        ...,
        description="CSS selector for password input field",
        examples=["#password", "input[type='password']"]
    )
    submit: str = Field(
        ...,
        description="CSS selector for submit button",
        examples=["button[type='submit']", "#login-btn", ".btn-login"]
    )
    success_indicator: Optional[str] = Field(
        None,
        description="CSS selector that appears on successful login",
        examples=[".user-profile", "#dashboard", ".logout-btn"]
    )
    failure_indicator: Optional[str] = Field(
        None,
        description="CSS selector that appears on failed login",
        examples=[".error-message", "#login-error", ".alert-danger"]
    )
    captcha_indicator: Optional[str] = Field(
        None,
        description="CSS selector for CAPTCHA element if present",
        examples=[".g-recaptcha", "#captcha", "iframe[src*='recaptcha']"]
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "username": "#email",
                "password": "#password",
                "submit": "button[type='submit']",
                "success_indicator": ".user-dashboard",
                "failure_indicator": ".login-error"
            }
        }
    }


class PreLoginAction(BaseModel):
    """
    Action to perform before login form submission.

    Used for sites that require clicking buttons to show login form,
    accepting cookies, or other pre-login interactions.
    """
    type: str = Field(
        ...,
        pattern="^(click|fill|wait|wait_for_selector)$",
        description="Action type: click, fill, wait, or wait_for_selector"
    )
    selector: Optional[str] = Field(
        None,
        description="CSS selector for the target element"
    )
    value: Optional[str] = Field(
        None,
        description="Value for fill action"
    )
    wait: int = Field(
        500,
        ge=0,
        le=30000,
        description="Milliseconds to wait after action"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"type": "click", "selector": ".cookie-accept", "wait": 500},
                {"type": "wait_for_selector", "selector": "#login-form", "wait": 1000}
            ]
        }
    }


# ============================================================
# Auth Configuration Models
# ============================================================

class FormAuthConfig(BaseModel):
    """Configuration for form-based authentication."""
    login_url: str = Field(
        ...,
        description="URL of the login page"
    )
    selectors: FormLoginSelectors = Field(
        ...,
        description="CSS selectors for login form elements"
    )
    pre_login_actions: Optional[List[PreLoginAction]] = Field(
        None,
        description="Actions to perform before filling login form"
    )
    wait_after_submit: int = Field(
        3000,
        ge=1000,
        le=30000,
        description="Milliseconds to wait after form submission"
    )


class OAuthConfig(BaseModel):
    """Configuration for OAuth-based authentication."""
    provider: OAuthProvider = Field(
        ...,
        description="OAuth provider name"
    )
    client_id: Optional[str] = Field(
        None,
        description="OAuth client ID (if using authorization flow)"
    )
    callback_url: Optional[str] = Field(
        None,
        description="OAuth callback URL"
    )
    scope: Optional[str] = Field(
        None,
        description="OAuth scope"
    )


class ApiKeyConfig(BaseModel):
    """Configuration for API key authentication."""
    header_name: str = Field(
        "Authorization",
        description="HTTP header name for API key"
    )
    header_prefix: str = Field(
        "Bearer ",
        description="Prefix before the API key value"
    )


class BasicAuthConfig(BaseModel):
    """Configuration for HTTP Basic authentication."""
    realm: Optional[str] = Field(
        None,
        description="Authentication realm (optional)"
    )


# ============================================================
# Credentials Models
# ============================================================

class CredentialsCreate(BaseModel):
    """
    Request model for storing authentication credentials.

    Sensitive fields (password, api_key) should be encrypted
    client-side before transmission in production environments.
    """
    username: Optional[str] = Field(
        None,
        description="Username or email for authentication"
    )
    password: Optional[str] = Field(
        None,
        description="Password (will be encrypted server-side)"
    )
    api_key: Optional[str] = Field(
        None,
        description="API key or token (will be encrypted server-side)"
    )
    oauth_token: Optional[str] = Field(
        None,
        description="OAuth access token"
    )
    oauth_refresh_token: Optional[str] = Field(
        None,
        description="OAuth refresh token"
    )
    cookies: Optional[Dict[str, Any]] = Field(
        None,
        description="Pre-authenticated cookies"
    )
    headers: Optional[Dict[str, str]] = Field(
        None,
        description="Custom HTTP headers"
    )
    custom_data: Optional[Dict[str, Any]] = Field(
        None,
        description="Provider-specific custom data"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "username": "user@example.com",
                    "password": "secure_password"
                },
                {
                    "api_key": "sk-1234567890abcdef"
                }
            ]
        }
    }


# ============================================================
# Auth Config Request/Response Models
# ============================================================

class AuthConfigCreate(BaseModel):
    """
    Request model for creating/updating source auth configuration.

    Combines auth type settings, selectors/configuration, and credentials.
    """
    auth_type: AuthType = Field(
        ...,
        description="Type of authentication"
    )
    form_config: Optional[FormAuthConfig] = Field(
        None,
        description="Form login configuration (required for auth_type=form)"
    )
    oauth_config: Optional[OAuthConfig] = Field(
        None,
        description="OAuth configuration (required for auth_type=oauth)"
    )
    api_key_config: Optional[ApiKeyConfig] = Field(
        None,
        description="API key configuration (required for auth_type=api_key)"
    )
    basic_config: Optional[BasicAuthConfig] = Field(
        None,
        description="Basic auth configuration (required for auth_type=basic)"
    )
    credentials: CredentialsCreate = Field(
        ...,
        description="Authentication credentials"
    )
    session_duration_hours: int = Field(
        24,
        ge=1,
        le=720,
        description="Session validity duration in hours (1-720)"
    )
    auto_refresh: bool = Field(
        True,
        description="Automatically refresh session before expiration"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "auth_type": "form",
                "form_config": {
                    "login_url": "https://example.com/login",
                    "selectors": {
                        "username": "#email",
                        "password": "#password",
                        "submit": "button[type=submit]",
                        "success_indicator": ".user-profile"
                    }
                },
                "credentials": {
                    "username": "user@example.com",
                    "password": "encrypted_password"
                },
                "session_duration_hours": 24,
                "auto_refresh": True
            }
        }
    }


class AuthConfigResponse(BaseModel):
    """
    Response model for auth configuration.

    Note: Sensitive credential data is NOT included in responses.
    """
    id: str = Field(
        ...,
        alias="_id",
        description="Configuration record ID"
    )
    source_id: str = Field(
        ...,
        description="Associated source ID"
    )
    auth_type: AuthType = Field(
        ...,
        description="Type of authentication"
    )
    form_config: Optional[FormAuthConfig] = Field(
        None,
        description="Form login configuration"
    )
    oauth_config: Optional[OAuthConfig] = Field(
        None,
        description="OAuth configuration"
    )
    api_key_config: Optional[ApiKeyConfig] = Field(
        None,
        description="API key configuration"
    )
    basic_config: Optional[BasicAuthConfig] = Field(
        None,
        description="Basic auth configuration"
    )
    session_duration_hours: int = Field(
        ...,
        description="Session validity duration"
    )
    auto_refresh: bool = Field(
        ...,
        description="Auto refresh enabled"
    )
    has_credentials: bool = Field(
        ...,
        description="Whether credentials are configured"
    )
    created_at: datetime = Field(
        ...,
        description="Configuration creation timestamp"
    )
    updated_at: Optional[datetime] = Field(
        None,
        description="Last update timestamp"
    )

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "_id": "507f1f77bcf86cd799439011",
                "source_id": "507f1f77bcf86cd799439010",
                "auth_type": "form",
                "form_config": {
                    "login_url": "https://example.com/login",
                    "selectors": {
                        "username": "#email",
                        "password": "#password",
                        "submit": "button[type=submit]"
                    }
                },
                "session_duration_hours": 24,
                "auto_refresh": True,
                "has_credentials": True,
                "created_at": "2025-02-05T10:00:00Z"
            }
        }
    }


# ============================================================
# Session Status Models
# ============================================================

class SessionStatusResponse(BaseModel):
    """
    Response model for authentication/session status.

    Provides comprehensive status information without exposing credentials.
    """
    source_id: str = Field(
        ...,
        description="Source identifier"
    )
    auth_configured: bool = Field(
        ...,
        description="Whether auth is configured for this source"
    )
    auth_type: Optional[AuthType] = Field(
        None,
        description="Configured auth type"
    )
    session_valid: bool = Field(
        ...,
        description="Whether current session is valid"
    )
    session_expires_at: Optional[datetime] = Field(
        None,
        description="Session expiration timestamp"
    )
    last_login: Optional[datetime] = Field(
        None,
        description="Last successful login timestamp"
    )
    last_used: Optional[datetime] = Field(
        None,
        description="Last session usage timestamp"
    )
    cookies_count: int = Field(
        0,
        description="Number of stored cookies"
    )
    needs_refresh: bool = Field(
        False,
        description="Whether session needs refresh soon"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "source_id": "507f1f77bcf86cd799439010",
                "auth_configured": True,
                "auth_type": "form",
                "session_valid": True,
                "session_expires_at": "2025-02-06T10:00:00Z",
                "last_login": "2025-02-05T10:00:00Z",
                "last_used": "2025-02-05T14:30:00Z",
                "cookies_count": 5,
                "needs_refresh": False
            }
        }
    }


class SessionRefreshRequest(BaseModel):
    """Request model for manual session refresh."""
    force_relogin: bool = Field(
        False,
        description="Force re-login even if session is valid"
    )
    extend_hours: Optional[int] = Field(
        None,
        ge=1,
        le=720,
        description="Hours to extend session (uses default if not specified)"
    )


class SessionRefreshResponse(BaseModel):
    """Response model for session refresh operation."""
    success: bool = Field(
        ...,
        description="Whether refresh was successful"
    )
    message: str = Field(
        ...,
        description="Status message"
    )
    new_expiration: Optional[datetime] = Field(
        None,
        description="New session expiration timestamp"
    )
    login_performed: bool = Field(
        False,
        description="Whether a new login was performed"
    )


# ============================================================
# Login Test Models
# ============================================================

class LoginTestRequest(BaseModel):
    """Request model for testing login configuration."""
    timeout_seconds: int = Field(
        60,
        ge=10,
        le=300,
        description="Maximum time to wait for login (seconds)"
    )
    capture_screenshot: bool = Field(
        True,
        description="Capture screenshot after login attempt"
    )


class LoginTestResponse(BaseModel):
    """Response model for login test results."""
    success: bool = Field(
        ...,
        description="Whether login test was successful"
    )
    message: str = Field(
        ...,
        description="Detailed result message"
    )
    error_code: Optional[str] = Field(
        None,
        description="Error code if failed"
    )
    requires_captcha: bool = Field(
        False,
        description="Whether CAPTCHA was detected"
    )
    requires_2fa: bool = Field(
        False,
        description="Whether 2FA was required"
    )
    cookies_captured: int = Field(
        0,
        description="Number of cookies captured"
    )
    execution_time_ms: int = Field(
        0,
        description="Execution time in milliseconds"
    )
    screenshot_path: Optional[str] = Field(
        None,
        description="Path to captured screenshot"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "message": "Login successful",
                "cookies_captured": 5,
                "execution_time_ms": 2500
            }
        }
    }


# ============================================================
# Bulk Operations
# ============================================================

class BulkSessionCleanupResponse(BaseModel):
    """Response model for bulk session cleanup."""
    expired_sessions_removed: int = Field(
        ...,
        description="Number of expired sessions removed"
    )
    invalid_sessions_removed: int = Field(
        ...,
        description="Number of invalid sessions removed"
    )
    total_removed: int = Field(
        ...,
        description="Total sessions removed"
    )

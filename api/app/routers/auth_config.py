"""
Auth Configuration Router.

Endpoints for managing authentication configuration for crawling sources.
Handles credential storage, session management, and login operations.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, status, BackgroundTasks

from app.models.auth_schemas import (
    AuthType,
    AuthConfigCreate,
    AuthConfigResponse,
    SessionStatusResponse,
    SessionRefreshRequest,
    SessionRefreshResponse,
    LoginTestRequest,
    LoginTestResponse,
    BulkSessionCleanupResponse
)
from app.services.mongo_service import MongoService, validate_object_id
from app.auth.dependencies import require_auth, require_scope, AuthContext

logger = logging.getLogger(__name__)
router = APIRouter()


# ============================================================
# Error Response Definitions
# ============================================================

ERROR_RESPONSES = {
    404: {
        "description": "Source or configuration not found",
        "content": {
            "application/json": {
                "example": {"detail": "Source not found"}
            }
        }
    },
    409: {
        "description": "Configuration already exists",
        "content": {
            "application/json": {
                "example": {"detail": "Auth configuration already exists for this source"}
            }
        }
    },
    400: {
        "description": "Invalid configuration",
        "content": {
            "application/json": {
                "example": {"detail": "Form configuration required for auth_type=form"}
            }
        }
    },
    401: {
        "description": "Authentication required",
        "content": {
            "application/json": {
                "example": {"detail": "Not authenticated"}
            }
        }
    },
    403: {
        "description": "Insufficient permissions",
        "content": {
            "application/json": {
                "example": {"detail": "Insufficient scope. Required: write"}
            }
        }
    }
}


def get_mongo():
    """Dependency for MongoDB connection with automatic cleanup."""
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


def get_session_manager():
    """
    Dependency for SessionManager.

    Returns a configured SessionManager instance.
    """
    # Import here to avoid circular imports
    from crawlers.auth.session_manager import SessionManager

    encryption_key = os.getenv("SESSION_ENCRYPTION_KEY")
    storage_path = os.getenv("SESSION_STORAGE_PATH")

    return SessionManager(
        encryption_key=encryption_key,
        storage_path=storage_path
    )


# ============================================================
# Auth Configuration Endpoints
# ============================================================

@router.post(
    "/{source_id}/auth-config",
    response_model=AuthConfigResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Configure authentication for a source",
    description="""
Configure authentication settings for a crawling source.

**Supported auth types:**
- `form`: Form-based login with username/password
- `oauth`: OAuth provider authentication (Google, GitHub, Kakao, Naver)
- `api_key`: API key/token authentication
- `basic`: HTTP Basic authentication
- `bearer`: Bearer token authentication
- `cookie`: Pre-authenticated cookies

**Security notes:**
- Credentials are encrypted before storage
- Passwords are never returned in responses
- Session tokens are securely stored

**Required scope:** `write`
""",
    responses={
        201: {"description": "Auth configuration created successfully"},
        **ERROR_RESPONSES
    }
)
async def create_auth_config(
    source_id: str,
    config: AuthConfigCreate,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write"))
):
    """Create or update authentication configuration for a source."""
    # Validate source_id
    source_oid = validate_object_id(source_id, "source_id")

    # Check source exists
    source = mongo.db.sources.find_one({"_id": source_oid})
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source not found"
        )

    # Validate configuration based on auth type
    _validate_auth_config(config)

    # Check for existing config
    existing = mongo.db.auth_configs.find_one({"source_id": source_oid})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Auth configuration already exists. Use PUT to update."
        )

    # Build config document
    now = datetime.utcnow()
    config_doc = {
        "source_id": source_oid,
        "auth_type": config.auth_type.value,
        "session_duration_hours": config.session_duration_hours,
        "auto_refresh": config.auto_refresh,
        "created_at": now,
        "updated_at": now
    }

    # Add type-specific config
    if config.form_config:
        config_doc["form_config"] = config.form_config.model_dump()
    if config.oauth_config:
        config_doc["oauth_config"] = config.oauth_config.model_dump()
    if config.api_key_config:
        config_doc["api_key_config"] = config.api_key_config.model_dump()
    if config.basic_config:
        config_doc["basic_config"] = config.basic_config.model_dump()

    # Insert config
    result = mongo.db.auth_configs.insert_one(config_doc)
    config_doc["_id"] = result.inserted_id

    # Store credentials separately (encrypted)
    if config.credentials:
        await _store_credentials(
            source_id=str(source_oid),
            credentials=config.credentials,
            auth_type=config.auth_type
        )
        config_doc["has_credentials"] = True
    else:
        config_doc["has_credentials"] = False

    logger.info(f"Created auth config for source: {source_id}")

    # Convert ObjectIds to strings for response
    config_doc["_id"] = str(config_doc["_id"])
    config_doc["source_id"] = str(config_doc["source_id"])

    return AuthConfigResponse(**config_doc)


@router.put(
    "/{source_id}/auth-config",
    response_model=AuthConfigResponse,
    summary="Update authentication configuration",
    description="""
Update existing authentication configuration for a source.

**Required scope:** `write`
""",
    responses={
        200: {"description": "Auth configuration updated successfully"},
        **ERROR_RESPONSES
    }
)
async def update_auth_config(
    source_id: str,
    config: AuthConfigCreate,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write"))
):
    """Update authentication configuration for a source."""
    source_oid = validate_object_id(source_id, "source_id")

    # Check source exists
    source = mongo.db.sources.find_one({"_id": source_oid})
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source not found"
        )

    # Validate configuration
    _validate_auth_config(config)

    # Build update document
    now = datetime.utcnow()
    update_doc = {
        "auth_type": config.auth_type.value,
        "session_duration_hours": config.session_duration_hours,
        "auto_refresh": config.auto_refresh,
        "updated_at": now
    }

    # Add type-specific config
    if config.form_config:
        update_doc["form_config"] = config.form_config.model_dump()
    else:
        update_doc["form_config"] = None

    if config.oauth_config:
        update_doc["oauth_config"] = config.oauth_config.model_dump()
    else:
        update_doc["oauth_config"] = None

    if config.api_key_config:
        update_doc["api_key_config"] = config.api_key_config.model_dump()
    else:
        update_doc["api_key_config"] = None

    if config.basic_config:
        update_doc["basic_config"] = config.basic_config.model_dump()
    else:
        update_doc["basic_config"] = None

    # Upsert config
    result = mongo.db.auth_configs.update_one(
        {"source_id": source_oid},
        {"$set": update_doc, "$setOnInsert": {"created_at": now}},
        upsert=True
    )

    # Update credentials
    if config.credentials:
        await _store_credentials(
            source_id=str(source_oid),
            credentials=config.credentials,
            auth_type=config.auth_type
        )
        update_doc["has_credentials"] = True
    else:
        update_doc["has_credentials"] = False

    # Get updated document
    updated = mongo.db.auth_configs.find_one({"source_id": source_oid})
    updated["_id"] = str(updated["_id"])
    updated["source_id"] = str(updated["source_id"])

    logger.info(f"Updated auth config for source: {source_id}")

    return AuthConfigResponse(**updated)


@router.get(
    "/{source_id}/auth-config",
    response_model=AuthConfigResponse,
    summary="Get authentication configuration",
    description="""
Retrieve authentication configuration for a source.

**Note:** Credentials are not included in response for security.

**Required scope:** `read`
""",
    responses={
        200: {"description": "Auth configuration retrieved"},
        **ERROR_RESPONSES
    }
)
async def get_auth_config(
    source_id: str,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth)
):
    """Get authentication configuration for a source."""
    source_oid = validate_object_id(source_id, "source_id")

    config = mongo.db.auth_configs.find_one({"source_id": source_oid})
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Auth configuration not found for this source"
        )

    # Check if credentials exist
    creds = mongo.db.auth_credentials.find_one({"source_id": str(source_oid)})
    config["has_credentials"] = creds is not None

    config["_id"] = str(config["_id"])
    config["source_id"] = str(config["source_id"])

    return AuthConfigResponse(**config)


@router.delete(
    "/{source_id}/auth-config",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete authentication configuration",
    description="""
Delete authentication configuration and credentials for a source.

**Warning:** This also invalidates any active sessions.

**Required scope:** `write`
""",
    responses={
        204: {"description": "Auth configuration deleted"},
        **ERROR_RESPONSES
    }
)
async def delete_auth_config(
    source_id: str,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write"))
):
    """Delete authentication configuration for a source."""
    source_oid = validate_object_id(source_id, "source_id")

    # Delete config
    result = mongo.db.auth_configs.delete_one({"source_id": source_oid})

    # Delete credentials
    mongo.db.auth_credentials.delete_one({"source_id": str(source_oid)})

    # Delete sessions
    mongo.db.auth_sessions.delete_one({"source_id": str(source_oid)})

    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Auth configuration not found"
        )

    logger.info(f"Deleted auth config for source: {source_id}")


# ============================================================
# Session Status Endpoints
# ============================================================

@router.get(
    "/{source_id}/auth-status",
    response_model=SessionStatusResponse,
    summary="Get authentication and session status",
    description="""
Check authentication configuration and session validity for a source.

Returns comprehensive status including:
- Whether auth is configured
- Session validity and expiration
- Last login and usage timestamps

**Required scope:** `read`
""",
    responses={
        200: {"description": "Status retrieved successfully"},
        **ERROR_RESPONSES
    }
)
async def get_auth_status(
    source_id: str,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth)
):
    """Get authentication and session status for a source."""
    source_oid = validate_object_id(source_id, "source_id")

    # Get auth config
    config = mongo.db.auth_configs.find_one({"source_id": source_oid})

    # Get session
    session = mongo.db.auth_sessions.find_one({"source_id": str(source_oid)})

    # Build response
    status_data = {
        "source_id": source_id,
        "auth_configured": config is not None,
        "auth_type": config.get("auth_type") if config else None,
        "session_valid": False,
        "session_expires_at": None,
        "last_login": None,
        "last_used": None,
        "cookies_count": 0,
        "needs_refresh": False
    }

    if session:
        expires_at = session.get("expires_at")
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)

        is_valid = session.get("is_valid", False)
        is_expired = expires_at and expires_at < datetime.utcnow() if expires_at else True

        status_data["session_valid"] = is_valid and not is_expired
        status_data["session_expires_at"] = expires_at

        if session.get("login_verified_at"):
            login_time = session["login_verified_at"]
            if isinstance(login_time, str):
                login_time = datetime.fromisoformat(login_time)
            status_data["last_login"] = login_time

        if session.get("last_used_at"):
            used_time = session["last_used_at"]
            if isinstance(used_time, str):
                used_time = datetime.fromisoformat(used_time)
            status_data["last_used"] = used_time

        cookies = session.get("cookies", {})
        status_data["cookies_count"] = len(cookies) if isinstance(cookies, dict) else 0

        # Check if needs refresh (within 2 hours of expiration)
        if expires_at and not is_expired:
            time_remaining = expires_at - datetime.utcnow()
            status_data["needs_refresh"] = time_remaining < timedelta(hours=2)

    return SessionStatusResponse(**status_data)


@router.post(
    "/{source_id}/auth-refresh",
    response_model=SessionRefreshResponse,
    summary="Refresh authentication session",
    description="""
Refresh or extend the authentication session for a source.

If `force_relogin` is true, performs a new login regardless of session state.
Otherwise, extends the existing session if valid.

**Required scope:** `write`
""",
    responses={
        200: {"description": "Session refreshed successfully"},
        **ERROR_RESPONSES
    }
)
async def refresh_session(
    source_id: str,
    request: SessionRefreshRequest,
    background_tasks: BackgroundTasks,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write"))
):
    """Refresh authentication session for a source."""
    source_oid = validate_object_id(source_id, "source_id")

    # Get auth config
    config = mongo.db.auth_configs.find_one({"source_id": source_oid})
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Auth configuration not found. Configure auth first."
        )

    # Get current session
    session = mongo.db.auth_sessions.find_one({"source_id": str(source_oid)})

    if request.force_relogin:
        # Perform new login in background
        background_tasks.add_task(
            _perform_login,
            source_id=str(source_oid),
            config=config
        )
        return SessionRefreshResponse(
            success=True,
            message="Login initiated in background",
            login_performed=True
        )

    if not session:
        # No session, need to login
        background_tasks.add_task(
            _perform_login,
            source_id=str(source_oid),
            config=config
        )
        return SessionRefreshResponse(
            success=True,
            message="No existing session. Login initiated.",
            login_performed=True
        )

    # Extend existing session
    extend_hours = request.extend_hours or config.get("session_duration_hours", 24)
    new_expiration = datetime.utcnow() + timedelta(hours=extend_hours)

    mongo.db.auth_sessions.update_one(
        {"source_id": str(source_oid)},
        {
            "$set": {
                "expires_at": new_expiration.isoformat(),
                "last_used_at": datetime.utcnow().isoformat()
            }
        }
    )

    logger.info(f"Extended session for source: {source_id}")

    return SessionRefreshResponse(
        success=True,
        message=f"Session extended by {extend_hours} hours",
        new_expiration=new_expiration,
        login_performed=False
    )


@router.delete(
    "/{source_id}/auth-session",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Invalidate authentication session",
    description="""
Invalidate the current session for a source.

The session will be marked as invalid and a new login will be required.

**Required scope:** `write`
""",
    responses={
        204: {"description": "Session invalidated"},
        **ERROR_RESPONSES
    }
)
async def invalidate_session(
    source_id: str,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write"))
):
    """Invalidate authentication session for a source."""
    source_oid = validate_object_id(source_id, "source_id")

    result = mongo.db.auth_sessions.update_one(
        {"source_id": str(source_oid)},
        {"$set": {"is_valid": False}}
    )

    if result.matched_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No session found for this source"
        )

    logger.info(f"Invalidated session for source: {source_id}")


# ============================================================
# Login Test Endpoint
# ============================================================

@router.post(
    "/{source_id}/auth-test",
    response_model=LoginTestResponse,
    summary="Test login configuration",
    description="""
Test the authentication configuration by attempting a login.

This performs an actual login attempt and returns detailed results.
Useful for validating selectors and credentials before scheduling crawls.

**Note:** This may trigger rate limiting or CAPTCHA on the target site.

**Required scope:** `write`
""",
    responses={
        200: {"description": "Login test completed"},
        **ERROR_RESPONSES
    }
)
async def test_login(
    source_id: str,
    request: LoginTestRequest,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write"))
):
    """Test login configuration for a source."""
    source_oid = validate_object_id(source_id, "source_id")

    # Get auth config
    config = mongo.db.auth_configs.find_one({"source_id": source_oid})
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Auth configuration not found"
        )

    # Get credentials
    creds = mongo.db.auth_credentials.find_one({"source_id": str(source_oid)})
    if not creds:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Credentials not configured"
        )

    # Perform login test
    try:
        result = await _test_login(
            config=config,
            credentials=creds,
            timeout_seconds=request.timeout_seconds,
            capture_screenshot=request.capture_screenshot
        )
        return result
    except Exception as e:
        logger.error(f"Login test failed: {e}")
        return LoginTestResponse(
            success=False,
            message=str(e),
            error_code="TEST_FAILED"
        )


# ============================================================
# Bulk Operations
# ============================================================

@router.post(
    "/auth-cleanup",
    response_model=BulkSessionCleanupResponse,
    summary="Cleanup expired sessions",
    description="""
Remove all expired and invalid sessions from the system.

This is a maintenance operation that should be run periodically.

**Required scope:** `admin`
""",
    responses={
        200: {"description": "Cleanup completed"},
        **ERROR_RESPONSES
    }
)
async def cleanup_sessions(
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("admin"))
):
    """Cleanup expired and invalid sessions."""
    now = datetime.utcnow().isoformat()

    # Remove expired sessions
    expired_result = mongo.db.auth_sessions.delete_many({
        "expires_at": {"$lt": now}
    })

    # Remove invalid sessions
    invalid_result = mongo.db.auth_sessions.delete_many({
        "is_valid": False
    })

    total = expired_result.deleted_count + invalid_result.deleted_count
    logger.info(f"Session cleanup: removed {total} sessions")

    return BulkSessionCleanupResponse(
        expired_sessions_removed=expired_result.deleted_count,
        invalid_sessions_removed=invalid_result.deleted_count,
        total_removed=total
    )


# ============================================================
# Helper Functions
# ============================================================

def _validate_auth_config(config: AuthConfigCreate) -> None:
    """Validate auth configuration based on type."""
    if config.auth_type == AuthType.FORM:
        if not config.form_config:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="form_config required for auth_type=form"
            )
        if not config.credentials.username or not config.credentials.password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username and password required for form auth"
            )

    elif config.auth_type == AuthType.OAUTH:
        if not config.oauth_config:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="oauth_config required for auth_type=oauth"
            )

    elif config.auth_type == AuthType.API_KEY:
        if not config.credentials.api_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="api_key required for auth_type=api_key"
            )

    elif config.auth_type == AuthType.BASIC:
        if not config.credentials.username or not config.credentials.password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username and password required for basic auth"
            )

    elif config.auth_type == AuthType.BEARER:
        if not config.credentials.api_key and not config.credentials.oauth_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="api_key or oauth_token required for bearer auth"
            )


async def _store_credentials(
    source_id: str,
    credentials,
    auth_type: AuthType
) -> None:
    """Store encrypted credentials."""
    from crawlers.auth.session_manager import SessionManager, AuthCredentials

    session_manager = get_session_manager()

    auth_creds = AuthCredentials(
        source_id=source_id,
        auth_type=auth_type,
        username=credentials.username,
        password=credentials.password,
        api_key=credentials.api_key,
        oauth_token=credentials.oauth_token,
        oauth_refresh_token=credentials.oauth_refresh_token,
        cookies=credentials.cookies,
        headers=credentials.headers,
        custom_data=credentials.custom_data
    )

    await session_manager.store_credentials(auth_creds)


async def _perform_login(source_id: str, config: dict) -> None:
    """Perform login in background."""
    # This would be implemented to actually perform login
    # using the AuthenticatedCrawler
    logger.info(f"Background login task for source: {source_id}")

    # Import here to avoid circular imports
    from crawlers.auth import AuthenticatedCrawler, SessionManager, PlaywrightConfig

    try:
        session_manager = get_session_manager()
        credentials = await session_manager.load_credentials(source_id)

        if not credentials:
            logger.error(f"No credentials found for source: {source_id}")
            return

        playwright_config = PlaywrightConfig(
            headless=True,
            timeout=30000
        )

        async with AuthenticatedCrawler(session_manager, playwright_config) as crawler:
            auth_type = config.get("auth_type")

            if auth_type == "form":
                form_config = config.get("form_config", {})
                selectors = form_config.get("selectors", {})

                result = await crawler.login_form(
                    url=form_config.get("login_url", ""),
                    username_selector=selectors.get("username", ""),
                    password_selector=selectors.get("password", ""),
                    submit_selector=selectors.get("submit", ""),
                    username=credentials.username or "",
                    password=credentials.password or "",
                    success_indicator=selectors.get("success_indicator"),
                    wait_after_submit=form_config.get("wait_after_submit", 3000)
                )

                if result.success:
                    await crawler.save_current_session(
                        source_id,
                        duration_hours=config.get("session_duration_hours", 24)
                    )
                    logger.info(f"Login successful for source: {source_id}")
                else:
                    logger.error(f"Login failed for source: {source_id}: {result.message}")

            elif auth_type == "api_key":
                api_config = config.get("api_key_config", {})
                await crawler.login_api_key(
                    api_key=credentials.api_key or "",
                    header_name=api_config.get("header_name", "Authorization"),
                    header_prefix=api_config.get("header_prefix", "Bearer ")
                )
                await crawler.save_current_session(
                    source_id,
                    duration_hours=config.get("session_duration_hours", 24)
                )

    except Exception as e:
        logger.error(f"Background login failed for source {source_id}: {e}")


async def _test_login(
    config: dict,
    credentials: dict,
    timeout_seconds: int,
    capture_screenshot: bool
) -> LoginTestResponse:
    """Test login and return results."""
    from crawlers.auth import AuthenticatedCrawler, SessionManager, PlaywrightConfig

    session_manager = get_session_manager()

    screenshot_dir = os.getenv("SCREENSHOT_DIR", "/tmp/screenshots")
    playwright_config = PlaywrightConfig(
        headless=True,
        timeout=timeout_seconds * 1000,
        trace_dir=screenshot_dir if capture_screenshot else None
    )

    async with AuthenticatedCrawler(session_manager, playwright_config) as crawler:
        auth_type = config.get("auth_type")

        if auth_type == "form":
            form_config = config.get("form_config", {})
            selectors = form_config.get("selectors", {})

            # Decrypt credentials
            session_manager_instance = get_session_manager()
            decrypted = session_manager_instance._decrypt_credentials(credentials)

            result = await crawler.login_form(
                url=form_config.get("login_url", ""),
                username_selector=selectors.get("username", ""),
                password_selector=selectors.get("password", ""),
                submit_selector=selectors.get("submit", ""),
                username=decrypted.get("username", ""),
                password=decrypted.get("password", ""),
                success_indicator=selectors.get("success_indicator"),
                failure_indicator=selectors.get("failure_indicator"),
                wait_after_submit=form_config.get("wait_after_submit", 3000),
                screenshot_on_failure=capture_screenshot
            )

            return LoginTestResponse(
                success=result.success,
                message=result.message,
                error_code=result.error_code,
                requires_captcha=result.requires_captcha,
                requires_2fa=result.requires_2fa,
                cookies_captured=result.cookies_count,
                execution_time_ms=result.execution_time_ms
            )

        else:
            return LoginTestResponse(
                success=False,
                message=f"Login test not implemented for auth_type: {auth_type}",
                error_code="NOT_IMPLEMENTED"
            )

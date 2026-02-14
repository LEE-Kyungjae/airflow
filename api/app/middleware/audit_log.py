"""
Audit logging middleware for comprehensive API request/response tracking.

Records structured audit events for every API request including:
    - Timestamp, duration, and status code
    - Authenticated user identity (user_id, auth_type)
    - Client metadata (IP, user-agent)
    - Request path and method
    - Request ID and correlation ID for traceability

Sensitive operations (authentication, data modification) are logged at
a higher detail level to support security forensics and compliance.

Audit logs are written to a dedicated structlog logger ("audit") to
allow routing to a separate log sink (file, SIEM, etc.) independent
of application logs.
"""

import os
import time
from typing import Set

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
import structlog

from app.middleware.request_id import get_request_id
from app.core.context import get_correlation_id


# Dedicated audit logger (separate from application logger)
audit_logger = structlog.get_logger("audit")

# Paths that are excluded from audit logging to reduce noise
_EXCLUDED_PATHS: Set[str] = {
    "/health",
    "/metrics",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/favicon.ico",
}

# Paths that constitute sensitive operations and receive enhanced logging
_SENSITIVE_PATH_PREFIXES = (
    "/api/auth",
    "/api/backup",
    "/api/e2e",
)

# HTTP methods that modify state
_WRITE_METHODS: Set[str] = {"POST", "PUT", "PATCH", "DELETE"}


class AuditLogMiddleware(BaseHTTPMiddleware):
    """
    Middleware that emits structured audit log entries for every API request.

    Configuration via environment variables:
        AUDIT_LOG_ENABLED: "true" (default) or "false"
        AUDIT_LOG_BODY:    "true" or "false" (default) -- log request body
                           for sensitive operations (use with caution)
    """

    def __init__(self, app) -> None:
        super().__init__(app)
        self.enabled = os.getenv("AUDIT_LOG_ENABLED", "true").lower() == "true"
        self.log_body = os.getenv("AUDIT_LOG_BODY", "false").lower() == "true"

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        if not self.enabled:
            return await call_next(request)

        path = request.url.path

        # Skip noisy infrastructure endpoints
        if path in _EXCLUDED_PATHS:
            return await call_next(request)

        start_time = time.monotonic()

        # Extract client information
        client_ip = self._get_client_ip(request)
        user_agent = request.headers.get("user-agent", "")
        method = request.method

        # Process the request and capture the response
        response: Response = await call_next(request)

        duration_ms = round((time.monotonic() - start_time) * 1000, 2)
        status_code = response.status_code

        # Extract user identity from request state (set by auth middleware)
        user_id = self._extract_user_id(request)
        auth_type = self._extract_auth_type(request)

        # Build the audit event
        audit_event = {
            "audit_action": "api_request",
            "timestamp_epoch": time.time(),
            "method": method,
            "path": path,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "client_ip": client_ip,
            "user_agent": user_agent[:256],  # Truncate to prevent log bloat
            "user_id": user_id,
            "auth_type": auth_type,
            "request_id": get_request_id(),
            "correlation_id": get_correlation_id(),
        }

        # Enhanced logging for sensitive operations
        is_sensitive = self._is_sensitive(path, method)
        if is_sensitive:
            audit_event["sensitive"] = True
            query_string = str(request.url.query) if request.url.query else ""
            if query_string:
                # Redact known sensitive query parameters
                audit_event["query_params"] = self._redact_query(query_string)

        # Log level based on response status
        if status_code >= 500:
            audit_logger.error("audit_request", **audit_event)
        elif status_code >= 400:
            audit_logger.warning("audit_request", **audit_event)
        elif is_sensitive:
            audit_logger.info("audit_request_sensitive", **audit_event)
        else:
            audit_logger.info("audit_request", **audit_event)

        return response

    @staticmethod
    def _get_client_ip(request: Request) -> str:
        """
        Extract the real client IP, respecting X-Forwarded-For when
        the application is behind a reverse proxy.
        """
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            # Take the first (leftmost) IP which is the original client
            return forwarded_for.split(",")[0].strip()
        if request.client:
            return request.client.host
        return "unknown"

    @staticmethod
    def _extract_user_id(request: Request) -> str:
        """Extract user ID from request state if authentication ran."""
        try:
            # The auth dependency stores the context in request.state
            auth_ctx = getattr(request.state, "auth_context", None)
            if auth_ctx and hasattr(auth_ctx, "user_id"):
                return auth_ctx.user_id or "anonymous"
        except Exception:
            pass
        return "anonymous"

    @staticmethod
    def _extract_auth_type(request: Request) -> str:
        """Extract authentication type from request state."""
        try:
            auth_ctx = getattr(request.state, "auth_context", None)
            if auth_ctx and hasattr(auth_ctx, "auth_type"):
                return auth_ctx.auth_type or "none"
        except Exception:
            pass
        return "none"

    @staticmethod
    def _is_sensitive(path: str, method: str) -> bool:
        """Determine if the request targets a sensitive operation."""
        # All auth operations are sensitive
        if path.startswith(_SENSITIVE_PATH_PREFIXES):
            return True
        # All write operations are sensitive
        if method in _WRITE_METHODS:
            return True
        return False

    @staticmethod
    def _redact_query(query_string: str) -> str:
        """Redact sensitive values from query parameters."""
        redact_keys = {"token", "key", "secret", "password", "api_key"}
        parts = []
        for param in query_string.split("&"):
            if "=" in param:
                key, _value = param.split("=", 1)
                if key.lower() in redact_keys:
                    parts.append(f"{key}=***REDACTED***")
                else:
                    parts.append(param)
            else:
                parts.append(param)
        return "&".join(parts)

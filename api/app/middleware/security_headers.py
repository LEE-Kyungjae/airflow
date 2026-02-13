"""
Security headers middleware for defense-in-depth HTTP response hardening.

Adds configurable security headers to all HTTP responses to mitigate
common web attack vectors including XSS, clickjacking, MIME sniffing,
and protocol downgrade attacks.

References:
    - OWASP Secure Headers Project
    - Mozilla Observatory recommendations
"""

import os
from typing import Dict, Optional

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


# Default Content-Security-Policy: restrictive baseline.
# Override via CONTENT_SECURITY_POLICY env var for application-specific needs.
_DEFAULT_CSP = (
    "default-src 'self'; "
    "script-src 'self'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data:; "
    "font-src 'self'; "
    "connect-src 'self'; "
    "frame-ancestors 'none'; "
    "base-uri 'self'; "
    "form-action 'self'"
)

# Relaxed CSP for development (allows docs UI assets from CDN)
_DEV_CSP = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
    "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
    "img-src 'self' data: https://fastapi.tiangolo.com; "
    "font-src 'self' https://cdn.jsdelivr.net; "
    "connect-src 'self'; "
    "frame-ancestors 'self'; "
    "base-uri 'self'; "
    "form-action 'self'"
)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Middleware that injects security-related HTTP response headers.

    All headers are configurable via environment variables.
    Sensible defaults are applied based on the current environment
    (production vs development).

    Headers applied:
        X-Content-Type-Options: nosniff
        X-Frame-Options: DENY
        X-XSS-Protection: 0  (modern recommendation; CSP is preferred)
        Strict-Transport-Security: max-age=31536000; includeSubDomains
        Content-Security-Policy: restrictive default
        Referrer-Policy: strict-origin-when-cross-origin
        Permissions-Policy: restrictive default
        Cache-Control: no-store (for API responses)
    """

    def __init__(
        self,
        app,
        custom_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(app)

        is_production = os.getenv("ENV") == "production"

        # Build header map from environment with secure defaults
        self.headers: Dict[str, str] = {
            "X-Content-Type-Options": os.getenv(
                "SECURITY_X_CONTENT_TYPE_OPTIONS", "nosniff"
            ),
            "X-Frame-Options": os.getenv(
                "SECURITY_X_FRAME_OPTIONS", "DENY"
            ),
            # X-XSS-Protection is set to 0 per modern best practice.
            # The header can cause *more* harm than good in older browsers.
            # CSP is the correct mitigation for XSS.
            "X-XSS-Protection": os.getenv(
                "SECURITY_X_XSS_PROTECTION", "0"
            ),
            "Referrer-Policy": os.getenv(
                "SECURITY_REFERRER_POLICY", "strict-origin-when-cross-origin"
            ),
            "Permissions-Policy": os.getenv(
                "SECURITY_PERMISSIONS_POLICY",
                "geolocation=(), camera=(), microphone=(), payment=()",
            ),
            "Cache-Control": os.getenv(
                "SECURITY_CACHE_CONTROL", "no-store"
            ),
        }

        # HSTS only in production (avoid locking dev environments into HTTPS)
        if is_production:
            hsts_value = os.getenv(
                "SECURITY_HSTS",
                "max-age=31536000; includeSubDomains",
            )
            self.headers["Strict-Transport-Security"] = hsts_value

        # CSP: use strict policy in production, relaxed in development
        default_csp = _DEFAULT_CSP if is_production else _DEV_CSP
        csp_value = os.getenv("CONTENT_SECURITY_POLICY", default_csp)
        self.headers["Content-Security-Policy"] = csp_value

        # Allow caller to override/extend with custom headers
        if custom_headers:
            self.headers.update(custom_headers)

        # Paths that should be excluded from certain headers
        # (e.g., Swagger/ReDoc UI needs relaxed CSP handled above)
        self._doc_paths = {"/docs", "/redoc", "/openapi.json"}

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        response = await call_next(request)

        for header_name, header_value in self.headers.items():
            # Do not overwrite headers already set by the application
            if header_name not in response.headers:
                response.headers[header_name] = header_value

        return response

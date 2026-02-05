"""
Request context management for correlation ID tracking.

Provides middleware and utilities for request tracing across
the application and external services.
"""

import uuid
from contextvars import ContextVar
from typing import Optional

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response
import structlog


# Context variable for correlation ID - accessible throughout request lifecycle
correlation_id_ctx: ContextVar[str] = ContextVar("correlation_id", default="")


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle correlation ID for request tracing.

    - Extracts correlation ID from X-Correlation-ID header or generates new one
    - Binds correlation ID to structlog context for all log messages
    - Adds correlation ID to response headers for client tracking
    """

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint
    ) -> Response:
        # Get correlation ID from header or generate new one
        correlation_id = request.headers.get(
            "X-Correlation-ID",
            str(uuid.uuid4())
        )

        # Set in context variable for access throughout request
        correlation_id_ctx.set(correlation_id)

        # Clear any previous context and bind new values
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            correlation_id=correlation_id,
            request_path=request.url.path,
            request_method=request.method,
            client_ip=request.client.host if request.client else None,
        )

        # Process request
        response = await call_next(request)

        # Add correlation ID to response headers
        response.headers["X-Correlation-ID"] = correlation_id

        return response


def get_correlation_id() -> str:
    """
    Get current correlation ID from context.

    Returns:
        The correlation ID for the current request, or empty string if not set.
    """
    return correlation_id_ctx.get()


def bind_context(**kwargs) -> None:
    """
    Bind additional context variables for logging.

    Args:
        **kwargs: Key-value pairs to add to log context.
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def unbind_context(*keys: str) -> None:
    """
    Remove context variables from logging context.

    Args:
        *keys: Keys to remove from context.
    """
    structlog.contextvars.unbind_contextvars(*keys)

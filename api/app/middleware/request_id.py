"""
Request ID middleware for end-to-end request traceability.

Generates a unique identifier for every inbound request, propagates it
through response headers (X-Request-ID), and binds it to the structlog
context so all log entries emitted during the request lifecycle carry
the same identifier.

If the caller supplies an X-Request-ID header, that value is reused
(useful for distributed tracing across services). Otherwise a new
UUID-4 is generated.

This middleware is distinct from the existing CorrelationIdMiddleware:
    - CorrelationIdMiddleware tracks X-Correlation-ID for cross-service
      business transaction tracing.
    - RequestIdMiddleware tracks X-Request-ID for per-request tracing
      within this service boundary.
"""

import uuid
from contextvars import ContextVar
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
import structlog


# Context variable accessible throughout the request lifecycle
request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")

REQUEST_ID_HEADER = "X-Request-ID"


class RequestIdMiddleware(BaseHTTPMiddleware):
    """
    Assign a unique request ID to every HTTP request.

    Behaviour:
        1. Read X-Request-ID from the inbound request headers.
        2. If absent, generate a new UUID-4.
        3. Store the value in a ContextVar for access anywhere in the call stack.
        4. Bind the value to structlog context for automatic inclusion in logs.
        5. Set the X-Request-ID response header for the caller.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        # Honour caller-supplied request ID (for distributed tracing),
        # but validate it is a reasonable length to prevent header injection.
        incoming_id = request.headers.get(REQUEST_ID_HEADER)
        if incoming_id and len(incoming_id) <= 128:
            req_id = incoming_id
        else:
            req_id = str(uuid.uuid4())

        # Store in context variable
        request_id_ctx.set(req_id)

        # Bind to structlog context (available to all loggers in this request)
        structlog.contextvars.bind_contextvars(request_id=req_id)

        # Process the request
        response = await call_next(request)

        # Always include the request ID in the response
        response.headers[REQUEST_ID_HEADER] = req_id

        return response


def get_request_id() -> str:
    """
    Return the current request ID from the context variable.

    Returns:
        The request ID string, or empty string if called outside a request.
    """
    return request_id_ctx.get()

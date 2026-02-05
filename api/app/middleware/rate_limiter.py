"""
Rate limiting middleware using slowapi.

Provides configurable rate limits for different endpoint types
to protect the API from abuse and ensure fair usage.
"""

import os
from datetime import datetime

from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware


def get_rate_limit_key(request: Request) -> str:
    """
    Get the rate limit key for a request.
    Uses client IP address by default, can be extended to use API keys.
    """
    # Could extend this to use API keys when authentication is added
    return get_remote_address(request)


# Create limiter instance
# For distributed deployments, configure Redis: storage_uri="redis://localhost:6379"
storage_uri = os.getenv("REDIS_URL", None)

limiter = Limiter(
    key_func=get_rate_limit_key,
    default_limits=["100/minute"],
    storage_uri=storage_uri,
)


async def rate_limit_exceeded_handler(
    request: Request,
    exc: RateLimitExceeded
) -> JSONResponse:
    """
    Custom handler for rate limit exceeded errors.
    Returns a JSON response with details about the rate limit.
    """
    return JSONResponse(
        status_code=429,
        content={
            "error": "Rate limit exceeded",
            "message": f"Too many requests. {exc.detail}",
            "retry_after": str(exc.detail).split("per")[0].strip() if exc.detail else "1 minute",
            "timestamp": datetime.utcnow().isoformat(),
        },
        headers={
            "Retry-After": "60",
            "X-RateLimit-Limit": str(exc.detail) if exc.detail else "100/minute",
        }
    )


# Rate limit decorators for different endpoint types
# Usage: @read_limit (for GET endpoints)
#        @write_limit (for POST/PUT endpoints)
#        @delete_limit (for DELETE endpoints)
#        @expensive_limit (for AI/batch operations)
#        @trigger_limit (for Airflow triggers)

def read_limit(func):
    """Rate limit for read operations (GET): 100/minute"""
    return limiter.limit("100/minute")(func)


def write_limit(func):
    """Rate limit for write operations (POST/PUT): 30/minute"""
    return limiter.limit("30/minute")(func)


def delete_limit(func):
    """Rate limit for delete operations: 10/minute"""
    return limiter.limit("10/minute")(func)


def expensive_limit(func):
    """Rate limit for expensive operations (AI, batch): 10/minute"""
    return limiter.limit("10/minute")(func)


def trigger_limit(func):
    """Rate limit for Airflow trigger operations: 20/minute"""
    return limiter.limit("20/minute")(func)


# Export SlowAPIMiddleware for use in main.py
__all__ = [
    "limiter",
    "RateLimitExceeded",
    "rate_limit_exceeded_handler",
    "SlowAPIMiddleware",
    "read_limit",
    "write_limit",
    "delete_limit",
    "expensive_limit",
    "trigger_limit",
]

"""
Prometheus Metrics Middleware - Request-level metrics collection.

Features:
- Request count by method, path, status_code
- Request duration histogram
- Active requests gauge
- Error rate tracking
- Automatic metric collection for all API endpoints
"""

import time
from typing import Callable
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.services.observability.prometheus import get_registry
from app.core import get_logger

logger = get_logger(__name__)


class PrometheusMetricsMiddleware(BaseHTTPMiddleware):
    """Middleware for collecting Prometheus metrics on HTTP requests."""

    def __init__(self, app):
        super().__init__(app)
        self.registry = get_registry()
        self._setup_metrics()

    def _setup_metrics(self):
        """Register HTTP request metrics."""
        # Request count by method, path, and status
        self.registry.register(
            "http_requests_total",
            "counter",
            "Total HTTP requests",
            labels=["method", "path", "status_code"]
        )

        # Request duration histogram
        self.registry.register(
            "http_request_duration_seconds",
            "histogram",
            "HTTP request duration in seconds",
            labels=["method", "path"],
            buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        )

        # Active requests gauge
        self.registry.register(
            "http_requests_active",
            "gauge",
            "Number of active HTTP requests",
            labels=["method", "path"]
        )

        # Error count
        self.registry.register(
            "http_errors_total",
            "counter",
            "Total HTTP errors (4xx and 5xx)",
            labels=["method", "path", "status_code"]
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request and collect metrics.

        Args:
            request: Incoming HTTP request
            call_next: Next middleware in chain

        Returns:
            HTTP response
        """
        # Normalize path (remove IDs and dynamic segments)
        path = self._normalize_path(request.url.path)
        method = request.method

        # Skip metrics collection for /metrics endpoint itself
        if path == "/metrics":
            return await call_next(request)

        # Track active requests
        self.registry.inc("http_requests_active", 1, labels={"method": method, "path": path})

        # Track request duration
        start_time = time.time()

        try:
            response = await call_next(request)
            status_code = response.status_code

            # Record metrics
            duration = time.time() - start_time
            self._record_request_metrics(method, path, status_code, duration)

            return response

        except Exception as e:
            # Record error
            duration = time.time() - start_time
            self._record_request_metrics(method, path, 500, duration)
            logger.error(f"Request failed: {method} {path} - {e}")
            raise

        finally:
            # Decrease active requests
            self.registry.inc("http_requests_active", -1, labels={"method": method, "path": path})

    def _record_request_metrics(self, method: str, path: str, status_code: int, duration: float):
        """
        Record request metrics.

        Args:
            method: HTTP method
            path: Request path
            status_code: HTTP status code
            duration: Request duration in seconds
        """
        # Request count
        self.registry.inc(
            "http_requests_total",
            1,
            labels={"method": method, "path": path, "status_code": str(status_code)}
        )

        # Request duration
        self.registry.observe(
            "http_request_duration_seconds",
            duration,
            labels={"method": method, "path": path}
        )

        # Error tracking
        if status_code >= 400:
            self.registry.inc(
                "http_errors_total",
                1,
                labels={"method": method, "path": path, "status_code": str(status_code)}
            )

    def _normalize_path(self, path: str) -> str:
        """
        Normalize path by removing IDs and dynamic segments.

        Args:
            path: Original request path

        Returns:
            Normalized path with IDs replaced by placeholders
        """
        # Handle common patterns
        parts = path.split('/')
        normalized = []

        for part in parts:
            # Replace MongoDB ObjectId-like strings
            if len(part) == 24 and all(c in '0123456789abcdef' for c in part.lower()):
                normalized.append(':id')
            # Replace UUID-like strings
            elif '-' in part and len(part) == 36:
                normalized.append(':uuid')
            # Replace numeric IDs
            elif part.isdigit():
                normalized.append(':id')
            else:
                normalized.append(part)

        return '/'.join(normalized)

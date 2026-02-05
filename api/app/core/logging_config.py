"""
Centralized logging configuration using structlog.

Provides structured JSON logging with context support for
request tracing and correlation IDs.
"""

import logging
import sys
import os
from typing import Any

import structlog


def configure_logging(
    json_output: bool = None,
    log_level: str = None
) -> None:
    """
    Configure structured logging for the entire application.

    Args:
        json_output: If True, output JSON format. If None, auto-detect from environment.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR). Defaults to INFO.
    """
    # Auto-detect from environment
    if json_output is None:
        json_output = os.getenv("LOG_FORMAT", "json").lower() == "json"

    if log_level is None:
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    # Shared processors for all loggers
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if json_output:
        # JSON output for production
        shared_processors.append(structlog.processors.JSONRenderer())
    else:
        # Console output for development
        shared_processors.append(
            structlog.dev.ConsoleRenderer(colors=True)
        )

    structlog.configure(
        processors=shared_processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level, logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure standard library logging to use structlog format
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level, logging.INFO),
    )

    # Suppress noisy third-party loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def get_logger(name: str = None) -> structlog.BoundLogger:
    """
    Get a structlog logger instance.

    Args:
        name: Logger name, typically __name__ from the calling module.

    Returns:
        A bound structlog logger with context support.
    """
    return structlog.get_logger(name)


# Type alias for type hints
Logger = structlog.BoundLogger

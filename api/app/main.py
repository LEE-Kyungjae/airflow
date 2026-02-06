"""
FastAPI Main Application for Crawler System.

This module provides the REST API for managing crawling sources,
crawlers, error handling, and dashboard data. It serves as the
central orchestration layer for the AI-powered web crawling
automation system integrated with Apache Airflow.
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from slowapi.middleware import SlowAPIMiddleware

from app.routers import sources, crawlers, errors, dashboard, quick_add, monitoring, auth, reviews, data_quality, metrics, lineage, export, backup
from app.services.mongo_service import MongoService
from app.auth import APIKeyAuth, JWTAuth
from app.core import configure_logging, get_logger, CorrelationIdMiddleware
from app.middleware.rate_limiter import limiter, RateLimitExceeded, rate_limit_exceeded_handler

# Configure structured logging
configure_logging()
logger = get_logger(__name__)


# ============================================================
# OpenAPI Configuration
# ============================================================

API_TITLE = "Crawler System API"
API_VERSION = "1.0.0"
API_DESCRIPTION = """
## AI-Powered Web Crawling Automation System

This REST API provides comprehensive management of automated web crawling pipelines
integrated with Apache Airflow for scheduling and orchestration.

### Key Features

- **Source Management**: Register and configure crawling targets with custom extraction fields
- **Crawler Automation**: AI-generated Python crawlers with automatic code regeneration
- **Self-Healing**: Automatic error recovery with intelligent retry mechanisms
- **Data Review**: Human-in-the-loop verification workflow for data quality assurance
- **Real-time Monitoring**: WebSocket-based live status updates and alerting

### Authentication

The API supports two authentication methods:

1. **JWT Bearer Token**: Obtain via `/api/auth/login` endpoint
2. **API Key**: Pass via `X-API-Key` header (admin-created keys)

### Error Codes

| Code | Description |
|------|-------------|
| SELECTOR_NOT_FOUND | CSS/XPath selector not found in document |
| TIMEOUT | Request or operation timed out |
| PARSE_ERROR | Failed to parse document structure |
| VALIDATION_ERROR | Extracted data failed validation |
| NETWORK_ERROR | Network connectivity issue |

### Rate Limiting

Default limits: 100 requests/minute for authenticated users, 20 requests/minute for anonymous.
"""

TAGS_METADATA = [
    {
        "name": "Authentication",
        "description": "User authentication, JWT token management, and API key administration.",
    },
    {
        "name": "Sources",
        "description": "Crawling source registration and management. Sources define what to crawl and extract.",
    },
    {
        "name": "Crawlers",
        "description": "Auto-generated crawler code management with version history and rollback support.",
    },
    {
        "name": "Errors",
        "description": "Error log viewing, resolution tracking, and one-click recovery operations.",
    },
    {
        "name": "Dashboard",
        "description": "System overview, statistics, execution trends, and health indicators.",
    },
    {
        "name": "Quick Add",
        "description": "Simplified source registration with AI-assisted field detection.",
    },
    {
        "name": "Monitoring",
        "description": "Real-time pipeline status, self-healing sessions, and WebSocket live updates.",
    },
    {
        "name": "Reviews",
        "description": "Data review workflow for human verification and correction of extracted data.",
    },
    {
        "name": "Data Quality",
        "description": "Data quality metrics, validation rules, and quality score tracking.",
    },
    {
        "name": "Metrics",
        "description": "Prometheus-compatible metrics endpoint for observability.",
    },
    {
        "name": "Lineage",
        "description": "Data lineage tracking from source to production.",
    },
    {
        "name": "Export",
        "description": "Data export to CSV, Excel, and JSON formats with streaming and async support.",
    },
    {
        "name": "Backup",
        "description": "Database backup and restore operations with cloud storage integration.",
    },
    {
        "name": "Health",
        "description": "System health check endpoints for load balancers and monitoring.",
    },
    {
        "name": "Root",
        "description": "API information and discovery.",
    },
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    logger.info("Starting Crawler System API...")

    # Test MongoDB connection
    try:
        mongo = MongoService()
        mongo.db.command('ping')
        logger.info("MongoDB connection successful")
        mongo.close()
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")

    yield

    # Shutdown
    logger.info("Shutting down Crawler System API...")


# Create FastAPI application
app = FastAPI(
    title=API_TITLE,
    description=API_DESCRIPTION,
    version=API_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=TAGS_METADATA,
    contact={
        "name": "Crawler System Team",
        "url": "https://github.com/your-org/crawler-system",
        "email": "support@crawler-system.dev",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    lifespan=lifespan
)

# CORS 설정 (환경별)
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "").split(",")
if not ALLOWED_ORIGINS or ALLOWED_ORIGINS == [""]:
    # 개발 환경 기본값
    ALLOWED_ORIGINS = ["http://localhost:3000", "http://localhost:8000", "http://localhost:5173"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if os.getenv("ENV") == "production" else ["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["Authorization", "Content-Type", "X-API-Key"],
    max_age=86400,
)

# Trusted Host 미들웨어 (프로덕션)
if os.getenv("ENV") == "production":
    ALLOWED_HOSTS = os.getenv("ALLOWED_HOSTS", "").split(",")
    if ALLOWED_HOSTS and ALLOWED_HOSTS != [""]:
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=ALLOWED_HOSTS
        )

# Rate limiting middleware
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# Correlation ID middleware for request tracing
app.add_middleware(CorrelationIdMiddleware)


# Exception handlers
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler."""
    # 로그에는 상세 정보 기록
    logger.error(f"Unhandled exception: {exc}", exc_info=True)

    # 프로덕션 환경에서는 상세 에러 메시지 숨김
    is_production = os.getenv("ENV") == "production"

    if is_production:
        # 프로덕션: 일반적인 에러 메시지만 반환
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "message": "서버 오류가 발생했습니다. 관리자에게 문의하세요.",
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    else:
        # 개발 환경: 디버깅을 위한 상세 정보 제공
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "message": str(exc),
                "type": type(exc).__name__,
                "timestamp": datetime.utcnow().isoformat()
            }
        )


# Health check endpoint
@app.get(
    "/health",
    tags=["Health"],
    summary="Health Check",
    description="Returns the health status of the API service. Used by load balancers and monitoring systems.",
    response_description="Health status with timestamp",
    responses={
        200: {
            "description": "Service is healthy",
            "content": {
                "application/json": {
                    "example": {
                        "status": "healthy",
                        "timestamp": "2025-02-05T12:00:00Z",
                        "service": "crawler-system-api"
                    }
                }
            }
        },
        503: {
            "description": "Service is unhealthy",
            "content": {
                "application/json": {
                    "example": {
                        "status": "unhealthy",
                        "timestamp": "2025-02-05T12:00:00Z",
                        "error": "Database connection failed"
                    }
                }
            }
        }
    }
)
async def health_check():
    """
    Perform a basic health check of the API service.

    This endpoint is designed for:
    - Load balancer health probes
    - Kubernetes liveness/readiness checks
    - Monitoring system uptime checks

    Returns a simple status object indicating service availability.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "crawler-system-api"
    }


# Root endpoint
@app.get(
    "/",
    tags=["Root"],
    summary="API Information",
    description="Returns basic API information and links to documentation.",
    response_description="API metadata with documentation links"
)
async def root():
    """
    Get API information and documentation links.

    This endpoint provides a starting point for API discovery,
    including links to interactive documentation and health check.
    """
    return {
        "name": API_TITLE,
        "version": API_VERSION,
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/health",
        "openapi": "/openapi.json"
    }


# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(sources.router, prefix="/api/sources", tags=["Sources"])
app.include_router(crawlers.router, prefix="/api/crawlers", tags=["Crawlers"])
app.include_router(errors.router, prefix="/api/errors", tags=["Errors"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Dashboard"])
app.include_router(quick_add.router, prefix="/api/quick-add", tags=["Quick Add"])
app.include_router(monitoring.router, prefix="/api/monitoring", tags=["Monitoring"])
app.include_router(reviews.router, prefix="/api/reviews", tags=["Reviews"])
app.include_router(data_quality.router, prefix="/api/data-quality", tags=["Data Quality"])
app.include_router(metrics.router, tags=["Metrics"])  # /metrics (no prefix for Prometheus)
app.include_router(lineage.router, prefix="/api/lineage", tags=["Lineage"])  # 데이터 리니지
app.include_router(export.router, prefix="/api/export", tags=["Export"])  # 데이터 내보내기
app.include_router(backup.router, prefix="/api/backup", tags=["Backup"])  # 백업 및 복구

"""
FastAPI Main Application for Crawler System.

This module provides the REST API for managing crawling sources,
crawlers, error handling, and dashboard data.
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from slowapi.middleware import SlowAPIMiddleware

from app.routers import sources, crawlers, errors, dashboard, quick_add, monitoring, auth, reviews
from app.services.mongo_service import MongoService
from app.auth import APIKeyAuth, JWTAuth
from app.core import configure_logging, get_logger, CorrelationIdMiddleware
from app.middleware.rate_limiter import limiter, RateLimitExceeded, rate_limit_exceeded_handler

# Configure structured logging
configure_logging()
logger = get_logger(__name__)


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
    title="Crawler System API",
    description="REST API for AI-powered web crawling automation with Airflow",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
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
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc),
            "timestamp": datetime.utcnow().isoformat()
        }
    )


# Health check endpoint
@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "crawler-system-api"
    }


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Crawler System API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
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

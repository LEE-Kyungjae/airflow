"""
FastAPI Main Application for Crawler System.

This module provides the REST API for managing crawling sources,
crawlers, error handling, and dashboard data.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.routers import sources, crawlers, errors, dashboard, quick_add, monitoring
from app.services.mongo_service import MongoService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
app.include_router(sources.router, prefix="/api/sources", tags=["Sources"])
app.include_router(crawlers.router, prefix="/api/crawlers", tags=["Crawlers"])
app.include_router(errors.router, prefix="/api/errors", tags=["Errors"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Dashboard"])
app.include_router(quick_add.router, prefix="/api/quick-add", tags=["Quick Add"])
app.include_router(monitoring.router, prefix="/api/monitoring", tags=["Monitoring"])

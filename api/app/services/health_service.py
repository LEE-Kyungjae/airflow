"""
Health Check Service - Detailed system health monitoring.

Features:
- MongoDB connectivity and response time checks
- PostgreSQL connectivity checks (graceful degradation if not configured)
- Disk space availability monitoring
- Memory usage monitoring
- Component-level health status
- Version and uptime tracking
"""

import os
import psutil
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict

from app.core import get_logger

logger = get_logger(__name__)


@dataclass
class ComponentHealth:
    """Individual component health status."""
    status: str  # "healthy", "degraded", "unhealthy"
    response_time_ms: Optional[float] = None
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


@dataclass
class SystemHealth:
    """Overall system health status."""
    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: str
    uptime_seconds: float
    version: str
    components: Dict[str, ComponentHealth]
    system_info: Optional[Dict[str, Any]] = None


class HealthService:
    """Service for system health checks."""

    def __init__(self):
        self.start_time = time.time()
        self.version = os.getenv("APP_VERSION", "1.0.0")

    def get_uptime(self) -> float:
        """Get application uptime in seconds."""
        return time.time() - self.start_time

    async def check_health(self, detailed: bool = False) -> Dict[str, Any]:
        """
        Perform health check.

        Args:
            detailed: If True, include detailed system information

        Returns:
            Health status dictionary
        """
        components = {}

        # Check MongoDB
        components["mongodb"] = await self._check_mongodb()

        # Check PostgreSQL (graceful degradation)
        components["postgresql"] = await self._check_postgresql()

        # Check disk space
        components["disk"] = self._check_disk_space()

        # Check memory
        components["memory"] = self._check_memory()

        # Determine overall status
        overall_status = self._determine_overall_status(components)

        health = SystemHealth(
            status=overall_status,
            timestamp=datetime.utcnow().isoformat(),
            uptime_seconds=self.get_uptime(),
            version=self.version,
            components={k: asdict(v) for k, v in components.items()},
            system_info=self._get_system_info() if detailed else None
        )

        return asdict(health)

    async def _check_mongodb(self) -> ComponentHealth:
        """Check MongoDB connectivity and response time."""
        try:
            from app.services.mongo_service import MongoService

            start = time.time()
            mongo = MongoService()
            mongo.db.command('ping')
            response_time = (time.time() - start) * 1000

            # Check if response time is acceptable
            if response_time > 1000:
                status = "degraded"
                message = f"High latency: {response_time:.2f}ms"
            else:
                status = "healthy"
                message = "Connected"

            mongo.close()

            return ComponentHealth(
                status=status,
                response_time_ms=round(response_time, 2),
                message=message
            )

        except Exception as e:
            logger.error(f"MongoDB health check failed: {e}")
            return ComponentHealth(
                status="unhealthy",
                message=f"Connection failed: {str(e)}"
            )

    async def _check_postgresql(self) -> ComponentHealth:
        """Check PostgreSQL connectivity (graceful degradation if not available)."""
        try:
            from app.services.postgres_service import get_pg

            start = time.time()
            pg = await get_pg()

            if not pg.is_available:
                return ComponentHealth(
                    status="degraded",
                    message="PostgreSQL not configured or asyncpg missing"
                )

            # Test query
            async with pg.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')

            response_time = (time.time() - start) * 1000

            if response_time > 1000:
                status = "degraded"
                message = f"High latency: {response_time:.2f}ms"
            else:
                status = "healthy"
                message = "Connected"

            return ComponentHealth(
                status=status,
                response_time_ms=round(response_time, 2),
                message=message
            )

        except ImportError:
            return ComponentHealth(
                status="degraded",
                message="PostgreSQL support not available (asyncpg not installed)"
            )
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return ComponentHealth(
                status="unhealthy",
                message=f"Connection failed: {str(e)}"
            )

    def _check_disk_space(self) -> ComponentHealth:
        """Check disk space availability."""
        try:
            disk = psutil.disk_usage('/')
            available_percent = (disk.free / disk.total) * 100

            if available_percent < 10:
                status = "unhealthy"
                message = f"Critical: Only {available_percent:.1f}% available"
            elif available_percent < 20:
                status = "degraded"
                message = f"Warning: {available_percent:.1f}% available"
            else:
                status = "healthy"
                message = f"{available_percent:.1f}% available"

            return ComponentHealth(
                status=status,
                message=message,
                details={
                    "total_gb": round(disk.total / (1024**3), 2),
                    "used_gb": round(disk.used / (1024**3), 2),
                    "free_gb": round(disk.free / (1024**3), 2),
                    "percent_used": round(disk.percent, 1)
                }
            )

        except Exception as e:
            logger.error(f"Disk space check failed: {e}")
            return ComponentHealth(
                status="unhealthy",
                message=f"Check failed: {str(e)}"
            )

    def _check_memory(self) -> ComponentHealth:
        """Check memory usage."""
        try:
            memory = psutil.virtual_memory()
            used_percent = memory.percent

            if used_percent > 90:
                status = "unhealthy"
                message = f"Critical: {used_percent:.1f}% used"
            elif used_percent > 85:
                status = "degraded"
                message = f"Warning: {used_percent:.1f}% used"
            else:
                status = "healthy"
                message = f"{used_percent:.1f}% used"

            return ComponentHealth(
                status=status,
                message=message,
                details={
                    "total_gb": round(memory.total / (1024**3), 2),
                    "available_gb": round(memory.available / (1024**3), 2),
                    "used_gb": round(memory.used / (1024**3), 2),
                    "percent_used": round(used_percent, 1)
                }
            )

        except Exception as e:
            logger.error(f"Memory check failed: {e}")
            return ComponentHealth(
                status="unhealthy",
                message=f"Check failed: {str(e)}"
            )

    def _determine_overall_status(self, components: Dict[str, ComponentHealth]) -> str:
        """
        Determine overall system status based on component health.

        Args:
            components: Dictionary of component health statuses

        Returns:
            Overall status: "healthy", "degraded", or "unhealthy"
        """
        # Count component statuses
        unhealthy_count = sum(1 for c in components.values() if c.status == "unhealthy")
        degraded_count = sum(1 for c in components.values() if c.status == "degraded")

        # Critical components (MongoDB must be healthy)
        critical_unhealthy = components.get("mongodb") and components["mongodb"].status == "unhealthy"

        if critical_unhealthy or unhealthy_count >= 2:
            return "unhealthy"
        elif unhealthy_count > 0 or degraded_count >= 2:
            return "degraded"
        else:
            return "healthy"

    def _get_system_info(self) -> Dict[str, Any]:
        """Get detailed system information."""
        try:
            import platform

            return {
                "python_version": platform.python_version(),
                "platform": platform.platform(),
                "processor": platform.processor(),
                "cpu_count": psutil.cpu_count(),
                "cpu_percent": psutil.cpu_percent(interval=0.1),
                "hostname": platform.node(),
                "boot_time": datetime.fromtimestamp(psutil.boot_time()).isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get system info: {e}")
            return {"error": str(e)}


# Global health service instance
_health_service: Optional[HealthService] = None


def get_health_service() -> HealthService:
    """Get or create the global health service instance."""
    global _health_service
    if _health_service is None:
        _health_service = HealthService()
    return _health_service

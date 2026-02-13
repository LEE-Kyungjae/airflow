"""
Startup Checks - Pre-flight system validation.

Features:
- Database connectivity verification
- Required environment variables check
- System information logging
- Graceful degradation for optional components
"""

import os
import sys
import platform
import psutil
from typing import Dict, Any, List, Tuple
from datetime import datetime

from app.core import get_logger

logger = get_logger(__name__)


class StartupChecks:
    """System startup validation and pre-flight checks."""

    def __init__(self):
        self.checks_passed = 0
        self.checks_failed = 0
        self.warnings = []

    async def run_all_checks(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Run all startup checks.

        Returns:
            Tuple of (all_passed, results_dict)
        """
        logger.info("=" * 60)
        logger.info("Starting Pre-Flight System Checks")
        logger.info("=" * 60)

        results = {
            "timestamp": datetime.utcnow().isoformat(),
            "checks": {},
            "system_info": {},
            "warnings": []
        }

        # Log system information
        results["system_info"] = self._log_system_info()

        # Check required environment variables
        env_check = self._check_environment_variables()
        results["checks"]["environment"] = env_check
        self._update_counters(env_check["passed"])

        # Check MongoDB connectivity
        mongo_check = await self._check_mongodb()
        results["checks"]["mongodb"] = mongo_check
        self._update_counters(mongo_check["passed"])

        # Check PostgreSQL connectivity (optional)
        pg_check = await self._check_postgresql()
        results["checks"]["postgresql"] = pg_check
        if not pg_check["passed"] and pg_check.get("optional"):
            self.warnings.append(pg_check["message"])
        else:
            self._update_counters(pg_check["passed"])

        # Check disk space
        disk_check = self._check_disk_space()
        results["checks"]["disk_space"] = disk_check
        if not disk_check["passed"]:
            self.warnings.append(disk_check["message"])

        # Check memory
        memory_check = self._check_memory()
        results["checks"]["memory"] = memory_check
        if not memory_check["passed"]:
            self.warnings.append(memory_check["message"])

        # Summary
        results["warnings"] = self.warnings
        results["summary"] = {
            "passed": self.checks_passed,
            "failed": self.checks_failed,
            "warnings": len(self.warnings)
        }

        all_passed = self.checks_failed == 0

        # Log results
        self._log_results(results, all_passed)

        return all_passed, results

    def _log_system_info(self) -> Dict[str, Any]:
        """Log and return system information."""
        info = {
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "processor": platform.processor(),
            "cpu_count": psutil.cpu_count(),
            "memory_total_gb": round(psutil.virtual_memory().total / (1024**3), 2),
            "disk_total_gb": round(psutil.disk_usage('/').total / (1024**3), 2),
            "hostname": platform.node(),
            "app_version": os.getenv("APP_VERSION", "1.0.0"),
            "environment": os.getenv("ENV", "development")
        }

        logger.info("\n📊 System Information:")
        logger.info(f"  Python Version: {info['python_version']}")
        logger.info(f"  Platform: {info['platform']}")
        logger.info(f"  CPU Count: {info['cpu_count']}")
        logger.info(f"  Memory: {info['memory_total_gb']} GB")
        logger.info(f"  Disk: {info['disk_total_gb']} GB")
        logger.info(f"  Environment: {info['environment']}")
        logger.info(f"  App Version: {info['app_version']}")

        return info

    def _check_environment_variables(self) -> Dict[str, Any]:
        """Check required environment variables."""
        required_vars = [
            "MONGO_URI",
        ]

        optional_vars = [
            "POSTGRES_URI",
            "OPENAI_API_KEY",
            "AIRFLOW_BASE_URL",
            "ALLOWED_ORIGINS",
            "ENV"
        ]

        missing = []
        present = []
        optional_missing = []

        # Check required
        for var in required_vars:
            if os.getenv(var):
                present.append(var)
            else:
                missing.append(var)

        # Check optional
        for var in optional_vars:
            if not os.getenv(var):
                optional_missing.append(var)

        passed = len(missing) == 0

        result = {
            "passed": passed,
            "message": "All required environment variables present" if passed else f"Missing: {', '.join(missing)}",
            "present": present,
            "missing": missing,
            "optional_missing": optional_missing
        }

        logger.info(f"\n🔐 Environment Variables: {'✅ PASS' if passed else '❌ FAIL'}")
        if missing:
            logger.error(f"  Missing required: {', '.join(missing)}")
        if optional_missing:
            logger.warning(f"  Missing optional: {', '.join(optional_missing)}")

        return result

    async def _check_mongodb(self) -> Dict[str, Any]:
        """Check MongoDB connectivity."""
        try:
            from app.services.mongo_service import MongoService

            mongo = MongoService()
            result = mongo.db.command('ping')
            mongo.close()

            logger.info("🗄️  MongoDB: ✅ PASS - Connected successfully")

            return {
                "passed": True,
                "message": "Connected successfully",
                "details": result
            }

        except Exception as e:
            logger.error(f"🗄️  MongoDB: ❌ FAIL - {str(e)}")
            return {
                "passed": False,
                "message": f"Connection failed: {str(e)}",
                "error": str(e)
            }

    async def _check_postgresql(self) -> Dict[str, Any]:
        """Check PostgreSQL connectivity (optional)."""
        try:
            from app.services.postgres_service import get_pg

            pg = await get_pg()

            if not pg.is_available:
                logger.warning("🐘 PostgreSQL: ⚠️  SKIP - Not configured (optional)")
                return {
                    "passed": True,
                    "optional": True,
                    "message": "Not configured (optional component)"
                }

            # Test query
            async with pg.pool.acquire() as conn:
                result = await conn.fetchval('SELECT version()')

            logger.info("🐘 PostgreSQL: ✅ PASS - Connected successfully")

            return {
                "passed": True,
                "message": "Connected successfully",
                "version": result
            }

        except ImportError:
            logger.warning("🐘 PostgreSQL: ⚠️  SKIP - asyncpg not installed (optional)")
            return {
                "passed": True,
                "optional": True,
                "message": "asyncpg not installed (optional component)"
            }
        except Exception as e:
            logger.error(f"🐘 PostgreSQL: ❌ FAIL - {str(e)}")
            return {
                "passed": False,
                "message": f"Connection failed: {str(e)}",
                "error": str(e)
            }

    def _check_disk_space(self) -> Dict[str, Any]:
        """Check available disk space."""
        try:
            disk = psutil.disk_usage('/')
            available_gb = disk.free / (1024**3)
            available_percent = (disk.free / disk.total) * 100

            # Warn if less than 20% or 10GB available
            passed = available_percent > 10 and available_gb > 5

            status = "✅ PASS" if passed else "⚠️  WARNING"
            logger.info(f"💾 Disk Space: {status} - {available_gb:.2f} GB free ({available_percent:.1f}%)")

            return {
                "passed": passed,
                "message": f"{available_gb:.2f} GB free ({available_percent:.1f}%)",
                "available_gb": round(available_gb, 2),
                "available_percent": round(available_percent, 1),
                "total_gb": round(disk.total / (1024**3), 2)
            }

        except Exception as e:
            logger.error(f"💾 Disk Space: ❌ FAIL - {str(e)}")
            return {
                "passed": False,
                "message": f"Check failed: {str(e)}",
                "error": str(e)
            }

    def _check_memory(self) -> Dict[str, Any]:
        """Check available memory."""
        try:
            memory = psutil.virtual_memory()
            available_gb = memory.available / (1024**3)
            available_percent = memory.available / memory.total * 100

            # Warn if less than 15% available
            passed = available_percent > 10

            status = "✅ PASS" if passed else "⚠️  WARNING"
            logger.info(f"🧠 Memory: {status} - {available_gb:.2f} GB free ({available_percent:.1f}%)")

            return {
                "passed": passed,
                "message": f"{available_gb:.2f} GB free ({available_percent:.1f}%)",
                "available_gb": round(available_gb, 2),
                "available_percent": round(available_percent, 1),
                "total_gb": round(memory.total / (1024**3), 2)
            }

        except Exception as e:
            logger.error(f"🧠 Memory: ❌ FAIL - {str(e)}")
            return {
                "passed": False,
                "message": f"Check failed: {str(e)}",
                "error": str(e)
            }

    def _update_counters(self, passed: bool):
        """Update pass/fail counters."""
        if passed:
            self.checks_passed += 1
        else:
            self.checks_failed += 1

    def _log_results(self, results: Dict[str, Any], all_passed: bool):
        """Log final results summary."""
        logger.info("\n" + "=" * 60)
        logger.info("Pre-Flight Check Results")
        logger.info("=" * 60)
        logger.info(f"Passed: {results['summary']['passed']}")
        logger.info(f"Failed: {results['summary']['failed']}")
        logger.info(f"Warnings: {results['summary']['warnings']}")

        if results['warnings']:
            logger.warning("\nWarnings:")
            for warning in results['warnings']:
                logger.warning(f"  ⚠️  {warning}")

        if all_passed:
            logger.info("\n✅ All critical checks passed - System ready")
        else:
            logger.error("\n❌ Some critical checks failed - Review errors above")

        logger.info("=" * 60)


async def run_startup_checks() -> Tuple[bool, Dict[str, Any]]:
    """
    Run startup checks and return results.

    Returns:
        Tuple of (all_passed, results_dict)
    """
    checks = StartupChecks()
    return await checks.run_all_checks()

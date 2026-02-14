"""
Startup secret and environment variable validator.

Validates that required secrets and configuration values are present
and meet minimum security requirements before the application starts.

Checks include:
    - Required variables are set (not empty)
    - Secret values meet minimum length and entropy thresholds
    - Development/default secrets are not used in production
    - Optional variables are reported if missing

Severity categories:
    CRITICAL: Application MUST NOT start without these in production.
    WARNING:  Application can start but functionality is degraded.
    INFO:     Optional configuration, logged for operator awareness.
"""

import math
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

import structlog

logger = structlog.get_logger("secret_validator")


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class SecretRule:
    """Definition of a secret/environment variable validation rule."""
    name: str
    severity: Severity
    description: str
    min_length: int = 0
    min_entropy: float = 0.0
    # Known insecure default values that must not be used in production
    forbidden_values: List[str] = field(default_factory=list)
    # If True, the variable is only required in production
    production_only: bool = False


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    variable: str
    severity: Severity
    passed: bool
    message: str


# ============================================================
# Secret validation rules
# ============================================================

SECRET_RULES: List[SecretRule] = [
    # --- CRITICAL: Application secrets ---
    SecretRule(
        name="JWT_SECRET_KEY",
        severity=Severity.CRITICAL,
        description="JWT token signing secret",
        min_length=32,
        min_entropy=3.0,
        forbidden_values=[
            "secret", "change-me", "your-secret-here",
            "jwt-secret", "development-secret",
        ],
        production_only=True,
    ),
    SecretRule(
        name="ADMIN_PASSWORD",
        severity=Severity.CRITICAL,
        description="Admin login password",
        min_length=12,
        min_entropy=2.5,
        forbidden_values=[
            "admin", "password", "change-this-to-a-strong-password",
            "admin123", "password123",
        ],
        production_only=True,
    ),
    SecretRule(
        name="MONGO_ROOT_PASSWORD",
        severity=Severity.CRITICAL,
        description="MongoDB root password",
        min_length=12,
        min_entropy=2.5,
        forbidden_values=[
            "password", "change-this-to-a-strong-password",
            "mongo", "mongodb",
        ],
        production_only=True,
    ),
    SecretRule(
        name="POSTGRES_PASSWORD",
        severity=Severity.CRITICAL,
        description="PostgreSQL password",
        min_length=12,
        min_entropy=2.5,
        forbidden_values=[
            "password", "change-this-to-a-strong-password",
            "postgres", "airflow",
        ],
        production_only=True,
    ),

    # --- CRITICAL: Encryption keys ---
    SecretRule(
        name="AIRFLOW_FERNET_KEY",
        severity=Severity.CRITICAL,
        description="Airflow connection encryption key",
        min_length=32,
        min_entropy=3.0,
        production_only=True,
    ),

    # --- WARNING: External service credentials ---
    SecretRule(
        name="OPENAI_API_KEY",
        severity=Severity.WARNING,
        description="OpenAI API key for AI features",
        min_length=10,
        forbidden_values=["your-openai-api-key-here", "sk-test"],
    ),
    SecretRule(
        name="SMTP_PASSWORD",
        severity=Severity.WARNING,
        description="SMTP password for email alerts",
        min_length=1,
        forbidden_values=["your-app-password"],
    ),
    SecretRule(
        name="GRAFANA_ADMIN_PASSWORD",
        severity=Severity.WARNING,
        description="Grafana admin password",
        min_length=8,
        forbidden_values=[
            "admin", "change-this-to-a-strong-password",
        ],
        production_only=True,
    ),
    SecretRule(
        name="_AIRFLOW_WWW_USER_PASSWORD",
        severity=Severity.WARNING,
        description="Airflow web UI password",
        min_length=8,
        forbidden_values=[
            "airflow", "change-this-password",
        ],
        production_only=True,
    ),

    # --- INFO: Optional configuration ---
    SecretRule(
        name="ENV",
        severity=Severity.INFO,
        description="Environment identifier (production/development)",
    ),
    SecretRule(
        name="ALLOWED_ORIGINS",
        severity=Severity.INFO,
        description="CORS allowed origins",
    ),
    SecretRule(
        name="ALLOWED_HOSTS",
        severity=Severity.INFO,
        description="Trusted host names (production)",
        production_only=True,
    ),
    SecretRule(
        name="REDIS_URL",
        severity=Severity.INFO,
        description="Redis URL for distributed rate limiting",
    ),
    SecretRule(
        name="AWS_ACCESS_KEY_ID",
        severity=Severity.INFO,
        description="AWS access key for S3 backups",
    ),
    SecretRule(
        name="AWS_SECRET_ACCESS_KEY",
        severity=Severity.INFO,
        description="AWS secret key for S3 backups",
    ),
]


def calculate_entropy(value: str) -> float:
    """
    Calculate Shannon entropy of a string in bits per character.

    Higher entropy indicates more randomness (better for secrets).
    Typical thresholds:
        < 2.0: Very low (dictionary word, repeated chars)
        2.0-3.0: Low (simple passphrase)
        3.0-4.0: Moderate (mixed case + digits)
        > 4.0: High (cryptographically random)
    """
    if not value:
        return 0.0
    length = len(value)
    freq = Counter(value)
    entropy = -sum(
        (count / length) * math.log2(count / length)
        for count in freq.values()
    )
    return round(entropy, 2)


def validate_secret(rule: SecretRule, is_production: bool) -> ValidationResult:
    """Validate a single secret against its rule."""
    value = os.getenv(rule.name, "")

    # Skip production-only checks in development
    if rule.production_only and not is_production:
        if not value:
            return ValidationResult(
                variable=rule.name,
                severity=Severity.INFO,
                passed=True,
                message=f"Not set (optional in development): {rule.description}",
            )

    # Check presence
    if not value:
        if rule.severity == Severity.INFO:
            return ValidationResult(
                variable=rule.name,
                severity=Severity.INFO,
                passed=True,
                message=f"Not set (optional): {rule.description}",
            )
        return ValidationResult(
            variable=rule.name,
            severity=rule.severity,
            passed=False,
            message=f"Missing required variable: {rule.description}",
        )

    # Check forbidden values (case-insensitive)
    if rule.forbidden_values:
        value_lower = value.lower().strip()
        for forbidden in rule.forbidden_values:
            if value_lower == forbidden.lower():
                msg = (
                    f"Using insecure default/forbidden value for "
                    f"{rule.description}. Change this before deploying."
                )
                return ValidationResult(
                    variable=rule.name,
                    severity=rule.severity,
                    passed=False,
                    message=msg,
                )

    # Check minimum length
    if rule.min_length > 0 and len(value) < rule.min_length:
        return ValidationResult(
            variable=rule.name,
            severity=rule.severity,
            passed=False,
            message=(
                f"Value too short ({len(value)} chars, "
                f"minimum {rule.min_length}): {rule.description}"
            ),
        )

    # Check entropy
    if rule.min_entropy > 0:
        entropy = calculate_entropy(value)
        if entropy < rule.min_entropy:
            return ValidationResult(
                variable=rule.name,
                severity=rule.severity,
                passed=False,
                message=(
                    f"Insufficient entropy ({entropy:.2f} bits, "
                    f"minimum {rule.min_entropy:.2f}): {rule.description}. "
                    f"Use a more random value."
                ),
            )

    return ValidationResult(
        variable=rule.name,
        severity=rule.severity,
        passed=True,
        message=f"OK: {rule.description}",
    )


def validate_all_secrets(
    rules: Optional[List[SecretRule]] = None,
    fail_on_critical: bool = True,
) -> List[ValidationResult]:
    """
    Validate all secrets and environment variables.

    Args:
        rules: Custom rules to validate. Defaults to SECRET_RULES.
        fail_on_critical: If True, raise SystemExit on CRITICAL failures
                         in production. Default True.

    Returns:
        List of ValidationResult objects.
    """
    if rules is None:
        rules = SECRET_RULES

    is_production = os.getenv("ENV") == "production"
    results: List[ValidationResult] = []
    critical_failures: List[ValidationResult] = []

    for rule in rules:
        result = validate_secret(rule, is_production)
        results.append(result)

        if not result.passed:
            if result.severity == Severity.CRITICAL:
                logger.error(
                    "secret_validation_failed",
                    variable=result.variable,
                    severity=result.severity.value,
                    message=result.message,
                )
                critical_failures.append(result)
            elif result.severity == Severity.WARNING:
                logger.warning(
                    "secret_validation_warning",
                    variable=result.variable,
                    severity=result.severity.value,
                    message=result.message,
                )
            else:
                logger.info(
                    "secret_validation_info",
                    variable=result.variable,
                    message=result.message,
                )
        else:
            logger.debug(
                "secret_validation_passed",
                variable=result.variable,
                message=result.message,
            )

    # Summary
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    logger.info(
        "secret_validation_summary",
        total=len(results),
        passed=passed,
        failed=failed,
        critical_failures=len(critical_failures),
        environment="production" if is_production else "development",
    )

    # In production, CRITICAL failures prevent startup
    if is_production and critical_failures and fail_on_critical:
        logger.critical(
            "startup_blocked",
            reason="Critical secret validation failures",
            failed_variables=[r.variable for r in critical_failures],
        )
        sys.exit(1)

    return results


def get_validation_report() -> Dict[str, object]:
    """
    Generate a validation report suitable for an admin-only API endpoint.

    Returns:
        Dictionary with summary and per-variable results.
        Secret values are NEVER included in the report.
    """
    is_production = os.getenv("ENV") == "production"
    results = []
    for rule in SECRET_RULES:
        result = validate_secret(rule, is_production)
        results.append({
            "variable": result.variable,
            "severity": result.severity.value,
            "passed": result.passed,
            "message": result.message,
            # Indicate presence without revealing value
            "is_set": bool(os.getenv(rule.name, "")),
        })

    passed = sum(1 for r in results if r["passed"])
    return {
        "environment": "production" if is_production else "development",
        "total_checks": len(results),
        "passed": passed,
        "failed": len(results) - passed,
        "results": results,
    }

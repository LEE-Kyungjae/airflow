"""
Central alert dispatching service.

Coordinates alert delivery across multiple channels with throttling
and persistence support.
"""

import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List

from app.core import get_logger
from .throttle import AlertThrottle, AlertSeverity
from .email_client import EmailClient
from .slack_client import SlackClient
from .discord_client import DiscordClient
from .webhook_client import WebhookClient

logger = get_logger(__name__)

# Re-export AlertSeverity for convenience
__all__ = ["AlertDispatcher", "AlertSeverity"]


class AlertDispatcher:
    """
    Central alert dispatching service with throttling.

    Sends alerts to all configured channels:
    - Email (SMTP)
    - Slack (webhook)
    - Discord (webhook)
    - Generic webhooks

    Features:
    - Automatic throttling based on severity
    - Parallel delivery to all channels
    - Optional MongoDB persistence
    """

    def __init__(self, mongo_service=None):
        """
        Initialize alert dispatcher.

        Args:
            mongo_service: Optional MongoDB service for alert persistence
        """
        self.mongo = mongo_service
        self.email_client = EmailClient()
        self.slack_client = SlackClient()
        self.discord_client = DiscordClient()
        self.webhook_client = WebhookClient()
        self.throttle = AlertThrottle()

    async def send_alert(
        self,
        title: str,
        message: str,
        severity: AlertSeverity,
        source_id: Optional[str] = None,
        error_code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        skip_throttle: bool = False,
    ) -> Dict[str, Any]:
        """
        Send alert to all configured channels with throttling.

        Args:
            title: Alert title/subject
            message: Alert message body
            severity: Severity level (info, warning, error, critical)
            source_id: Optional source identifier
            error_code: Optional error code for classification
            metadata: Optional additional data to include
            skip_throttle: If True, bypass throttling (use sparingly)

        Returns:
            Dictionary with send results:
            {
                "sent": bool,
                "throttled": bool,
                "channels": {"email": bool, "slack": bool, ...},
                "alert_id": str (if persisted)
            }
        """
        # Generate alert key for throttling
        alert_key = self._generate_alert_key(title, source_id, error_code)

        # Check throttling
        if not skip_throttle and self.throttle.should_throttle(alert_key, severity):
            logger.info(
                "Alert throttled",
                alert_key=alert_key,
                severity=severity.value,
                title=title,
            )
            return {
                "sent": False,
                "throttled": True,
                "reason": "Rate limit exceeded for this alert type",
                "throttle_status": self.throttle.get_throttle_status(alert_key, severity),
            }

        # Build alert data
        alert_data = {
            "title": title,
            "message": message,
            "severity": severity.value,
            "source_id": source_id,
            "error_code": error_code,
            "metadata": metadata or {},
            "created_at": datetime.utcnow(),
        }

        # Store alert if MongoDB is configured
        alert_id = None
        if self.mongo:
            alert_id = await self._store_alert(alert_data)

        # Send to all channels in parallel
        results = await asyncio.gather(
            self._send_email(title, message, severity, source_id, error_code, metadata),
            self._send_slack(title, message, severity, source_id, error_code, metadata),
            self._send_discord(title, message, severity, source_id, error_code, metadata),
            self._send_webhooks(title, message, severity, source_id, error_code, metadata),
            return_exceptions=True,
        )

        # Process results
        channel_results = {
            "email": self._process_result(results[0]),
            "slack": self._process_result(results[1]),
            "discord": self._process_result(results[2]),
            "webhook": self._process_result(results[3]),
        }

        # Record throttle only if at least one channel succeeded
        any_success = any(channel_results.values())
        if any_success:
            self.throttle.record_sent(alert_key)

        logger.info(
            "Alert dispatched",
            title=title,
            severity=severity.value,
            channels=channel_results,
            any_success=any_success,
        )

        return {
            "sent": any_success,
            "throttled": False,
            "channels": channel_results,
            "alert_id": str(alert_id) if alert_id else None,
        }

    async def send_error_alert(
        self,
        error_type: str,
        error_message: str,
        source_name: str,
        source_url: str,
        error_code: Optional[str] = None,
        stack_trace: Optional[str] = None,
        auto_recoverable: bool = False,
    ) -> Dict[str, Any]:
        """
        Convenience method for sending crawler error alerts.

        Args:
            error_type: Type of error (e.g., "ConnectionError", "ParseError")
            error_message: Error message
            source_name: Name of the source
            source_url: URL of the source
            error_code: Error classification code
            stack_trace: Optional stack trace
            auto_recoverable: Whether error can be auto-recovered

        Returns:
            Alert dispatch result
        """
        severity = AlertSeverity.WARNING if auto_recoverable else AlertSeverity.ERROR

        title = f"Crawler Error: {error_type}"
        message = f"""
소스: {source_name}
URL: {source_url}
에러 타입: {error_type}
메시지: {error_message}
자동 복구: {'가능' if auto_recoverable else '불가능 - 수동 조치 필요'}
        """.strip()

        metadata = {
            "source_url": source_url,
            "auto_recoverable": auto_recoverable,
        }
        if stack_trace:
            metadata["stack_trace"] = stack_trace[:1000]  # Truncate

        return await self.send_alert(
            title=title,
            message=message,
            severity=severity,
            source_id=source_name,
            error_code=error_code,
            metadata=metadata,
        )

    async def send_healing_failed_alert(
        self,
        source_id: str,
        error_code: str,
        attempts: int,
        max_attempts: int,
        diagnosis: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send alert when self-healing fails and requires admin intervention.

        Args:
            source_id: Source identifier
            error_code: Error classification code
            attempts: Number of healing attempts made
            max_attempts: Maximum attempts allowed
            diagnosis: Diagnosis information if available

        Returns:
            Alert dispatch result
        """
        title = "Self-Healing Failed - Admin Intervention Required"
        message = f"""
소스 ID: {source_id}
에러 코드: {error_code}
시도 횟수: {attempts}/{max_attempts}

자동 복구 시도가 실패했습니다.
관리자의 수동 조치가 필요합니다.
        """.strip()

        if diagnosis:
            message += f"\n\n진단 결과:\n{diagnosis}"

        return await self.send_alert(
            title=title,
            message=message,
            severity=AlertSeverity.CRITICAL,
            source_id=source_id,
            error_code=error_code,
            metadata={
                "attempts": attempts,
                "max_attempts": max_attempts,
                "requires_admin": True,
            },
            skip_throttle=True,  # Critical alerts bypass throttle
        )

    def _generate_alert_key(
        self,
        title: str,
        source_id: Optional[str],
        error_code: Optional[str],
    ) -> str:
        """Generate unique key for throttling similar alerts."""
        parts = [title]
        if source_id:
            parts.append(source_id)
        if error_code:
            parts.append(error_code)
        return ":".join(parts)

    def _process_result(self, result) -> bool:
        """Process async gather result, handling exceptions."""
        if isinstance(result, Exception):
            return False
        return bool(result)

    async def _send_email(self, title, message, severity, source_id, error_code, metadata) -> bool:
        """Send via email client."""
        try:
            return await self.email_client.send(
                title=title,
                message=message,
                severity=severity,
                source_id=source_id,
                error_code=error_code,
                metadata=metadata,
            )
        except Exception as e:
            logger.error("Email send failed", error=str(e))
            return False

    async def _send_slack(self, title, message, severity, source_id, error_code, metadata) -> bool:
        """Send via Slack client."""
        try:
            return await self.slack_client.send(
                title=title,
                message=message,
                severity=severity,
                source_id=source_id,
                error_code=error_code,
                metadata=metadata,
            )
        except Exception as e:
            logger.error("Slack send failed", error=str(e))
            return False

    async def _send_discord(self, title, message, severity, source_id, error_code, metadata) -> bool:
        """Send via Discord client."""
        try:
            return await self.discord_client.send(
                title=title,
                message=message,
                severity=severity,
                source_id=source_id,
                error_code=error_code,
                metadata=metadata,
            )
        except Exception as e:
            logger.error("Discord send failed", error=str(e))
            return False

    async def _send_webhooks(self, title, message, severity, source_id, error_code, metadata) -> bool:
        """Send via webhook client."""
        try:
            return await self.webhook_client.send(
                title=title,
                message=message,
                severity=severity,
                source_id=source_id,
                error_code=error_code,
                metadata=metadata,
            )
        except Exception as e:
            logger.error("Webhook send failed", error=str(e))
            return False

    async def _store_alert(self, alert_data: dict) -> Optional[str]:
        """Store alert in MongoDB."""
        try:
            result = self.mongo.db.alerts.insert_one(alert_data)
            return str(result.inserted_id)
        except Exception as e:
            logger.error("Failed to store alert", error=str(e))
            return None

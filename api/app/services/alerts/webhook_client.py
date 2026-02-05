"""
Generic webhook client for sending alert notifications to custom endpoints.

Sends JSON payloads to configured webhook URLs.
"""

import os
from datetime import datetime
from typing import Optional, List

import httpx

from app.core import get_logger
from .throttle import AlertSeverity

logger = get_logger(__name__)


class WebhookClient:
    """
    Generic webhook client for sending alert notifications.

    Configuration via environment variable:
    - ALERT_WEBHOOK_URLS: Comma-separated list of webhook URLs
    """

    def __init__(self):
        """Initialize webhook client with environment configuration."""
        urls_str = os.getenv("ALERT_WEBHOOK_URLS", "")
        self.webhook_urls: List[str] = [
            url.strip() for url in urls_str.split(",") if url.strip()
        ]
        self.enabled = bool(self.webhook_urls)

        if not self.enabled:
            logger.debug("Webhook client not configured. Set ALERT_WEBHOOK_URLS to enable.")

    async def send(
        self,
        title: str,
        message: str,
        severity: AlertSeverity,
        source_id: Optional[str] = None,
        error_code: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> bool:
        """
        Send an alert to all configured webhooks.

        Args:
            title: Alert title
            message: Alert message body
            severity: Alert severity level
            source_id: Optional source identifier
            error_code: Optional error code
            metadata: Optional additional metadata

        Returns:
            True if at least one webhook succeeded, False otherwise
        """
        if not self.enabled:
            logger.debug("Webhook client disabled, skipping send")
            return False

        # Build standard payload
        payload = {
            "alert": {
                "title": title,
                "message": message,
                "severity": severity.value,
                "timestamp": datetime.utcnow().isoformat(),
            },
            "context": {
                "source_id": source_id,
                "error_code": error_code,
            },
            "metadata": metadata or {},
        }

        success_count = 0

        async with httpx.AsyncClient() as client:
            for url in self.webhook_urls:
                try:
                    response = await client.post(
                        url,
                        json=payload,
                        timeout=10.0,
                        headers={
                            "Content-Type": "application/json",
                            "User-Agent": "CrawlerSystem-AlertService/1.0",
                        },
                    )

                    if response.status_code < 300:
                        success_count += 1
                        logger.info(
                            "Alert sent to webhook",
                            url=url[:50],  # Truncate for logging
                            status_code=response.status_code,
                        )
                    else:
                        logger.warning(
                            "Webhook returned non-success status",
                            url=url[:50],
                            status_code=response.status_code,
                        )

                except Exception as e:
                    logger.error(
                        "Error sending to webhook",
                        url=url[:50],
                        error=str(e),
                    )

        if success_count > 0:
            logger.info(
                "Webhooks sent",
                success=success_count,
                total=len(self.webhook_urls),
                severity=severity.value,
            )

        return success_count > 0

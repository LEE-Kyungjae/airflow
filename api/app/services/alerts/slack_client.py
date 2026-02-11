"""
Slack webhook client for sending alert notifications.

Uses Slack's Block Kit for rich message formatting.
"""

import os
from typing import Optional

import httpx

try:
    from app.core import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
from .throttle import AlertSeverity


class SlackClient:
    """
    Slack webhook client for sending alert notifications.

    Configuration via environment variable:
    - SLACK_WEBHOOK_URL: Slack incoming webhook URL
    """

    def __init__(self):
        """Initialize Slack client with environment configuration."""
        self.webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        self.enabled = bool(self.webhook_url)

        if not self.enabled:
            logger.debug("Slack client not configured. Set SLACK_WEBHOOK_URL to enable.")

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
        Send an alert to Slack.

        Args:
            title: Alert title
            message: Alert message body
            severity: Alert severity level
            source_id: Optional source identifier
            error_code: Optional error code
            metadata: Optional additional metadata

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            logger.debug("Slack client disabled, skipping send")
            return False

        try:
            emoji = self._get_emoji(severity)
            color = self._get_color(severity)

            # Build Slack Block Kit payload
            payload = {
                "attachments": [
                    {
                        "color": color,
                        "blocks": [
                            {
                                "type": "header",
                                "text": {
                                    "type": "plain_text",
                                    "text": f"{emoji} {title}",
                                    "emoji": True,
                                },
                            },
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": message[:3000],  # Slack limit
                                },
                            },
                            {
                                "type": "context",
                                "elements": [
                                    {
                                        "type": "mrkdwn",
                                        "text": f"*Severity:* {severity.value.upper()}",
                                    },
                                ],
                            },
                        ],
                    }
                ]
            }

            # Add source and error code to context if provided
            context_elements = payload["attachments"][0]["blocks"][2]["elements"]
            if source_id:
                context_elements.append({
                    "type": "mrkdwn",
                    "text": f"*Source:* {source_id}",
                })
            if error_code:
                context_elements.append({
                    "type": "mrkdwn",
                    "text": f"*Error:* {error_code}",
                })

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                    timeout=10.0,
                )

            if response.status_code == 200:
                logger.info(
                    "Alert sent to Slack",
                    severity=severity.value,
                    title=title,
                )
                return True
            else:
                logger.error(
                    "Failed to send Slack alert",
                    status_code=response.status_code,
                    response=response.text,
                )
                return False

        except Exception as e:
            logger.error(
                "Error sending Slack alert",
                error=str(e),
                severity=severity.value,
            )
            return False

    def _get_emoji(self, severity: AlertSeverity) -> str:
        """Get emoji for severity level."""
        emojis = {
            AlertSeverity.INFO: "ℹ️",
            AlertSeverity.WARNING: "⚠️",
            AlertSeverity.ERROR: "🚨",
            AlertSeverity.CRITICAL: "🔴",
        }
        return emojis.get(severity, "📢")

    def _get_color(self, severity: AlertSeverity) -> str:
        """Get color hex code for severity level."""
        colors = {
            AlertSeverity.INFO: "#3498db",
            AlertSeverity.WARNING: "#f39c12",
            AlertSeverity.ERROR: "#e74c3c",
            AlertSeverity.CRITICAL: "#8e44ad",
        }
        return colors.get(severity, "#95a5a6")

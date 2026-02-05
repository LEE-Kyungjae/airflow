"""
Discord webhook client for sending alert notifications.

Uses Discord's embed format for rich message display.
"""

import os
from datetime import datetime
from typing import Optional

import httpx

from app.core import get_logger
from .throttle import AlertSeverity

logger = get_logger(__name__)


class DiscordClient:
    """
    Discord webhook client for sending alert notifications.

    Configuration via environment variable:
    - DISCORD_WEBHOOK_URL: Discord incoming webhook URL
    """

    def __init__(self):
        """Initialize Discord client with environment configuration."""
        self.webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        self.enabled = bool(self.webhook_url)

        if not self.enabled:
            logger.debug("Discord client not configured. Set DISCORD_WEBHOOK_URL to enable.")

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
        Send an alert to Discord.

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
            logger.debug("Discord client disabled, skipping send")
            return False

        try:
            emoji = self._get_emoji(severity)
            color = self._get_color_int(severity)

            # Build Discord embed
            embed = {
                "title": f"{emoji} {title}",
                "description": message[:4096],  # Discord limit
                "color": color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {
                    "text": f"Severity: {severity.value.upper()}"
                },
                "fields": [],
            }

            # Add optional fields
            if source_id:
                embed["fields"].append({
                    "name": "Source",
                    "value": source_id,
                    "inline": True,
                })
            if error_code:
                embed["fields"].append({
                    "name": "Error Code",
                    "value": error_code,
                    "inline": True,
                })

            # Add metadata fields
            if metadata:
                for key, value in list(metadata.items())[:5]:  # Limit fields
                    embed["fields"].append({
                        "name": str(key),
                        "value": str(value)[:1024],
                        "inline": True,
                    })

            payload = {
                "username": "Crawler System",
                "embeds": [embed],
            }

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                    timeout=10.0,
                )

            # Discord returns 204 No Content on success
            if response.status_code in (200, 204):
                logger.info(
                    "Alert sent to Discord",
                    severity=severity.value,
                    title=title,
                )
                return True
            else:
                logger.error(
                    "Failed to send Discord alert",
                    status_code=response.status_code,
                    response=response.text,
                )
                return False

        except Exception as e:
            logger.error(
                "Error sending Discord alert",
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

    def _get_color_int(self, severity: AlertSeverity) -> int:
        """Get color as integer for Discord embed."""
        colors = {
            AlertSeverity.INFO: 0x3498DB,
            AlertSeverity.WARNING: 0xF39C12,
            AlertSeverity.ERROR: 0xE74C3C,
            AlertSeverity.CRITICAL: 0x8E44AD,
        }
        return colors.get(severity, 0x95A5A6)

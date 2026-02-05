"""
Alert service package for multi-channel notifications.

Provides a unified interface for sending alerts through various channels:
- Email (SMTP)
- Slack (Webhook)
- Discord (Webhook)
- Generic Webhook
"""

from .dispatcher import AlertDispatcher, AlertSeverity
from .email_client import EmailClient
from .slack_client import SlackClient
from .discord_client import DiscordClient
from .webhook_client import WebhookClient
from .throttle import AlertThrottle

__all__ = [
    "AlertDispatcher",
    "AlertSeverity",
    "EmailClient",
    "SlackClient",
    "DiscordClient",
    "WebhookClient",
    "AlertThrottle",
]

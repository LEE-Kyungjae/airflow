"""
Email client for sending alert notifications via SMTP.

Supports HTML email templates with severity-based styling.
"""

import os
import smtplib
import asyncio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List, Optional

try:
    from app.core import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
from .throttle import AlertSeverity


class EmailClient:
    """
    SMTP email client for sending alert notifications.

    Configuration via environment variables:
    - SMTP_HOST: SMTP server hostname
    - SMTP_PORT: SMTP server port (default: 587)
    - SMTP_USER: SMTP username/email
    - SMTP_PASSWORD: SMTP password or app password
    - ALERT_EMAIL: Comma-separated recipient email addresses
    """

    def __init__(self):
        """Initialize email client with environment configuration."""
        self.host = os.getenv("SMTP_HOST")
        self.port = int(os.getenv("SMTP_PORT", "587"))
        self.user = os.getenv("SMTP_USER")
        self.password = os.getenv("SMTP_PASSWORD")
        self.from_email = os.getenv("SMTP_FROM", self.user)

        # Parse recipient list
        recipients_str = os.getenv("ALERT_EMAIL", "")
        self.recipients = [
            r.strip() for r in recipients_str.split(",") if r.strip()
        ]

        # Check if properly configured
        self.enabled = all([self.host, self.user, self.password, self.recipients])

        if not self.enabled:
            logger.warning(
                "Email client not configured. Set SMTP_HOST, SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL"
            )

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
        Send an alert email.

        Args:
            title: Email subject/title
            message: Alert message body
            severity: Alert severity level
            source_id: Optional source identifier
            error_code: Optional error code
            metadata: Optional additional metadata

        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.debug("Email client disabled, skipping send")
            return False

        if not self.recipients:
            logger.warning("No email recipients configured")
            return False

        try:
            # Create email message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[{severity.value.upper()}] {title}"
            msg["From"] = self.from_email
            msg["To"] = ", ".join(self.recipients)

            # Build HTML content
            html_content = self._build_html_template(
                title=title,
                message=message,
                severity=severity,
                source_id=source_id,
                error_code=error_code,
                metadata=metadata,
            )

            # Attach both plain text and HTML
            text_content = f"{title}\n\n{message}"
            if source_id:
                text_content += f"\n\nSource: {source_id}"
            if error_code:
                text_content += f"\nError Code: {error_code}"

            msg.attach(MIMEText(text_content, "plain"))
            msg.attach(MIMEText(html_content, "html"))

            # Send email in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._send_sync, msg)

            logger.info(
                "Alert email sent",
                recipients=self.recipients,
                severity=severity.value,
                title=title,
            )
            return True

        except Exception as e:
            logger.error(
                "Failed to send alert email",
                error=str(e),
                severity=severity.value,
                title=title,
            )
            return False

    def _send_sync(self, msg: MIMEMultipart) -> None:
        """
        Synchronous email send (run in thread pool).

        Args:
            msg: Email message to send
        """
        with smtplib.SMTP(self.host, self.port) as server:
            server.starttls()
            server.login(self.user, self.password)
            server.send_message(msg)

    def _build_html_template(
        self,
        title: str,
        message: str,
        severity: AlertSeverity,
        source_id: Optional[str] = None,
        error_code: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> str:
        """
        Build HTML email template with severity-based styling.

        Args:
            title: Alert title
            message: Alert message
            severity: Severity level for styling
            source_id: Optional source identifier
            error_code: Optional error code
            metadata: Optional additional data

        Returns:
            HTML string for email body
        """
        # Severity-based colors
        colors = {
            AlertSeverity.INFO: ("#3498db", "#ebf5fb"),
            AlertSeverity.WARNING: ("#f39c12", "#fef9e7"),
            AlertSeverity.ERROR: ("#e74c3c", "#fdedec"),
            AlertSeverity.CRITICAL: ("#8e44ad", "#f5eef8"),
        }

        border_color, bg_color = colors.get(severity, ("#95a5a6", "#f8f9fa"))

        # Build metadata section if provided
        metadata_html = ""
        if metadata:
            metadata_items = "".join(
                f"<tr><td style='padding: 5px; font-weight: bold;'>{k}:</td><td style='padding: 5px;'>{v}</td></tr>"
                for k, v in metadata.items()
            )
            metadata_html = f"""
            <table style="margin-top: 15px; border-collapse: collapse;">
                {metadata_items}
            </table>
            """

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
        </head>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; background-color: #f5f5f5;">
            <div style="max-width: 600px; margin: 0 auto; background-color: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <!-- Header -->
                <div style="background-color: {border_color}; color: white; padding: 20px;">
                    <h2 style="margin: 0; font-size: 18px;">🚨 Crawler System Alert</h2>
                </div>

                <!-- Content -->
                <div style="padding: 20px;">
                    <div style="background-color: {bg_color}; border-left: 4px solid {border_color}; padding: 15px; margin-bottom: 20px;">
                        <h3 style="margin: 0 0 10px 0; color: {border_color};">
                            [{severity.value.upper()}] {title}
                        </h3>
                        <p style="margin: 0; color: #333; white-space: pre-wrap;">{message}</p>
                    </div>

                    <!-- Details -->
                    <div style="color: #666; font-size: 14px;">
                        {f'<p><strong>Source:</strong> {source_id}</p>' if source_id else ''}
                        {f'<p><strong>Error Code:</strong> {error_code}</p>' if error_code else ''}
                        <p><strong>Time:</strong> {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} UTC</p>
                    </div>

                    {metadata_html}
                </div>

                <!-- Footer -->
                <div style="background-color: #f8f9fa; padding: 15px; text-align: center; color: #666; font-size: 12px;">
                    <p style="margin: 0;">This alert was sent by Crawler System API</p>
                </div>
            </div>
        </body>
        </html>
        """

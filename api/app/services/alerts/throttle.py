"""
Alert throttling mechanism to prevent notification flooding.

Implements time-window and count-based throttling based on severity levels.
"""

from datetime import datetime, timedelta
from collections import defaultdict
from enum import Enum
from typing import Dict, List


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertThrottle:
    """
    Prevents alert flooding with time-based and count-based throttling.

    Different severity levels have different throttle windows:
    - CRITICAL: 5 minutes (urgent, allow more frequent alerts)
    - ERROR: 15 minutes
    - WARNING: 30 minutes
    - INFO: 1 hour
    """

    # Throttle windows by severity
    THROTTLE_WINDOWS: Dict[AlertSeverity, timedelta] = {
        AlertSeverity.INFO: timedelta(hours=1),
        AlertSeverity.WARNING: timedelta(minutes=30),
        AlertSeverity.ERROR: timedelta(minutes=15),
        AlertSeverity.CRITICAL: timedelta(minutes=5),
    }

    # Max alerts per window by severity
    MAX_PER_WINDOW: Dict[AlertSeverity, int] = {
        AlertSeverity.INFO: 5,
        AlertSeverity.WARNING: 10,
        AlertSeverity.ERROR: 20,
        AlertSeverity.CRITICAL: 50,
    }

    def __init__(self):
        """Initialize throttle with empty sent alerts tracking."""
        self._sent_alerts: Dict[str, List[datetime]] = defaultdict(list)

    def should_throttle(self, alert_key: str, severity: AlertSeverity) -> bool:
        """
        Check if an alert should be throttled.

        Args:
            alert_key: Unique key identifying the alert type/source
            severity: Alert severity level

        Returns:
            True if alert should be throttled, False otherwise
        """
        window = self.THROTTLE_WINDOWS.get(severity, timedelta(minutes=15))
        max_count = self.MAX_PER_WINDOW.get(severity, 10)
        cutoff = datetime.utcnow() - window

        # Clean old entries outside the window
        self._sent_alerts[alert_key] = [
            t for t in self._sent_alerts[alert_key] if t > cutoff
        ]

        # Check if we've exceeded the limit
        return len(self._sent_alerts[alert_key]) >= max_count

    def record_sent(self, alert_key: str) -> None:
        """
        Record that an alert was sent.

        Args:
            alert_key: Unique key identifying the alert type/source
        """
        self._sent_alerts[alert_key].append(datetime.utcnow())

    def get_throttle_status(self, alert_key: str, severity: AlertSeverity) -> dict:
        """
        Get the current throttle status for an alert key.

        Args:
            alert_key: Unique key identifying the alert type/source
            severity: Alert severity level

        Returns:
            Dictionary with throttle status information
        """
        window = self.THROTTLE_WINDOWS.get(severity, timedelta(minutes=15))
        max_count = self.MAX_PER_WINDOW.get(severity, 10)
        cutoff = datetime.utcnow() - window

        recent_count = len([
            t for t in self._sent_alerts[alert_key] if t > cutoff
        ])

        return {
            "alert_key": alert_key,
            "severity": severity.value,
            "window_minutes": window.total_seconds() / 60,
            "max_per_window": max_count,
            "recent_count": recent_count,
            "throttled": recent_count >= max_count,
            "remaining": max(0, max_count - recent_count),
        }

    def clear(self, alert_key: str = None) -> None:
        """
        Clear throttle history.

        Args:
            alert_key: If provided, clear only this key. Otherwise clear all.
        """
        if alert_key:
            self._sent_alerts.pop(alert_key, None)
        else:
            self._sent_alerts.clear()

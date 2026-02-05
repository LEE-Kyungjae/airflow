"""
ETL Observability Module - Comprehensive monitoring for data pipelines.

Components:
- MetricsCollector: Pipeline execution metrics collection and storage
- AlertRuleEngine: Configurable alert rules and automated triggers
- SLAMonitor: SLA definition, tracking, and breach detection
- FreshnessTracker: Data freshness monitoring and staleness detection
- ObservabilityDashboard: Aggregated metrics and KPIs for dashboards
- PrometheusMetricsExporter: Prometheus 포맷 메트릭 내보내기

Collections:
- pipeline_metrics: Execution metrics per run
- alert_rules: Alert rule definitions
- alert_history: Alert trigger history
- sla_definitions: SLA configurations
- sla_breaches: SLA violation records
- freshness_config: Freshness requirements per source
"""

from .metrics import MetricsCollector, PipelineMetric, MetricType
from .alerts import AlertRuleEngine, AlertRule, AlertCondition, AlertAction
from .sla import SLAMonitor, SLADefinition, SLABreach, SLAType
from .freshness import FreshnessTracker, FreshnessConfig, FreshnessStatus
from .dashboard import ObservabilityDashboard
from .prometheus import (
    PrometheusMetricsExporter,
    PrometheusRegistry,
    get_registry,
    track_execution_time,
    count_calls,
    record_pipeline_execution,
    record_healing_event,
    record_schema_drift,
    record_data_quality,
)

__all__ = [
    # Metrics
    "MetricsCollector",
    "PipelineMetric",
    "MetricType",
    # Alerts
    "AlertRuleEngine",
    "AlertRule",
    "AlertCondition",
    "AlertAction",
    # SLA
    "SLAMonitor",
    "SLADefinition",
    "SLABreach",
    "SLAType",
    # Freshness
    "FreshnessTracker",
    "FreshnessConfig",
    "FreshnessStatus",
    # Dashboard
    "ObservabilityDashboard",
    # Prometheus
    "PrometheusMetricsExporter",
    "PrometheusRegistry",
    "get_registry",
    "track_execution_time",
    "count_calls",
    "record_pipeline_execution",
    "record_healing_event",
    "record_schema_drift",
    "record_data_quality",
]

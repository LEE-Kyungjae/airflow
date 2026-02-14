"""
Prometheus Metrics Exporter - 프로메테우스 메트릭 내보내기

기능:
- Prometheus 포맷 메트릭 노출 (/metrics)
- 커스텀 ETL 메트릭 정의
- Pushgateway 지원 (Airflow 배치 작업용)
- 레이블 기반 다차원 메트릭
"""

import time
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from functools import wraps
from dataclasses import dataclass, field
from enum import Enum
import threading
import logging

logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """프로메테우스 메트릭 타입"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricValue:
    """메트릭 값과 레이블"""
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: Optional[float] = None


@dataclass
class MetricDefinition:
    """메트릭 정의"""
    name: str
    metric_type: MetricType
    help_text: str
    labels: List[str] = field(default_factory=list)
    buckets: Optional[List[float]] = None  # 히스토그램용


class PrometheusRegistry:
    """프로메테우스 메트릭 레지스트리"""

    def __init__(self):
        self._metrics: Dict[str, MetricDefinition] = {}
        self._values: Dict[str, List[MetricValue]] = {}
        self._lock = threading.RLock()

        # ETL 표준 메트릭 정의
        self._register_default_metrics()

    def _register_default_metrics(self):
        """기본 ETL 메트릭 등록"""
        # Pipeline 실행 메트릭
        self.register(
            "etl_pipeline_executions_total",
            MetricType.COUNTER,
            "Total number of pipeline executions",
            labels=["source_id", "status"]
        )

        self.register(
            "etl_pipeline_execution_duration_seconds",
            MetricType.HISTOGRAM,
            "Pipeline execution duration in seconds",
            labels=["source_id"],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
        )

        self.register(
            "etl_pipeline_last_execution_timestamp",
            MetricType.GAUGE,
            "Timestamp of last pipeline execution",
            labels=["source_id"]
        )

        # 레코드 처리 메트릭
        self.register(
            "etl_records_processed_total",
            MetricType.COUNTER,
            "Total number of records processed",
            labels=["source_id", "stage"]  # stage: extracted, transformed, loaded
        )

        self.register(
            "etl_records_failed_total",
            MetricType.COUNTER,
            "Total number of failed records",
            labels=["source_id", "error_type"]
        )

        self.register(
            "etl_records_skipped_total",
            MetricType.COUNTER,
            "Total number of skipped records (duplicates)",
            labels=["source_id"]
        )

        # 에러 메트릭
        self.register(
            "etl_errors_total",
            MetricType.COUNTER,
            "Total number of errors",
            labels=["source_id", "error_code", "error_type"]
        )

        self.register(
            "etl_error_rate",
            MetricType.GAUGE,
            "Current error rate (errors per 100 records)",
            labels=["source_id"]
        )

        # 소스 상태 메트릭
        self.register(
            "etl_source_status",
            MetricType.GAUGE,
            "Source status (1=active, 0=inactive, -1=error)",
            labels=["source_id", "source_name"]
        )

        self.register(
            "etl_sources_total",
            MetricType.GAUGE,
            "Total number of sources by status",
            labels=["status"]
        )

        # 자가치유 메트릭
        self.register(
            "etl_healing_sessions_total",
            MetricType.COUNTER,
            "Total healing sessions initiated",
            labels=["source_id", "error_code"]
        )

        self.register(
            "etl_healing_sessions_active",
            MetricType.GAUGE,
            "Number of active healing sessions",
            labels=["status"]  # diagnosing, ai_solving, waiting_admin
        )

        self.register(
            "etl_healing_success_rate",
            MetricType.GAUGE,
            "Healing success rate percentage",
            labels=[]
        )

        # 품질 메트릭
        self.register(
            "etl_data_quality_score",
            MetricType.GAUGE,
            "Data quality score (0-100)",
            labels=["source_id"]
        )

        self.register(
            "etl_schema_drift_detected_total",
            MetricType.COUNTER,
            "Total schema drift events detected",
            labels=["source_id", "drift_type"]
        )

        # 시스템 상태
        self.register(
            "etl_system_health_score",
            MetricType.GAUGE,
            "Overall system health score (0-100)",
            labels=[]
        )

        self.register(
            "etl_mongodb_connection_status",
            MetricType.GAUGE,
            "MongoDB connection status (1=connected, 0=disconnected)",
            labels=[]
        )

        self.register(
            "etl_airflow_status",
            MetricType.GAUGE,
            "Airflow scheduler status (1=healthy, 0=degraded)",
            labels=[]
        )

        # 처리량 메트릭
        self.register(
            "etl_throughput_records_per_second",
            MetricType.GAUGE,
            "Current throughput in records per second",
            labels=["source_id"]
        )

        self.register(
            "etl_processing_latency_seconds",
            MetricType.HISTOGRAM,
            "Processing latency per record in seconds",
            labels=["source_id", "stage"],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )

    def register(
        self,
        name: str,
        metric_type,  # Can be MetricType or str
        help_text: str,
        labels: List[str] = None,
        buckets: List[float] = None
    ):
        """메트릭 등록"""
        # Convert string to MetricType if needed
        if isinstance(metric_type, str):
            metric_type = MetricType(metric_type)

        with self._lock:
            self._metrics[name] = MetricDefinition(
                name=name,
                metric_type=metric_type,
                help_text=help_text,
                labels=labels or [],
                buckets=buckets
            )
            self._values[name] = []

    def set(self, name: str, value: float, labels: Dict[str, str] = None):
        """게이지 값 설정"""
        with self._lock:
            if name not in self._metrics:
                logger.warning(f"Unknown metric: {name}")
                return

            labels = labels or {}

            # 기존 값 찾아서 업데이트
            for mv in self._values[name]:
                if mv.labels == labels:
                    mv.value = value
                    mv.timestamp = time.time()
                    return

            # 새 값 추가
            self._values[name].append(MetricValue(
                value=value,
                labels=labels,
                timestamp=time.time()
            ))

    def inc(self, name: str, value: float = 1, labels: Dict[str, str] = None):
        """카운터 증가"""
        with self._lock:
            if name not in self._metrics:
                logger.warning(f"Unknown metric: {name}")
                return

            labels = labels or {}

            # 기존 값 찾아서 증가
            for mv in self._values[name]:
                if mv.labels == labels:
                    mv.value += value
                    mv.timestamp = time.time()
                    return

            # 새 값 추가
            self._values[name].append(MetricValue(
                value=value,
                labels=labels,
                timestamp=time.time()
            ))

    def observe(self, name: str, value: float, labels: Dict[str, str] = None):
        """히스토그램 관측값 추가"""
        with self._lock:
            if name not in self._metrics:
                logger.warning(f"Unknown metric: {name}")
                return

            metric = self._metrics[name]
            if metric.metric_type != MetricType.HISTOGRAM:
                logger.warning(f"Metric {name} is not a histogram")
                return

            labels = labels or {}

            # 히스토그램은 버킷별로 카운터 증가
            # 간소화된 구현: sum과 count만 저장
            sum_name = f"{name}_sum"
            count_name = f"{name}_count"

            # sum 업데이트
            found_sum = False
            for mv in self._values.get(sum_name, []):
                if mv.labels == labels:
                    mv.value += value
                    found_sum = True
                    break
            if not found_sum:
                if sum_name not in self._values:
                    self._values[sum_name] = []
                self._values[sum_name].append(MetricValue(value=value, labels=labels))

            # count 업데이트
            found_count = False
            for mv in self._values.get(count_name, []):
                if mv.labels == labels:
                    mv.value += 1
                    found_count = True
                    break
            if not found_count:
                if count_name not in self._values:
                    self._values[count_name] = []
                self._values[count_name].append(MetricValue(value=1, labels=labels))

            # 버킷 카운터 업데이트
            if metric.buckets:
                for bucket in metric.buckets:
                    bucket_name = f"{name}_bucket"
                    bucket_labels = {**labels, "le": str(bucket)}

                    if value <= bucket:
                        found = False
                        for mv in self._values.get(bucket_name, []):
                            if mv.labels == bucket_labels:
                                mv.value += 1
                                found = True
                                break
                        if not found:
                            if bucket_name not in self._values:
                                self._values[bucket_name] = []
                            self._values[bucket_name].append(MetricValue(value=1, labels=bucket_labels))

                # +Inf 버킷
                inf_labels = {**labels, "le": "+Inf"}
                found = False
                bucket_name = f"{name}_bucket"
                for mv in self._values.get(bucket_name, []):
                    if mv.labels == inf_labels:
                        mv.value += 1
                        found = True
                        break
                if not found:
                    if bucket_name not in self._values:
                        self._values[bucket_name] = []
                    self._values[bucket_name].append(MetricValue(value=1, labels=inf_labels))

    def export(self) -> str:
        """Prometheus 텍스트 포맷으로 내보내기"""
        lines = []

        with self._lock:
            for name, metric in self._metrics.items():
                # HELP 라인
                lines.append(f"# HELP {name} {metric.help_text}")
                # TYPE 라인
                lines.append(f"# TYPE {name} {metric.metric_type.value}")

                # 값 라인
                if metric.metric_type == MetricType.HISTOGRAM:
                    # 히스토그램은 별도 처리
                    self._export_histogram(lines, name, metric)
                else:
                    for mv in self._values.get(name, []):
                        label_str = self._format_labels(mv.labels)
                        lines.append(f"{name}{label_str} {mv.value}")

        return "\n".join(lines) + "\n"

    def _export_histogram(self, lines: List[str], name: str, metric: MetricDefinition):
        """히스토그램 내보내기"""
        # 버킷
        for mv in self._values.get(f"{name}_bucket", []):
            label_str = self._format_labels(mv.labels)
            lines.append(f"{name}_bucket{label_str} {mv.value}")

        # sum
        for mv in self._values.get(f"{name}_sum", []):
            label_str = self._format_labels(mv.labels)
            lines.append(f"{name}_sum{label_str} {mv.value}")

        # count
        for mv in self._values.get(f"{name}_count", []):
            label_str = self._format_labels(mv.labels)
            lines.append(f"{name}_count{label_str} {mv.value}")

    def _format_labels(self, labels: Dict[str, str]) -> str:
        """레이블 포맷팅"""
        if not labels:
            return ""
        pairs = [f'{k}="{v}"' for k, v in sorted(labels.items())]
        return "{" + ",".join(pairs) + "}"

    def clear(self):
        """모든 값 초기화"""
        with self._lock:
            for name in self._values:
                self._values[name] = []


# 전역 레지스트리
_registry = PrometheusRegistry()


def get_registry() -> PrometheusRegistry:
    """전역 레지스트리 반환"""
    return _registry


class PrometheusMetricsExporter:
    """Prometheus 메트릭 익스포터"""

    def __init__(self, mongo_service=None, pushgateway_url: str = None):
        """
        Args:
            mongo_service: MongoDB 서비스
            pushgateway_url: Pushgateway URL (배치 작업용)
        """
        self.mongo = mongo_service
        self.pushgateway_url = pushgateway_url or os.environ.get("PROMETHEUS_PUSHGATEWAY_URL")
        self.registry = get_registry()
        self._last_collect_time: Optional[datetime] = None

    async def collect_metrics(self):
        """MongoDB에서 메트릭 수집 및 업데이트"""
        if not self.mongo:
            return

        try:
            await self._collect_source_metrics()
            await self._collect_execution_metrics()
            await self._collect_healing_metrics()
            await self._collect_system_metrics()

            self._last_collect_time = datetime.utcnow()

        except Exception as e:
            logger.error(f"Failed to collect metrics: {e}")

    async def _collect_source_metrics(self):
        """소스 관련 메트릭 수집"""
        if not self.mongo:
            return

        # 소스 상태별 카운트
        status_counts = {}
        for status in ['active', 'inactive', 'error', 'pending']:
            count = self.mongo.db.sources.count_documents({'status': status})
            status_counts[status] = count
            self.registry.set(
                "etl_sources_total",
                count,
                labels={"status": status}
            )

        # 개별 소스 상태
        sources = list(self.mongo.db.sources.find({}, {"_id": 1, "name": 1, "status": 1}))
        for source in sources:
            source_id = str(source["_id"])
            status_value = {"active": 1, "inactive": 0, "error": -1}.get(
                source.get("status", "unknown"), 0
            )
            self.registry.set(
                "etl_source_status",
                status_value,
                labels={
                    "source_id": source_id,
                    "source_name": source.get("name", "unknown")
                }
            )

    async def _collect_execution_metrics(self):
        """실행 관련 메트릭 수집"""
        if not self.mongo:
            return

        since = datetime.utcnow() - timedelta(hours=1)

        # 최근 실행 통계
        pipeline = [
            {"$match": {"started_at": {"$gte": since}}},
            {
                "$group": {
                    "_id": {"source_id": "$source_id", "status": "$status"},
                    "count": {"$sum": 1},
                    "total_records": {"$sum": "$records_loaded"},
                    "total_errors": {"$sum": "$error_count"},
                    "total_execution_time": {"$sum": "$execution_time_ms"}
                }
            }
        ]

        try:
            results = list(self.mongo.db.pipeline_metrics.aggregate(pipeline))

            for result in results:
                source_id = result["_id"]["source_id"]
                status = result["_id"]["status"]

                # 실행 횟수 (이 시간대의 증분)
                # 참고: Counter는 monotonically increasing이므로 실제로는
                # 전체 누적값을 저장해야 함. 여기서는 간소화
                self.registry.set(
                    "etl_pipeline_executions_total",
                    result["count"],
                    labels={"source_id": source_id, "status": status}
                )

                # 처리된 레코드
                self.registry.set(
                    "etl_records_processed_total",
                    result["total_records"],
                    labels={"source_id": source_id, "stage": "loaded"}
                )

                # 에러율
                if result["total_records"] > 0:
                    error_rate = (result["total_errors"] / result["total_records"]) * 100
                    self.registry.set(
                        "etl_error_rate",
                        error_rate,
                        labels={"source_id": source_id}
                    )

        except Exception as e:
            logger.error(f"Failed to collect execution metrics: {e}")

    async def _collect_healing_metrics(self):
        """자가치유 관련 메트릭 수집"""
        if not self.mongo:
            return

        # 활성 치유 세션
        for status in ['diagnosing', 'ai_solving', 'source_check', 'waiting_admin']:
            count = self.mongo.db.healing_sessions.count_documents({'status': status})
            self.registry.set(
                "etl_healing_sessions_active",
                count,
                labels={"status": status}
            )

        # 치유 성공률
        total_resolved = self.mongo.db.healing_sessions.count_documents({
            'status': 'resolved'
        })
        total_failed = self.mongo.db.healing_sessions.count_documents({
            'status': 'failed'
        })
        total = total_resolved + total_failed
        if total > 0:
            success_rate = (total_resolved / total) * 100
            self.registry.set("etl_healing_success_rate", success_rate)

    async def _collect_system_metrics(self):
        """시스템 상태 메트릭 수집"""
        if not self.mongo:
            return

        # MongoDB 연결 상태
        try:
            self.mongo.db.command('ping')
            self.registry.set("etl_mongodb_connection_status", 1)
        except Exception:
            self.registry.set("etl_mongodb_connection_status", 0)

        # Airflow 상태 (최근 1시간 내 실행 있으면 healthy)
        hour_ago = datetime.utcnow() - timedelta(hours=1)
        recent_runs = self.mongo.db.crawl_results.count_documents({
            'executed_at': {'$gte': hour_ago}
        })
        self.registry.set("etl_airflow_status", 1 if recent_runs > 0 else 0)

        # 시스템 헬스 점수
        score = await self._calculate_health_score()
        self.registry.set("etl_system_health_score", score)

    async def _calculate_health_score(self) -> float:
        """시스템 헬스 점수 계산"""
        if not self.mongo:
            return 0

        score = 100.0

        # 실패한 소스
        failed_sources = self.mongo.db.sources.count_documents({'status': 'error'})
        score -= min(30, failed_sources * 5)

        # 활성 치유 세션
        active_healing = self.mongo.db.healing_sessions.count_documents({
            'status': {'$in': ['diagnosing', 'ai_solving', 'source_check']}
        })
        if active_healing > 5:
            score -= 10

        # 미처리 관리자 대기
        pending_admin = self.mongo.db.healing_sessions.count_documents({
            'status': 'waiting_admin'
        })
        if pending_admin > 3:
            score -= 15

        return max(0, score)

    def export(self) -> str:
        """Prometheus 포맷으로 메트릭 내보내기"""
        return self.registry.export()

    async def push_to_gateway(self, job_name: str, instance: str = None):
        """
        Pushgateway로 메트릭 푸시 (배치 작업용)

        Args:
            job_name: 작업 이름
            instance: 인스턴스 식별자
        """
        if not self.pushgateway_url:
            logger.warning("Pushgateway URL not configured")
            return False

        import httpx

        # 메트릭 수집
        await self.collect_metrics()

        # Pushgateway URL 구성
        url = f"{self.pushgateway_url}/metrics/job/{job_name}"
        if instance:
            url += f"/instance/{instance}"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    content=self.export(),
                    headers={"Content-Type": "text/plain"}
                )
                response.raise_for_status()
                logger.info(f"Metrics pushed to gateway: {job_name}")
                return True

        except Exception as e:
            logger.error(f"Failed to push metrics: {e}")
            return False


def track_execution_time(metric_name: str, labels_func: Callable = None):
    """
    실행 시간 추적 데코레이터

    Args:
        metric_name: 히스토그램 메트릭 이름
        labels_func: 레이블 생성 함수 (args, kwargs -> dict)
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.time()
            try:
                return await func(*args, **kwargs)
            finally:
                duration = time.time() - start
                labels = labels_func(args, kwargs) if labels_func else {}
                get_registry().observe(metric_name, duration, labels)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.time() - start
                labels = labels_func(args, kwargs) if labels_func else {}
                get_registry().observe(metric_name, duration, labels)

        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


def count_calls(metric_name: str, labels_func: Callable = None):
    """
    호출 횟수 카운트 데코레이터

    Args:
        metric_name: 카운터 메트릭 이름
        labels_func: 레이블 생성 함수
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            labels = labels_func(args, kwargs) if labels_func else {}
            get_registry().inc(metric_name, 1, labels)
            return await func(*args, **kwargs)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            labels = labels_func(args, kwargs) if labels_func else {}
            get_registry().inc(metric_name, 1, labels)
            return func(*args, **kwargs)

        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


# 편의 함수
def record_pipeline_execution(
    source_id: str,
    status: str,
    duration_seconds: float,
    records_processed: int = 0,
    records_failed: int = 0,
    error_type: str = None
):
    """
    파이프라인 실행 메트릭 기록

    Args:
        source_id: 소스 ID
        status: 상태 (success, failed, partial)
        duration_seconds: 실행 시간 (초)
        records_processed: 처리된 레코드 수
        records_failed: 실패한 레코드 수
        error_type: 에러 타입 (있는 경우)
    """
    registry = get_registry()

    # 실행 카운트
    registry.inc(
        "etl_pipeline_executions_total",
        1,
        labels={"source_id": source_id, "status": status}
    )

    # 실행 시간
    registry.observe(
        "etl_pipeline_execution_duration_seconds",
        duration_seconds,
        labels={"source_id": source_id}
    )

    # 마지막 실행 시간
    registry.set(
        "etl_pipeline_last_execution_timestamp",
        time.time(),
        labels={"source_id": source_id}
    )

    # 레코드 처리
    if records_processed > 0:
        registry.inc(
            "etl_records_processed_total",
            records_processed,
            labels={"source_id": source_id, "stage": "loaded"}
        )

    # 실패 레코드
    if records_failed > 0:
        registry.inc(
            "etl_records_failed_total",
            records_failed,
            labels={"source_id": source_id, "error_type": error_type or "unknown"}
        )

    # 에러율
    total = records_processed + records_failed
    if total > 0:
        error_rate = (records_failed / total) * 100
        registry.set(
            "etl_error_rate",
            error_rate,
            labels={"source_id": source_id}
        )


def record_healing_event(
    source_id: str,
    error_code: str,
    status: str = "started"
):
    """
    자가치유 이벤트 기록

    Args:
        source_id: 소스 ID
        error_code: 에러 코드
        status: 상태
    """
    registry = get_registry()

    if status == "started":
        registry.inc(
            "etl_healing_sessions_total",
            1,
            labels={"source_id": source_id, "error_code": error_code}
        )


def record_schema_drift(source_id: str, drift_type: str):
    """
    스키마 드리프트 이벤트 기록

    Args:
        source_id: 소스 ID
        drift_type: 드리프트 타입 (added_fields, removed_fields, type_change)
    """
    get_registry().inc(
        "etl_schema_drift_detected_total",
        1,
        labels={"source_id": source_id, "drift_type": drift_type}
    )


def record_data_quality(source_id: str, quality_score: float):
    """
    데이터 품질 점수 기록

    Args:
        source_id: 소스 ID
        quality_score: 품질 점수 (0-100)
    """
    get_registry().set(
        "etl_data_quality_score",
        quality_score,
        labels={"source_id": source_id}
    )
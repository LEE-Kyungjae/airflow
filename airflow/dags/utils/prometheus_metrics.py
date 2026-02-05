"""
Prometheus Metrics Utility for Airflow DAGs

Airflow 배치 작업에서 Prometheus Pushgateway로 메트릭을 전송합니다.
"""

import os
import time
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional
from functools import wraps
from dataclasses import dataclass, field
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


@dataclass
class DAGMetrics:
    """DAG 실행 메트릭"""
    dag_id: str
    run_id: str
    source_id: Optional[str] = None

    # 시간
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    # 레코드
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_skipped: int = 0
    records_failed: int = 0

    # 상태
    status: str = "running"
    error_message: Optional[str] = None

    # 품질
    quality_score: Optional[float] = None

    def duration_seconds(self) -> float:
        """실행 시간 (초)"""
        end = self.end_time or time.time()
        return end - self.start_time

    def to_prometheus_format(self) -> str:
        """Prometheus 텍스트 포맷으로 변환"""
        lines = []
        labels = self._format_labels()

        # 실행 시간
        lines.append(
            f'etl_dag_execution_duration_seconds{labels} {self.duration_seconds()}'
        )

        # 레코드 카운트
        lines.append(
            f'etl_dag_records_extracted{labels} {self.records_extracted}'
        )
        lines.append(
            f'etl_dag_records_transformed{labels} {self.records_transformed}'
        )
        lines.append(
            f'etl_dag_records_loaded{labels} {self.records_loaded}'
        )
        lines.append(
            f'etl_dag_records_skipped{labels} {self.records_skipped}'
        )
        lines.append(
            f'etl_dag_records_failed{labels} {self.records_failed}'
        )

        # 상태 (success=1, failed=0)
        status_value = 1 if self.status == "success" else 0
        lines.append(
            f'etl_dag_success{labels} {status_value}'
        )

        # 품질 점수
        if self.quality_score is not None:
            lines.append(
                f'etl_dag_quality_score{labels} {self.quality_score}'
            )

        # 타임스탬프
        lines.append(
            f'etl_dag_last_run_timestamp{labels} {time.time()}'
        )

        return "\n".join(lines) + "\n"

    def _format_labels(self) -> str:
        """레이블 포맷팅"""
        labels = {
            "dag_id": self.dag_id,
            "run_id": self.run_id[:50] if self.run_id else "unknown",
        }
        if self.source_id:
            labels["source_id"] = self.source_id

        pairs = [f'{k}="{v}"' for k, v in labels.items()]
        return "{" + ",".join(pairs) + "}"


class PrometheusClient:
    """Prometheus Pushgateway 클라이언트"""

    def __init__(self, pushgateway_url: str = None):
        """
        Args:
            pushgateway_url: Pushgateway URL (없으면 환경변수에서)
        """
        self.pushgateway_url = (
            pushgateway_url or
            os.environ.get("PROMETHEUS_PUSHGATEWAY_URL", "http://pushgateway:9091")
        )
        self._enabled = bool(self.pushgateway_url)

    def push(
        self,
        job_name: str,
        metrics: str,
        grouping_key: Dict[str, str] = None
    ) -> bool:
        """
        Pushgateway로 메트릭 전송

        Args:
            job_name: 작업 이름
            metrics: Prometheus 포맷 메트릭 문자열
            grouping_key: 그룹화 키

        Returns:
            성공 여부
        """
        if not self._enabled:
            logger.debug("Pushgateway disabled, skipping push")
            return False

        import requests

        try:
            # URL 구성
            url = f"{self.pushgateway_url}/metrics/job/{job_name}"
            if grouping_key:
                for key, value in grouping_key.items():
                    url += f"/{key}/{value}"

            # 전송
            response = requests.post(
                url,
                data=metrics,
                headers={"Content-Type": "text/plain"},
                timeout=10
            )
            response.raise_for_status()

            logger.info(f"Metrics pushed to Pushgateway: {job_name}")
            return True

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to push metrics to Pushgateway: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error pushing metrics: {e}")
            return False

    def delete(self, job_name: str, grouping_key: Dict[str, str] = None) -> bool:
        """
        Pushgateway에서 메트릭 삭제

        Args:
            job_name: 작업 이름
            grouping_key: 그룹화 키

        Returns:
            성공 여부
        """
        if not self._enabled:
            return False

        import requests

        try:
            url = f"{self.pushgateway_url}/metrics/job/{job_name}"
            if grouping_key:
                for key, value in grouping_key.items():
                    url += f"/{key}/{value}"

            response = requests.delete(url, timeout=10)
            response.raise_for_status()
            return True

        except Exception as e:
            logger.warning(f"Failed to delete metrics: {e}")
            return False


# 전역 클라이언트
_client: Optional[PrometheusClient] = None


def get_client() -> PrometheusClient:
    """전역 클라이언트 반환"""
    global _client
    if _client is None:
        _client = PrometheusClient()
    return _client


@contextmanager
def track_dag_execution(dag_id: str, run_id: str, source_id: str = None):
    """
    DAG 실행 메트릭 추적 컨텍스트 매니저

    사용법:
        with track_dag_execution("my_dag", run_id, source_id) as metrics:
            # 크롤링 작업
            metrics.records_extracted = 100
            metrics.records_loaded = 95

    Args:
        dag_id: DAG ID
        run_id: 실행 ID
        source_id: 소스 ID

    Yields:
        DAGMetrics 객체
    """
    metrics = DAGMetrics(
        dag_id=dag_id,
        run_id=run_id,
        source_id=source_id
    )

    try:
        yield metrics
        metrics.status = "success"

    except Exception as e:
        metrics.status = "failed"
        metrics.error_message = str(e)
        raise

    finally:
        metrics.end_time = time.time()

        # Pushgateway로 전송
        client = get_client()
        client.push(
            job_name=f"airflow_dag_{dag_id}",
            metrics=metrics.to_prometheus_format(),
            grouping_key={"instance": source_id or "default"}
        )


def track_task_execution(task_id: str):
    """
    Airflow Task 실행 시간 추적 데코레이터

    사용법:
        @track_task_execution("extract_data")
        def extract_data(**context):
            ...

    Args:
        task_id: Task ID
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            status = "success"
            error_msg = None

            try:
                return func(*args, **kwargs)
            except Exception as e:
                status = "failed"
                error_msg = str(e)
                raise
            finally:
                duration = time.time() - start_time

                # 메트릭 문자열 생성
                dag_id = kwargs.get("dag_run", {}).dag_id if "dag_run" in kwargs else "unknown"
                labels = f'{{dag_id="{dag_id}",task_id="{task_id}"}}'

                metrics = f"""
etl_task_duration_seconds{labels} {duration}
etl_task_success{labels} {1 if status == "success" else 0}
""".strip()

                # 전송
                client = get_client()
                client.push(
                    job_name=f"airflow_task_{task_id}",
                    metrics=metrics
                )

        return wrapper
    return decorator


def record_crawl_metrics(
    source_id: str,
    records_extracted: int,
    records_loaded: int,
    records_failed: int = 0,
    execution_time_seconds: float = 0,
    quality_score: float = None
):
    """
    크롤링 메트릭 기록 (간편 함수)

    Args:
        source_id: 소스 ID
        records_extracted: 추출된 레코드 수
        records_loaded: 적재된 레코드 수
        records_failed: 실패한 레코드 수
        execution_time_seconds: 실행 시간 (초)
        quality_score: 품질 점수 (0-100)
    """
    labels = f'{{source_id="{source_id}"}}'

    metrics_lines = [
        f'etl_crawl_records_extracted{labels} {records_extracted}',
        f'etl_crawl_records_loaded{labels} {records_loaded}',
        f'etl_crawl_records_failed{labels} {records_failed}',
        f'etl_crawl_duration_seconds{labels} {execution_time_seconds}',
        f'etl_crawl_last_run_timestamp{labels} {time.time()}',
    ]

    # 성공률
    total = records_extracted or 1
    success_rate = ((records_loaded) / total) * 100
    metrics_lines.append(f'etl_crawl_success_rate{labels} {success_rate}')

    # 품질 점수
    if quality_score is not None:
        metrics_lines.append(f'etl_crawl_quality_score{labels} {quality_score}')

    client = get_client()
    client.push(
        job_name="airflow_crawl",
        metrics="\n".join(metrics_lines) + "\n",
        grouping_key={"source_id": source_id}
    )


def record_error_metric(
    source_id: str,
    error_code: str,
    error_type: str = "unknown"
):
    """
    에러 메트릭 기록

    Args:
        source_id: 소스 ID
        error_code: 에러 코드
        error_type: 에러 타입
    """
    labels = f'{{source_id="{source_id}",error_code="{error_code}",error_type="{error_type}"}}'

    metrics = f'etl_crawl_errors_total{labels} 1\n'

    client = get_client()
    client.push(
        job_name="airflow_errors",
        metrics=metrics,
        grouping_key={"source_id": source_id}
    )


def record_healing_metric(
    source_id: str,
    session_id: str,
    status: str,
    attempts: int
):
    """
    자가치유 세션 메트릭 기록

    Args:
        source_id: 소스 ID
        session_id: 세션 ID
        status: 상태 (started, in_progress, resolved, failed)
        attempts: 시도 횟수
    """
    labels = f'{{source_id="{source_id}",status="{status}"}}'
    session_hash = hashlib.md5(session_id.encode()).hexdigest()[:8]

    metrics_lines = [
        f'etl_healing_attempts{labels} {attempts}',
        f'etl_healing_status{labels} {1 if status == "resolved" else 0}',
    ]

    client = get_client()
    client.push(
        job_name="airflow_healing",
        metrics="\n".join(metrics_lines) + "\n",
        grouping_key={"session": session_hash}
    )


class MetricsAggregator:
    """배치 메트릭 집계기"""

    def __init__(self, job_name: str):
        self.job_name = job_name
        self._metrics: Dict[str, float] = {}
        self._labels: Dict[str, Dict[str, str]] = {}

    def set(self, name: str, value: float, labels: Dict[str, str] = None):
        """게이지 설정"""
        key = self._make_key(name, labels)
        self._metrics[key] = value
        self._labels[key] = labels or {}

    def inc(self, name: str, value: float = 1, labels: Dict[str, str] = None):
        """카운터 증가"""
        key = self._make_key(name, labels)
        self._metrics[key] = self._metrics.get(key, 0) + value
        self._labels[key] = labels or {}

    def _make_key(self, name: str, labels: Dict[str, str] = None) -> str:
        """고유 키 생성"""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}|{label_str}"

    def push(self, grouping_key: Dict[str, str] = None) -> bool:
        """집계된 메트릭 전송"""
        lines = []

        for key, value in self._metrics.items():
            parts = key.split("|", 1)
            name = parts[0]
            labels = self._labels.get(key, {})

            if labels:
                label_str = "{" + ",".join(f'{k}="{v}"' for k, v in labels.items()) + "}"
            else:
                label_str = ""

            lines.append(f"{name}{label_str} {value}")

        metrics = "\n".join(lines) + "\n"

        client = get_client()
        return client.push(self.job_name, metrics, grouping_key)

    def clear(self):
        """메트릭 초기화"""
        self._metrics.clear()
        self._labels.clear()
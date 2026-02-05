"""
Prometheus Metrics Router - 프로메테우스 메트릭 엔드포인트

/metrics 엔드포인트를 통해 Prometheus가 스크레이핑할 수 있는
메트릭을 노출합니다.
"""

from fastapi import APIRouter, Response, Depends
from fastapi.responses import PlainTextResponse

from app.services.mongo_service import MongoService
from app.services.observability.prometheus import (
    PrometheusMetricsExporter,
    get_registry
)
from app.core import get_logger

logger = get_logger(__name__)
router = APIRouter()


def get_mongo():
    """MongoDB 서비스 의존성"""
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


@router.get(
    "/metrics",
    response_class=PlainTextResponse,
    summary="Prometheus Metrics",
    description="Prometheus 스크레이핑용 메트릭 엔드포인트"
)
async def get_metrics(mongo: MongoService = Depends(get_mongo)):
    """
    Prometheus 메트릭 반환

    이 엔드포인트는 Prometheus 서버가 주기적으로 스크레이핑합니다.
    모든 ETL 파이프라인 메트릭을 Prometheus 텍스트 포맷으로 반환합니다.

    Returns:
        Prometheus 텍스트 포맷의 메트릭
    """
    try:
        exporter = PrometheusMetricsExporter(mongo_service=mongo)

        # MongoDB에서 최신 메트릭 수집
        await exporter.collect_metrics()

        # Prometheus 포맷으로 내보내기
        metrics_output = exporter.export()

        return Response(
            content=metrics_output,
            media_type="text/plain; version=0.0.4; charset=utf-8"
        )

    except Exception as e:
        logger.error(f"Failed to export metrics: {e}")
        # 메트릭 수집 실패해도 기본 메트릭은 반환
        return Response(
            content=get_registry().export(),
            media_type="text/plain; version=0.0.4; charset=utf-8"
        )


@router.get(
    "/metrics/health",
    summary="Metrics Health Check",
    description="메트릭 시스템 상태 확인"
)
async def metrics_health():
    """
    메트릭 시스템 헬스 체크

    Returns:
        메트릭 시스템 상태
    """
    registry = get_registry()

    return {
        "status": "healthy",
        "registered_metrics": len(registry._metrics),
        "total_values": sum(len(v) for v in registry._values.values()),
        "message": "Prometheus metrics endpoint is operational"
    }
"""
Data Quality Router - 데이터 품질 모니터링 API

엔드포인트:
- GET /validation-results: 검증 결과 목록
- GET /validation-results/{id}: 검증 결과 상세
- GET /quality-trend/{source_id}: 품질 트렌드
- GET /anomalies: 이상 감지 목록
- POST /anomalies/{id}/acknowledge: 이상 감지 확인
- GET /report/{source_id}: 품질 리포트
- POST /validate: 수동 데이터 검증
"""

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Depends, Body
from pydantic import BaseModel, Field

from app.services.mongo_service import MongoService
from app.services.data_quality import (
    DataValidator,
    ValidationResult,
    DataQualityMonitor,
    EncodingRule,
    DateRule,
    RequiredFieldRule,
    RangeRule,
    FormatRule,
)
from app.services.data_quality.report import QualityReportGenerator
from app.services.alerts import AlertDispatcher
from app.core import get_logger

logger = get_logger(__name__)
router = APIRouter()


# ============== Models ==============

class ValidationRequest(BaseModel):
    """수동 검증 요청"""
    source_id: str
    run_id: str
    data: List[Dict[str, Any]]
    rules: Optional[Dict[str, Any]] = None


class AnomalyResponse(BaseModel):
    """이상 감지 응답"""
    id: str
    source_id: str
    detected_at: str
    anomaly_type: str
    severity: str
    description: str
    acknowledged: bool


class QualityTrendResponse(BaseModel):
    """품질 트렌드 응답"""
    source_id: str
    avg_quality_score: float
    min_quality_score: float
    max_quality_score: float
    total_validations: int
    trend: List[Dict]
    top_issues: List[Dict]


# ============== Dependencies ==============

def get_mongo():
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


def get_quality_monitor(mongo: MongoService = Depends(get_mongo)):
    alert_dispatcher = AlertDispatcher(mongo_service=mongo)
    return DataQualityMonitor(mongo_service=mongo, alert_dispatcher=alert_dispatcher)


def get_report_generator(mongo: MongoService = Depends(get_mongo)):
    return QualityReportGenerator(mongo_service=mongo)


# ============== Endpoints ==============

@router.get("/validation-results")
async def list_validation_results(
    source_id: Optional[str] = Query(None, description="소스 ID 필터"),
    min_score: Optional[float] = Query(None, ge=0, le=100, description="최소 품질 점수"),
    max_score: Optional[float] = Query(None, ge=0, le=100, description="최대 품질 점수"),
    has_issues: Optional[bool] = Query(None, description="이슈 존재 여부"),
    days: int = Query(7, ge=1, le=90, description="조회 기간 (일)"),
    limit: int = Query(50, ge=1, le=200),
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    검증 결과 목록 조회

    필터:
    - source_id: 특정 소스만
    - min_score/max_score: 품질 점수 범위
    - has_issues: 이슈가 있는 결과만
    - days: 조회 기간
    """
    since = datetime.utcnow() - timedelta(days=days)

    query = {"created_at": {"$gte": since}}

    if source_id:
        query["source_id"] = source_id

    if min_score is not None:
        query["quality_score"] = {"$gte": min_score}

    if max_score is not None:
        if "quality_score" in query:
            query["quality_score"]["$lte"] = max_score
        else:
            query["quality_score"] = {"$lte": max_score}

    if has_issues is not None:
        if has_issues:
            query["issue_summary.total"] = {"$gt": 0}
        else:
            query["issue_summary.total"] = 0

    results = list(
        mongo.db.validation_results
        .find(query)
        .sort("created_at", -1)
        .limit(limit)
    )

    # Summary stats
    total_query = dict(query)
    total_count = mongo.db.validation_results.count_documents(total_query)

    avg_score_pipeline = [
        {"$match": query},
        {"$group": {"_id": None, "avg": {"$avg": "$quality_score"}}}
    ]
    avg_result = list(mongo.db.validation_results.aggregate(avg_score_pipeline))
    avg_score = avg_result[0]["avg"] if avg_result else 0

    return {
        "total": total_count,
        "avg_quality_score": round(avg_score, 1),
        "results": [
            {
                "id": str(r["_id"]),
                "source_id": r["source_id"],
                "run_id": r["run_id"],
                "total_records": r["total_records"],
                "quality_score": r["quality_score"],
                "is_valid": r["is_valid"],
                "issue_count": r["issue_summary"]["total"],
                "issues_by_severity": r["issue_summary"]["by_severity"],
                "validated_at": r["validated_at"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in results
        ]
    }


@router.get("/validation-results/{result_id}")
async def get_validation_result(
    result_id: str,
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """검증 결과 상세 조회"""
    from bson import ObjectId

    result = mongo.db.validation_results.find_one({"_id": ObjectId(result_id)})

    if not result:
        raise HTTPException(status_code=404, detail="검증 결과를 찾을 수 없습니다")

    return {
        "id": str(result["_id"]),
        "source_id": result["source_id"],
        "run_id": result["run_id"],
        "total_records": result["total_records"],
        "quality_score": result["quality_score"],
        "is_valid": result["is_valid"],
        "issue_summary": result["issue_summary"],
        "issues": result.get("issues", [])[:500],  # Limit issues returned
        "field_stats": result.get("field_stats", {}),
        "validated_at": result["validated_at"],
        "created_at": result["created_at"].isoformat(),
    }


@router.get("/quality-trend/{source_id}")
async def get_quality_trend(
    source_id: str,
    days: int = Query(30, ge=1, le=90, description="조회 기간 (일)"),
    monitor: DataQualityMonitor = Depends(get_quality_monitor)
) -> Dict[str, Any]:
    """
    소스별 품질 트렌드 조회

    - 일별 품질 점수 추이
    - 이슈 발생 추이
    - 주요 이슈 유형
    """
    trend = await monitor.get_quality_trend(source_id, days=days)

    if not trend:
        raise HTTPException(status_code=404, detail="해당 소스의 트렌드 데이터가 없습니다")

    return {
        "source_id": trend.source_id,
        "period": f"Last {days} days",
        "summary": {
            "avg_quality_score": round(trend.avg_quality_score, 1),
            "min_quality_score": round(trend.min_quality_score, 1),
            "max_quality_score": round(trend.max_quality_score, 1),
            "total_validations": trend.total_validations,
            "total_issues": trend.total_issues,
        },
        "trend": trend.issue_trend,
        "top_issues": trend.top_issues,
    }


@router.get("/anomalies")
async def list_anomalies(
    source_id: Optional[str] = Query(None, description="소스 ID 필터"),
    severity: Optional[str] = Query(None, description="심각도 필터"),
    acknowledged: Optional[bool] = Query(None, description="확인 여부"),
    hours: int = Query(24, ge=1, le=168, description="조회 기간 (시간)"),
    monitor: DataQualityMonitor = Depends(get_quality_monitor)
) -> Dict[str, Any]:
    """
    이상 감지 목록 조회

    이상 유형:
    - score_drop: 품질 점수 급락
    - issue_spike: 이슈 급증
    - low_score: 최저 점수 미달
    """
    anomalies = await monitor.get_recent_anomalies(
        source_id=source_id,
        hours=hours,
        acknowledged=acknowledged
    )

    # Filter by severity if specified
    if severity:
        anomalies = [a for a in anomalies if a["severity"] == severity]

    return {
        "total": len(anomalies),
        "unacknowledged": sum(1 for a in anomalies if not a["acknowledged"]),
        "anomalies": anomalies
    }


@router.post("/anomalies/{anomaly_id}/acknowledge")
async def acknowledge_anomaly(
    anomaly_id: str,
    monitor: DataQualityMonitor = Depends(get_quality_monitor)
) -> Dict[str, Any]:
    """이상 감지 확인 처리"""
    success = await monitor.acknowledge_anomaly(anomaly_id)

    if not success:
        raise HTTPException(status_code=404, detail="이상 감지를 찾을 수 없습니다")

    return {"success": True, "message": "확인 처리되었습니다"}


@router.get("/report/{source_id}")
async def get_quality_report(
    source_id: str,
    days: int = Query(7, ge=1, le=30, description="리포트 기간 (일)"),
    generator: QualityReportGenerator = Depends(get_report_generator)
) -> Dict[str, Any]:
    """
    종합 품질 리포트 생성

    포함 내용:
    - 요약 통계
    - 이슈 분석
    - 필드별 상세
    - 트렌드
    - 권장 조치
    """
    report = await generator.generate_report(source_id, days=days)

    if not report:
        raise HTTPException(status_code=404, detail="해당 소스의 검증 데이터가 없습니다")

    return report.to_dict()


@router.post("/validate")
async def validate_data(
    request: ValidationRequest,
    mongo: MongoService = Depends(get_mongo),
    monitor: DataQualityMonitor = Depends(get_quality_monitor)
) -> Dict[str, Any]:
    """
    수동 데이터 검증

    요청 본문:
    - source_id: 소스 ID
    - run_id: 실행 ID
    - data: 검증할 데이터 배열
    - rules: 적용할 규칙 (선택)
    """
    # Get source config for validation rules
    source = mongo.get_source(request.source_id)

    if source:
        # Use source-specific validator
        validator = DataValidator.create_for_source(source)
    else:
        # Use default validator
        validator = DataValidator()

    # Validate data
    result = validator.validate_batch(
        records=request.data,
        source_id=request.source_id,
        run_id=request.run_id
    )

    # Store result
    result_id = await monitor.store_validation_result(result)

    logger.info(
        "Data validated",
        source_id=request.source_id,
        run_id=request.run_id,
        records=len(request.data),
        quality_score=result.quality_score,
        issues=len(result.issues),
    )

    return {
        "result_id": result_id,
        "source_id": result.source_id,
        "run_id": result.run_id,
        "total_records": result.total_records,
        "quality_score": result.quality_score,
        "is_valid": result.is_valid,
        "issue_summary": {
            "total": len(result.issues),
            "by_severity": result.issue_count_by_severity,
            "by_rule": result.issue_count_by_rule,
        },
        "sample_issues": [
            {
                "rule": i.rule_name,
                "field": i.field_name,
                "severity": i.severity.value,
                "message": i.message,
                "row": i.row_index,
            }
            for i in result.issues[:20]
        ]
    }


@router.get("/change-stats/{source_id}")
async def get_change_detection_stats(
    source_id: str,
    days: int = Query(7, ge=1, le=30, description="조회 기간 (일)"),
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    변경 감지 통계 조회

    - 전체 레코드 수
    - 스킵된 레코드 수 (변경 없음)
    - 트래픽 절감률
    """
    from app.services.change_detection import ChangeDetectionService

    service = ChangeDetectionService(mongo)
    stats = await service.get_change_stats(source_id, days=days)

    return {
        "source_id": source_id,
        **stats
    }


@router.get("/timeline")
async def get_validation_timeline(
    source_id: Optional[str] = Query(None, description="소스 ID 필터"),
    days: int = Query(7, ge=1, le=30, description="조회 기간 (일)"),
    interval: str = Query("hour", description="집계 단위: hour, day"),
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    데이터 수집/검증 시계열 데이터

    - 시간별/일별 검증 건수
    - 품질 점수 추이
    - 이슈 발생 추이
    """
    since = datetime.utcnow() - timedelta(days=days)

    # Build match query
    match_query = {"created_at": {"$gte": since}}
    if source_id:
        match_query["source_id"] = source_id

    # Date format based on interval
    if interval == "hour":
        date_format = "%Y-%m-%d %H:00"
        date_trunc = {
            "year": {"$year": "$created_at"},
            "month": {"$month": "$created_at"},
            "day": {"$dayOfMonth": "$created_at"},
            "hour": {"$hour": "$created_at"},
        }
    else:  # day
        date_format = "%Y-%m-%d"
        date_trunc = {
            "year": {"$year": "$created_at"},
            "month": {"$month": "$created_at"},
            "day": {"$dayOfMonth": "$created_at"},
        }

    # Aggregate by time interval
    pipeline = [
        {"$match": match_query},
        {
            "$group": {
                "_id": date_trunc,
                "timestamp": {"$first": "$created_at"},
                "validation_count": {"$sum": 1},
                "total_records": {"$sum": "$total_records"},
                "avg_quality_score": {"$avg": "$quality_score"},
                "min_quality_score": {"$min": "$quality_score"},
                "max_quality_score": {"$max": "$quality_score"},
                "total_issues": {"$sum": "$issue_summary.total"},
                "critical_issues": {"$sum": "$issue_summary.by_severity.critical"},
                "error_issues": {"$sum": "$issue_summary.by_severity.error"},
                "warning_issues": {"$sum": "$issue_summary.by_severity.warning"},
            }
        },
        {"$sort": {"timestamp": 1}},
    ]

    results = list(mongo.db.validation_results.aggregate(pipeline))

    # Format timeline data
    timeline = []
    for r in results:
        ts = r["timestamp"]
        if interval == "hour":
            time_label = ts.strftime("%m/%d %H:00")
        else:
            time_label = ts.strftime("%m/%d")

        timeline.append({
            "timestamp": ts.isoformat(),
            "label": time_label,
            "validation_count": r["validation_count"],
            "total_records": r["total_records"],
            "avg_quality_score": round(r["avg_quality_score"], 1),
            "min_quality_score": round(r["min_quality_score"], 1),
            "max_quality_score": round(r["max_quality_score"], 1),
            "total_issues": r["total_issues"],
            "critical_issues": r.get("critical_issues") or 0,
            "error_issues": r.get("error_issues") or 0,
            "warning_issues": r.get("warning_issues") or 0,
        })

    # Calculate summary
    if timeline:
        total_validations = sum(t["validation_count"] for t in timeline)
        total_records = sum(t["total_records"] for t in timeline)
        avg_score = sum(t["avg_quality_score"] for t in timeline) / len(timeline)
    else:
        total_validations = 0
        total_records = 0
        avg_score = 0

    return {
        "period": {
            "start": since.isoformat(),
            "end": datetime.utcnow().isoformat(),
            "days": days,
            "interval": interval,
        },
        "summary": {
            "total_validations": total_validations,
            "total_records": total_records,
            "avg_quality_score": round(avg_score, 1),
        },
        "timeline": timeline,
    }


@router.get("/dashboard")
async def get_quality_dashboard(
    days: int = Query(7, ge=1, le=30),
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    데이터 품질 대시보드

    - 전체 품질 점수
    - 소스별 상태
    - 최근 이슈
    - 이상 감지
    """
    since = datetime.utcnow() - timedelta(days=days)
    since_24h = datetime.utcnow() - timedelta(hours=24)

    # Overall stats
    overall_pipeline = [
        {"$match": {"created_at": {"$gte": since}}},
        {
            "$group": {
                "_id": None,
                "avg_score": {"$avg": "$quality_score"},
                "total_validations": {"$sum": 1},
                "total_issues": {"$sum": "$issue_summary.total"},
            }
        }
    ]
    overall = list(mongo.db.validation_results.aggregate(overall_pipeline))
    overall_stats = overall[0] if overall else {"avg_score": 0, "total_validations": 0, "total_issues": 0}

    # Issues in last 24 hours
    issues_24h_pipeline = [
        {"$match": {"created_at": {"$gte": since_24h}}},
        {"$group": {"_id": None, "total": {"$sum": "$issue_summary.total"}}}
    ]
    issues_24h = list(mongo.db.validation_results.aggregate(issues_24h_pipeline))
    total_issues_24h = issues_24h[0]["total"] if issues_24h else 0

    # Unique sources count
    unique_sources = len(mongo.db.validation_results.distinct("source_id", {"created_at": {"$gte": since}}))

    # Source-level stats with trend calculation
    source_pipeline = [
        {"$match": {"created_at": {"$gte": since}}},
        {"$sort": {"created_at": 1}},
        {
            "$group": {
                "_id": "$source_id",
                "avg_score": {"$avg": "$quality_score"},
                "first_score": {"$first": "$quality_score"},
                "latest_score": {"$last": "$quality_score"},
                "last_validation": {"$last": "$created_at"},
                "validations": {"$sum": 1},
                "issues": {"$sum": "$issue_summary.total"},
            }
        },
        {"$sort": {"avg_score": 1}},
        {"$limit": 20}
    ]
    source_stats = list(mongo.db.validation_results.aggregate(source_pipeline))

    # Sources below threshold (70%)
    sources_below = sum(1 for s in source_stats if s["avg_score"] < 70)

    # Get source names from sources collection
    source_names = {}
    for s in source_stats:
        source_doc = mongo.db.sources.find_one({"_id": s["_id"]})
        if source_doc:
            source_names[s["_id"]] = source_doc.get("name", s["_id"])
        else:
            source_names[s["_id"]] = s["_id"]

    # Recent issues by type with affected sources
    issue_pipeline = [
        {"$match": {"created_at": {"$gte": since}}},
        {"$unwind": "$issues"},
        {
            "$group": {
                "_id": "$issues.rule_name",
                "count": {"$sum": 1},
                "affected_sources": {"$addToSet": "$source_id"}
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    issue_types = list(mongo.db.validation_results.aggregate(issue_pipeline))

    # Unacknowledged anomalies count
    anomaly_count = mongo.db.quality_anomalies.count_documents({
        "detected_at": {"$gte": since},
        "acknowledged": False
    })

    # Recent anomalies
    recent_anomalies = list(
        mongo.db.quality_anomalies
        .find({"detected_at": {"$gte": since}, "acknowledged": False})
        .sort("detected_at", -1)
        .limit(10)
    )

    # Calculate trend: up if latest > first, down if latest < first, stable otherwise
    def get_trend(first_score, latest_score):
        diff = latest_score - first_score
        if diff > 5:
            return "up"
        elif diff < -5:
            return "down"
        return "stable"

    return {
        "overall_stats": {
            "total_sources": unique_sources,
            "avg_quality_score": round(overall_stats["avg_score"] or 0, 1),
            "total_issues_24h": total_issues_24h,
            "anomalies_count": anomaly_count,
            "sources_below_threshold": sources_below,
        },
        "source_scores": [
            {
                "source_id": s["_id"],
                "source_name": source_names.get(s["_id"], s["_id"]),
                "quality_score": round(s["latest_score"], 1),
                "last_validation": s["last_validation"].isoformat() if s["last_validation"] else None,
                "trend": get_trend(s["first_score"], s["latest_score"]),
            }
            for s in source_stats
        ],
        "recent_anomalies": [
            {
                "id": str(a["_id"]),
                "source_id": a["source_id"],
                "detected_at": a["detected_at"].isoformat(),
                "anomaly_type": a["anomaly_type"],
                "severity": a["severity"],
                "description": a["description"],
                "acknowledged": a["acknowledged"],
            }
            for a in recent_anomalies
        ],
        "top_issues": [
            {
                "rule": i["_id"],
                "count": i["count"],
                "affected_sources": len(i["affected_sources"])
            }
            for i in issue_types
        ],
    }

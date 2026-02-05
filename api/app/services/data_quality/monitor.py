"""
Data Quality Monitor - 데이터 품질 모니터링 및 추적

기능:
- 검증 결과 저장
- 품질 트렌드 분석
- 이상 감지
- 알림 트리거
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from app.core import get_logger
from app.services.alerts import AlertDispatcher, AlertSeverity
from .validator import ValidationResult, ValidationSeverity as ValSeverity

logger = get_logger(__name__)


@dataclass
class QualityTrend:
    """품질 트렌드"""
    source_id: str
    period_start: datetime
    period_end: datetime
    avg_quality_score: float
    min_quality_score: float
    max_quality_score: float
    total_validations: int
    total_issues: int
    issue_trend: List[Dict]  # [{date, count}]
    top_issues: List[Dict]   # [{rule, count}]


@dataclass
class QualityAnomaly:
    """품질 이상 감지"""
    source_id: str
    detected_at: datetime
    anomaly_type: str  # score_drop, issue_spike, new_issue_type
    severity: str
    description: str
    baseline_value: float
    current_value: float
    threshold: float


class DataQualityMonitor:
    """데이터 품질 모니터링 서비스"""

    # 이상 감지 임계값
    ANOMALY_THRESHOLDS = {
        "score_drop_warning": 10,    # 점수 10점 하락 시 경고
        "score_drop_error": 20,      # 점수 20점 하락 시 에러
        "issue_spike_multiplier": 2,  # 이슈 수 2배 증가 시
        "min_quality_score": 70,     # 최소 품질 점수
    }

    def __init__(self, mongo_service=None, alert_dispatcher: AlertDispatcher = None):
        self.mongo = mongo_service
        self.alert_dispatcher = alert_dispatcher

    async def store_validation_result(self, result: ValidationResult) -> str:
        """검증 결과 저장"""
        if not self.mongo:
            logger.warning("MongoDB not configured, skipping result storage")
            return None

        doc = result.to_dict()
        doc["created_at"] = datetime.utcnow()

        inserted = self.mongo.db.validation_results.insert_one(doc)

        logger.info(
            "Validation result stored",
            source_id=result.source_id,
            run_id=result.run_id,
            quality_score=result.quality_score,
            issue_count=len(result.issues),
        )

        # Check for anomalies
        await self._check_anomalies(result)

        return str(inserted.inserted_id)

    async def _check_anomalies(self, result: ValidationResult):
        """이상 감지 및 알림"""
        anomalies = []

        # Get baseline (last 7 days average)
        baseline = await self._get_baseline(result.source_id)

        if baseline:
            # Score drop detection
            score_drop = baseline["avg_quality_score"] - result.quality_score

            if score_drop >= self.ANOMALY_THRESHOLDS["score_drop_error"]:
                anomalies.append(QualityAnomaly(
                    source_id=result.source_id,
                    detected_at=datetime.utcnow(),
                    anomaly_type="score_drop",
                    severity="error",
                    description=f"품질 점수 급락: {baseline['avg_quality_score']:.1f} → {result.quality_score:.1f}",
                    baseline_value=baseline["avg_quality_score"],
                    current_value=result.quality_score,
                    threshold=self.ANOMALY_THRESHOLDS["score_drop_error"],
                ))
            elif score_drop >= self.ANOMALY_THRESHOLDS["score_drop_warning"]:
                anomalies.append(QualityAnomaly(
                    source_id=result.source_id,
                    detected_at=datetime.utcnow(),
                    anomaly_type="score_drop",
                    severity="warning",
                    description=f"품질 점수 하락: {baseline['avg_quality_score']:.1f} → {result.quality_score:.1f}",
                    baseline_value=baseline["avg_quality_score"],
                    current_value=result.quality_score,
                    threshold=self.ANOMALY_THRESHOLDS["score_drop_warning"],
                ))

            # Issue spike detection
            if baseline["avg_issues"] > 0:
                issue_ratio = len(result.issues) / baseline["avg_issues"]
                if issue_ratio >= self.ANOMALY_THRESHOLDS["issue_spike_multiplier"]:
                    anomalies.append(QualityAnomaly(
                        source_id=result.source_id,
                        detected_at=datetime.utcnow(),
                        anomaly_type="issue_spike",
                        severity="warning",
                        description=f"이슈 급증: {baseline['avg_issues']:.0f} → {len(result.issues)}",
                        baseline_value=baseline["avg_issues"],
                        current_value=len(result.issues),
                        threshold=self.ANOMALY_THRESHOLDS["issue_spike_multiplier"],
                    ))

        # Critical score check
        if result.quality_score < self.ANOMALY_THRESHOLDS["min_quality_score"]:
            anomalies.append(QualityAnomaly(
                source_id=result.source_id,
                detected_at=datetime.utcnow(),
                anomaly_type="low_score",
                severity="error",
                description=f"품질 점수 임계값 미달: {result.quality_score:.1f}",
                baseline_value=self.ANOMALY_THRESHOLDS["min_quality_score"],
                current_value=result.quality_score,
                threshold=self.ANOMALY_THRESHOLDS["min_quality_score"],
            ))

        # Store and alert anomalies
        for anomaly in anomalies:
            await self._store_anomaly(anomaly)
            await self._send_anomaly_alert(anomaly, result)

    async def _get_baseline(self, source_id: str, days: int = 7) -> Optional[Dict]:
        """기준선 계산 (최근 N일 평균)"""
        if not self.mongo:
            return None

        since = datetime.utcnow() - timedelta(days=days)

        pipeline = [
            {
                "$match": {
                    "source_id": source_id,
                    "created_at": {"$gte": since}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "avg_quality_score": {"$avg": "$quality_score"},
                    "avg_issues": {"$avg": "$issue_summary.total"},
                    "count": {"$sum": 1}
                }
            }
        ]

        results = list(self.mongo.db.validation_results.aggregate(pipeline))

        if results and results[0]["count"] >= 3:  # 최소 3회 이상
            return {
                "avg_quality_score": results[0]["avg_quality_score"],
                "avg_issues": results[0]["avg_issues"],
            }

        return None

    async def _store_anomaly(self, anomaly: QualityAnomaly):
        """이상 감지 결과 저장"""
        if not self.mongo:
            return

        self.mongo.db.quality_anomalies.insert_one({
            "source_id": anomaly.source_id,
            "detected_at": anomaly.detected_at,
            "anomaly_type": anomaly.anomaly_type,
            "severity": anomaly.severity,
            "description": anomaly.description,
            "baseline_value": anomaly.baseline_value,
            "current_value": anomaly.current_value,
            "threshold": anomaly.threshold,
            "acknowledged": False,
        })

    async def _send_anomaly_alert(self, anomaly: QualityAnomaly, result: ValidationResult):
        """이상 감지 알림 발송"""
        if not self.alert_dispatcher:
            return

        severity_map = {
            "info": AlertSeverity.INFO,
            "warning": AlertSeverity.WARNING,
            "error": AlertSeverity.ERROR,
        }

        # 주요 이슈 요약
        top_issues = sorted(
            result.issue_count_by_rule.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        issue_summary = "\n".join([f"  - {rule}: {count}건" for rule, count in top_issues])

        message = f"""
데이터 품질 이상 감지

소스: {result.source_id}
Run ID: {result.run_id}
이상 유형: {anomaly.anomaly_type}

{anomaly.description}

품질 점수: {result.quality_score:.1f}점
총 이슈: {len(result.issues)}건

주요 이슈:
{issue_summary}
        """.strip()

        await self.alert_dispatcher.send_alert(
            title=f"데이터 품질 이상: {anomaly.anomaly_type}",
            message=message,
            severity=severity_map.get(anomaly.severity, AlertSeverity.WARNING),
            source_id=result.source_id,
            metadata={
                "quality_score": result.quality_score,
                "issue_count": len(result.issues),
                "anomaly_type": anomaly.anomaly_type,
            }
        )

    async def get_quality_trend(
        self,
        source_id: str,
        days: int = 30
    ) -> Optional[QualityTrend]:
        """품질 트렌드 조회"""
        if not self.mongo:
            return None

        since = datetime.utcnow() - timedelta(days=days)

        # Aggregate by day
        pipeline = [
            {
                "$match": {
                    "source_id": source_id,
                    "created_at": {"$gte": since}
                }
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}
                    },
                    "avg_score": {"$avg": "$quality_score"},
                    "total_issues": {"$sum": "$issue_summary.total"},
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]

        daily_data = list(self.mongo.db.validation_results.aggregate(pipeline))

        if not daily_data:
            return None

        # Overall stats
        overall_pipeline = [
            {
                "$match": {
                    "source_id": source_id,
                    "created_at": {"$gte": since}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "avg_score": {"$avg": "$quality_score"},
                    "min_score": {"$min": "$quality_score"},
                    "max_score": {"$max": "$quality_score"},
                    "total": {"$sum": 1},
                    "total_issues": {"$sum": "$issue_summary.total"}
                }
            }
        ]

        overall = list(self.mongo.db.validation_results.aggregate(overall_pipeline))

        if not overall:
            return None

        stats = overall[0]

        # Top issues by rule
        issue_pipeline = [
            {
                "$match": {
                    "source_id": source_id,
                    "created_at": {"$gte": since}
                }
            },
            {"$unwind": "$issues"},
            {
                "$group": {
                    "_id": "$issues.rule_name",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]

        top_issues = list(self.mongo.db.validation_results.aggregate(issue_pipeline))

        return QualityTrend(
            source_id=source_id,
            period_start=since,
            period_end=datetime.utcnow(),
            avg_quality_score=stats["avg_score"],
            min_quality_score=stats["min_score"],
            max_quality_score=stats["max_score"],
            total_validations=stats["total"],
            total_issues=stats["total_issues"],
            issue_trend=[
                {"date": d["_id"], "score": d["avg_score"], "issues": d["total_issues"]}
                for d in daily_data
            ],
            top_issues=[
                {"rule": i["_id"], "count": i["count"]}
                for i in top_issues
            ]
        )

    async def get_recent_anomalies(
        self,
        source_id: str = None,
        hours: int = 24,
        acknowledged: bool = None
    ) -> List[Dict]:
        """최근 이상 감지 조회"""
        if not self.mongo:
            return []

        since = datetime.utcnow() - timedelta(hours=hours)

        query = {"detected_at": {"$gte": since}}
        if source_id:
            query["source_id"] = source_id
        if acknowledged is not None:
            query["acknowledged"] = acknowledged

        anomalies = list(
            self.mongo.db.quality_anomalies
            .find(query)
            .sort("detected_at", -1)
            .limit(100)
        )

        return [
            {
                "id": str(a["_id"]),
                "source_id": a["source_id"],
                "detected_at": a["detected_at"].isoformat(),
                "anomaly_type": a["anomaly_type"],
                "severity": a["severity"],
                "description": a["description"],
                "acknowledged": a["acknowledged"],
            }
            for a in anomalies
        ]

    async def acknowledge_anomaly(self, anomaly_id: str) -> bool:
        """이상 감지 확인 처리"""
        if not self.mongo:
            return False

        from bson import ObjectId

        result = self.mongo.db.quality_anomalies.update_one(
            {"_id": ObjectId(anomaly_id)},
            {
                "$set": {
                    "acknowledged": True,
                    "acknowledged_at": datetime.utcnow()
                }
            }
        )

        return result.modified_count > 0

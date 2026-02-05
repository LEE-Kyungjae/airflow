"""
Quality Report - 데이터 품질 리포트 생성

기능:
- 종합 품질 리포트
- 필드별 상세 분석
- 이슈 패턴 분석
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict

from app.core import get_logger

logger = get_logger(__name__)


@dataclass
class FieldQualityReport:
    """필드별 품질 리포트"""
    field_name: str
    total_records: int
    null_count: int
    null_rate: float
    empty_count: int
    empty_rate: float
    unique_count: int
    unique_rate: float
    issue_count: int
    top_issues: List[Dict]


@dataclass
class QualityReport:
    """종합 품질 리포트"""
    source_id: str
    report_period: str
    generated_at: datetime

    # 요약 통계
    total_validations: int
    total_records: int
    avg_quality_score: float
    min_quality_score: float
    max_quality_score: float

    # 이슈 통계
    total_issues: int
    issues_by_severity: Dict[str, int]
    issues_by_rule: Dict[str, int]
    issues_by_field: Dict[str, int]

    # 필드별 상세
    field_reports: List[FieldQualityReport]

    # 트렌드
    quality_trend: List[Dict]

    # 권장 조치
    recommendations: List[str]

    def to_dict(self) -> Dict:
        """딕셔너리 변환"""
        return {
            "source_id": self.source_id,
            "report_period": self.report_period,
            "generated_at": self.generated_at.isoformat(),
            "summary": {
                "total_validations": self.total_validations,
                "total_records": self.total_records,
                "avg_quality_score": self.avg_quality_score,
                "min_quality_score": self.min_quality_score,
                "max_quality_score": self.max_quality_score,
            },
            "issues": {
                "total": self.total_issues,
                "by_severity": self.issues_by_severity,
                "by_rule": self.issues_by_rule,
                "by_field": self.issues_by_field,
            },
            "field_reports": [asdict(f) for f in self.field_reports],
            "quality_trend": self.quality_trend,
            "recommendations": self.recommendations,
        }


class QualityReportGenerator:
    """품질 리포트 생성기"""

    def __init__(self, mongo_service=None):
        self.mongo = mongo_service

    async def generate_report(
        self,
        source_id: str,
        days: int = 7
    ) -> Optional[QualityReport]:
        """종합 품질 리포트 생성"""
        if not self.mongo:
            logger.warning("MongoDB not configured")
            return None

        since = datetime.utcnow() - timedelta(days=days)

        # 기본 통계
        stats = await self._get_summary_stats(source_id, since)
        if not stats:
            return None

        # 이슈 통계
        issue_stats = await self._get_issue_stats(source_id, since)

        # 필드별 분석
        field_reports = await self._get_field_reports(source_id, since)

        # 트렌드
        trend = await self._get_quality_trend(source_id, since)

        # 권장 조치 생성
        recommendations = self._generate_recommendations(
            stats, issue_stats, field_reports
        )

        return QualityReport(
            source_id=source_id,
            report_period=f"Last {days} days",
            generated_at=datetime.utcnow(),
            total_validations=stats["total_validations"],
            total_records=stats["total_records"],
            avg_quality_score=stats["avg_score"],
            min_quality_score=stats["min_score"],
            max_quality_score=stats["max_score"],
            total_issues=issue_stats["total"],
            issues_by_severity=issue_stats["by_severity"],
            issues_by_rule=issue_stats["by_rule"],
            issues_by_field=issue_stats["by_field"],
            field_reports=field_reports,
            quality_trend=trend,
            recommendations=recommendations,
        )

    async def _get_summary_stats(self, source_id: str, since: datetime) -> Optional[Dict]:
        """요약 통계"""
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
                    "total_validations": {"$sum": 1},
                    "total_records": {"$sum": "$total_records"},
                    "avg_score": {"$avg": "$quality_score"},
                    "min_score": {"$min": "$quality_score"},
                    "max_score": {"$max": "$quality_score"},
                }
            }
        ]

        results = list(self.mongo.db.validation_results.aggregate(pipeline))

        if not results:
            return None

        return results[0]

    async def _get_issue_stats(self, source_id: str, since: datetime) -> Dict:
        """이슈 통계"""
        # By severity
        severity_pipeline = [
            {
                "$match": {
                    "source_id": source_id,
                    "created_at": {"$gte": since}
                }
            },
            {"$unwind": "$issues"},
            {
                "$group": {
                    "_id": "$issues.severity",
                    "count": {"$sum": 1}
                }
            }
        ]

        severity_results = list(self.mongo.db.validation_results.aggregate(severity_pipeline))
        by_severity = {r["_id"]: r["count"] for r in severity_results}

        # By rule
        rule_pipeline = [
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
            {"$limit": 20}
        ]

        rule_results = list(self.mongo.db.validation_results.aggregate(rule_pipeline))
        by_rule = {r["_id"]: r["count"] for r in rule_results}

        # By field
        field_pipeline = [
            {
                "$match": {
                    "source_id": source_id,
                    "created_at": {"$gte": since}
                }
            },
            {"$unwind": "$issues"},
            {
                "$group": {
                    "_id": "$issues.field_name",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]

        field_results = list(self.mongo.db.validation_results.aggregate(field_pipeline))
        by_field = {r["_id"]: r["count"] for r in field_results}

        total = sum(by_severity.values())

        return {
            "total": total,
            "by_severity": by_severity,
            "by_rule": by_rule,
            "by_field": by_field,
        }

    async def _get_field_reports(self, source_id: str, since: datetime) -> List[FieldQualityReport]:
        """필드별 상세 분석"""
        # Get latest validation result for field stats
        latest = self.mongo.db.validation_results.find_one(
            {"source_id": source_id},
            sort=[("created_at", -1)]
        )

        if not latest or "field_stats" not in latest:
            return []

        # Get field issues
        field_issues_pipeline = [
            {
                "$match": {
                    "source_id": source_id,
                    "created_at": {"$gte": since}
                }
            },
            {"$unwind": "$issues"},
            {
                "$group": {
                    "_id": {
                        "field": "$issues.field_name",
                        "rule": "$issues.rule_name"
                    },
                    "count": {"$sum": 1}
                }
            }
        ]

        field_issues = list(self.mongo.db.validation_results.aggregate(field_issues_pipeline))

        # Group issues by field
        issues_by_field = {}
        for item in field_issues:
            field = item["_id"]["field"]
            rule = item["_id"]["rule"]
            if field not in issues_by_field:
                issues_by_field[field] = {"total": 0, "rules": {}}
            issues_by_field[field]["total"] += item["count"]
            issues_by_field[field]["rules"][rule] = item["count"]

        # Build reports
        reports = []
        for field_name, stats in latest.get("field_stats", {}).items():
            top_issues = []
            if field_name in issues_by_field:
                top_issues = [
                    {"rule": rule, "count": count}
                    for rule, count in sorted(
                        issues_by_field[field_name]["rules"].items(),
                        key=lambda x: x[1],
                        reverse=True
                    )[:5]
                ]

            reports.append(FieldQualityReport(
                field_name=field_name,
                total_records=stats.get("total", 0),
                null_count=stats.get("null_count", 0),
                null_rate=stats.get("null_rate", 0),
                empty_count=stats.get("empty_count", 0),
                empty_rate=stats.get("empty_rate", 0),
                unique_count=stats.get("unique_count", 0),
                unique_rate=round(
                    stats.get("unique_count", 0) / stats.get("total", 1) * 100, 2
                ),
                issue_count=issues_by_field.get(field_name, {}).get("total", 0),
                top_issues=top_issues,
            ))

        return reports

    async def _get_quality_trend(self, source_id: str, since: datetime) -> List[Dict]:
        """품질 트렌드"""
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
                    "validations": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]

        results = list(self.mongo.db.validation_results.aggregate(pipeline))

        return [
            {
                "date": r["_id"],
                "quality_score": round(r["avg_score"], 1),
                "issue_count": r["total_issues"],
                "validation_count": r["validations"],
            }
            for r in results
        ]

    def _generate_recommendations(
        self,
        stats: Dict,
        issue_stats: Dict,
        field_reports: List[FieldQualityReport]
    ) -> List[str]:
        """권장 조치 생성"""
        recommendations = []

        # 품질 점수 기반
        if stats["avg_score"] < 80:
            recommendations.append(
                f"평균 품질 점수가 {stats['avg_score']:.1f}점으로 낮습니다. "
                "데이터 소스 및 크롤러 설정을 검토하세요."
            )

        if stats["min_score"] < 50:
            recommendations.append(
                f"최저 품질 점수가 {stats['min_score']:.1f}점입니다. "
                "해당 실행 결과를 상세 점검하세요."
            )

        # 이슈 유형별
        if issue_stats["by_rule"]:
            top_rule = max(issue_stats["by_rule"].items(), key=lambda x: x[1])
            if top_rule[1] > 100:
                recommendations.append(
                    f"'{top_rule[0]}' 규칙 위반이 {top_rule[1]}건으로 가장 많습니다. "
                    "해당 유형의 데이터 검토가 필요합니다."
                )

        # 인코딩 이슈
        encoding_issues = issue_stats["by_rule"].get("encoding_check", 0)
        if encoding_issues > 0:
            recommendations.append(
                f"인코딩 오류가 {encoding_issues}건 감지되었습니다. "
                "원본 데이터의 문자 인코딩(EUC-KR, UTF-8 등)을 확인하세요."
            )

        # 날짜 이슈
        date_issues = sum(
            v for k, v in issue_stats["by_rule"].items()
            if "date" in k.lower()
        )
        if date_issues > 0:
            recommendations.append(
                f"날짜 관련 오류가 {date_issues}건 발생했습니다. "
                "미래 날짜, 형식 오류 등을 점검하세요."
            )

        # 필드별 분석
        for field in field_reports:
            if field.null_rate > 50:
                recommendations.append(
                    f"'{field.field_name}' 필드의 NULL 비율이 {field.null_rate}%로 높습니다. "
                    "필수 필드 여부를 확인하세요."
                )

            if field.empty_rate > 30:
                recommendations.append(
                    f"'{field.field_name}' 필드의 빈 값 비율이 {field.empty_rate}%입니다. "
                    "데이터 수집 로직을 점검하세요."
                )

        if not recommendations:
            recommendations.append("현재 심각한 품질 이슈가 감지되지 않았습니다.")

        return recommendations[:10]  # 최대 10개

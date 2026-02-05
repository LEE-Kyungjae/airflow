"""
Contract Reporter - 검증 결과 리포트 생성

다양한 형식의 검증 결과 리포트를 생성합니다:
- 요약 리포트
- 상세 리포트
- HTML 리포트
- JSON 리포트
- 대시보드용 메트릭
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from collections import defaultdict

from .contract import ContractValidationResult
from .expectations import ExpectationSeverity, ExpectationResult


@dataclass
class ValidationTrend:
    """검증 트렌드 데이터"""
    date: str
    total_validations: int
    success_count: int
    failure_count: int
    success_rate: float
    avg_quality_score: float


@dataclass
class ContractHealthMetrics:
    """계약 건강 상태 메트릭"""
    contract_id: str
    contract_name: str
    last_validation: Optional[datetime]
    total_runs: int
    recent_success_rate: float  # 최근 7일
    overall_success_rate: float
    most_failed_expectations: List[Dict[str, Any]]
    health_status: str  # healthy, warning, critical


class ContractReporter:
    """
    검증 결과 리포터

    검증 결과를 다양한 형식의 리포트로 생성합니다.
    """

    def __init__(self, mongo_service=None):
        self.mongo = mongo_service

    def generate_summary_report(
        self,
        result: ContractValidationResult,
        include_details: bool = False
    ) -> Dict[str, Any]:
        """
        검증 결과 요약 리포트 생성

        Args:
            result: 검증 결과
            include_details: 상세 정보 포함 여부

        Returns:
            요약 리포트 딕셔너리
        """
        report = {
            "report_type": "summary",
            "generated_at": datetime.utcnow().isoformat(),
            "contract": {
                "id": result.contract_id,
                "name": result.contract_name,
                "version": result.contract_version,
            },
            "validation": {
                "run_time": result.run_time.isoformat(),
                "success": result.success,
                "success_rate": round(result.success_rate, 2),
            },
            "statistics": {
                "total_expectations": len(result.results),
                "passed": result.passed_count,
                "failed": result.failed_count,
                "data_row_count": result.statistics.get("data_row_count", 0),
            },
            "failures": {
                "critical": len(result.critical_failures),
                "error": len(result.error_failures),
                "warning": len(result.warning_failures),
            }
        }

        if include_details:
            report["failed_expectations"] = [
                {
                    "type": r.expectation_type,
                    "column": r.column,
                    "severity": r.severity.value,
                    "unexpected_count": r.unexpected_count,
                    "unexpected_percent": round(r.unexpected_percent, 2),
                }
                for r in result.results if not r.success
            ]

        return report

    def generate_detailed_report(
        self,
        result: ContractValidationResult,
        max_samples: int = 10
    ) -> Dict[str, Any]:
        """
        상세 검증 리포트 생성

        Args:
            result: 검증 결과
            max_samples: 샘플 값 최대 개수

        Returns:
            상세 리포트 딕셔너리
        """
        report = self.generate_summary_report(result, include_details=True)
        report["report_type"] = "detailed"

        # 기대치별 상세 결과
        expectations_detail = []
        for exp_result in result.results:
            detail = {
                "expectation_type": exp_result.expectation_type,
                "column": exp_result.column,
                "success": exp_result.success,
                "result": exp_result.result.value,
                "severity": exp_result.severity.value,
                "statistics": {
                    "element_count": exp_result.element_count,
                    "unexpected_count": exp_result.unexpected_count,
                    "unexpected_percent": round(exp_result.unexpected_percent, 2),
                },
                "details": exp_result.details,
            }

            # 실패한 경우 샘플 추가
            if not exp_result.success:
                detail["unexpected_samples"] = exp_result.unexpected_values[:max_samples]
                detail["sample_indices"] = exp_result.unexpected_index_list[:max_samples]

            if exp_result.exception_info:
                detail["exception"] = exp_result.exception_info

            expectations_detail.append(detail)

        report["expectations"] = expectations_detail

        return report

    def generate_html_report(
        self,
        result: ContractValidationResult,
        title: Optional[str] = None
    ) -> str:
        """
        HTML 형식 리포트 생성

        Args:
            result: 검증 결과
            title: 리포트 제목

        Returns:
            HTML 문자열
        """
        title = title or f"Data Contract Validation Report: {result.contract_name}"

        # 상태 색상
        status_color = "#28a745" if result.success else "#dc3545"
        status_text = "PASSED" if result.success else "FAILED"

        # 실패 기대치 행 생성
        failure_rows = ""
        for exp_result in result.results:
            if not exp_result.success:
                severity_color = {
                    "critical": "#dc3545",
                    "error": "#fd7e14",
                    "warning": "#ffc107",
                    "info": "#17a2b8"
                }.get(exp_result.severity.value, "#6c757d")

                failure_rows += f"""
                <tr>
                    <td>{exp_result.expectation_type}</td>
                    <td>{exp_result.column or '-'}</td>
                    <td><span style="color: {severity_color}; font-weight: bold;">{exp_result.severity.value.upper()}</span></td>
                    <td>{exp_result.unexpected_count}</td>
                    <td>{exp_result.unexpected_percent:.2f}%</td>
                </tr>
                """

        # 모든 기대치 상태 표시
        all_expectations_rows = ""
        for exp_result in result.results:
            status_icon = "&#10003;" if exp_result.success else "&#10007;"
            row_color = "#d4edda" if exp_result.success else "#f8d7da"

            all_expectations_rows += f"""
            <tr style="background-color: {row_color}">
                <td>{status_icon}</td>
                <td>{exp_result.expectation_type}</td>
                <td>{exp_result.column or '-'}</td>
                <td>{exp_result.element_count}</td>
                <td>{exp_result.unexpected_count}</td>
            </tr>
            """

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        .status-badge {{ display: inline-block; padding: 8px 16px; border-radius: 4px; color: white; font-weight: bold; font-size: 18px; }}
        .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .summary-card {{ background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; }}
        .summary-card .value {{ font-size: 24px; font-weight: bold; color: #333; }}
        .summary-card .label {{ color: #666; margin-top: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #007bff; color: white; }}
        tr:hover {{ background-color: #f5f5f5; }}
        .meta-info {{ color: #666; font-size: 14px; margin-bottom: 20px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>

        <div class="meta-info">
            <p>Contract ID: {result.contract_id} | Version: {result.contract_version}</p>
            <p>Validation Time: {result.run_time.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        </div>

        <div style="margin: 20px 0;">
            <span class="status-badge" style="background-color: {status_color};">{status_text}</span>
        </div>

        <div class="summary-grid">
            <div class="summary-card">
                <div class="value">{result.success_rate:.1f}%</div>
                <div class="label">Success Rate</div>
            </div>
            <div class="summary-card">
                <div class="value">{len(result.results)}</div>
                <div class="label">Total Expectations</div>
            </div>
            <div class="summary-card">
                <div class="value" style="color: #28a745;">{result.passed_count}</div>
                <div class="label">Passed</div>
            </div>
            <div class="summary-card">
                <div class="value" style="color: #dc3545;">{result.failed_count}</div>
                <div class="label">Failed</div>
            </div>
            <div class="summary-card">
                <div class="value">{result.statistics.get('data_row_count', 0)}</div>
                <div class="label">Data Rows</div>
            </div>
        </div>

        <h2>Failure Summary</h2>
        <div class="summary-grid">
            <div class="summary-card">
                <div class="value" style="color: #dc3545;">{len(result.critical_failures)}</div>
                <div class="label">Critical</div>
            </div>
            <div class="summary-card">
                <div class="value" style="color: #fd7e14;">{len(result.error_failures)}</div>
                <div class="label">Error</div>
            </div>
            <div class="summary-card">
                <div class="value" style="color: #ffc107;">{len(result.warning_failures)}</div>
                <div class="label">Warning</div>
            </div>
        </div>

        {f'''
        <h2>Failed Expectations</h2>
        <table>
            <tr>
                <th>Expectation Type</th>
                <th>Column</th>
                <th>Severity</th>
                <th>Unexpected Count</th>
                <th>Unexpected %</th>
            </tr>
            {failure_rows}
        </table>
        ''' if failure_rows else ''}

        <h2>All Expectations</h2>
        <table>
            <tr>
                <th>Status</th>
                <th>Expectation Type</th>
                <th>Column</th>
                <th>Element Count</th>
                <th>Unexpected Count</th>
            </tr>
            {all_expectations_rows}
        </table>

        <div style="margin-top: 30px; color: #666; font-size: 12px;">
            Generated by Data Contract Validator | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
        </div>
    </div>
</body>
</html>
        """

        return html

    def generate_json_report(
        self,
        result: ContractValidationResult,
        pretty: bool = True
    ) -> str:
        """
        JSON 형식 리포트 생성

        Args:
            result: 검증 결과
            pretty: 예쁘게 출력 여부

        Returns:
            JSON 문자열
        """
        report = self.generate_detailed_report(result)
        if pretty:
            return json.dumps(report, indent=2, ensure_ascii=False, default=str)
        return json.dumps(report, ensure_ascii=False, default=str)

    async def generate_dashboard_metrics(
        self,
        source_id: Optional[str] = None,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        대시보드용 메트릭 생성

        Args:
            source_id: 특정 소스로 필터링 (None이면 전체)
            days: 조회 기간 (일)

        Returns:
            대시보드 메트릭
        """
        if not self.mongo:
            return {"error": "MongoDB not configured"}

        since = datetime.utcnow() - timedelta(days=days)

        # 쿼리 조건
        query = {"run_time": {"$gte": since.isoformat()}}
        if source_id:
            query["source_id"] = source_id

        # 데이터 조회
        validations = list(
            self.mongo.db.contract_validations.find(query).sort("run_time", -1)
        )

        if not validations:
            return {
                "period_days": days,
                "source_id": source_id,
                "total_validations": 0,
                "metrics": {}
            }

        # 메트릭 계산
        total = len(validations)
        success_count = sum(1 for v in validations if v.get("success", False))
        failure_count = total - success_count

        # 일별 트렌드
        daily_stats = defaultdict(lambda: {"total": 0, "success": 0, "quality_scores": []})
        for v in validations:
            run_time = v.get("run_time", "")
            if isinstance(run_time, str):
                date = run_time[:10]
            else:
                date = run_time.strftime("%Y-%m-%d")

            daily_stats[date]["total"] += 1
            if v.get("success"):
                daily_stats[date]["success"] += 1
            daily_stats[date]["quality_scores"].append(
                v.get("statistics", {}).get("success_rate", 0)
            )

        trends = []
        for date, stats in sorted(daily_stats.items()):
            trends.append(ValidationTrend(
                date=date,
                total_validations=stats["total"],
                success_count=stats["success"],
                failure_count=stats["total"] - stats["success"],
                success_rate=round((stats["success"] / stats["total"] * 100) if stats["total"] > 0 else 0, 2),
                avg_quality_score=round(sum(stats["quality_scores"]) / len(stats["quality_scores"]) if stats["quality_scores"] else 0, 2)
            ))

        # 가장 많이 실패한 기대치
        failed_expectations = defaultdict(int)
        for v in validations:
            for r in v.get("results", []):
                if not r.get("success", True):
                    key = f"{r.get('expectation_type')}:{r.get('column', 'N/A')}"
                    failed_expectations[key] += 1

        top_failures = [
            {"expectation": k, "count": v}
            for k, v in sorted(failed_expectations.items(), key=lambda x: -x[1])[:10]
        ]

        # 계약별 통계
        contract_stats = defaultdict(lambda: {"total": 0, "success": 0})
        for v in validations:
            contract_name = v.get("contract_name", "unknown")
            contract_stats[contract_name]["total"] += 1
            if v.get("success"):
                contract_stats[contract_name]["success"] += 1

        contracts_summary = [
            {
                "contract_name": name,
                "total_runs": stats["total"],
                "success_rate": round((stats["success"] / stats["total"] * 100) if stats["total"] > 0 else 0, 2)
            }
            for name, stats in contract_stats.items()
        ]

        return {
            "period_days": days,
            "source_id": source_id,
            "generated_at": datetime.utcnow().isoformat(),
            "summary": {
                "total_validations": total,
                "success_count": success_count,
                "failure_count": failure_count,
                "overall_success_rate": round((success_count / total * 100) if total > 0 else 0, 2),
            },
            "trends": [
                {
                    "date": t.date,
                    "total": t.total_validations,
                    "success": t.success_count,
                    "failure": t.failure_count,
                    "success_rate": t.success_rate,
                    "avg_quality": t.avg_quality_score,
                }
                for t in trends
            ],
            "top_failures": top_failures,
            "contracts": contracts_summary,
        }

    async def get_contract_health(
        self,
        contract_id: str
    ) -> Optional[ContractHealthMetrics]:
        """
        계약 건강 상태 조회

        Args:
            contract_id: 계약 ID

        Returns:
            ContractHealthMetrics 또는 None
        """
        if not self.mongo:
            return None

        # 최근 30일 데이터 조회
        since = datetime.utcnow() - timedelta(days=30)
        recent_since = datetime.utcnow() - timedelta(days=7)

        all_validations = list(
            self.mongo.db.contract_validations.find({
                "contract_id": contract_id,
                "run_time": {"$gte": since.isoformat()}
            }).sort("run_time", -1)
        )

        if not all_validations:
            return None

        # 최신 검증 정보
        latest = all_validations[0]
        contract_name = latest.get("contract_name", "unknown")

        # 전체 성공률
        total_success = sum(1 for v in all_validations if v.get("success"))
        overall_rate = (total_success / len(all_validations) * 100) if all_validations else 0

        # 최근 7일 성공률
        recent_validations = [
            v for v in all_validations
            if v.get("run_time", "") >= recent_since.isoformat()
        ]
        recent_success = sum(1 for v in recent_validations if v.get("success"))
        recent_rate = (recent_success / len(recent_validations) * 100) if recent_validations else overall_rate

        # 가장 많이 실패한 기대치
        failed_expectations = defaultdict(int)
        for v in all_validations:
            for r in v.get("results", []):
                if not r.get("success", True):
                    key = f"{r.get('expectation_type')}:{r.get('column', 'N/A')}"
                    failed_expectations[key] += 1

        most_failed = [
            {"expectation": k, "count": v}
            for k, v in sorted(failed_expectations.items(), key=lambda x: -x[1])[:5]
        ]

        # 건강 상태 결정
        if recent_rate >= 90:
            health_status = "healthy"
        elif recent_rate >= 70:
            health_status = "warning"
        else:
            health_status = "critical"

        return ContractHealthMetrics(
            contract_id=contract_id,
            contract_name=contract_name,
            last_validation=datetime.fromisoformat(latest.get("run_time")) if isinstance(latest.get("run_time"), str) else latest.get("run_time"),
            total_runs=len(all_validations),
            recent_success_rate=round(recent_rate, 2),
            overall_success_rate=round(overall_rate, 2),
            most_failed_expectations=most_failed,
            health_status=health_status,
        )

    def format_console_report(
        self,
        result: ContractValidationResult,
        use_colors: bool = True
    ) -> str:
        """
        콘솔 출력용 텍스트 리포트 생성

        Args:
            result: 검증 결과
            use_colors: ANSI 색상 사용 여부

        Returns:
            텍스트 리포트
        """
        # ANSI 색상 코드
        if use_colors:
            GREEN = "\033[92m"
            RED = "\033[91m"
            YELLOW = "\033[93m"
            BLUE = "\033[94m"
            BOLD = "\033[1m"
            RESET = "\033[0m"
        else:
            GREEN = RED = YELLOW = BLUE = BOLD = RESET = ""

        lines = [
            f"\n{BOLD}{'='*60}{RESET}",
            f"{BOLD}Data Contract Validation Report{RESET}",
            f"{'='*60}",
            f"",
            f"Contract: {result.contract_name} (v{result.contract_version})",
            f"Contract ID: {result.contract_id}",
            f"Run Time: {result.run_time.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"",
        ]

        # 전체 상태
        status = f"{GREEN}PASSED{RESET}" if result.success else f"{RED}FAILED{RESET}"
        lines.append(f"{BOLD}Status: {status}{RESET}")
        lines.append(f"Success Rate: {result.success_rate:.1f}%")
        lines.append(f"")

        # 요약 통계
        lines.append(f"{BOLD}Summary:{RESET}")
        lines.append(f"  Total Expectations: {len(result.results)}")
        lines.append(f"  Passed: {GREEN}{result.passed_count}{RESET}")
        lines.append(f"  Failed: {RED}{result.failed_count}{RESET}")
        lines.append(f"  Data Rows: {result.statistics.get('data_row_count', 0)}")
        lines.append(f"")

        # 실패 분석
        if result.failed_count > 0:
            lines.append(f"{BOLD}Failures by Severity:{RESET}")
            lines.append(f"  {RED}Critical: {len(result.critical_failures)}{RESET}")
            lines.append(f"  {YELLOW}Error: {len(result.error_failures)}{RESET}")
            lines.append(f"  Warning: {len(result.warning_failures)}")
            lines.append(f"")

            lines.append(f"{BOLD}Failed Expectations:{RESET}")
            for exp_result in result.results:
                if not exp_result.success:
                    severity_color = {
                        "critical": RED,
                        "error": YELLOW,
                        "warning": BLUE,
                    }.get(exp_result.severity.value, "")

                    lines.append(
                        f"  [{severity_color}{exp_result.severity.value.upper()}{RESET}] "
                        f"{exp_result.expectation_type}"
                        f"{f' ({exp_result.column})' if exp_result.column else ''}: "
                        f"{exp_result.unexpected_count} unexpected ({exp_result.unexpected_percent:.1f}%)"
                    )

        lines.append(f"\n{'='*60}\n")

        return "\n".join(lines)

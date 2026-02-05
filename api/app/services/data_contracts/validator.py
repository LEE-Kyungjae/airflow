"""
Contract Validator - 데이터 계약 검증 실행기

ContractValidator는 DataContract를 사용하여 데이터를 검증하고
결과를 저장/알림 처리합니다.

기능:
- 계약 기반 데이터 검증
- 검증 결과 MongoDB 저장
- 실패 시 알림 트리거
- 검증 이력 관리
- ETL 파이프라인 연동
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

from .contract import DataContract, ContractValidationResult, ContractStatus, ContractTemplates
from .expectations import ExpectationSeverity

logger = logging.getLogger(__name__)


class ValidationAction(str, Enum):
    """검증 실패 시 액션"""
    PASS = "pass"           # 통과 (무시)
    WARN = "warn"           # 경고 로깅
    REJECT = "reject"       # 거부 (staging 분류)
    ALERT = "alert"         # 알림 발송
    QUARANTINE = "quarantine"  # 격리 (별도 컬렉션)


@dataclass
class ValidationConfig:
    """검증 설정"""
    # 심각도별 액션
    on_critical: ValidationAction = ValidationAction.REJECT
    on_error: ValidationAction = ValidationAction.REJECT
    on_warning: ValidationAction = ValidationAction.WARN
    on_info: ValidationAction = ValidationAction.PASS

    # 알림 설정
    alert_on_failure: bool = True
    alert_threshold_percent: float = 10.0  # 실패율이 이 값 이상이면 알림

    # 저장 설정
    save_results: bool = True
    save_unexpected_samples: int = 10  # 저장할 예상 외 값 개수

    # 재시도 설정
    retry_on_failure: bool = False
    max_retries: int = 1


@dataclass
class ValidatedData:
    """검증된 데이터 결과"""
    # 검증 통과 데이터
    valid_data: List[Dict[str, Any]] = field(default_factory=list)

    # 검증 실패 데이터 (staging으로 분류)
    invalid_data: List[Dict[str, Any]] = field(default_factory=list)

    # 격리 데이터 (심각한 문제)
    quarantined_data: List[Dict[str, Any]] = field(default_factory=list)

    # 검증 결과
    validation_result: Optional[ContractValidationResult] = None

    # 통계
    total_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    quarantined_count: int = 0

    @property
    def success_rate(self) -> float:
        if self.total_count == 0:
            return 100.0
        return (self.valid_count / self.total_count) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_count": self.total_count,
            "valid_count": self.valid_count,
            "invalid_count": self.invalid_count,
            "quarantined_count": self.quarantined_count,
            "success_rate": round(self.success_rate, 2),
            "validation_result": self.validation_result.to_dict() if self.validation_result else None,
        }


class ContractValidator:
    """
    데이터 계약 검증기

    DataContract를 사용하여 데이터를 검증하고,
    결과에 따라 적절한 액션을 수행합니다.

    Example:
        validator = ContractValidator(mongo_service)
        contract = ContractTemplates.news_articles()

        result = await validator.validate(
            contract=contract,
            data=crawled_data,
            source_id="source_123"
        )

        if result.validation_result.success:
            # ETL 진행
            pass
        else:
            # Staging으로 분류됨
            pass
    """

    def __init__(
        self,
        mongo_service=None,
        alert_dispatcher=None,
        config: Optional[ValidationConfig] = None
    ):
        """
        ContractValidator 초기화

        Args:
            mongo_service: MongoDB 서비스 (결과 저장용)
            alert_dispatcher: 알림 발송기
            config: 검증 설정
        """
        self.mongo = mongo_service
        self.alert_dispatcher = alert_dispatcher
        self.config = config or ValidationConfig()

    async def validate(
        self,
        contract: DataContract,
        data: List[Dict[str, Any]],
        source_id: str,
        run_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> ValidatedData:
        """
        계약을 사용하여 데이터 검증

        Args:
            contract: 검증에 사용할 데이터 계약
            data: 검증할 데이터
            source_id: 소스 ID
            run_id: 실행 ID (없으면 자동 생성)
            context: 추가 컨텍스트 정보

        Returns:
            ValidatedData: 검증 결과와 분류된 데이터
        """
        run_id = run_id or f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        context = context or {}

        # 1. 계약 검증 실행
        validation_result = contract.validate(
            data=data,
            evaluation_parameters=context
        )

        logger.info(
            f"Contract validation completed: {contract.name}",
            extra={
                "source_id": source_id,
                "success": validation_result.success,
                "passed": validation_result.passed_count,
                "failed": validation_result.failed_count,
            }
        )

        # 2. 데이터 분류
        validated_data = self._classify_data(
            data=data,
            validation_result=validation_result
        )
        validated_data.validation_result = validation_result

        # 3. 결과 저장
        if self.config.save_results and self.mongo:
            await self._save_validation_result(
                validation_result=validation_result,
                source_id=source_id,
                run_id=run_id,
                context=context
            )

        # 4. 알림 처리
        if not validation_result.success and self.config.alert_on_failure:
            await self._handle_failure_alert(
                validation_result=validation_result,
                source_id=source_id,
                context=context
            )

        return validated_data

    async def validate_with_auto_contract(
        self,
        data: List[Dict[str, Any]],
        source_id: str,
        data_category: str,
        run_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> ValidatedData:
        """
        데이터 카테고리에 맞는 계약을 자동 선택하여 검증

        Args:
            data: 검증할 데이터
            source_id: 소스 ID
            data_category: 데이터 카테고리 (news_article, financial_data 등)
            run_id: 실행 ID
            context: 추가 컨텍스트

        Returns:
            ValidatedData
        """
        # 카테고리별 계약 자동 선택
        category_mapping = {
            "news_article": ContractTemplates.news_articles,
            "financial_data": ContractTemplates.financial_data,
            "stock_price": ContractTemplates.stock_prices,
            "exchange_rate": ContractTemplates.exchange_rates,
        }

        template_func = category_mapping.get(data_category, ContractTemplates.generic_table)
        contract = template_func(source_id=source_id)

        return await self.validate(
            contract=contract,
            data=data,
            source_id=source_id,
            run_id=run_id,
            context=context
        )

    async def validate_and_route(
        self,
        contract: DataContract,
        data: List[Dict[str, Any]],
        source_id: str,
        on_valid: Optional[Callable] = None,
        on_invalid: Optional[Callable] = None,
        on_quarantine: Optional[Callable] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> ValidatedData:
        """
        검증 후 결과에 따라 라우팅

        Args:
            contract: 데이터 계약
            data: 검증할 데이터
            source_id: 소스 ID
            on_valid: 유효 데이터 처리 콜백
            on_invalid: 무효 데이터 처리 콜백 (staging)
            on_quarantine: 격리 데이터 처리 콜백
            context: 추가 컨텍스트

        Returns:
            ValidatedData
        """
        validated_data = await self.validate(
            contract=contract,
            data=data,
            source_id=source_id,
            context=context
        )

        # 라우팅 실행
        if on_valid and validated_data.valid_data:
            await self._safe_callback(on_valid, validated_data.valid_data)

        if on_invalid and validated_data.invalid_data:
            await self._safe_callback(on_invalid, validated_data.invalid_data)

        if on_quarantine and validated_data.quarantined_data:
            await self._safe_callback(on_quarantine, validated_data.quarantined_data)

        return validated_data

    def _classify_data(
        self,
        data: List[Dict[str, Any]],
        validation_result: ContractValidationResult
    ) -> ValidatedData:
        """
        검증 결과에 따라 데이터 분류

        행 단위로 문제가 있는 데이터를 분류합니다.
        """
        # 문제 있는 행 인덱스 수집
        problematic_indices: Dict[int, str] = {}  # index -> severity

        for result in validation_result.results:
            if not result.success:
                severity = result.severity.value
                for idx in result.unexpected_index_list:
                    # 더 심각한 문제가 있으면 유지
                    existing = problematic_indices.get(idx)
                    if existing is None or self._severity_priority(severity) > self._severity_priority(existing):
                        problematic_indices[idx] = severity

        # 데이터 분류
        valid_data = []
        invalid_data = []
        quarantined_data = []

        for idx, record in enumerate(data):
            severity = problematic_indices.get(idx)

            if severity is None:
                # 문제 없음
                valid_data.append(record)
            elif severity == "critical":
                # 격리
                record_with_meta = {**record, "_validation_issue": "critical"}
                quarantined_data.append(record_with_meta)
            else:
                # 무효 (staging으로)
                record_with_meta = {**record, "_validation_issue": severity}
                invalid_data.append(record_with_meta)

        return ValidatedData(
            valid_data=valid_data,
            invalid_data=invalid_data,
            quarantined_data=quarantined_data,
            total_count=len(data),
            valid_count=len(valid_data),
            invalid_count=len(invalid_data),
            quarantined_count=len(quarantined_data),
        )

    def _severity_priority(self, severity: str) -> int:
        """심각도 우선순위"""
        priorities = {
            "critical": 4,
            "error": 3,
            "warning": 2,
            "info": 1,
        }
        return priorities.get(severity, 0)

    async def _save_validation_result(
        self,
        validation_result: ContractValidationResult,
        source_id: str,
        run_id: str,
        context: Dict[str, Any]
    ):
        """검증 결과 MongoDB 저장"""
        try:
            result_doc = {
                **validation_result.to_dict(),
                "source_id": source_id,
                "run_id": run_id,
                "context": context,
                "saved_at": datetime.utcnow(),
            }

            # 예상 외 값 샘플만 저장
            for result in result_doc.get("results", []):
                if "unexpected_values" in result:
                    result["unexpected_values"] = result["unexpected_values"][:self.config.save_unexpected_samples]
                if "unexpected_index_list" in result:
                    result["unexpected_index_list"] = result["unexpected_index_list"][:100]

            self.mongo.db.contract_validations.insert_one(result_doc)

            logger.debug(
                f"Validation result saved",
                extra={"contract_id": validation_result.contract_id, "run_id": run_id}
            )

        except Exception as e:
            logger.error(f"Failed to save validation result: {e}")

    async def _handle_failure_alert(
        self,
        validation_result: ContractValidationResult,
        source_id: str,
        context: Dict[str, Any]
    ):
        """실패 알림 처리"""
        if not self.alert_dispatcher:
            return

        # 실패율 체크
        failure_rate = 100 - validation_result.success_rate
        if failure_rate < self.config.alert_threshold_percent:
            return

        try:
            from ..alerts import AlertSeverity

            # 심각도 결정
            if validation_result.critical_failures:
                severity = AlertSeverity.CRITICAL
            elif validation_result.error_failures:
                severity = AlertSeverity.ERROR
            else:
                severity = AlertSeverity.WARNING

            # 알림 발송
            await self.alert_dispatcher.send_alert(
                title=f"Data Contract Validation Failed: {validation_result.contract_name}",
                message=self._format_failure_message(validation_result),
                severity=severity,
                source_id=source_id,
                error_code="CONTRACT_VALIDATION_FAILED",
                metadata={
                    "contract_id": validation_result.contract_id,
                    "contract_version": validation_result.contract_version,
                    "failure_rate": round(failure_rate, 2),
                    "critical_failures": len(validation_result.critical_failures),
                    "error_failures": len(validation_result.error_failures),
                    **context
                }
            )

        except Exception as e:
            logger.error(f"Failed to send alert: {e}")

    def _format_failure_message(self, result: ContractValidationResult) -> str:
        """실패 알림 메시지 포맷"""
        lines = [
            f"계약: {result.contract_name} (v{result.contract_version})",
            f"검증 시간: {result.run_time.isoformat()}",
            f"성공률: {result.success_rate:.1f}%",
            f"",
            f"실패 요약:",
            f"  - Critical: {len(result.critical_failures)}",
            f"  - Error: {len(result.error_failures)}",
            f"  - Warning: {len(result.warning_failures)}",
        ]

        # 주요 실패 항목 추가
        all_failures = result.critical_failures + result.error_failures
        if all_failures:
            lines.append("")
            lines.append("주요 실패 항목:")
            for failure in all_failures[:5]:
                lines.append(f"  - [{failure.column}] {failure.expectation_type}: {failure.unexpected_count}건")

        return "\n".join(lines)

    async def _safe_callback(self, callback: Callable, data: List[Dict[str, Any]]):
        """콜백 안전 실행"""
        try:
            import asyncio
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            logger.error(f"Callback execution failed: {e}")


class ContractRegistry:
    """
    계약 레지스트리

    소스별 데이터 계약을 관리하고 조회합니다.
    """

    def __init__(self, mongo_service=None):
        self.mongo = mongo_service
        self._cache: Dict[str, DataContract] = {}

    async def register(self, contract: DataContract) -> str:
        """계약 등록"""
        if self.mongo:
            result = self.mongo.db.data_contracts.update_one(
                {"contract_id": contract.contract_id},
                {"$set": contract.to_dict()},
                upsert=True
            )

        self._cache[contract.contract_id] = contract
        logger.info(f"Contract registered: {contract.name} ({contract.contract_id})")
        return contract.contract_id

    async def get_by_id(self, contract_id: str) -> Optional[DataContract]:
        """ID로 계약 조회"""
        # 캐시 확인
        if contract_id in self._cache:
            return self._cache[contract_id]

        # DB 조회
        if self.mongo:
            doc = self.mongo.db.data_contracts.find_one({"contract_id": contract_id})
            if doc:
                contract = DataContract.from_dict(doc)
                self._cache[contract_id] = contract
                return contract

        return None

    async def get_by_source(self, source_id: str) -> Optional[DataContract]:
        """소스 ID로 계약 조회"""
        # 캐시에서 검색
        for contract in self._cache.values():
            if contract.source_id == source_id and contract.status == ContractStatus.ACTIVE:
                return contract

        # DB 조회
        if self.mongo:
            doc = self.mongo.db.data_contracts.find_one({
                "source_id": source_id,
                "status": ContractStatus.ACTIVE.value
            })
            if doc:
                contract = DataContract.from_dict(doc)
                self._cache[contract.contract_id] = contract
                return contract

        return None

    async def get_all_active(self) -> List[DataContract]:
        """모든 활성 계약 조회"""
        contracts = []

        if self.mongo:
            cursor = self.mongo.db.data_contracts.find({"status": ContractStatus.ACTIVE.value})
            for doc in cursor:
                contract = DataContract.from_dict(doc)
                self._cache[contract.contract_id] = contract
                contracts.append(contract)

        return contracts

    async def deactivate(self, contract_id: str) -> bool:
        """계약 비활성화"""
        if self.mongo:
            result = self.mongo.db.data_contracts.update_one(
                {"contract_id": contract_id},
                {"$set": {"status": ContractStatus.DEPRECATED.value}}
            )

            if result.modified_count > 0:
                if contract_id in self._cache:
                    self._cache[contract_id].status = ContractStatus.DEPRECATED
                return True

        return False

    def clear_cache(self):
        """캐시 초기화"""
        self._cache.clear()

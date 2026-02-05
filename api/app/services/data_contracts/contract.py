"""
Data Contract - Great Expectations 스타일의 데이터 계약 정의

DataContract는 데이터에 대한 기대치 모음을 정의하고 관리합니다.
- 여러 기대치를 그룹화
- JSON/YAML 형태로 직렬화/역직렬화
- 소스별 계약 관리
- 버전 관리 지원
"""

import json
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Type, Union
from dataclasses import dataclass, field
from enum import Enum

from .expectations import (
    Expectation,
    ExpectationValidationResult,
    ExpectationSeverity,
    ExpectationResult,
    ExpectColumnNotNull,
    ExpectColumnUnique,
    ExpectColumnValuesInRange,
    ExpectColumnValuesToMatchRegex,
    ExpectTableRowCountBetween,
    ExpectColumnValuesToBeOfType,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValueLengthToBeBetween,
    ExpectColumnPairValuesToBeEqual,
)


class ContractStatus(str, Enum):
    """계약 상태"""
    DRAFT = "draft"         # 초안 (검증 미적용)
    ACTIVE = "active"       # 활성화됨
    DEPRECATED = "deprecated"  # 더 이상 사용 안 함
    ARCHIVED = "archived"   # 보관됨


@dataclass
class ContractValidationResult:
    """계약 전체 검증 결과"""
    contract_id: str
    contract_name: str
    contract_version: str
    success: bool
    run_time: datetime
    statistics: Dict[str, Any]
    results: List[ExpectationValidationResult]
    evaluation_parameters: Dict[str, Any] = field(default_factory=dict)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.success)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def success_rate(self) -> float:
        if not self.results:
            return 100.0
        return (self.passed_count / len(self.results)) * 100

    @property
    def critical_failures(self) -> List[ExpectationValidationResult]:
        return [r for r in self.results if not r.success and r.severity == ExpectationSeverity.CRITICAL]

    @property
    def error_failures(self) -> List[ExpectationValidationResult]:
        return [r for r in self.results if not r.success and r.severity == ExpectationSeverity.ERROR]

    @property
    def warning_failures(self) -> List[ExpectationValidationResult]:
        return [r for r in self.results if not r.success and r.severity == ExpectationSeverity.WARNING]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "contract_name": self.contract_name,
            "contract_version": self.contract_version,
            "success": self.success,
            "run_time": self.run_time.isoformat(),
            "statistics": {
                "total_expectations": len(self.results),
                "passed": self.passed_count,
                "failed": self.failed_count,
                "success_rate": round(self.success_rate, 2),
                "critical_failures": len(self.critical_failures),
                "error_failures": len(self.error_failures),
                "warning_failures": len(self.warning_failures),
            },
            "evaluation_parameters": self.evaluation_parameters,
            "results": [r.to_dict() for r in self.results],
        }


class DataContract:
    """
    데이터 계약 정의

    Great Expectations 스타일의 데이터 계약으로,
    여러 기대치를 그룹화하고 관리합니다.

    Example:
        contract = DataContract(
            name="news_articles",
            description="뉴스 기사 데이터 품질 계약"
        )
        contract.add_expectation(expect_column_not_null("title"))
        contract.add_expectation(expect_column_unique("url"))
        result = contract.validate(data)
    """

    # 기대치 타입 맵핑 (직렬화/역직렬화용)
    EXPECTATION_TYPES: Dict[str, Type[Expectation]] = {
        "expect_column_not_null": ExpectColumnNotNull,
        "expect_column_unique": ExpectColumnUnique,
        "expect_column_values_in_range": ExpectColumnValuesInRange,
        "expect_column_values_to_match_regex": ExpectColumnValuesToMatchRegex,
        "expect_table_row_count_between": ExpectTableRowCountBetween,
        "expect_column_values_to_be_of_type": ExpectColumnValuesToBeOfType,
        "expect_column_values_to_be_in_set": ExpectColumnValuesToBeInSet,
        "expect_column_value_length_to_be_between": ExpectColumnValueLengthToBeBetween,
        "expect_column_pair_values_to_be_equal": ExpectColumnPairValuesToBeEqual,
    }

    def __init__(
        self,
        name: str,
        description: str = "",
        source_id: Optional[str] = None,
        version: str = "1.0.0",
        status: ContractStatus = ContractStatus.DRAFT,
        meta: Optional[Dict[str, Any]] = None,
        fail_on_error: bool = True,
        fail_on_warning: bool = False,
    ):
        """
        DataContract 초기화

        Args:
            name: 계약 이름
            description: 계약 설명
            source_id: 연결된 소스 ID
            version: 버전 (Semantic Versioning)
            status: 계약 상태
            meta: 추가 메타데이터
            fail_on_error: ERROR 레벨 실패 시 전체 실패 처리
            fail_on_warning: WARNING 레벨 실패 시 전체 실패 처리
        """
        self.name = name
        self.description = description
        self.source_id = source_id
        self.version = version
        self.status = status
        self.meta = meta or {}
        self.fail_on_error = fail_on_error
        self.fail_on_warning = fail_on_warning

        self._expectations: List[Expectation] = []
        self._created_at = datetime.utcnow()
        self._updated_at = datetime.utcnow()

    @property
    def contract_id(self) -> str:
        """계약 고유 ID 생성"""
        unique_str = f"{self.name}:{self.source_id}:{self.version}"
        return hashlib.md5(unique_str.encode()).hexdigest()[:12]

    @property
    def expectations(self) -> List[Expectation]:
        """등록된 기대치 목록"""
        return self._expectations.copy()

    def add_expectation(self, expectation: Expectation) -> "DataContract":
        """
        기대치 추가

        Args:
            expectation: 추가할 기대치

        Returns:
            self (체이닝 지원)
        """
        self._expectations.append(expectation)
        self._updated_at = datetime.utcnow()
        return self

    def add_expectations(self, expectations: List[Expectation]) -> "DataContract":
        """
        여러 기대치 일괄 추가

        Args:
            expectations: 추가할 기대치 목록

        Returns:
            self (체이닝 지원)
        """
        self._expectations.extend(expectations)
        self._updated_at = datetime.utcnow()
        return self

    def remove_expectation(self, expectation_type: str, column: Optional[str] = None) -> bool:
        """
        기대치 제거

        Args:
            expectation_type: 기대치 타입
            column: 컬럼명 (특정 컬럼의 기대치만 제거)

        Returns:
            제거 성공 여부
        """
        original_count = len(self._expectations)

        if column:
            self._expectations = [
                e for e in self._expectations
                if not (e.expectation_type == expectation_type and getattr(e, 'column', None) == column)
            ]
        else:
            self._expectations = [
                e for e in self._expectations
                if e.expectation_type != expectation_type
            ]

        if len(self._expectations) < original_count:
            self._updated_at = datetime.utcnow()
            return True
        return False

    def validate(
        self,
        data: List[Dict[str, Any]],
        catch_exceptions: bool = True,
        evaluation_parameters: Optional[Dict[str, Any]] = None
    ) -> ContractValidationResult:
        """
        데이터에 대해 모든 기대치 검증 실행

        Args:
            data: 검증할 데이터 목록
            catch_exceptions: 예외 발생 시 무시 여부
            evaluation_parameters: 평가 파라미터

        Returns:
            ContractValidationResult
        """
        run_time = datetime.utcnow()
        results: List[ExpectationValidationResult] = []
        evaluation_parameters = evaluation_parameters or {}

        for expectation in self._expectations:
            try:
                result = expectation.validate(data)
                results.append(result)
            except Exception as e:
                if catch_exceptions:
                    # 실패 결과 생성
                    error_result = ExpectationValidationResult(
                        expectation_type=expectation.expectation_type,
                        success=False,
                        result=ExpectationResult.SKIPPED,
                        severity=expectation.severity,
                        column=getattr(expectation, 'column', None),
                        exception_info=str(e),
                    )
                    results.append(error_result)
                else:
                    raise

        # 전체 성공 여부 판정
        success = self._determine_overall_success(results)

        return ContractValidationResult(
            contract_id=self.contract_id,
            contract_name=self.name,
            contract_version=self.version,
            success=success,
            run_time=run_time,
            statistics={
                "evaluated_expectations": len(results),
                "data_row_count": len(data),
            },
            results=results,
            evaluation_parameters=evaluation_parameters,
        )

    def _determine_overall_success(self, results: List[ExpectationValidationResult]) -> bool:
        """전체 성공 여부 결정"""
        for result in results:
            if not result.success:
                if result.severity == ExpectationSeverity.CRITICAL:
                    return False
                if result.severity == ExpectationSeverity.ERROR and self.fail_on_error:
                    return False
                if result.severity == ExpectationSeverity.WARNING and self.fail_on_warning:
                    return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        """계약을 딕셔너리로 직렬화"""
        return {
            "contract_id": self.contract_id,
            "name": self.name,
            "description": self.description,
            "source_id": self.source_id,
            "version": self.version,
            "status": self.status.value,
            "meta": self.meta,
            "fail_on_error": self.fail_on_error,
            "fail_on_warning": self.fail_on_warning,
            "created_at": self._created_at.isoformat(),
            "updated_at": self._updated_at.isoformat(),
            "expectations": [
                self._serialize_expectation(e) for e in self._expectations
            ]
        }

    def _serialize_expectation(self, expectation: Expectation) -> Dict[str, Any]:
        """기대치를 딕셔너리로 직렬화"""
        config = {
            "expectation_type": expectation.expectation_type,
            "severity": expectation.severity.value,
            "meta": expectation.meta,
        }

        # 기대치별 특정 속성 추가
        if hasattr(expectation, 'column'):
            config["column"] = expectation.column
        if hasattr(expectation, 'mostly'):
            config["mostly"] = expectation.mostly
        if hasattr(expectation, 'min_value'):
            config["min_value"] = expectation.min_value
        if hasattr(expectation, 'max_value'):
            config["max_value"] = expectation.max_value
        if hasattr(expectation, 'regex'):
            config["regex"] = expectation.regex
        if hasattr(expectation, 'preset'):
            config["preset"] = expectation.preset
        if hasattr(expectation, 'type_'):
            config["type_"] = expectation.type_
        if hasattr(expectation, 'value_set'):
            config["value_set"] = list(expectation.value_set)
        if hasattr(expectation, 'column_A'):
            config["column_A"] = expectation.column_A
        if hasattr(expectation, 'column_B'):
            config["column_B"] = expectation.column_B
        if hasattr(expectation, 'allow_null'):
            config["allow_null"] = expectation.allow_null
        if hasattr(expectation, 'strict_min'):
            config["strict_min"] = expectation.strict_min
        if hasattr(expectation, 'strict_max'):
            config["strict_max"] = expectation.strict_max

        return config

    def to_json(self, indent: int = 2) -> str:
        """JSON 문자열로 직렬화"""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False, default=str)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataContract":
        """딕셔너리에서 계약 역직렬화"""
        contract = cls(
            name=data["name"],
            description=data.get("description", ""),
            source_id=data.get("source_id"),
            version=data.get("version", "1.0.0"),
            status=ContractStatus(data.get("status", "draft")),
            meta=data.get("meta", {}),
            fail_on_error=data.get("fail_on_error", True),
            fail_on_warning=data.get("fail_on_warning", False),
        )

        # 기대치 역직렬화
        for exp_config in data.get("expectations", []):
            expectation = cls._deserialize_expectation(exp_config)
            if expectation:
                contract.add_expectation(expectation)

        return contract

    @classmethod
    def _deserialize_expectation(cls, config: Dict[str, Any]) -> Optional[Expectation]:
        """기대치 역직렬화"""
        exp_type = config.get("expectation_type")
        if exp_type not in cls.EXPECTATION_TYPES:
            return None

        exp_class = cls.EXPECTATION_TYPES[exp_type]

        # 공통 파라미터 추출
        kwargs = {
            "severity": ExpectationSeverity(config.get("severity", "error")),
            "meta": config.get("meta", {}),
        }

        # 기대치 타입별 파라미터 추출
        if "column" in config:
            kwargs["column"] = config["column"]
        if "mostly" in config:
            kwargs["mostly"] = config["mostly"]
        if "min_value" in config:
            kwargs["min_value"] = config["min_value"]
        if "max_value" in config:
            kwargs["max_value"] = config["max_value"]
        if "regex" in config:
            kwargs["regex"] = config["regex"]
        if "preset" in config:
            kwargs["preset"] = config["preset"]
        if "type_" in config:
            kwargs["type_"] = config["type_"]
        if "value_set" in config:
            kwargs["value_set"] = config["value_set"]
        if "column_A" in config:
            kwargs["column_A"] = config["column_A"]
        if "column_B" in config:
            kwargs["column_B"] = config["column_B"]
        if "allow_null" in config:
            kwargs["allow_null"] = config["allow_null"]
        if "strict_min" in config:
            kwargs["strict_min"] = config["strict_min"]
        if "strict_max" in config:
            kwargs["strict_max"] = config["strict_max"]

        try:
            return exp_class(**kwargs)
        except Exception:
            return None

    @classmethod
    def from_json(cls, json_str: str) -> "DataContract":
        """JSON 문자열에서 계약 역직렬화"""
        data = json.loads(json_str)
        return cls.from_dict(data)


class ContractBuilder:
    """
    DataContract 빌더

    체이닝 방식으로 계약을 구성할 수 있습니다.

    Example:
        contract = (
            ContractBuilder("news_articles")
            .with_description("뉴스 기사 품질 계약")
            .expect_column_not_null("title")
            .expect_column_not_null("content")
            .expect_column_unique("url")
            .expect_column_values_in_range("view_count", min_value=0)
            .expect_table_row_count_between(min_value=1)
            .build()
        )
    """

    def __init__(self, name: str, source_id: Optional[str] = None):
        self._contract = DataContract(name=name, source_id=source_id)

    def with_description(self, description: str) -> "ContractBuilder":
        self._contract.description = description
        return self

    def with_version(self, version: str) -> "ContractBuilder":
        self._contract.version = version
        return self

    def with_meta(self, meta: Dict[str, Any]) -> "ContractBuilder":
        self._contract.meta = meta
        return self

    def fail_on_error(self, value: bool = True) -> "ContractBuilder":
        self._contract.fail_on_error = value
        return self

    def fail_on_warning(self, value: bool = True) -> "ContractBuilder":
        self._contract.fail_on_warning = value
        return self

    def expect_column_not_null(
        self,
        column: str,
        mostly: float = 1.0,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR
    ) -> "ContractBuilder":
        self._contract.add_expectation(
            ExpectColumnNotNull(column=column, mostly=mostly, severity=severity)
        )
        return self

    def expect_column_unique(
        self,
        column: str,
        mostly: float = 1.0,
        severity: ExpectationSeverity = ExpectationSeverity.WARNING
    ) -> "ContractBuilder":
        self._contract.add_expectation(
            ExpectColumnUnique(column=column, mostly=mostly, severity=severity)
        )
        return self

    def expect_column_values_in_range(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        mostly: float = 1.0,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR
    ) -> "ContractBuilder":
        self._contract.add_expectation(
            ExpectColumnValuesInRange(
                column=column,
                min_value=min_value,
                max_value=max_value,
                mostly=mostly,
                severity=severity
            )
        )
        return self

    def expect_column_values_to_match_regex(
        self,
        column: str,
        regex: Optional[str] = None,
        preset: Optional[str] = None,
        mostly: float = 1.0,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR
    ) -> "ContractBuilder":
        self._contract.add_expectation(
            ExpectColumnValuesToMatchRegex(
                column=column,
                regex=regex,
                preset=preset,
                mostly=mostly,
                severity=severity
            )
        )
        return self

    def expect_table_row_count_between(
        self,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR
    ) -> "ContractBuilder":
        self._contract.add_expectation(
            ExpectTableRowCountBetween(
                min_value=min_value,
                max_value=max_value,
                severity=severity
            )
        )
        return self

    def expect_column_values_to_be_of_type(
        self,
        column: str,
        type_: str,
        mostly: float = 1.0,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR
    ) -> "ContractBuilder":
        self._contract.add_expectation(
            ExpectColumnValuesToBeOfType(
                column=column,
                type_=type_,
                mostly=mostly,
                severity=severity
            )
        )
        return self

    def expect_column_values_to_be_in_set(
        self,
        column: str,
        value_set: Union[List[Any], set],
        mostly: float = 1.0,
        severity: ExpectationSeverity = ExpectationSeverity.ERROR
    ) -> "ContractBuilder":
        self._contract.add_expectation(
            ExpectColumnValuesToBeInSet(
                column=column,
                value_set=value_set,
                mostly=mostly,
                severity=severity
            )
        )
        return self

    def expect_column_value_length_to_be_between(
        self,
        column: str,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        mostly: float = 1.0,
        severity: ExpectationSeverity = ExpectationSeverity.WARNING
    ) -> "ContractBuilder":
        self._contract.add_expectation(
            ExpectColumnValueLengthToBeBetween(
                column=column,
                min_value=min_value,
                max_value=max_value,
                mostly=mostly,
                severity=severity
            )
        )
        return self

    def add_custom_expectation(self, expectation: Expectation) -> "ContractBuilder":
        """커스텀 기대치 추가"""
        self._contract.add_expectation(expectation)
        return self

    def build(self) -> DataContract:
        """계약 빌드 완료"""
        return self._contract


# 사전 정의 계약 템플릿
class ContractTemplates:
    """사전 정의된 계약 템플릿"""

    @staticmethod
    def news_articles(source_id: Optional[str] = None) -> DataContract:
        """뉴스 기사 데이터 계약"""
        return (
            ContractBuilder("news_articles", source_id)
            .with_description("뉴스 기사 데이터 품질 계약")
            .expect_column_not_null("title")
            .expect_column_not_null("url")
            .expect_column_unique("url", mostly=0.99)
            .expect_column_value_length_to_be_between("title", min_value=5, max_value=300)
            .expect_column_values_to_match_regex("url", preset="url", mostly=0.95)
            .expect_table_row_count_between(min_value=1)
            .build()
        )

    @staticmethod
    def financial_data(source_id: Optional[str] = None) -> DataContract:
        """금융 데이터 계약"""
        return (
            ContractBuilder("financial_data", source_id)
            .with_description("금융 데이터 품질 계약")
            .expect_column_not_null("name")
            .expect_column_not_null("price")
            .expect_column_values_in_range("price", min_value=0)
            .expect_column_values_to_be_of_type("price", "number")
            .expect_table_row_count_between(min_value=1)
            .build()
        )

    @staticmethod
    def stock_prices(source_id: Optional[str] = None) -> DataContract:
        """주식 가격 데이터 계약"""
        return (
            ContractBuilder("stock_prices", source_id)
            .with_description("주식 가격 데이터 품질 계약")
            .expect_column_not_null("stock_code")
            .expect_column_not_null("price")
            .expect_column_values_to_match_regex("stock_code", preset="stock_code_kr", mostly=0.95)
            .expect_column_values_in_range("price", min_value=0)
            .expect_column_values_in_range("volume", min_value=0)
            .expect_table_row_count_between(min_value=1)
            .build()
        )

    @staticmethod
    def exchange_rates(source_id: Optional[str] = None) -> DataContract:
        """환율 데이터 계약"""
        return (
            ContractBuilder("exchange_rates", source_id)
            .with_description("환율 데이터 품질 계약")
            .expect_column_not_null("currency_code")
            .expect_column_not_null("base_rate")
            .expect_column_value_length_to_be_between("currency_code", min_value=3, max_value=3)
            .expect_column_values_in_range("base_rate", min_value=0)
            .expect_table_row_count_between(min_value=1)
            .build()
        )

    @staticmethod
    def generic_table(
        source_id: Optional[str] = None,
        required_columns: Optional[List[str]] = None
    ) -> DataContract:
        """일반 테이블 데이터 계약"""
        builder = (
            ContractBuilder("generic_table", source_id)
            .with_description("일반 테이블 데이터 품질 계약")
            .expect_table_row_count_between(min_value=1)
        )

        for col in (required_columns or []):
            builder.expect_column_not_null(col)

        return builder.build()

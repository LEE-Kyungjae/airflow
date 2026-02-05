"""
Data Contracts - Great Expectations 스타일의 데이터 품질 계약 시스템

이 모듈은 데이터에 대한 기대치(Expectations)를 정의하고,
자동으로 검증하며, 검증 결과를 리포팅합니다.

주요 기능:
1. 기대치 정의 (Expectations)
   - expect_column_not_null: NULL 값 불허
   - expect_column_unique: 고유값 검증
   - expect_column_values_in_range: 숫자 범위 검증
   - expect_column_values_to_match_regex: 정규식 패턴 검증
   - expect_table_row_count_between: 행 개수 범위 검증
   - expect_column_values_to_be_of_type: 데이터 타입 검증
   - expect_column_values_to_be_in_set: 허용 값 집합 검증
   - expect_column_value_length_to_be_between: 값 길이 검증
   - expect_column_pair_values_to_be_equal: 컬럼 쌍 값 동일성 검증

2. 데이터 계약 (DataContract)
   - 여러 기대치를 그룹화하여 관리
   - JSON/딕셔너리 직렬화/역직렬화 지원
   - 버전 관리 및 상태 관리
   - 빌더 패턴 지원

3. 검증 실행 (ContractValidator)
   - 계약 기반 데이터 검증
   - 검증 결과 MongoDB 저장
   - 실패 시 알림 트리거
   - ETL 파이프라인 연동

4. 결과 리포팅 (ContractReporter)
   - 요약/상세 리포트 생성
   - HTML/JSON 형식 지원
   - 대시보드용 메트릭 제공

사용 예시:
```python
from api.app.services.data_contracts import (
    DataContract,
    ContractBuilder,
    ContractValidator,
    ContractReporter,
    ContractTemplates,
    expect_column_not_null,
    expect_column_unique,
    expect_column_values_in_range,
)

# 방법 1: 기대치 직접 추가
contract = DataContract(name="my_data", description="데이터 품질 계약")
contract.add_expectation(expect_column_not_null("id"))
contract.add_expectation(expect_column_unique("email"))
contract.add_expectation(expect_column_values_in_range("price", min_value=0))

# 방법 2: 빌더 패턴 사용
contract = (
    ContractBuilder("my_data")
    .with_description("데이터 품질 계약")
    .expect_column_not_null("id")
    .expect_column_unique("email")
    .expect_column_values_in_range("price", min_value=0)
    .build()
)

# 방법 3: 사전 정의 템플릿 사용
contract = ContractTemplates.news_articles(source_id="source_123")

# 검증 실행
validator = ContractValidator(mongo_service=mongo)
result = await validator.validate(
    contract=contract,
    data=crawled_data,
    source_id="source_123"
)

# 리포트 생성
reporter = ContractReporter(mongo_service=mongo)
html_report = reporter.generate_html_report(result.validation_result)
```

ETL 파이프라인 연동:
```python
from api.app.services.data_contracts import ETLContractIntegration

# ETL에 계약 검증 통합
integration = ETLContractIntegration(mongo_service=mongo)
validated = await integration.validate_before_load(
    data=transformed_data,
    source_id=source_id,
    data_category="news_article"
)

if validated.validation_result.success:
    # Production으로 적재
    pass
else:
    # Staging으로 분류됨
    # invalid_data에 검증 실패 데이터 포함
    pass
```
"""

# Expectations
from .expectations import (
    # 기본 클래스
    Expectation,
    ExpectationValidationResult,
    ExpectationResult,
    ExpectationSeverity,

    # 기대치 클래스
    ExpectColumnNotNull,
    ExpectColumnUnique,
    ExpectColumnValuesInRange,
    ExpectColumnValuesToMatchRegex,
    ExpectTableRowCountBetween,
    ExpectColumnValuesToBeOfType,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValueLengthToBeBetween,
    ExpectColumnPairValuesToBeEqual,

    # 편의 함수
    expect_column_not_null,
    expect_column_unique,
    expect_column_values_in_range,
    expect_column_values_to_match_regex,
    expect_table_row_count_between,
    expect_column_values_to_be_of_type,
    expect_column_values_to_be_in_set,
    expect_column_value_length_to_be_between,
    expect_column_pair_values_to_be_equal,
)

# Contract
from .contract import (
    DataContract,
    ContractBuilder,
    ContractValidationResult,
    ContractStatus,
    ContractTemplates,
)

# Validator
from .validator import (
    ContractValidator,
    ContractRegistry,
    ValidationConfig,
    ValidationAction,
    ValidatedData,
)

# Reporter
from .reporter import (
    ContractReporter,
    ValidationTrend,
    ContractHealthMetrics,
)

__all__ = [
    # Expectations
    "Expectation",
    "ExpectationValidationResult",
    "ExpectationResult",
    "ExpectationSeverity",
    "ExpectColumnNotNull",
    "ExpectColumnUnique",
    "ExpectColumnValuesInRange",
    "ExpectColumnValuesToMatchRegex",
    "ExpectTableRowCountBetween",
    "ExpectColumnValuesToBeOfType",
    "ExpectColumnValuesToBeInSet",
    "ExpectColumnValueLengthToBeBetween",
    "ExpectColumnPairValuesToBeEqual",
    "expect_column_not_null",
    "expect_column_unique",
    "expect_column_values_in_range",
    "expect_column_values_to_match_regex",
    "expect_table_row_count_between",
    "expect_column_values_to_be_of_type",
    "expect_column_values_to_be_in_set",
    "expect_column_value_length_to_be_between",
    "expect_column_pair_values_to_be_equal",

    # Contract
    "DataContract",
    "ContractBuilder",
    "ContractValidationResult",
    "ContractStatus",
    "ContractTemplates",

    # Validator
    "ContractValidator",
    "ContractRegistry",
    "ValidationConfig",
    "ValidationAction",
    "ValidatedData",

    # Reporter
    "ContractReporter",
    "ValidationTrend",
    "ContractHealthMetrics",

    # Integration
    "ETLContractIntegration",
]


class ETLContractIntegration:
    """
    ETL 파이프라인과 데이터 계약 연동

    ETL 변환 후 자동으로 데이터 계약을 검증하고,
    실패한 데이터를 staging으로 분류합니다.
    """

    def __init__(
        self,
        mongo_service=None,
        alert_dispatcher=None,
        config: ValidationConfig = None
    ):
        self.mongo = mongo_service
        self.validator = ContractValidator(
            mongo_service=mongo_service,
            alert_dispatcher=alert_dispatcher,
            config=config or ValidationConfig()
        )
        self.registry = ContractRegistry(mongo_service=mongo_service)
        self.reporter = ContractReporter(mongo_service=mongo_service)

    async def validate_before_load(
        self,
        data: list,
        source_id: str,
        data_category: str = None,
        contract: DataContract = None,
        run_id: str = None,
        context: dict = None
    ) -> ValidatedData:
        """
        ETL Load 단계 전 데이터 검증

        Args:
            data: 변환된 데이터
            source_id: 소스 ID
            data_category: 데이터 카테고리 (contract가 없을 때 자동 선택)
            contract: 사용할 데이터 계약 (None이면 자동 선택)
            run_id: 실행 ID
            context: 추가 컨텍스트

        Returns:
            ValidatedData: 검증 결과와 분류된 데이터
        """
        # 계약 결정
        if contract is None:
            # 소스에 등록된 계약 확인
            contract = await self.registry.get_by_source(source_id)

            if contract is None and data_category:
                # 카테고리 기반 템플릿 사용
                category_templates = {
                    "news_article": ContractTemplates.news_articles,
                    "financial_data": ContractTemplates.financial_data,
                    "stock_price": ContractTemplates.stock_prices,
                    "exchange_rate": ContractTemplates.exchange_rates,
                }
                template_func = category_templates.get(
                    data_category,
                    lambda **kw: ContractTemplates.generic_table(**kw)
                )
                contract = template_func(source_id=source_id)

        if contract is None:
            # 기본 계약 사용
            contract = ContractTemplates.generic_table(source_id=source_id)

        # 검증 실행
        return await self.validator.validate(
            contract=contract,
            data=data,
            source_id=source_id,
            run_id=run_id,
            context=context or {}
        )

    async def get_or_create_contract(
        self,
        source_id: str,
        data_category: str,
        required_columns: list = None
    ) -> DataContract:
        """
        소스에 대한 계약 조회 또는 생성

        Args:
            source_id: 소스 ID
            data_category: 데이터 카테고리
            required_columns: 필수 컬럼 목록

        Returns:
            DataContract
        """
        # 기존 계약 확인
        existing = await self.registry.get_by_source(source_id)
        if existing:
            return existing

        # 새 계약 생성
        category_templates = {
            "news_article": ContractTemplates.news_articles,
            "financial_data": ContractTemplates.financial_data,
            "stock_price": ContractTemplates.stock_prices,
            "exchange_rate": ContractTemplates.exchange_rates,
        }

        template_func = category_templates.get(data_category)
        if template_func:
            contract = template_func(source_id=source_id)
        else:
            contract = ContractTemplates.generic_table(
                source_id=source_id,
                required_columns=required_columns
            )

        # 등록
        await self.registry.register(contract)
        return contract

    async def generate_validation_report(
        self,
        validation_result: ContractValidationResult,
        format: str = "summary"
    ) -> dict:
        """
        검증 결과 리포트 생성

        Args:
            validation_result: 검증 결과
            format: 리포트 형식 (summary, detailed, html, json)

        Returns:
            리포트 데이터
        """
        if format == "summary":
            return self.reporter.generate_summary_report(validation_result)
        elif format == "detailed":
            return self.reporter.generate_detailed_report(validation_result)
        elif format == "html":
            return {"html": self.reporter.generate_html_report(validation_result)}
        elif format == "json":
            return {"json": self.reporter.generate_json_report(validation_result)}
        else:
            return self.reporter.generate_summary_report(validation_result)

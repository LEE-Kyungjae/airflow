"""
Tests for DataContract validation system.

Covers:
- Expectation classes (ExpectColumnNotNull, etc.)
- DataContract creation and validation
- ContractBuilder fluent interface
- ContractTemplates
- ContractValidator and routing
- Contract serialization/deserialization
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, AsyncMock, patch


class TestExpectationSeverity:
    """Tests for ExpectationSeverity enum."""

    def test_severity_values(self):
        """Test ExpectationSeverity enum values."""
        from api.app.services.data_contracts.expectations import ExpectationSeverity

        assert ExpectationSeverity.CRITICAL.value == "critical"
        assert ExpectationSeverity.ERROR.value == "error"
        assert ExpectationSeverity.WARNING.value == "warning"
        assert ExpectationSeverity.INFO.value == "info"


class TestExpectationResult:
    """Tests for ExpectationResult enum."""

    def test_result_values(self):
        """Test ExpectationResult enum values."""
        from api.app.services.data_contracts.expectations import ExpectationResult

        assert ExpectationResult.PASSED.value == "passed"
        assert ExpectationResult.FAILED.value == "failed"
        assert ExpectationResult.SKIPPED.value == "skipped"


class TestExpectColumnNotNull:
    """Tests for ExpectColumnNotNull expectation."""

    def test_expect_column_not_null_pass(self):
        """Test column not null expectation passes with valid data."""
        from api.app.services.data_contracts.expectations import ExpectColumnNotNull

        expectation = ExpectColumnNotNull(column="title")
        data = [
            {"title": "Article 1"},
            {"title": "Article 2"},
            {"title": "Article 3"}
        ]

        result = expectation.validate(data)

        assert result.success is True
        assert result.observed_value == 0  # 0 null values

    def test_expect_column_not_null_fail(self):
        """Test column not null expectation fails with null values."""
        from api.app.services.data_contracts.expectations import ExpectColumnNotNull

        expectation = ExpectColumnNotNull(column="title")
        data = [
            {"title": "Article 1"},
            {"title": None},
            {"title": "Article 3"}
        ]

        result = expectation.validate(data)

        assert result.success is False
        assert result.observed_value == 1  # 1 null value
        assert 1 in result.unexpected_index_list

    def test_expect_column_not_null_with_mostly(self):
        """Test column not null expectation with mostly parameter."""
        from api.app.services.data_contracts.expectations import ExpectColumnNotNull

        expectation = ExpectColumnNotNull(column="title", mostly=0.9)
        data = [
            {"title": "Article 1"},
            {"title": "Article 2"},
            {"title": "Article 3"},
            {"title": "Article 4"},
            {"title": "Article 5"},
            {"title": "Article 6"},
            {"title": "Article 7"},
            {"title": "Article 8"},
            {"title": "Article 9"},
            {"title": None}  # 10% null - exactly at threshold
        ]

        result = expectation.validate(data)

        assert result.success is True

    def test_expect_column_not_null_missing_column(self):
        """Test column not null expectation when column is missing."""
        from api.app.services.data_contracts.expectations import ExpectColumnNotNull

        expectation = ExpectColumnNotNull(column="title")
        data = [
            {"content": "No title field"},
            {"content": "Also no title"}
        ]

        result = expectation.validate(data)

        assert result.success is False
        # All records missing the column should be counted as null


class TestExpectColumnUnique:
    """Tests for ExpectColumnUnique expectation."""

    def test_expect_column_unique_pass(self):
        """Test column unique expectation passes with unique values."""
        from api.app.services.data_contracts.expectations import ExpectColumnUnique

        expectation = ExpectColumnUnique(column="id")
        data = [
            {"id": "1"},
            {"id": "2"},
            {"id": "3"}
        ]

        result = expectation.validate(data)

        assert result.success is True

    def test_expect_column_unique_fail(self):
        """Test column unique expectation fails with duplicates."""
        from api.app.services.data_contracts.expectations import ExpectColumnUnique

        expectation = ExpectColumnUnique(column="id")
        data = [
            {"id": "1"},
            {"id": "2"},
            {"id": "1"}  # Duplicate
        ]

        result = expectation.validate(data)

        assert result.success is False
        assert len(result.unexpected_values) > 0

    def test_expect_column_unique_with_mostly(self):
        """Test column unique expectation with mostly parameter."""
        from api.app.services.data_contracts.expectations import ExpectColumnUnique

        expectation = ExpectColumnUnique(column="id", mostly=0.8)
        data = [
            {"id": "1"},
            {"id": "2"},
            {"id": "3"},
            {"id": "4"},
            {"id": "1"}  # 20% duplicates (4/5 unique = 80%)
        ]

        result = expectation.validate(data)

        assert result.success is True


class TestExpectColumnValuesInRange:
    """Tests for ExpectColumnValuesInRange expectation."""

    def test_expect_values_in_range_pass(self):
        """Test values in range expectation passes."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesInRange

        expectation = ExpectColumnValuesInRange(column="price", min_value=0, max_value=100)
        data = [
            {"price": 10},
            {"price": 50},
            {"price": 100}
        ]

        result = expectation.validate(data)

        assert result.success is True

    def test_expect_values_in_range_fail_below_min(self):
        """Test values in range fails when below minimum."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesInRange

        expectation = ExpectColumnValuesInRange(column="price", min_value=0)
        data = [
            {"price": 10},
            {"price": -5},  # Below min
            {"price": 20}
        ]

        result = expectation.validate(data)

        assert result.success is False
        assert -5 in result.unexpected_values

    def test_expect_values_in_range_fail_above_max(self):
        """Test values in range fails when above maximum."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesInRange

        expectation = ExpectColumnValuesInRange(column="price", max_value=100)
        data = [
            {"price": 10},
            {"price": 150},  # Above max
            {"price": 50}
        ]

        result = expectation.validate(data)

        assert result.success is False

    def test_expect_values_in_range_min_only(self):
        """Test values in range with only minimum."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesInRange

        expectation = ExpectColumnValuesInRange(column="count", min_value=0)
        data = [
            {"count": 0},
            {"count": 1000000},
            {"count": 1}
        ]

        result = expectation.validate(data)

        assert result.success is True


class TestExpectColumnValuesToMatchRegex:
    """Tests for ExpectColumnValuesToMatchRegex expectation."""

    def test_expect_regex_pass(self):
        """Test regex expectation passes with matching values."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesToMatchRegex

        expectation = ExpectColumnValuesToMatchRegex(column="email", regex=r".+@.+\..+")
        data = [
            {"email": "test@example.com"},
            {"email": "user@domain.org"}
        ]

        result = expectation.validate(data)

        assert result.success is True

    def test_expect_regex_fail(self):
        """Test regex expectation fails with non-matching values."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesToMatchRegex

        expectation = ExpectColumnValuesToMatchRegex(column="email", regex=r".+@.+\..+")
        data = [
            {"email": "test@example.com"},
            {"email": "invalid-email"}  # No @ or domain
        ]

        result = expectation.validate(data)

        assert result.success is False

    def test_expect_regex_preset_url(self):
        """Test regex expectation with URL preset."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesToMatchRegex

        expectation = ExpectColumnValuesToMatchRegex(column="url", preset="url")
        data = [
            {"url": "https://example.com/page"},
            {"url": "http://test.org"}
        ]

        result = expectation.validate(data)

        assert result.success is True


class TestExpectTableRowCountBetween:
    """Tests for ExpectTableRowCountBetween expectation."""

    def test_expect_row_count_pass(self):
        """Test row count expectation passes."""
        from api.app.services.data_contracts.expectations import ExpectTableRowCountBetween

        expectation = ExpectTableRowCountBetween(min_value=1, max_value=100)
        data = [{"id": i} for i in range(50)]

        result = expectation.validate(data)

        assert result.success is True
        assert result.observed_value == 50

    def test_expect_row_count_fail_too_few(self):
        """Test row count fails when too few rows."""
        from api.app.services.data_contracts.expectations import ExpectTableRowCountBetween

        expectation = ExpectTableRowCountBetween(min_value=10)
        data = [{"id": 1}]

        result = expectation.validate(data)

        assert result.success is False

    def test_expect_row_count_fail_too_many(self):
        """Test row count fails when too many rows."""
        from api.app.services.data_contracts.expectations import ExpectTableRowCountBetween

        expectation = ExpectTableRowCountBetween(max_value=5)
        data = [{"id": i} for i in range(10)]

        result = expectation.validate(data)

        assert result.success is False


class TestExpectColumnValuesToBeOfType:
    """Tests for ExpectColumnValuesToBeOfType expectation."""

    def test_expect_type_string_pass(self):
        """Test type expectation passes for string type."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesToBeOfType

        expectation = ExpectColumnValuesToBeOfType(column="name", type_="str")
        data = [
            {"name": "Alice"},
            {"name": "Bob"}
        ]

        result = expectation.validate(data)

        assert result.success is True

    def test_expect_type_number_pass(self):
        """Test type expectation passes for number type."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesToBeOfType

        expectation = ExpectColumnValuesToBeOfType(column="value", type_="number")
        data = [
            {"value": 10},
            {"value": 3.14},
            {"value": 0}
        ]

        result = expectation.validate(data)

        assert result.success is True

    def test_expect_type_fail(self):
        """Test type expectation fails with wrong type."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesToBeOfType

        expectation = ExpectColumnValuesToBeOfType(column="age", type_="int")
        data = [
            {"age": 25},
            {"age": "thirty"}  # String instead of int
        ]

        result = expectation.validate(data)

        assert result.success is False


class TestExpectColumnValuesToBeInSet:
    """Tests for ExpectColumnValuesToBeInSet expectation."""

    def test_expect_in_set_pass(self):
        """Test in set expectation passes."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesToBeInSet

        expectation = ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["active", "inactive", "pending"]
        )
        data = [
            {"status": "active"},
            {"status": "inactive"}
        ]

        result = expectation.validate(data)

        assert result.success is True

    def test_expect_in_set_fail(self):
        """Test in set expectation fails with value not in set."""
        from api.app.services.data_contracts.expectations import ExpectColumnValuesToBeInSet

        expectation = ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["active", "inactive"]
        )
        data = [
            {"status": "active"},
            {"status": "unknown"}  # Not in set
        ]

        result = expectation.validate(data)

        assert result.success is False


class TestExpectColumnValueLengthToBeBetween:
    """Tests for ExpectColumnValueLengthToBeBetween expectation."""

    def test_expect_length_pass(self):
        """Test length expectation passes."""
        from api.app.services.data_contracts.expectations import ExpectColumnValueLengthToBeBetween

        expectation = ExpectColumnValueLengthToBeBetween(
            column="title",
            min_value=5,
            max_value=100
        )
        data = [
            {"title": "Valid Title"},
            {"title": "Another Valid Title"}
        ]

        result = expectation.validate(data)

        assert result.success is True

    def test_expect_length_fail_too_short(self):
        """Test length expectation fails when too short."""
        from api.app.services.data_contracts.expectations import ExpectColumnValueLengthToBeBetween

        expectation = ExpectColumnValueLengthToBeBetween(column="title", min_value=10)
        data = [
            {"title": "OK"},  # Too short
            {"title": "This is long enough"}
        ]

        result = expectation.validate(data)

        assert result.success is False

    def test_expect_length_fail_too_long(self):
        """Test length expectation fails when too long."""
        from api.app.services.data_contracts.expectations import ExpectColumnValueLengthToBeBetween

        expectation = ExpectColumnValueLengthToBeBetween(column="title", max_value=10)
        data = [
            {"title": "Short"},
            {"title": "This is way too long for the limit"}
        ]

        result = expectation.validate(data)

        assert result.success is False


class TestDataContract:
    """Tests for DataContract class."""

    def test_contract_creation(self):
        """Test basic contract creation."""
        from api.app.services.data_contracts.contract import DataContract, ContractStatus

        contract = DataContract(
            name="test_contract",
            description="Test description",
            source_id="source_123"
        )

        assert contract.name == "test_contract"
        assert contract.status == ContractStatus.DRAFT
        assert len(contract.expectations) == 0

    def test_contract_add_expectation(self, sample_data_contract):
        """Test adding expectation to contract."""
        from api.app.services.data_contracts.expectations import ExpectColumnNotNull

        initial_count = len(sample_data_contract.expectations)
        sample_data_contract.add_expectation(
            ExpectColumnNotNull(column="new_column")
        )

        assert len(sample_data_contract.expectations) == initial_count + 1

    def test_contract_add_expectations_batch(self):
        """Test adding multiple expectations at once."""
        from api.app.services.data_contracts.contract import DataContract
        from api.app.services.data_contracts.expectations import (
            ExpectColumnNotNull, ExpectColumnUnique
        )

        contract = DataContract(name="test")
        contract.add_expectations([
            ExpectColumnNotNull(column="a"),
            ExpectColumnNotNull(column="b"),
            ExpectColumnUnique(column="c")
        ])

        assert len(contract.expectations) == 3

    def test_contract_remove_expectation(self, sample_data_contract):
        """Test removing expectation from contract."""
        initial_count = len(sample_data_contract.expectations)
        result = sample_data_contract.remove_expectation(
            "expect_column_not_null", column="title"
        )

        assert result is True
        assert len(sample_data_contract.expectations) < initial_count

    def test_contract_validate_success(self, sample_data_contract):
        """Test contract validation passes with valid data."""
        data = [
            {"title": "Article 1", "url": "https://example.com/1"},
            {"title": "Article 2", "url": "https://example.com/2"}
        ]

        result = sample_data_contract.validate(data)

        assert result.success is True
        assert result.passed_count > 0

    def test_contract_validate_failure(self, sample_data_contract):
        """Test contract validation fails with invalid data."""
        data = [
            {"title": None, "url": "https://example.com/1"},  # Missing title
            {"url": "https://example.com/2"}  # Also missing title
        ]

        result = sample_data_contract.validate(data)

        assert result.success is False
        assert result.failed_count > 0

    def test_contract_to_dict(self, sample_data_contract):
        """Test contract serialization."""
        result = sample_data_contract.to_dict()

        assert result["name"] == "test_news_contract"
        assert "expectations" in result
        assert "contract_id" in result
        assert "version" in result

    def test_contract_to_json(self, sample_data_contract):
        """Test contract JSON serialization."""
        result = sample_data_contract.to_json()

        assert isinstance(result, str)
        assert "test_news_contract" in result

    def test_contract_from_dict(self, sample_data_contract):
        """Test contract deserialization from dict."""
        from api.app.services.data_contracts.contract import DataContract

        data = sample_data_contract.to_dict()
        restored = DataContract.from_dict(data)

        assert restored.name == sample_data_contract.name
        assert len(restored.expectations) == len(sample_data_contract.expectations)

    def test_contract_from_json(self, sample_data_contract):
        """Test contract deserialization from JSON."""
        from api.app.services.data_contracts.contract import DataContract

        json_str = sample_data_contract.to_json()
        restored = DataContract.from_json(json_str)

        assert restored.name == sample_data_contract.name


class TestContractValidationResult:
    """Tests for ContractValidationResult dataclass."""

    def test_validation_result_properties(self, sample_data_contract):
        """Test validation result computed properties."""
        data = [
            {"title": "Article", "url": "https://example.com"}
        ]

        result = sample_data_contract.validate(data)

        assert hasattr(result, "success_rate")
        assert hasattr(result, "critical_failures")
        assert hasattr(result, "error_failures")
        assert hasattr(result, "warning_failures")

    def test_validation_result_to_dict(self, sample_data_contract):
        """Test validation result serialization."""
        data = [{"title": "Test", "url": "https://example.com"}]
        result = sample_data_contract.validate(data)
        result_dict = result.to_dict()

        assert "contract_id" in result_dict
        assert "contract_name" in result_dict
        assert "success" in result_dict
        assert "statistics" in result_dict


class TestContractBuilder:
    """Tests for ContractBuilder fluent interface."""

    def test_builder_basic_usage(self):
        """Test basic ContractBuilder usage."""
        from api.app.services.data_contracts.contract import ContractBuilder

        contract = (
            ContractBuilder("my_contract")
            .with_description("Test contract")
            .with_version("2.0.0")
            .expect_column_not_null("field1")
            .build()
        )

        assert contract.name == "my_contract"
        assert contract.description == "Test contract"
        assert contract.version == "2.0.0"
        assert len(contract.expectations) == 1

    def test_builder_chaining(self):
        """Test ContractBuilder method chaining."""
        from api.app.services.data_contracts.contract import ContractBuilder

        contract = (
            ContractBuilder("test")
            .expect_column_not_null("a")
            .expect_column_not_null("b")
            .expect_column_unique("c")
            .expect_column_values_in_range("d", min_value=0)
            .expect_table_row_count_between(min_value=1)
            .build()
        )

        assert len(contract.expectations) == 5

    def test_builder_with_meta(self):
        """Test ContractBuilder with metadata."""
        from api.app.services.data_contracts.contract import ContractBuilder

        contract = (
            ContractBuilder("test")
            .with_meta({"owner": "team-data", "priority": "high"})
            .build()
        )

        assert contract.meta["owner"] == "team-data"
        assert contract.meta["priority"] == "high"

    def test_builder_fail_on_warning(self):
        """Test ContractBuilder fail_on_warning setting."""
        from api.app.services.data_contracts.contract import ContractBuilder

        contract = (
            ContractBuilder("test")
            .fail_on_warning(True)
            .build()
        )

        assert contract.fail_on_warning is True


class TestContractTemplates:
    """Tests for ContractTemplates factory methods."""

    def test_news_articles_template(self):
        """Test news articles template."""
        from api.app.services.data_contracts.contract import ContractTemplates

        contract = ContractTemplates.news_articles(source_id="test_source")

        assert contract.name == "news_articles"
        assert len(contract.expectations) > 0

        # Validate with sample data
        data = [{"title": "Test Article", "url": "https://example.com/article"}]
        result = contract.validate(data)
        assert result.success is True

    def test_financial_data_template(self):
        """Test financial data template."""
        from api.app.services.data_contracts.contract import ContractTemplates

        contract = ContractTemplates.financial_data(source_id="test_source")

        assert contract.name == "financial_data"
        assert len(contract.expectations) > 0

    def test_stock_prices_template(self):
        """Test stock prices template."""
        from api.app.services.data_contracts.contract import ContractTemplates

        contract = ContractTemplates.stock_prices()

        assert contract.name == "stock_prices"

        # Validate with sample data
        data = [{"stock_code": "005930", "price": 70000}]
        result = contract.validate(data)
        assert result.success is True

    def test_exchange_rates_template(self):
        """Test exchange rates template."""
        from api.app.services.data_contracts.contract import ContractTemplates

        contract = ContractTemplates.exchange_rates()

        assert contract.name == "exchange_rates"

    def test_generic_table_template(self):
        """Test generic table template with required columns."""
        from api.app.services.data_contracts.contract import ContractTemplates

        contract = ContractTemplates.generic_table(
            required_columns=["id", "name", "value"]
        )

        assert contract.name == "generic_table"
        # Should have row count + 3 not null expectations
        assert len(contract.expectations) >= 4


class TestContractStatus:
    """Tests for ContractStatus enum."""

    def test_contract_status_values(self):
        """Test ContractStatus enum values."""
        from api.app.services.data_contracts.contract import ContractStatus

        assert ContractStatus.DRAFT.value == "draft"
        assert ContractStatus.ACTIVE.value == "active"
        assert ContractStatus.DEPRECATED.value == "deprecated"
        assert ContractStatus.ARCHIVED.value == "archived"


class TestValidatedData:
    """Tests for ValidatedData dataclass."""

    def test_validated_data_creation(self):
        """Test ValidatedData creation."""
        from api.app.services.data_contracts.validator import ValidatedData

        data = ValidatedData(
            valid_data=[{"id": 1}, {"id": 2}],
            invalid_data=[{"id": 3}],
            quarantined_data=[],
            total_count=3,
            valid_count=2,
            invalid_count=1,
            quarantined_count=0
        )

        assert data.success_rate == pytest.approx(66.67, rel=0.1)

    def test_validated_data_to_dict(self):
        """Test ValidatedData serialization."""
        from api.app.services.data_contracts.validator import ValidatedData

        data = ValidatedData(
            valid_data=[],
            invalid_data=[],
            quarantined_data=[],
            total_count=0,
            valid_count=0,
            invalid_count=0,
            quarantined_count=0
        )

        result = data.to_dict()

        assert "total_count" in result
        assert "success_rate" in result


@pytest.mark.asyncio
class TestContractValidator:
    """Tests for ContractValidator class."""

    async def test_validator_validate_success(
        self, mock_mongo_service, sample_data_contract
    ):
        """Test validator with passing validation."""
        from api.app.services.data_contracts.validator import (
            ContractValidator, ValidationConfig
        )

        validator = ContractValidator(
            mongo_service=mock_mongo_service,
            config=ValidationConfig(save_results=False, alert_on_failure=False)
        )

        data = [
            {"title": "Article 1", "url": "https://example.com/1"},
            {"title": "Article 2", "url": "https://example.com/2"}
        ]

        result = await validator.validate(
            contract=sample_data_contract,
            data=data,
            source_id="test_source"
        )

        assert result.validation_result.success is True
        assert len(result.valid_data) == 2

    async def test_validator_validate_with_invalid_data(
        self, mock_mongo_service, sample_data_contract
    ):
        """Test validator with invalid data."""
        from api.app.services.data_contracts.validator import (
            ContractValidator, ValidationConfig
        )

        validator = ContractValidator(
            mongo_service=mock_mongo_service,
            config=ValidationConfig(save_results=False, alert_on_failure=False)
        )

        data = [
            {"title": "Valid Article", "url": "https://example.com/1"},
            {"title": None, "url": "invalid-url"},  # Invalid
        ]

        result = await validator.validate(
            contract=sample_data_contract,
            data=data,
            source_id="test_source"
        )

        # Should have some invalid data classified
        assert result.total_count == 2

    async def test_validator_validate_with_auto_contract(self, mock_mongo_service):
        """Test validator with auto-selected contract."""
        from api.app.services.data_contracts.validator import (
            ContractValidator, ValidationConfig
        )

        validator = ContractValidator(
            mongo_service=mock_mongo_service,
            config=ValidationConfig(save_results=False, alert_on_failure=False)
        )

        data = [
            {"title": "News Article", "url": "https://example.com"}
        ]

        result = await validator.validate_with_auto_contract(
            data=data,
            source_id="test_source",
            data_category="news_article"
        )

        assert result is not None
        assert result.total_count == 1


class TestValidationConfig:
    """Tests for ValidationConfig dataclass."""

    def test_validation_config_defaults(self):
        """Test ValidationConfig default values."""
        from api.app.services.data_contracts.validator import ValidationConfig

        config = ValidationConfig()

        assert config.alert_on_failure is True
        assert config.alert_threshold_percent == 10.0
        assert config.save_results is True

    def test_validation_config_custom(self):
        """Test ValidationConfig with custom values."""
        from api.app.services.data_contracts.validator import (
            ValidationConfig, ValidationAction
        )

        config = ValidationConfig(
            on_critical=ValidationAction.QUARANTINE,
            alert_on_failure=False,
            save_results=False
        )

        assert config.on_critical == ValidationAction.QUARANTINE
        assert config.alert_on_failure is False


class TestContractRegistry:
    """Tests for ContractRegistry class."""

    @pytest.mark.asyncio
    async def test_registry_register(self, mock_mongo_service, sample_data_contract):
        """Test registering a contract."""
        from api.app.services.data_contracts.validator import ContractRegistry

        mock_mongo_service.db.data_contracts.update_one.return_value = MagicMock()

        registry = ContractRegistry(mock_mongo_service)
        contract_id = await registry.register(sample_data_contract)

        assert contract_id is not None

    @pytest.mark.asyncio
    async def test_registry_get_by_id(self, mock_mongo_service, sample_data_contract):
        """Test getting contract by ID."""
        from api.app.services.data_contracts.validator import ContractRegistry

        registry = ContractRegistry(mock_mongo_service)

        # Add to cache
        registry._cache[sample_data_contract.contract_id] = sample_data_contract

        result = await registry.get_by_id(sample_data_contract.contract_id)

        assert result is not None
        assert result.name == sample_data_contract.name

    @pytest.mark.asyncio
    async def test_registry_get_by_id_not_found(self, mock_mongo_service):
        """Test getting non-existent contract."""
        from api.app.services.data_contracts.validator import ContractRegistry

        mock_mongo_service.db.data_contracts.find_one.return_value = None

        registry = ContractRegistry(mock_mongo_service)
        result = await registry.get_by_id("nonexistent_id")

        assert result is None

    def test_registry_clear_cache(self, mock_mongo_service, sample_data_contract):
        """Test clearing registry cache."""
        from api.app.services.data_contracts.validator import ContractRegistry

        registry = ContractRegistry(mock_mongo_service)
        registry._cache[sample_data_contract.contract_id] = sample_data_contract

        registry.clear_cache()

        assert len(registry._cache) == 0

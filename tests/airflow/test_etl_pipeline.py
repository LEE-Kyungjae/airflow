"""
Tests for ETL Pipeline transformation and loading operations.

Covers:
- DataTransformer transformations
- DataCategory detection
- Date parsing (Korean and relative dates)
- Number parsing (Korean units, symbols)
- Quality validation
- Deduplication
- DataLoader operations
- ETLPipeline integration
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, AsyncMock, patch
import hashlib


class TestDataCategory:
    """Tests for DataCategory enum and detection."""

    def test_data_category_values(self):
        """Test DataCategory enum values."""
        from airflow.dags.utils.etl_pipeline import DataCategory

        assert DataCategory.NEWS_ARTICLE.value == "news_article"
        assert DataCategory.FINANCIAL_DATA.value == "financial_data"
        assert DataCategory.STOCK_PRICE.value == "stock_price"
        assert DataCategory.EXCHANGE_RATE.value == "exchange_rate"
        assert DataCategory.MARKET_INDEX.value == "market_index"
        assert DataCategory.ANNOUNCEMENT.value == "announcement"
        assert DataCategory.GENERIC.value == "generic"


class TestQualityLevel:
    """Tests for QualityLevel enum."""

    def test_quality_level_values(self):
        """Test QualityLevel enum values."""
        from airflow.dags.utils.etl_pipeline import QualityLevel

        assert QualityLevel.HIGH.value == "high"
        assert QualityLevel.MEDIUM.value == "medium"
        assert QualityLevel.LOW.value == "low"
        assert QualityLevel.INVALID.value == "invalid"


class TestTransformConfig:
    """Tests for TransformConfig dataclass."""

    def test_transform_config_defaults(self):
        """Test TransformConfig default values."""
        from airflow.dags.utils.etl_pipeline import TransformConfig, DataCategory

        config = TransformConfig(category=DataCategory.NEWS_ARTICLE)

        assert config.category == DataCategory.NEWS_ARTICLE
        assert config.date_format == "%Y-%m-%d %H:%M:%S"
        assert config.timezone == "Asia/Seoul"
        assert config.deduplicate is True
        assert config.dedup_fields == []
        assert config.required_fields == []
        assert config.quality_threshold == 0.7

    def test_transform_config_custom_values(self):
        """Test TransformConfig with custom values."""
        from airflow.dags.utils.etl_pipeline import TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.FINANCIAL_DATA,
            required_fields=["name", "price"],
            dedup_fields=["stock_code"],
            quality_threshold=0.8
        )

        assert config.required_fields == ["name", "price"]
        assert config.dedup_fields == ["stock_code"]
        assert config.quality_threshold == 0.8


class TestDataTransformerDateParsing:
    """Tests for DataTransformer date parsing."""

    def test_parse_iso_date(self):
        """Test parsing ISO format date."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_date("2024-01-15T10:30:00")

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_korean_date_with_time(self):
        """Test parsing Korean date format with time."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_date("2024년 1월 15일 14:30")

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 14
        assert result.minute == 30

    def test_parse_korean_date_only(self):
        """Test parsing Korean date format without time."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_date("2024년 1월 15일")

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_dot_separated_date(self):
        """Test parsing dot-separated date format."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_date("2024.01.15")

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_relative_minutes_ago(self):
        """Test parsing relative time (minutes ago)."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_date("30분 전")

        assert result is not None
        # Should be within last hour
        assert datetime.now() - result < timedelta(hours=1)

    def test_parse_relative_hours_ago(self):
        """Test parsing relative time (hours ago)."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_date("2시간 전")

        assert result is not None
        # Should be within last 3 hours
        assert datetime.now() - result < timedelta(hours=3)

    def test_parse_relative_days_ago(self):
        """Test parsing relative time (days ago)."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_date("3일 전")

        assert result is not None
        # Should be within last 4 days
        assert datetime.now() - result < timedelta(days=4)

    def test_parse_date_none_value(self):
        """Test parsing None date value."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_date(None)

        assert result is None

    def test_parse_date_datetime_input(self):
        """Test parsing datetime object input."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        now = datetime.utcnow()
        result = transformer._parse_date(now)

        assert result == now


class TestDataTransformerNumberParsing:
    """Tests for DataTransformer number parsing."""

    def test_parse_simple_number(self):
        """Test parsing simple number."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number("12345")

        assert result == 12345.0

    def test_parse_number_with_commas(self):
        """Test parsing number with comma separators."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number("1,234,567")

        assert result == 1234567.0

    def test_parse_number_with_percent(self):
        """Test parsing number with percent sign."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number("15.5%")

        assert result == 15.5

    def test_parse_number_with_won_symbol(self):
        """Test parsing number with won symbol."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number("1,000원")

        assert result == 1000.0

    def test_parse_korean_unit_man(self):
        """Test parsing number with Korean unit (만)."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number("5만")

        assert result == 50000.0

    def test_parse_korean_unit_eok(self):
        """Test parsing number with Korean unit (억)."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number("3억")

        assert result == 300000000.0

    def test_parse_negative_number(self):
        """Test parsing negative number."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number("-500")

        assert result == -500.0

    def test_parse_number_with_down_arrow(self):
        """Test parsing number with down arrow (negative indicator)."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number("▼1,500")

        assert result == -1500.0

    def test_parse_number_with_up_arrow(self):
        """Test parsing number with up arrow (positive indicator)."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number("▲2,500")

        assert result == 2500.0

    def test_parse_number_none_value(self):
        """Test parsing None number value."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number(None)

        assert result is None

    def test_parse_number_float_input(self):
        """Test parsing float input."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._parse_number(123.45)

        assert result == 123.45


class TestDataTransformerTextCleaning:
    """Tests for DataTransformer text cleaning."""

    def test_clean_text_basic(self):
        """Test basic text cleaning."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._clean_text("  Hello   World  ")

        assert result == "Hello World"

    def test_clean_text_with_special_chars(self):
        """Test text cleaning with special characters."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._clean_text("Hello\xa0World\u200b!")

        assert result == "Hello World!"

    def test_clean_text_empty(self):
        """Test cleaning empty text."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._clean_text("")

        assert result == ""

    def test_clean_text_none(self):
        """Test cleaning None text."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))
        result = transformer._clean_text(None)

        assert result == ""


class TestDataTransformerNewsTransform:
    """Tests for news article transformation."""

    def test_transform_news_basic(self, sample_news_articles):
        """Test basic news transformation."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.NEWS_ARTICLE,
            required_fields=["title"],
            quality_threshold=0.5
        )
        transformer = DataTransformer(config)

        result = transformer.transform(sample_news_articles)

        assert len(result) >= 1
        assert all("title" in r for r in result)
        assert all("_quality_score" in r for r in result)
        assert all("_crawled_at" in r for r in result)

    def test_transform_news_adds_content_hash(self, sample_news_articles):
        """Test that news transformation adds content hash."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.NEWS_ARTICLE,
            quality_threshold=0.5
        )
        transformer = DataTransformer(config)

        result = transformer.transform(sample_news_articles)

        for record in result:
            assert "content_hash" in record
            assert len(record["content_hash"]) == 32  # MD5 hash length

    def test_transform_news_generates_summary(self):
        """Test that news transformation generates summary if missing."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.NEWS_ARTICLE,
            quality_threshold=0.5
        )
        transformer = DataTransformer(config)

        data = [{"title": "Test", "content": "A" * 300}]
        result = transformer.transform(data)

        assert len(result) == 1
        assert "summary" in result[0]
        assert len(result[0]["summary"]) <= 203  # 200 + "..."


class TestDataTransformerFinancialTransform:
    """Tests for financial data transformation."""

    def test_transform_financial_basic(self, sample_financial_data):
        """Test basic financial data transformation."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.FINANCIAL_DATA,
            quality_threshold=0.3
        )
        transformer = DataTransformer(config)

        result = transformer.transform(sample_financial_data)

        assert len(result) >= 1
        # Check that numeric fields are converted
        for record in result:
            if "price" in record and record["price"] is not None:
                assert isinstance(record["price"], float)

    def test_transform_financial_stock_code_normalization(self, sample_financial_data):
        """Test stock code normalization in financial transform."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.STOCK_PRICE,
            quality_threshold=0.3
        )
        transformer = DataTransformer(config)

        result = transformer.transform(sample_financial_data)

        for record in result:
            if "stock_code" in record:
                # Stock code should be 6 digits
                assert len(record["stock_code"]) == 6


class TestDataTransformerExchangeRateTransform:
    """Tests for exchange rate transformation."""

    def test_transform_exchange_rate_basic(self, sample_exchange_rates):
        """Test basic exchange rate transformation."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.EXCHANGE_RATE,
            quality_threshold=0.3
        )
        transformer = DataTransformer(config)

        result = transformer.transform(sample_exchange_rates)

        assert len(result) >= 1
        for record in result:
            if "currency_code" in record:
                assert len(record["currency_code"]) == 3


class TestDataTransformerQualityValidation:
    """Tests for quality validation."""

    def test_quality_score_high(self):
        """Test high quality score for complete record."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.NEWS_ARTICLE,
            required_fields=["title", "url"],
            quality_threshold=0.5
        )
        transformer = DataTransformer(config)

        data = [{"title": "Test Title", "url": "https://example.com"}]
        result = transformer.transform(data)

        assert len(result) == 1
        assert result[0]["_quality_score"] >= 0.8

    def test_quality_score_low_missing_fields(self):
        """Test low quality score for missing required fields."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.NEWS_ARTICLE,
            required_fields=["title", "content", "url"],
            quality_threshold=0.3
        )
        transformer = DataTransformer(config)

        data = [{"title": "Only Title"}]  # Missing content and url
        result = transformer.transform(data)

        assert len(result) == 1
        assert result[0]["_quality_score"] < 0.8

    def test_quality_level_assignment(self):
        """Test quality level assignment based on score."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.NEWS_ARTICLE,
            quality_threshold=0.3
        )
        transformer = DataTransformer(config)

        assert transformer._get_quality_level(0.9).value == "high"
        assert transformer._get_quality_level(0.7).value == "medium"
        assert transformer._get_quality_level(0.5).value == "low"
        assert transformer._get_quality_level(0.3).value == "invalid"


class TestETLPipelineCategoryDetection:
    """Tests for automatic category detection."""

    def test_detect_news_article_category(self, mock_mongo_service):
        """Test detection of news article category."""
        from airflow.dags.utils.etl_pipeline import ETLPipeline, DataCategory

        pipeline = ETLPipeline(mock_mongo_service)

        data = [{"title": "News Title", "content": "News content", "summary": "Summary"}]
        result = pipeline._detect_category(data)

        assert result == DataCategory.NEWS_ARTICLE

    def test_detect_stock_price_category(self, mock_mongo_service):
        """Test detection of stock price category."""
        from airflow.dags.utils.etl_pipeline import ETLPipeline, DataCategory

        pipeline = ETLPipeline(mock_mongo_service)

        data = [{"stock_code": "005930", "price": 70000}]
        result = pipeline._detect_category(data)

        assert result == DataCategory.STOCK_PRICE

    def test_detect_exchange_rate_category(self, mock_mongo_service):
        """Test detection of exchange rate category."""
        from airflow.dags.utils.etl_pipeline import ETLPipeline, DataCategory

        pipeline = ETLPipeline(mock_mongo_service)

        data = [{"currency_code": "USD", "exchange_rate": 1320.5}]
        result = pipeline._detect_category(data)

        assert result == DataCategory.EXCHANGE_RATE

    def test_detect_financial_data_category(self, mock_mongo_service):
        """Test detection of generic financial data category."""
        from airflow.dags.utils.etl_pipeline import ETLPipeline, DataCategory

        pipeline = ETLPipeline(mock_mongo_service)

        data = [{"value": 100, "change": 5, "change_rate": 0.05}]
        result = pipeline._detect_category(data)

        assert result == DataCategory.FINANCIAL_DATA

    def test_detect_generic_category(self, mock_mongo_service):
        """Test detection of generic category."""
        from airflow.dags.utils.etl_pipeline import ETLPipeline, DataCategory

        pipeline = ETLPipeline(mock_mongo_service)

        data = [{"random_field": "value", "another": 123}]
        result = pipeline._detect_category(data)

        assert result == DataCategory.GENERIC

    def test_detect_empty_data(self, mock_mongo_service):
        """Test detection with empty data."""
        from airflow.dags.utils.etl_pipeline import ETLPipeline, DataCategory

        pipeline = ETLPipeline(mock_mongo_service)

        result = pipeline._detect_category([])

        assert result == DataCategory.GENERIC


class TestETLPipelineDeduplication:
    """Tests for data deduplication."""

    def test_deduplicate_by_single_field(self, mock_mongo_service):
        """Test deduplication by single field."""
        from airflow.dags.utils.etl_pipeline import ETLPipeline

        pipeline = ETLPipeline(mock_mongo_service)

        data = [
            {"id": "1", "value": "a"},
            {"id": "2", "value": "b"},
            {"id": "1", "value": "c"},  # Duplicate by id
        ]
        result = pipeline._deduplicate(data, ["id"])

        assert len(result) == 2
        assert result[0]["value"] == "a"  # First occurrence kept

    def test_deduplicate_by_multiple_fields(self, mock_mongo_service):
        """Test deduplication by multiple fields."""
        from airflow.dags.utils.etl_pipeline import ETLPipeline

        pipeline = ETLPipeline(mock_mongo_service)

        data = [
            {"code": "A", "date": "2024-01-01", "value": 1},
            {"code": "A", "date": "2024-01-02", "value": 2},
            {"code": "A", "date": "2024-01-01", "value": 3},  # Duplicate
        ]
        result = pipeline._deduplicate(data, ["code", "date"])

        assert len(result) == 2

    def test_deduplicate_empty_data(self, mock_mongo_service):
        """Test deduplication with empty data."""
        from airflow.dags.utils.etl_pipeline import ETLPipeline

        pipeline = ETLPipeline(mock_mongo_service)

        result = pipeline._deduplicate([], ["id"])

        assert len(result) == 0


class TestETLResult:
    """Tests for ETLResult dataclass."""

    def test_etl_result_creation(self):
        """Test ETLResult creation with all fields."""
        from airflow.dags.utils.etl_pipeline import ETLResult, DataCategory

        result = ETLResult(
            success=True,
            source_id="source123",
            category=DataCategory.NEWS_ARTICLE,
            extracted_count=100,
            transformed_count=95,
            loaded_count=90,
            duplicate_count=5,
            invalid_count=5,
            quality_score=0.85,
            errors=[],
            warnings=["Some warning"],
            sample_data=[],
            execution_time_ms=1500,
            metadata={"collection": "news"},
            skipped_unchanged=10,
            new_records=80,
            modified_records=10
        )

        assert result.success is True
        assert result.extracted_count == 100
        assert result.transformed_count == 95
        assert result.loaded_count == 90
        assert result.quality_score == 0.85


class TestDataLoaderCollectionMapping:
    """Tests for DataLoader staging collection mapping."""

    def test_staging_collection_map(self):
        """Test staging collection mapping."""
        from airflow.dags.utils.etl_pipeline import DataLoader

        assert DataLoader.STAGING_COLLECTION_MAP["news_articles"] == "staging_news"
        assert DataLoader.STAGING_COLLECTION_MAP["financial_data"] == "staging_financial"
        assert DataLoader.STAGING_COLLECTION_MAP["stock_prices"] == "staging_financial"
        assert DataLoader.STAGING_COLLECTION_MAP["exchange_rates"] == "staging_financial"
        assert DataLoader.STAGING_COLLECTION_MAP["announcements"] == "staging_news"


@pytest.mark.asyncio
class TestETLPipelineRun:
    """Tests for ETLPipeline.run() method."""

    async def test_run_with_empty_data(self, mock_mongo_service):
        """Test running pipeline with empty data."""
        from airflow.dags.utils.etl_pipeline import ETLPipeline, DataCategory

        pipeline = ETLPipeline(mock_mongo_service)

        with patch.object(pipeline, '_detect_category') as mock_detect:
            mock_detect.return_value = DataCategory.NEWS_ARTICLE

            result = await pipeline.run(
                raw_data=[],
                source_id="test_source",
                category=DataCategory.NEWS_ARTICLE,
                skip_unchanged=False,
                use_staging=False
            )

            # Empty data should return early
            assert result.extracted_count == 0 or result.transformed_count == 0

    async def test_run_basic_transformation(self, mock_mongo_service, sample_news_articles):
        """Test running basic pipeline transformation."""
        from airflow.dags.utils.etl_pipeline import (
            ETLPipeline, DataCategory, TransformConfig, LoadConfig, DataLoader
        )

        # Mock the collection
        mock_collection = MagicMock()
        mock_collection.update_one.return_value = MagicMock(upserted_id="new_id", modified_count=0)
        mock_collection.insert_one.return_value = MagicMock(inserted_id="inserted_id")
        mock_collection.create_index.return_value = "index"
        mock_mongo_service.db.__getitem__.return_value = mock_collection

        pipeline = ETLPipeline(mock_mongo_service)

        # Run with skip_unchanged=False to avoid change detection
        result = await pipeline.run(
            raw_data=sample_news_articles,
            source_id="test_source",
            category=DataCategory.NEWS_ARTICLE,
            skip_unchanged=False,
            use_staging=False
        )

        assert result.extracted_count == len(sample_news_articles)
        assert result.category == DataCategory.NEWS_ARTICLE


class TestURLNormalization:
    """Tests for URL normalization."""

    def test_normalize_url_basic(self):
        """Test basic URL normalization."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))

        result = transformer._normalize_url("  https://example.com/page  ")
        assert result == "https://example.com/page"

    def test_normalize_url_protocol_less(self):
        """Test URL normalization for protocol-less URLs."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))

        result = transformer._normalize_url("//example.com/page")
        assert result == "https://example.com/page"

    def test_normalize_url_empty(self):
        """Test URL normalization for empty string."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))

        result = transformer._normalize_url("")
        assert result == ""


class TestStockCodeNormalization:
    """Tests for stock code normalization."""

    def test_normalize_stock_code_basic(self):
        """Test basic stock code normalization."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))

        result = transformer._normalize_stock_code("5930")
        assert result == "005930"  # Zero-padded to 6 digits

    def test_normalize_stock_code_with_letters(self):
        """Test stock code normalization with letters."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))

        result = transformer._normalize_stock_code("A005930")
        assert result == "005930"  # Letters removed

    def test_normalize_stock_code_empty(self):
        """Test stock code normalization for empty string."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))

        result = transformer._normalize_stock_code("")
        assert result == ""


class TestContentHashGeneration:
    """Tests for content hash generation."""

    def test_generate_content_hash_basic(self):
        """Test basic content hash generation."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))

        result = transformer._generate_content_hash("title", "url")

        expected = hashlib.md5("title|url".encode()).hexdigest()
        assert result == expected

    def test_generate_content_hash_with_none(self):
        """Test content hash generation with None values."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        transformer = DataTransformer(TransformConfig(category=DataCategory.GENERIC))

        result = transformer._generate_content_hash("title", None, "other")

        expected = hashlib.md5("title|other".encode()).hexdigest()
        assert result == expected


class TestFieldMappingApplication:
    """Tests for field mapping application."""

    def test_apply_field_mappings_basic(self):
        """Test basic field mapping application."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(
            category=DataCategory.GENERIC,
            field_mappings={"old_name": "new_name", "another": "mapped"}
        )
        transformer = DataTransformer(config)

        record = {"old_name": "value1", "another": "value2", "unchanged": "value3"}
        result = transformer._apply_field_mappings(record)

        assert "new_name" in result
        assert "mapped" in result
        assert "unchanged" in result
        assert "old_name" not in result

    def test_apply_field_mappings_empty(self):
        """Test field mapping with empty mappings."""
        from airflow.dags.utils.etl_pipeline import DataTransformer, TransformConfig, DataCategory

        config = TransformConfig(category=DataCategory.GENERIC)
        transformer = DataTransformer(config)

        record = {"field1": "value1"}
        result = transformer._apply_field_mappings(record)

        assert result == record

"""
Schema Registry - 스키마 버전 관리 및 진화

MongoDB 컬렉션별 스키마를 관리하고 버전별 이력을 추적합니다.

주요 기능:
- 스키마 버전 관리 (v1, v2, v3...)
- 스키마 호환성 검증 (BACKWARD, FORWARD, FULL)
- 스키마 진화 추적 및 마이그레이션
- 데이터 검증

사용 예시:
    from api.app.services.schema_registry import (
        SchemaRegistry,
        Schema,
        FieldSchema,
        FieldType,
        CompatibilityMode,
        DataCategory,
    )

    # 레지스트리 초기화
    registry = SchemaRegistry(mongo_service)

    # 스키마 등록
    schema = Schema(fields=[
        FieldSchema(name="title", field_type=FieldType.STRING, required=True),
        FieldSchema(name="price", field_type=FieldType.FLOAT),
    ])
    version, result = registry.register_schema("my_source", schema)

    # 스키마 조회
    current = registry.get_schema("my_source")

    # 데이터 검증
    from api.app.services.schema_registry import SchemaValidator
    validator = SchemaValidator()
    result = validator.validate(my_data, schema)
"""

# Models
from .models import (
    # Core models
    Schema,
    SchemaVersion,
    FieldSchema,
    # Enums
    FieldType,
    CompatibilityMode,
    DataCategory,
    # Results
    CompatibilityIssue,
    CompatibilityResult,
    # Default schemas
    DEFAULT_SCHEMAS,
    get_default_schema,
)

# Registry
from .registry import SchemaRegistry

# Compatibility checker
from .compatibility import (
    CompatibilityChecker,
    check_compatibility,
    TYPE_WIDENING_RULES,
    TYPE_NARROWING_RULES,
)

# Validator
from .validator import (
    SchemaValidator,
    ValidationError,
    ValidationResult,
    BatchValidationResult,
    validate_data,
    validate_by_category,
)

# Detector
from .detector import (
    SchemaDetector,
    FieldStats,
)

# Evolution
from .evolution import (
    SchemaEvolution,
    EvolutionAction,
    MigrationPlan,
    MigrationStep,
    MigrationResult,
)

__all__ = [
    # Core models
    "Schema",
    "SchemaVersion",
    "FieldSchema",
    # Enums
    "FieldType",
    "CompatibilityMode",
    "DataCategory",
    # Compatibility
    "CompatibilityIssue",
    "CompatibilityResult",
    "CompatibilityChecker",
    "check_compatibility",
    "TYPE_WIDENING_RULES",
    "TYPE_NARROWING_RULES",
    # Registry
    "SchemaRegistry",
    # Validator
    "SchemaValidator",
    "ValidationError",
    "ValidationResult",
    "BatchValidationResult",
    "validate_data",
    "validate_by_category",
    # Detector
    "SchemaDetector",
    "FieldStats",
    # Evolution
    "SchemaEvolution",
    "EvolutionAction",
    "MigrationPlan",
    "MigrationStep",
    "MigrationResult",
    # Default schemas
    "DEFAULT_SCHEMAS",
    "get_default_schema",
]

__version__ = "1.0.0"

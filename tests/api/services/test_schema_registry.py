"""
Tests for SchemaRegistry and compatibility checking.

Covers:
- Schema models (FieldSchema, Schema, SchemaVersion)
- CompatibilityMode behavior
- CompatibilityChecker type checking
- SchemaRegistry registration and retrieval
- Schema versioning and deprecation
- Schema comparison and drift detection
- Default schema templates
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock


class TestFieldType:
    """Tests for FieldType enum."""

    def test_field_type_values(self):
        """Test all FieldType enum values."""
        from api.app.services.schema_registry.models import FieldType

        assert FieldType.STRING.value == "string"
        assert FieldType.INTEGER.value == "integer"
        assert FieldType.FLOAT.value == "float"
        assert FieldType.BOOLEAN.value == "boolean"
        assert FieldType.DATE.value == "date"
        assert FieldType.DATETIME.value == "datetime"
        assert FieldType.ARRAY.value == "array"
        assert FieldType.OBJECT.value == "object"
        assert FieldType.ANY.value == "any"

    def test_from_python_type_string(self):
        """Test type inference for string."""
        from api.app.services.schema_registry.models import FieldType

        result = FieldType.from_python_type("hello")
        assert result == FieldType.STRING

    def test_from_python_type_integer(self):
        """Test type inference for integer."""
        from api.app.services.schema_registry.models import FieldType

        result = FieldType.from_python_type(42)
        assert result == FieldType.INTEGER

    def test_from_python_type_float(self):
        """Test type inference for float."""
        from api.app.services.schema_registry.models import FieldType

        result = FieldType.from_python_type(3.14)
        assert result == FieldType.FLOAT

    def test_from_python_type_boolean(self):
        """Test type inference for boolean."""
        from api.app.services.schema_registry.models import FieldType

        result = FieldType.from_python_type(True)
        assert result == FieldType.BOOLEAN

    def test_from_python_type_list(self):
        """Test type inference for list."""
        from api.app.services.schema_registry.models import FieldType

        result = FieldType.from_python_type([1, 2, 3])
        assert result == FieldType.ARRAY

    def test_from_python_type_dict(self):
        """Test type inference for dict."""
        from api.app.services.schema_registry.models import FieldType

        result = FieldType.from_python_type({"key": "value"})
        assert result == FieldType.OBJECT

    def test_from_python_type_datetime(self):
        """Test type inference for datetime."""
        from api.app.services.schema_registry.models import FieldType

        result = FieldType.from_python_type(datetime.now())
        assert result == FieldType.DATETIME

    def test_from_python_type_none(self):
        """Test type inference for None."""
        from api.app.services.schema_registry.models import FieldType

        result = FieldType.from_python_type(None)
        assert result == FieldType.ANY


class TestCompatibilityMode:
    """Tests for CompatibilityMode enum."""

    def test_compatibility_mode_values(self):
        """Test all CompatibilityMode enum values."""
        from api.app.services.schema_registry.models import CompatibilityMode

        assert CompatibilityMode.NONE.value == "none"
        assert CompatibilityMode.BACKWARD.value == "backward"
        assert CompatibilityMode.FORWARD.value == "forward"
        assert CompatibilityMode.FULL.value == "full"
        assert CompatibilityMode.BACKWARD_TRANSITIVE.value == "backward_transitive"
        assert CompatibilityMode.FORWARD_TRANSITIVE.value == "forward_transitive"
        assert CompatibilityMode.FULL_TRANSITIVE.value == "full_transitive"


class TestFieldSchema:
    """Tests for FieldSchema dataclass."""

    def test_field_schema_creation(self, sample_field_schema):
        """Test basic FieldSchema creation."""
        assert sample_field_schema.name == "title"
        assert sample_field_schema.required is True
        assert sample_field_schema.nullable is False

    def test_field_schema_defaults(self):
        """Test FieldSchema default values."""
        from api.app.services.schema_registry.models import FieldSchema, FieldType

        field = FieldSchema(name="test_field", field_type=FieldType.STRING)

        assert field.required is False
        assert field.nullable is True
        assert field.default is None
        assert field.description == ""
        assert field.deprecated is False

    def test_field_schema_to_dict(self, sample_field_schema):
        """Test FieldSchema serialization to dict."""
        result = sample_field_schema.to_dict()

        assert result["name"] == "title"
        assert result["type"] == "string"
        assert result["required"] is True
        assert result["nullable"] is False
        assert "min_length" in result
        assert "max_length" in result

    def test_field_schema_from_dict(self):
        """Test FieldSchema deserialization from dict."""
        from api.app.services.schema_registry.models import FieldSchema, FieldType

        data = {
            "name": "test_field",
            "type": "integer",
            "required": True,
            "nullable": False,
            "min_value": 0,
            "max_value": 100
        }

        field = FieldSchema.from_dict(data)

        assert field.name == "test_field"
        assert field.field_type == FieldType.INTEGER
        assert field.required is True
        assert field.min_value == 0
        assert field.max_value == 100

    def test_field_schema_with_enum_values(self):
        """Test FieldSchema with enum constraint."""
        from api.app.services.schema_registry.models import FieldSchema, FieldType

        field = FieldSchema(
            name="status",
            field_type=FieldType.STRING,
            enum_values=["active", "inactive", "pending"]
        )

        result = field.to_dict()
        assert result["enum"] == ["active", "inactive", "pending"]


class TestSchema:
    """Tests for Schema dataclass."""

    def test_schema_creation(self, sample_schema):
        """Test basic Schema creation."""
        assert len(sample_schema.fields) == 5
        assert sample_schema.description == "Test news article schema"

    def test_schema_get_field(self, sample_schema):
        """Test getting field by name."""
        field = sample_schema.get_field("title")
        assert field is not None
        assert field.name == "title"

    def test_schema_get_field_not_found(self, sample_schema):
        """Test getting non-existent field."""
        field = sample_schema.get_field("nonexistent")
        assert field is None

    def test_schema_get_field_names(self, sample_schema):
        """Test getting all field names."""
        names = sample_schema.get_field_names()
        assert "title" in names
        assert "content" in names
        assert "url" in names

    def test_schema_get_required_fields(self, sample_schema):
        """Test getting required field names."""
        required = sample_schema.get_required_fields()
        assert "title" in required

    def test_schema_get_optional_fields(self, sample_schema):
        """Test getting optional field names."""
        optional = sample_schema.get_optional_fields()
        assert "content" in optional
        assert "url" in optional

    def test_schema_add_field(self, sample_schema):
        """Test adding a new field."""
        from api.app.services.schema_registry.models import FieldSchema, FieldType

        new_field = FieldSchema(name="author", field_type=FieldType.STRING)
        sample_schema.add_field(new_field)

        assert sample_schema.get_field("author") is not None

    def test_schema_add_duplicate_field_raises(self, sample_schema):
        """Test adding duplicate field raises error."""
        from api.app.services.schema_registry.models import FieldSchema, FieldType

        with pytest.raises(ValueError, match="already exists"):
            sample_schema.add_field(FieldSchema(name="title", field_type=FieldType.STRING))

    def test_schema_remove_field(self, sample_schema):
        """Test removing a field."""
        result = sample_schema.remove_field("content")
        assert result is True
        assert sample_schema.get_field("content") is None

    def test_schema_remove_nonexistent_field(self, sample_schema):
        """Test removing non-existent field."""
        result = sample_schema.remove_field("nonexistent")
        assert result is False

    def test_schema_compute_fingerprint(self, sample_schema):
        """Test computing schema fingerprint."""
        fingerprint = sample_schema.compute_fingerprint()

        assert len(fingerprint) == 16
        # Same schema should produce same fingerprint
        assert fingerprint == sample_schema.compute_fingerprint()

    def test_schema_to_dict(self, sample_schema):
        """Test schema serialization."""
        result = sample_schema.to_dict()

        assert "fields" in result
        assert "description" in result
        assert len(result["fields"]) == 5

    def test_schema_from_dict(self):
        """Test schema deserialization."""
        from api.app.services.schema_registry.models import Schema, DataCategory

        data = {
            "fields": [
                {"name": "id", "type": "string", "required": True},
                {"name": "value", "type": "integer", "required": False}
            ],
            "description": "Test schema",
            "data_category": "generic"
        }

        schema = Schema.from_dict(data)

        assert len(schema.fields) == 2
        assert schema.data_category == DataCategory.GENERIC

    def test_schema_clone(self, sample_schema):
        """Test schema cloning."""
        clone = sample_schema.clone()

        assert clone is not sample_schema
        assert clone.compute_fingerprint() == sample_schema.compute_fingerprint()
        assert len(clone.fields) == len(sample_schema.fields)


class TestSchemaVersion:
    """Tests for SchemaVersion dataclass."""

    def test_schema_version_creation(self, sample_schema):
        """Test SchemaVersion creation."""
        from api.app.services.schema_registry.models import SchemaVersion, CompatibilityMode

        version = SchemaVersion(
            version=1,
            schema=sample_schema,
            fingerprint=sample_schema.compute_fingerprint(),
            created_at=datetime.utcnow(),
            created_by="test_user",
            change_description="Initial version",
            is_active=True,
            compatibility_mode=CompatibilityMode.BACKWARD,
            tags=["initial", "test"]
        )

        assert version.version == 1
        assert version.is_active is True
        assert version.created_by == "test_user"
        assert "initial" in version.tags

    def test_schema_version_to_dict(self, sample_schema):
        """Test SchemaVersion serialization."""
        from api.app.services.schema_registry.models import SchemaVersion, CompatibilityMode

        version = SchemaVersion(
            version=1,
            schema=sample_schema,
            fingerprint=sample_schema.compute_fingerprint(),
            created_at=datetime.utcnow(),
            created_by="test"
        )

        result = version.to_dict()

        assert result["version"] == 1
        assert "schema" in result
        assert "fingerprint" in result
        assert "created_at" in result

    def test_schema_version_from_dict(self, sample_schema):
        """Test SchemaVersion deserialization."""
        from api.app.services.schema_registry.models import SchemaVersion

        data = {
            "version": 2,
            "schema": sample_schema.to_dict(),
            "fingerprint": "abc123",
            "created_at": datetime.utcnow().isoformat(),
            "created_by": "system",
            "change_description": "Test change",
            "is_active": True,
            "compatibility_mode": "backward",
            "tags": ["test"]
        }

        version = SchemaVersion.from_dict(data)

        assert version.version == 2
        assert version.created_by == "system"
        assert len(version.tags) == 1


class TestCompatibilityIssue:
    """Tests for CompatibilityIssue dataclass."""

    def test_compatibility_issue_creation(self):
        """Test CompatibilityIssue creation."""
        from api.app.services.schema_registry.models import CompatibilityIssue

        issue = CompatibilityIssue(
            field_name="test_field",
            issue_type="type_change",
            severity="error",
            message="Type changed from string to integer",
            old_value="string",
            new_value="integer"
        )

        assert issue.field_name == "test_field"
        assert issue.is_error() is True
        assert issue.is_warning() is False

    def test_compatibility_issue_to_dict(self):
        """Test CompatibilityIssue serialization."""
        from api.app.services.schema_registry.models import CompatibilityIssue

        issue = CompatibilityIssue(
            field_name="field",
            issue_type="removed",
            severity="warning",
            message="Field removed"
        )

        result = issue.to_dict()

        assert result["field_name"] == "field"
        assert result["severity"] == "warning"


class TestCompatibilityResult:
    """Tests for CompatibilityResult dataclass."""

    def test_compatibility_result_compatible(self):
        """Test compatible CompatibilityResult."""
        from api.app.services.schema_registry.models import CompatibilityResult, CompatibilityMode

        result = CompatibilityResult(
            is_compatible=True,
            issues=[],
            mode=CompatibilityMode.BACKWARD
        )

        assert result.is_compatible is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0

    def test_compatibility_result_with_issues(self):
        """Test CompatibilityResult with issues."""
        from api.app.services.schema_registry.models import (
            CompatibilityResult, CompatibilityIssue, CompatibilityMode
        )

        issues = [
            CompatibilityIssue("f1", "error1", "error", "Error message"),
            CompatibilityIssue("f2", "warn1", "warning", "Warning message"),
            CompatibilityIssue("f3", "info1", "info", "Info message")
        ]

        result = CompatibilityResult(
            is_compatible=False,
            issues=issues,
            mode=CompatibilityMode.FULL
        )

        assert result.is_compatible is False
        assert len(result.errors) == 1
        assert len(result.warnings) == 1
        assert len(result.info_items) == 1

    def test_compatibility_result_to_dict(self):
        """Test CompatibilityResult serialization."""
        from api.app.services.schema_registry.models import CompatibilityResult, CompatibilityMode

        result = CompatibilityResult(
            is_compatible=True,
            issues=[],
            mode=CompatibilityMode.BACKWARD
        )

        data = result.to_dict()

        assert data["is_compatible"] is True
        assert data["error_count"] == 0
        assert data["mode"] == "backward"


class TestCompatibilityChecker:
    """Tests for CompatibilityChecker class."""

    def test_check_compatibility_none_mode(self, sample_schema, modified_schema):
        """Test compatibility check with NONE mode."""
        from api.app.services.schema_registry.compatibility import CompatibilityChecker
        from api.app.services.schema_registry.models import CompatibilityMode

        checker = CompatibilityChecker()
        result = checker.check_compatibility(
            sample_schema, modified_schema, CompatibilityMode.NONE
        )

        assert result.is_compatible is True

    def test_check_compatibility_backward_add_optional_field(
        self, sample_schema, modified_schema
    ):
        """Test backward compatibility when adding optional field."""
        from api.app.services.schema_registry.compatibility import CompatibilityChecker
        from api.app.services.schema_registry.models import CompatibilityMode

        checker = CompatibilityChecker()
        result = checker.check_compatibility(
            sample_schema, modified_schema, CompatibilityMode.BACKWARD
        )

        # Adding optional field is backward compatible
        assert result.is_compatible is True

    def test_check_compatibility_backward_add_required_field_no_default(self, sample_schema):
        """Test backward compatibility fails when adding required field without default."""
        from api.app.services.schema_registry.compatibility import CompatibilityChecker
        from api.app.services.schema_registry.models import (
            CompatibilityMode, FieldSchema, FieldType
        )

        checker = CompatibilityChecker()

        new_schema = sample_schema.clone()
        new_schema.add_field(FieldSchema(
            name="new_required",
            field_type=FieldType.STRING,
            required=True
        ))

        result = checker.check_compatibility(
            sample_schema, new_schema, CompatibilityMode.BACKWARD
        )

        assert result.is_compatible is False
        assert len(result.errors) > 0

    def test_check_compatibility_forward_remove_field(self, sample_schema):
        """Test forward compatibility fails when removing field."""
        from api.app.services.schema_registry.compatibility import CompatibilityChecker
        from api.app.services.schema_registry.models import CompatibilityMode

        checker = CompatibilityChecker()

        new_schema = sample_schema.clone()
        new_schema.remove_field("content")

        result = checker.check_compatibility(
            sample_schema, new_schema, CompatibilityMode.FORWARD
        )

        assert result.is_compatible is False
        error_types = [i.issue_type for i in result.errors]
        assert "removed_field" in error_types

    def test_check_compatibility_full_mode(self, sample_schema, incompatible_schema):
        """Test full compatibility check fails for incompatible changes."""
        from api.app.services.schema_registry.compatibility import CompatibilityChecker
        from api.app.services.schema_registry.models import CompatibilityMode

        checker = CompatibilityChecker()

        result = checker.check_compatibility(
            sample_schema, incompatible_schema, CompatibilityMode.FULL
        )

        assert result.is_compatible is False

    def test_check_type_widening_backward(self):
        """Test type widening is allowed in backward mode."""
        from api.app.services.schema_registry.compatibility import CompatibilityChecker
        from api.app.services.schema_registry.models import (
            Schema, FieldSchema, FieldType, CompatibilityMode
        )

        checker = CompatibilityChecker()

        old_schema = Schema(fields=[
            FieldSchema(name="value", field_type=FieldType.INTEGER)
        ])

        new_schema = Schema(fields=[
            FieldSchema(name="value", field_type=FieldType.FLOAT)  # Widening
        ])

        result = checker.check_compatibility(
            old_schema, new_schema, CompatibilityMode.BACKWARD
        )

        # Type widening (int -> float) should be allowed in BACKWARD mode
        assert result.is_compatible is True

    def test_check_type_narrowing_backward(self):
        """Test type narrowing fails in backward mode."""
        from api.app.services.schema_registry.compatibility import CompatibilityChecker
        from api.app.services.schema_registry.models import (
            Schema, FieldSchema, FieldType, CompatibilityMode
        )

        checker = CompatibilityChecker()

        old_schema = Schema(fields=[
            FieldSchema(name="value", field_type=FieldType.FLOAT)
        ])

        new_schema = Schema(fields=[
            FieldSchema(name="value", field_type=FieldType.INTEGER)  # Narrowing
        ])

        result = checker.check_compatibility(
            old_schema, new_schema, CompatibilityMode.BACKWARD
        )

        # Type narrowing should fail in BACKWARD mode
        assert result.is_compatible is False

    def test_check_nullable_change(self):
        """Test nullable change detection."""
        from api.app.services.schema_registry.compatibility import CompatibilityChecker
        from api.app.services.schema_registry.models import (
            Schema, FieldSchema, FieldType, CompatibilityMode
        )

        checker = CompatibilityChecker()

        old_schema = Schema(fields=[
            FieldSchema(name="field", field_type=FieldType.STRING, nullable=True)
        ])

        new_schema = Schema(fields=[
            FieldSchema(name="field", field_type=FieldType.STRING, nullable=False)
        ])

        result = checker.check_compatibility(
            old_schema, new_schema, CompatibilityMode.BACKWARD
        )

        # Removing nullable is a breaking change for BACKWARD
        assert result.is_compatible is False

    def test_is_type_compatible(self):
        """Test type compatibility checking."""
        from api.app.services.schema_registry.compatibility import CompatibilityChecker
        from api.app.services.schema_registry.models import FieldType, CompatibilityMode

        checker = CompatibilityChecker()

        # Same type is always compatible
        assert checker.is_type_compatible(
            FieldType.STRING, FieldType.STRING, CompatibilityMode.FULL
        ) is True

        # NONE mode allows any change
        assert checker.is_type_compatible(
            FieldType.STRING, FieldType.INTEGER, CompatibilityMode.NONE
        ) is True


class TestSchemaRegistry:
    """Tests for SchemaRegistry class."""

    def test_registry_register_first_schema(self, mock_mongo_service, sample_schema):
        """Test registering first schema version."""
        from api.app.services.schema_registry.registry import SchemaRegistry

        mock_mongo_service.db.schema_registry.find.return_value.sort.return_value = []
        mock_mongo_service.db.schema_registry.insert_one.return_value = MagicMock()

        registry = SchemaRegistry(mock_mongo_service)

        version, compat_result = registry.register_schema(
            source_id="test_source",
            schema=sample_schema,
            created_by="test_user",
            change_description="Initial version"
        )

        assert version.version == 1
        assert version.is_active is True
        assert compat_result.is_compatible is True

    def test_registry_register_unchanged_schema(self, mock_mongo_service, sample_schema):
        """Test registering unchanged schema returns existing version."""
        from api.app.services.schema_registry.registry import SchemaRegistry
        from api.app.services.schema_registry.models import SchemaVersion

        existing_version = SchemaVersion(
            version=1,
            schema=sample_schema,
            fingerprint=sample_schema.compute_fingerprint(),
            created_at=datetime.utcnow(),
            is_active=True
        )

        registry = SchemaRegistry(None)  # In-memory mode
        registry._cache["test_source"] = [existing_version]

        version, compat_result = registry.register_schema(
            source_id="test_source",
            schema=sample_schema
        )

        # Should return existing version
        assert version.version == 1

    def test_registry_register_incompatible_schema_raises(
        self, mock_mongo_service, sample_schema, incompatible_schema
    ):
        """Test registering incompatible schema raises error."""
        from api.app.services.schema_registry.registry import SchemaRegistry
        from api.app.services.schema_registry.models import SchemaVersion, CompatibilityMode

        existing_version = SchemaVersion(
            version=1,
            schema=sample_schema,
            fingerprint=sample_schema.compute_fingerprint(),
            created_at=datetime.utcnow(),
            is_active=True
        )

        registry = SchemaRegistry(None)
        registry._cache["test_source"] = [existing_version]

        with pytest.raises(ValueError, match="incompatible"):
            registry.register_schema(
                source_id="test_source",
                schema=incompatible_schema,
                compatibility_mode=CompatibilityMode.FULL
            )

    def test_registry_get_schema_latest(self, sample_schema):
        """Test getting latest schema version."""
        from api.app.services.schema_registry.registry import SchemaRegistry
        from api.app.services.schema_registry.models import SchemaVersion

        versions = [
            SchemaVersion(version=1, schema=sample_schema,
                         fingerprint="fp1", created_at=datetime.utcnow(),
                         is_active=True),
            SchemaVersion(version=2, schema=sample_schema.clone(),
                         fingerprint="fp2", created_at=datetime.utcnow(),
                         is_active=True)
        ]

        registry = SchemaRegistry(None)
        registry._cache["test_source:False"] = versions

        result = registry.get_schema("test_source")

        assert result.version == 2

    def test_registry_get_schema_specific_version(self, sample_schema):
        """Test getting specific schema version."""
        from api.app.services.schema_registry.registry import SchemaRegistry
        from api.app.services.schema_registry.models import SchemaVersion

        versions = [
            SchemaVersion(version=1, schema=sample_schema,
                         fingerprint="fp1", created_at=datetime.utcnow()),
            SchemaVersion(version=2, schema=sample_schema.clone(),
                         fingerprint="fp2", created_at=datetime.utcnow())
        ]

        registry = SchemaRegistry(None)
        registry._cache["test_source:False"] = versions

        result = registry.get_schema("test_source", version=1)

        assert result.version == 1

    def test_registry_get_schema_not_found(self):
        """Test getting schema that doesn't exist."""
        from api.app.services.schema_registry.registry import SchemaRegistry

        registry = SchemaRegistry(None)
        result = registry.get_schema("nonexistent_source")

        assert result is None

    def test_registry_get_version_history(self, sample_schema):
        """Test getting schema version history."""
        from api.app.services.schema_registry.registry import SchemaRegistry
        from api.app.services.schema_registry.models import SchemaVersion

        versions = [
            SchemaVersion(version=1, schema=sample_schema,
                         fingerprint="fp1", created_at=datetime.utcnow(),
                         change_description="Initial"),
            SchemaVersion(version=2, schema=sample_schema.clone(),
                         fingerprint="fp2", created_at=datetime.utcnow(),
                         change_description="Update")
        ]

        registry = SchemaRegistry(None)
        registry._cache["test_source:True"] = versions

        history = registry.get_version_history("test_source", limit=10)

        assert len(history) == 2
        assert history[0]["version"] == 1
        assert history[1]["version"] == 2

    def test_registry_check_compatibility(self, sample_schema, modified_schema):
        """Test checking compatibility without registration."""
        from api.app.services.schema_registry.registry import SchemaRegistry
        from api.app.services.schema_registry.models import SchemaVersion, CompatibilityMode

        existing_version = SchemaVersion(
            version=1,
            schema=sample_schema,
            fingerprint=sample_schema.compute_fingerprint(),
            created_at=datetime.utcnow(),
            compatibility_mode=CompatibilityMode.BACKWARD
        )

        registry = SchemaRegistry(None)
        registry._cache["test_source:False"] = [existing_version]

        result = registry.check_compatibility("test_source", modified_schema)

        assert result.is_compatible is True

    def test_registry_compare_schemas(self, sample_schema, modified_schema):
        """Test comparing two schema versions."""
        from api.app.services.schema_registry.registry import SchemaRegistry
        from api.app.services.schema_registry.models import SchemaVersion

        versions = [
            SchemaVersion(version=1, schema=sample_schema,
                         fingerprint="fp1", created_at=datetime.utcnow()),
            SchemaVersion(version=2, schema=modified_schema,
                         fingerprint="fp2", created_at=datetime.utcnow())
        ]

        registry = SchemaRegistry(None)
        registry._cache["test_source:False"] = versions

        comparison = registry.compare_schemas("test_source", 1, 2)

        assert comparison["version1"]["version"] == 1
        assert comparison["version2"]["version"] == 2
        assert "added_fields" in comparison["changes"]

    def test_registry_export_schema(self, sample_schema):
        """Test exporting schema."""
        from api.app.services.schema_registry.registry import SchemaRegistry
        from api.app.services.schema_registry.models import SchemaVersion

        version = SchemaVersion(
            version=1,
            schema=sample_schema,
            fingerprint=sample_schema.compute_fingerprint(),
            created_at=datetime.utcnow(),
            created_by="test"
        )

        registry = SchemaRegistry(None)
        registry._cache["test_source:False"] = [version]

        exported = registry.export_schema("test_source")

        assert exported is not None
        assert exported["source_id"] == "test_source"
        assert exported["version"] == 1
        assert "schema" in exported
        assert "metadata" in exported

    def test_registry_health_check_memory_mode(self):
        """Test health check in memory mode."""
        from api.app.services.schema_registry.registry import SchemaRegistry

        registry = SchemaRegistry(None)
        result = registry.health_check()

        assert result["status"] == "memory_mode"

    def test_registry_invalidate_cache(self, sample_schema):
        """Test cache invalidation."""
        from api.app.services.schema_registry.registry import SchemaRegistry

        registry = SchemaRegistry(None)
        registry._cache["test_source:False"] = []
        registry._cache["test_source:True"] = []
        registry._cache["other_source:False"] = []

        registry.invalidate_cache("test_source")

        assert "test_source:False" not in registry._cache
        assert "test_source:True" not in registry._cache
        assert "other_source:False" in registry._cache

    def test_registry_invalidate_all_cache(self, sample_schema):
        """Test invalidating all cache."""
        from api.app.services.schema_registry.registry import SchemaRegistry

        registry = SchemaRegistry(None)
        registry._cache["source1:False"] = []
        registry._cache["source2:False"] = []

        registry.invalidate_cache()  # Clear all

        assert len(registry._cache) == 0


class TestDefaultSchemas:
    """Tests for default schema templates."""

    def test_get_default_schema_news_article(self):
        """Test getting default news article schema."""
        from api.app.services.schema_registry.models import get_default_schema, DataCategory

        schema = get_default_schema(DataCategory.NEWS_ARTICLE)

        assert schema is not None
        assert schema.get_field("title") is not None
        assert schema.get_field("content") is not None
        assert schema.data_category == DataCategory.NEWS_ARTICLE

    def test_get_default_schema_stock_price(self):
        """Test getting default stock price schema."""
        from api.app.services.schema_registry.models import get_default_schema, DataCategory

        schema = get_default_schema(DataCategory.STOCK_PRICE)

        assert schema is not None
        assert schema.get_field("stock_code") is not None
        assert schema.get_field("price") is not None
        assert schema.data_category == DataCategory.STOCK_PRICE

    def test_get_default_schema_exchange_rate(self):
        """Test getting default exchange rate schema."""
        from api.app.services.schema_registry.models import get_default_schema, DataCategory

        schema = get_default_schema(DataCategory.EXCHANGE_RATE)

        assert schema is not None
        assert schema.get_field("currency_code") is not None
        assert schema.get_field("base_rate") is not None

    def test_get_default_schema_generic(self):
        """Test getting default generic schema."""
        from api.app.services.schema_registry.models import get_default_schema, DataCategory

        schema = get_default_schema(DataCategory.GENERIC)

        assert schema is not None
        assert schema.data_category == DataCategory.GENERIC

    def test_default_schemas_are_cloned(self):
        """Test that default schemas are cloned (not same instance)."""
        from api.app.services.schema_registry.models import get_default_schema, DataCategory

        schema1 = get_default_schema(DataCategory.NEWS_ARTICLE)
        schema2 = get_default_schema(DataCategory.NEWS_ARTICLE)

        assert schema1 is not schema2
        assert schema1.compute_fingerprint() == schema2.compute_fingerprint()

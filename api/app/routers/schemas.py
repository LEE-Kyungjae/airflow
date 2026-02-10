"""
Schema Registry Router.

Endpoints for managing data schemas, validation, and automatic detection.
"""

import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel, Field

from app.core import get_logger
from app.services.mongo_service import MongoService
from app.services.schema_registry import (
    SchemaRegistry,
    SchemaValidator,
    SchemaDetector,
    Schema,
    FieldSchema,
    FieldType,
    CompatibilityMode,
    DataCategory,
)
from app.auth.dependencies import require_auth, require_scope, AuthContext

logger = get_logger(__name__)
router = APIRouter()


# ============================================================
# Pydantic Models
# ============================================================

class FieldSchemaModel(BaseModel):
    """Field schema definition"""
    name: str
    field_type: str
    required: bool = False
    nullable: bool = True
    default: Any = None
    description: str = ""

    class Config:
        use_enum_values = True


class SchemaModel(BaseModel):
    """Schema definition"""
    fields: List[FieldSchemaModel]
    description: str = ""
    category: Optional[str] = None
    compatibility_mode: str = "backward"

    class Config:
        use_enum_values = True


class RegisterSchemaRequest(BaseModel):
    """Request to register a schema"""
    schema: SchemaModel = Field(..., alias="schema")
    change_description: str = ""
    tags: List[str] = Field(default_factory=list)

    class Config:
        populate_by_name = True


class ValidateDataRequest(BaseModel):
    """Request to validate data"""
    data: Dict[str, Any]


class ValidateBatchRequest(BaseModel):
    """Request to validate multiple records"""
    data: List[Dict[str, Any]]


class DetectSchemaRequest(BaseModel):
    """Request to detect schema from sample data"""
    sample_data: List[Dict[str, Any]]
    category: Optional[str] = None


class ValidationResponse(BaseModel):
    """Validation result response"""
    is_valid: bool
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    error_count: int = 0
    warning_count: int = 0


class SchemaResponse(BaseModel):
    """Schema response"""
    source_id: str
    version: int
    schema: Dict[str, Any] = Field(..., alias="schema")
    fingerprint: str
    created_at: str
    created_by: str
    is_active: bool
    compatibility_mode: str

    class Config:
        populate_by_name = True


# ============================================================
# Dependencies
# ============================================================

def get_mongo():
    """Dependency for MongoDB connection with automatic cleanup."""
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


# ============================================================
# Endpoints
# ============================================================

@router.get(
    "/",
    summary="List all registered schemas",
    description="Get all registered schemas across all sources",
    response_model=List[SchemaResponse]
)
async def list_schemas(
    auth: AuthContext = Depends(require_auth),
    mongo: MongoService = Depends(get_mongo)
):
    """List all registered schemas"""
    try:
        registry = SchemaRegistry(mongo_service=mongo)
        schemas = registry.get_all_schemas()

        # Convert to response format
        response = []
        for schema_version in schemas:
            response.append({
                "source_id": schema_version.source_id,
                "version": schema_version.version,
                "schema": schema_version.schema.to_dict() if hasattr(schema_version.schema, 'to_dict') else {},
                "fingerprint": schema_version.fingerprint,
                "created_at": schema_version.created_at.isoformat() if schema_version.created_at else "",
                "created_by": schema_version.created_by,
                "is_active": schema_version.is_active,
                "compatibility_mode": schema_version.compatibility_mode.value if hasattr(schema_version.compatibility_mode, 'value') else str(schema_version.compatibility_mode)
            })

        logger.info(f"Listed {len(response)} schemas")
        return response

    except Exception as e:
        logger.error(f"Failed to list schemas: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list schemas: {str(e)}"
        )


@router.post(
    "/{source_id}",
    summary="Register or update schema",
    description="Register a new schema version for a source",
    status_code=status.HTTP_201_CREATED
)
async def register_schema(
    source_id: str,
    request: RegisterSchemaRequest,
    auth: AuthContext = Depends(require_scope("write")),
    mongo: MongoService = Depends(get_mongo)
):
    """Register or update a schema for a source"""
    try:
        # Convert request to Schema object
        field_schemas = []
        for field_data in request.schema.fields:
            field_schemas.append(FieldSchema(
                name=field_data.name,
                field_type=FieldType(field_data.field_type),
                required=field_data.required,
                nullable=field_data.nullable,
                default=field_data.default,
                description=field_data.description
            ))

        schema = Schema(
            fields=field_schemas,
            description=request.schema.description,
            category=DataCategory(request.schema.category) if request.schema.category else None
        )

        registry = SchemaRegistry(mongo_service=mongo)
        schema_version, compat_result = registry.register_schema(
            source_id=source_id,
            schema=schema,
            created_by=auth.user_id or "system",
            change_description=request.change_description,
            compatibility_mode=CompatibilityMode(request.schema.compatibility_mode),
            tags=request.tags
        )

        logger.info(f"Registered schema v{schema_version.version} for {source_id}")

        return {
            "source_id": source_id,
            "version": schema_version.version,
            "fingerprint": schema_version.fingerprint,
            "compatibility": {
                "is_compatible": compat_result.is_compatible,
                "issues": [
                    {
                        "severity": issue.severity,
                        "field": issue.field_name,
                        "message": issue.message
                    }
                    for issue in compat_result.issues
                ] if compat_result.issues else []
            },
            "message": f"Schema v{schema_version.version} registered successfully"
        }

    except ValueError as e:
        logger.warning(f"Invalid schema data: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to register schema: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to register schema: {str(e)}"
        )


@router.get(
    "/{source_id}",
    summary="Get current schema",
    description="Get the current active schema for a source",
    response_model=SchemaResponse
)
async def get_schema(
    source_id: str,
    auth: AuthContext = Depends(require_auth),
    mongo: MongoService = Depends(get_mongo)
):
    """Get current schema for a source"""
    try:
        registry = SchemaRegistry(mongo_service=mongo)
        schema_version = registry.get_schema(source_id)

        if not schema_version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No schema found for source: {source_id}"
            )

        return {
            "source_id": schema_version.source_id,
            "version": schema_version.version,
            "schema": schema_version.schema.to_dict() if hasattr(schema_version.schema, 'to_dict') else {},
            "fingerprint": schema_version.fingerprint,
            "created_at": schema_version.created_at.isoformat() if schema_version.created_at else "",
            "created_by": schema_version.created_by,
            "is_active": schema_version.is_active,
            "compatibility_mode": schema_version.compatibility_mode.value if hasattr(schema_version.compatibility_mode, 'value') else str(schema_version.compatibility_mode)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get schema: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get schema: {str(e)}"
        )


@router.get(
    "/{source_id}/history",
    summary="Get schema history",
    description="Get version history for a source schema",
    response_model=List[SchemaResponse]
)
async def get_schema_history(
    source_id: str,
    auth: AuthContext = Depends(require_auth),
    mongo: MongoService = Depends(get_mongo)
):
    """Get schema version history for a source"""
    try:
        registry = SchemaRegistry(mongo_service=mongo)
        history = registry.get_schema_history(source_id)

        if not history:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No schema history found for source: {source_id}"
            )

        response = []
        for schema_version in history:
            response.append({
                "source_id": schema_version.source_id,
                "version": schema_version.version,
                "schema": schema_version.schema.to_dict() if hasattr(schema_version.schema, 'to_dict') else {},
                "fingerprint": schema_version.fingerprint,
                "created_at": schema_version.created_at.isoformat() if schema_version.created_at else "",
                "created_by": schema_version.created_by,
                "is_active": schema_version.is_active,
                "compatibility_mode": schema_version.compatibility_mode.value if hasattr(schema_version.compatibility_mode, 'value') else str(schema_version.compatibility_mode)
            })

        logger.info(f"Retrieved {len(response)} schema versions for {source_id}")
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get schema history: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get schema history: {str(e)}"
        )


@router.post(
    "/{source_id}/validate",
    summary="Validate data",
    description="Validate data against the source schema",
    response_model=ValidationResponse
)
async def validate_data(
    source_id: str,
    request: ValidateDataRequest,
    auth: AuthContext = Depends(require_scope("write")),
    mongo: MongoService = Depends(get_mongo)
):
    """Validate data against source schema"""
    try:
        # Get schema
        registry = SchemaRegistry(mongo_service=mongo)
        schema_version = registry.get_schema(source_id)

        if not schema_version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No schema found for source: {source_id}"
            )

        # Validate
        validator = SchemaValidator()
        result = validator.validate(request.data, schema_version.schema)

        return {
            "is_valid": result.is_valid,
            "errors": [e.to_dict() for e in result.errors] if result.errors else [],
            "warnings": result.warnings,
            "error_count": len(result.errors),
            "warning_count": len(result.warnings)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Validation failed: {str(e)}"
        )


@router.post(
    "/{source_id}/detect",
    summary="Auto-detect schema",
    description="Automatically detect schema from sample data",
    status_code=status.HTTP_201_CREATED
)
async def detect_schema(
    source_id: str,
    request: DetectSchemaRequest,
    auth: AuthContext = Depends(require_scope("write")),
    mongo: MongoService = Depends(get_mongo)
):
    """Auto-detect schema from sample data"""
    try:
        if not request.sample_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Sample data is required"
            )

        # Detect schema
        detector = SchemaDetector()
        detected_schema = detector.detect_from_data(request.sample_data)

        # Set category if provided
        if request.category:
            detected_schema.category = DataCategory(request.category)

        # Register detected schema
        registry = SchemaRegistry(mongo_service=mongo)
        schema_version, compat_result = registry.register_schema(
            source_id=source_id,
            schema=detected_schema,
            created_by=auth.user_id or "system",
            change_description="Auto-detected from sample data",
            compatibility_mode=CompatibilityMode.BACKWARD
        )

        logger.info(f"Auto-detected and registered schema v{schema_version.version} for {source_id}")

        return {
            "source_id": source_id,
            "version": schema_version.version,
            "schema": detected_schema.to_dict() if hasattr(detected_schema, 'to_dict') else {},
            "fingerprint": schema_version.fingerprint,
            "sample_count": len(request.sample_data),
            "field_count": len(detected_schema.fields),
            "message": f"Schema auto-detected and registered as v{schema_version.version}"
        }

    except ValueError as e:
        logger.warning(f"Invalid request: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Schema detection failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema detection failed: {str(e)}"
        )

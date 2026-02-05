"""
Schema Registry Models - 스키마 데이터 모델 정의

MongoDB 컬렉션별 스키마 정의 및 버전 관리를 위한 핵심 모델들
"""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CompatibilityMode(str, Enum):
    """
    스키마 호환성 모드

    - NONE: 호환성 검사 없음
    - BACKWARD: 새 스키마가 이전 데이터를 읽을 수 있음
    - FORWARD: 이전 스키마가 새 데이터를 읽을 수 있음
    - FULL: 양방향 호환
    - *_TRANSITIVE: 모든 버전에 대해 호환성 보장
    """
    NONE = "none"
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"
    BACKWARD_TRANSITIVE = "backward_transitive"
    FORWARD_TRANSITIVE = "forward_transitive"
    FULL_TRANSITIVE = "full_transitive"


class FieldType(str, Enum):
    """필드 데이터 타입"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    ARRAY = "array"
    OBJECT = "object"
    ANY = "any"

    @classmethod
    def from_python_type(cls, value: Any) -> "FieldType":
        """Python 타입에서 FieldType 추론"""
        if value is None:
            return cls.ANY
        if isinstance(value, bool):
            return cls.BOOLEAN
        if isinstance(value, int):
            return cls.INTEGER
        if isinstance(value, float):
            return cls.FLOAT
        if isinstance(value, str):
            return cls.STRING
        if isinstance(value, list):
            return cls.ARRAY
        if isinstance(value, dict):
            return cls.OBJECT
        if isinstance(value, datetime):
            return cls.DATETIME
        return cls.ANY


class DataCategory(str, Enum):
    """
    데이터 카테고리 (ETL Pipeline과 동기화)

    각 카테고리별로 기본 스키마가 정의됨
    """
    NEWS_ARTICLE = "news_article"
    FINANCIAL_DATA = "financial_data"
    MARKET_INDEX = "market_index"
    EXCHANGE_RATE = "exchange_rate"
    STOCK_PRICE = "stock_price"
    ANNOUNCEMENT = "announcement"
    TABLE_DATA = "table_data"
    GENERIC = "generic"


@dataclass
class FieldSchema:
    """
    필드 스키마 정의

    MongoDB 문서의 개별 필드에 대한 스키마 정보
    """
    name: str
    field_type: FieldType
    required: bool = False
    nullable: bool = True
    default: Any = None
    description: str = ""
    # 검증 제약 조건
    pattern: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    enum_values: Optional[List[Any]] = None
    # 중첩 스키마 (OBJECT/ARRAY 타입용)
    nested_schema: Optional["Schema"] = None
    # 메타데이터
    examples: List[Any] = field(default_factory=list)
    deprecated: bool = False
    deprecated_message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        result = {
            "name": self.name,
            "type": self.field_type.value,
            "required": self.required,
            "nullable": self.nullable,
        }
        if self.default is not None:
            result["default"] = self.default
        if self.description:
            result["description"] = self.description
        if self.pattern:
            result["pattern"] = self.pattern
        if self.min_value is not None:
            result["min_value"] = self.min_value
        if self.max_value is not None:
            result["max_value"] = self.max_value
        if self.min_length is not None:
            result["min_length"] = self.min_length
        if self.max_length is not None:
            result["max_length"] = self.max_length
        if self.enum_values:
            result["enum"] = self.enum_values
        if self.nested_schema:
            result["nested_schema"] = self.nested_schema.to_dict()
        if self.examples:
            result["examples"] = self.examples[:5]
        if self.deprecated:
            result["deprecated"] = True
            result["deprecated_message"] = self.deprecated_message
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FieldSchema":
        """딕셔너리에서 생성"""
        nested = None
        if "nested_schema" in data:
            nested = Schema.from_dict(data["nested_schema"])

        return cls(
            name=data["name"],
            field_type=FieldType(data.get("type", "string")),
            required=data.get("required", False),
            nullable=data.get("nullable", True),
            default=data.get("default"),
            description=data.get("description", ""),
            pattern=data.get("pattern"),
            min_value=data.get("min_value"),
            max_value=data.get("max_value"),
            min_length=data.get("min_length"),
            max_length=data.get("max_length"),
            enum_values=data.get("enum"),
            nested_schema=nested,
            examples=data.get("examples", []),
            deprecated=data.get("deprecated", False),
            deprecated_message=data.get("deprecated_message", ""),
        )


@dataclass
class Schema:
    """
    스키마 정의

    MongoDB 컬렉션 또는 소스의 전체 스키마
    """
    fields: List[FieldSchema] = field(default_factory=list)
    description: str = ""
    data_category: Optional[DataCategory] = None
    collection_name: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_field(self, name: str) -> Optional[FieldSchema]:
        """필드 이름으로 필드 스키마 조회"""
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def get_field_names(self) -> Set[str]:
        """모든 필드 이름 반환"""
        return {f.name for f in self.fields}

    def get_required_fields(self) -> Set[str]:
        """필수 필드 이름들 반환"""
        return {f.name for f in self.fields if f.required}

    def get_optional_fields(self) -> Set[str]:
        """선택 필드 이름들 반환"""
        return {f.name for f in self.fields if not f.required}

    def add_field(self, field_schema: FieldSchema) -> None:
        """필드 추가"""
        existing = self.get_field(field_schema.name)
        if existing:
            raise ValueError(f"Field '{field_schema.name}' already exists")
        self.fields.append(field_schema)

    def remove_field(self, name: str) -> bool:
        """필드 제거"""
        for i, f in enumerate(self.fields):
            if f.name == name:
                self.fields.pop(i)
                return True
        return False

    def compute_fingerprint(self) -> str:
        """
        스키마 지문(fingerprint) 계산

        스키마의 고유 식별자로, 스키마 변경 감지에 사용
        """
        schema_dict = self.to_dict()
        schema_dict.pop("metadata", None)
        schema_str = json.dumps(schema_dict, sort_keys=True, default=str)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        result = {
            "fields": [f.to_dict() for f in self.fields],
            "description": self.description,
            "metadata": self.metadata,
        }
        if self.data_category:
            result["data_category"] = self.data_category.value
        if self.collection_name:
            result["collection_name"] = self.collection_name
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Schema":
        """딕셔너리에서 생성"""
        category = None
        if "data_category" in data:
            category = DataCategory(data["data_category"])

        return cls(
            fields=[FieldSchema.from_dict(f) for f in data.get("fields", [])],
            description=data.get("description", ""),
            data_category=category,
            collection_name=data.get("collection_name"),
            metadata=data.get("metadata", {}),
        )

    def clone(self) -> "Schema":
        """스키마 복제"""
        return Schema.from_dict(self.to_dict())


@dataclass
class SchemaVersion:
    """
    스키마 버전

    특정 시점의 스키마 스냅샷
    """
    version: int
    schema: Schema
    fingerprint: str
    created_at: datetime
    created_by: str = "system"
    change_description: str = ""
    is_active: bool = True
    # 추가 메타데이터
    compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            "version": self.version,
            "schema": self.schema.to_dict(),
            "fingerprint": self.fingerprint,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "created_by": self.created_by,
            "change_description": self.change_description,
            "is_active": self.is_active,
            "compatibility_mode": self.compatibility_mode.value,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchemaVersion":
        """딕셔너리에서 생성"""
        created_at = data["created_at"]
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)

        compat_mode = CompatibilityMode.BACKWARD
        if "compatibility_mode" in data:
            compat_mode = CompatibilityMode(data["compatibility_mode"])

        return cls(
            version=data["version"],
            schema=Schema.from_dict(data["schema"]),
            fingerprint=data["fingerprint"],
            created_at=created_at,
            created_by=data.get("created_by", "system"),
            change_description=data.get("change_description", ""),
            is_active=data.get("is_active", True),
            compatibility_mode=compat_mode,
            tags=data.get("tags", []),
        )

    def __repr__(self) -> str:
        return f"SchemaVersion(v{self.version}, fingerprint={self.fingerprint[:8]}..., active={self.is_active})"


@dataclass
class CompatibilityIssue:
    """
    호환성 검사 결과의 개별 이슈
    """
    field_name: str
    issue_type: str
    severity: str  # "error", "warning", "info"
    message: str
    old_value: Any = None
    new_value: Any = None

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        result = {
            "field_name": self.field_name,
            "issue_type": self.issue_type,
            "severity": self.severity,
            "message": self.message,
        }
        if self.old_value is not None:
            result["old_value"] = self.old_value
        if self.new_value is not None:
            result["new_value"] = self.new_value
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompatibilityIssue":
        """딕셔너리에서 생성"""
        return cls(
            field_name=data["field_name"],
            issue_type=data["issue_type"],
            severity=data["severity"],
            message=data["message"],
            old_value=data.get("old_value"),
            new_value=data.get("new_value"),
        )

    def is_error(self) -> bool:
        return self.severity == "error"

    def is_warning(self) -> bool:
        return self.severity == "warning"


@dataclass
class CompatibilityResult:
    """
    호환성 검사 전체 결과
    """
    is_compatible: bool
    issues: List[CompatibilityIssue] = field(default_factory=list)
    checked_at: datetime = field(default_factory=datetime.utcnow)
    mode: CompatibilityMode = CompatibilityMode.BACKWARD

    @property
    def errors(self) -> List[CompatibilityIssue]:
        """에러 레벨 이슈만 반환"""
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> List[CompatibilityIssue]:
        """경고 레벨 이슈만 반환"""
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def info_items(self) -> List[CompatibilityIssue]:
        """정보 레벨 이슈만 반환"""
        return [i for i in self.issues if i.severity == "info"]

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            "is_compatible": self.is_compatible,
            "issues": [i.to_dict() for i in self.issues],
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "info_count": len(self.info_items),
            "checked_at": self.checked_at.isoformat(),
            "mode": self.mode.value,
        }


# 카테고리별 기본 스키마 정의
DEFAULT_SCHEMAS: Dict[DataCategory, Schema] = {
    DataCategory.NEWS_ARTICLE: Schema(
        fields=[
            FieldSchema(name="title", field_type=FieldType.STRING, required=True, description="기사 제목"),
            FieldSchema(name="content", field_type=FieldType.STRING, required=False, description="기사 본문"),
            FieldSchema(name="summary", field_type=FieldType.STRING, required=False, description="기사 요약"),
            FieldSchema(name="url", field_type=FieldType.STRING, required=False, pattern=r"^https?://"),
            FieldSchema(name="published_at", field_type=FieldType.DATETIME, required=False, description="발행일시"),
            FieldSchema(name="source", field_type=FieldType.STRING, required=False, description="출처"),
            FieldSchema(name="author", field_type=FieldType.STRING, required=False, description="저자"),
            FieldSchema(name="category", field_type=FieldType.STRING, required=False, description="카테고리"),
            FieldSchema(name="tags", field_type=FieldType.ARRAY, required=False, description="태그"),
            FieldSchema(name="content_hash", field_type=FieldType.STRING, required=False, description="콘텐츠 해시"),
        ],
        description="뉴스 기사 스키마",
        data_category=DataCategory.NEWS_ARTICLE,
        collection_name="news_articles",
    ),

    DataCategory.FINANCIAL_DATA: Schema(
        fields=[
            FieldSchema(name="name", field_type=FieldType.STRING, required=True, description="종목/항목명"),
            FieldSchema(name="code", field_type=FieldType.STRING, required=False, description="종목 코드"),
            FieldSchema(name="price", field_type=FieldType.FLOAT, required=False, description="가격"),
            FieldSchema(name="change", field_type=FieldType.FLOAT, required=False, description="변동값"),
            FieldSchema(name="change_rate", field_type=FieldType.FLOAT, required=False, description="변동률(%)"),
            FieldSchema(name="volume", field_type=FieldType.INTEGER, required=False, description="거래량"),
            FieldSchema(name="trade_date", field_type=FieldType.DATE, required=False, description="거래일"),
        ],
        description="금융 데이터 스키마",
        data_category=DataCategory.FINANCIAL_DATA,
        collection_name="financial_data",
    ),

    DataCategory.STOCK_PRICE: Schema(
        fields=[
            FieldSchema(name="stock_code", field_type=FieldType.STRING, required=True, description="종목 코드"),
            FieldSchema(name="name", field_type=FieldType.STRING, required=False, description="종목명"),
            FieldSchema(name="price", field_type=FieldType.FLOAT, required=True, description="현재가"),
            FieldSchema(name="open", field_type=FieldType.FLOAT, required=False, description="시가"),
            FieldSchema(name="high", field_type=FieldType.FLOAT, required=False, description="고가"),
            FieldSchema(name="low", field_type=FieldType.FLOAT, required=False, description="저가"),
            FieldSchema(name="close", field_type=FieldType.FLOAT, required=False, description="종가"),
            FieldSchema(name="volume", field_type=FieldType.INTEGER, required=False, description="거래량"),
            FieldSchema(name="change", field_type=FieldType.FLOAT, required=False, description="변동값"),
            FieldSchema(name="change_rate", field_type=FieldType.FLOAT, required=False, description="변동률(%)"),
            FieldSchema(name="market_cap", field_type=FieldType.FLOAT, required=False, description="시가총액"),
            FieldSchema(name="per", field_type=FieldType.FLOAT, required=False, description="PER"),
            FieldSchema(name="pbr", field_type=FieldType.FLOAT, required=False, description="PBR"),
            FieldSchema(name="trade_date", field_type=FieldType.DATE, required=False, description="거래일"),
        ],
        description="주식 시세 스키마",
        data_category=DataCategory.STOCK_PRICE,
        collection_name="stock_prices",
    ),

    DataCategory.EXCHANGE_RATE: Schema(
        fields=[
            FieldSchema(name="currency_code", field_type=FieldType.STRING, required=True, description="통화 코드", max_length=3),
            FieldSchema(name="currency_name", field_type=FieldType.STRING, required=False, description="통화명"),
            FieldSchema(name="base_rate", field_type=FieldType.FLOAT, required=False, description="매매기준율"),
            FieldSchema(name="buy_rate", field_type=FieldType.FLOAT, required=False, description="살 때"),
            FieldSchema(name="sell_rate", field_type=FieldType.FLOAT, required=False, description="팔 때"),
            FieldSchema(name="send_rate", field_type=FieldType.FLOAT, required=False, description="송금 보낼 때"),
            FieldSchema(name="receive_rate", field_type=FieldType.FLOAT, required=False, description="송금 받을 때"),
            FieldSchema(name="change", field_type=FieldType.FLOAT, required=False, description="변동값"),
            FieldSchema(name="change_rate", field_type=FieldType.FLOAT, required=False, description="변동률(%)"),
            FieldSchema(name="trade_date", field_type=FieldType.DATE, required=False, description="거래일"),
        ],
        description="환율 스키마",
        data_category=DataCategory.EXCHANGE_RATE,
        collection_name="exchange_rates",
    ),

    DataCategory.MARKET_INDEX: Schema(
        fields=[
            FieldSchema(name="index_code", field_type=FieldType.STRING, required=True, description="지수 코드"),
            FieldSchema(name="name", field_type=FieldType.STRING, required=False, description="지수명"),
            FieldSchema(name="value", field_type=FieldType.FLOAT, required=False, description="지수값"),
            FieldSchema(name="change", field_type=FieldType.FLOAT, required=False, description="변동값"),
            FieldSchema(name="change_rate", field_type=FieldType.FLOAT, required=False, description="변동률(%)"),
            FieldSchema(name="open", field_type=FieldType.FLOAT, required=False, description="시가"),
            FieldSchema(name="high", field_type=FieldType.FLOAT, required=False, description="고가"),
            FieldSchema(name="low", field_type=FieldType.FLOAT, required=False, description="저가"),
            FieldSchema(name="volume", field_type=FieldType.INTEGER, required=False, description="거래량"),
            FieldSchema(name="trade_date", field_type=FieldType.DATE, required=False, description="거래일"),
        ],
        description="시장 지수 스키마",
        data_category=DataCategory.MARKET_INDEX,
        collection_name="market_indices",
    ),

    DataCategory.ANNOUNCEMENT: Schema(
        fields=[
            FieldSchema(name="title", field_type=FieldType.STRING, required=True, description="공시 제목"),
            FieldSchema(name="content", field_type=FieldType.STRING, required=False, description="공시 내용"),
            FieldSchema(name="company_name", field_type=FieldType.STRING, required=False, description="회사명"),
            FieldSchema(name="stock_code", field_type=FieldType.STRING, required=False, description="종목 코드"),
            FieldSchema(name="announcement_type", field_type=FieldType.STRING, required=False, description="공시 유형"),
            FieldSchema(name="published_at", field_type=FieldType.DATETIME, required=False, description="공시일시"),
            FieldSchema(name="url", field_type=FieldType.STRING, required=False, description="공시 URL"),
            FieldSchema(name="content_hash", field_type=FieldType.STRING, required=False, description="콘텐츠 해시"),
        ],
        description="공시 스키마",
        data_category=DataCategory.ANNOUNCEMENT,
        collection_name="announcements",
    ),

    DataCategory.TABLE_DATA: Schema(
        fields=[
            FieldSchema(name="table_name", field_type=FieldType.STRING, required=False, description="테이블명"),
            FieldSchema(name="headers", field_type=FieldType.ARRAY, required=False, description="헤더 목록"),
            FieldSchema(name="rows", field_type=FieldType.ARRAY, required=False, description="데이터 행"),
            FieldSchema(name="extracted_at", field_type=FieldType.DATETIME, required=False, description="추출 시각"),
        ],
        description="테이블 데이터 스키마",
        data_category=DataCategory.TABLE_DATA,
        collection_name="table_data",
    ),

    DataCategory.GENERIC: Schema(
        fields=[
            FieldSchema(name="data", field_type=FieldType.OBJECT, required=False, description="일반 데이터"),
        ],
        description="일반 스키마",
        data_category=DataCategory.GENERIC,
        collection_name="generic_data",
    ),
}


def get_default_schema(category: DataCategory) -> Schema:
    """카테고리별 기본 스키마 반환"""
    return DEFAULT_SCHEMAS.get(category, DEFAULT_SCHEMAS[DataCategory.GENERIC]).clone()

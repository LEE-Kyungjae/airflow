"""
Data Catalog Models - Dataset, Column, Tag 데이터 모델 정의

데이터 카탈로그 시스템의 핵심 데이터 구조를 정의합니다.

주요 기능:
- 데이터셋 메타데이터 관리
- 컬럼 레벨 문서화
- 태그 기반 분류
- 품질 메트릭 연동
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json


class DatasetType(str, Enum):
    """데이터셋 유형"""
    SOURCE = "source"               # 원본 소스 (크롤링 대상)
    STAGING = "staging"             # 스테이징 테이블
    TRANSFORMED = "transformed"     # 변환된 데이터
    AGGREGATED = "aggregated"       # 집계 데이터
    FINAL = "final"                 # 최종 분석용


class DatasetStatus(str, Enum):
    """데이터셋 상태"""
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"
    DRAFT = "draft"


class ColumnType(str, Enum):
    """컬럼 데이터 타입"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    ARRAY = "array"
    OBJECT = "object"
    BINARY = "binary"
    UNKNOWN = "unknown"


class SensitivityLevel(str, Enum):
    """데이터 민감도 수준"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class TagCategory(str, Enum):
    """태그 카테고리"""
    DOMAIN = "domain"           # 비즈니스 도메인 (금융, 뉴스 등)
    TECHNICAL = "technical"     # 기술적 분류 (HTML, PDF 등)
    QUALITY = "quality"         # 품질 관련 (검증됨, 미검증 등)
    USAGE = "usage"             # 용도 (분석용, 보고서용 등)
    CUSTOM = "custom"           # 사용자 정의


@dataclass
class Tag:
    """태그 정의"""
    name: str
    category: TagCategory
    description: str = ""
    color: str = "#808080"      # 표시 색상
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = "system"
    usage_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category.value,
            "description": self.description,
            "color": self.color,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "created_by": self.created_by,
            "usage_count": self.usage_count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Tag":
        return cls(
            name=data["name"],
            category=TagCategory(data.get("category", "custom")),
            description=data.get("description", ""),
            color=data.get("color", "#808080"),
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data.get("created_at"), str) else data.get("created_at", datetime.utcnow()),
            created_by=data.get("created_by", "system"),
            usage_count=data.get("usage_count", 0),
        )


@dataclass
class QualityMetrics:
    """데이터 품질 메트릭"""
    completeness: float = 0.0       # 완전성 (0-100)
    accuracy: float = 0.0           # 정확도 (0-100)
    consistency: float = 0.0        # 일관성 (0-100)
    timeliness: float = 0.0         # 적시성 (0-100)
    uniqueness: float = 0.0         # 고유성 (0-100)
    validity: float = 0.0           # 유효성 (0-100)
    overall_score: float = 0.0      # 종합 점수 (0-100)
    last_assessed_at: Optional[datetime] = None
    assessed_records: int = 0
    failed_checks: int = 0

    def calculate_overall(self) -> float:
        """종합 점수 계산"""
        weights = {
            "completeness": 0.20,
            "accuracy": 0.25,
            "consistency": 0.15,
            "timeliness": 0.10,
            "uniqueness": 0.15,
            "validity": 0.15,
        }
        self.overall_score = round(
            self.completeness * weights["completeness"] +
            self.accuracy * weights["accuracy"] +
            self.consistency * weights["consistency"] +
            self.timeliness * weights["timeliness"] +
            self.uniqueness * weights["uniqueness"] +
            self.validity * weights["validity"],
            2
        )
        return self.overall_score

    def to_dict(self) -> Dict[str, Any]:
        return {
            "completeness": self.completeness,
            "accuracy": self.accuracy,
            "consistency": self.consistency,
            "timeliness": self.timeliness,
            "uniqueness": self.uniqueness,
            "validity": self.validity,
            "overall_score": self.overall_score,
            "last_assessed_at": self.last_assessed_at.isoformat() if self.last_assessed_at else None,
            "assessed_records": self.assessed_records,
            "failed_checks": self.failed_checks,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QualityMetrics":
        last_assessed = data.get("last_assessed_at")
        if isinstance(last_assessed, str):
            last_assessed = datetime.fromisoformat(last_assessed)

        return cls(
            completeness=data.get("completeness", 0.0),
            accuracy=data.get("accuracy", 0.0),
            consistency=data.get("consistency", 0.0),
            timeliness=data.get("timeliness", 0.0),
            uniqueness=data.get("uniqueness", 0.0),
            validity=data.get("validity", 0.0),
            overall_score=data.get("overall_score", 0.0),
            last_assessed_at=last_assessed,
            assessed_records=data.get("assessed_records", 0),
            failed_checks=data.get("failed_checks", 0),
        )


@dataclass
class ColumnStatistics:
    """컬럼 통계 정보"""
    null_count: int = 0
    null_percentage: float = 0.0
    unique_count: int = 0
    unique_percentage: float = 0.0
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_deviation: Optional[float] = None
    most_common: List[Dict[str, Any]] = field(default_factory=list)  # [{value, count}]
    sample_values: List[Any] = field(default_factory=list)
    last_computed_at: Optional[datetime] = None
    total_records: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "null_count": self.null_count,
            "null_percentage": self.null_percentage,
            "unique_count": self.unique_count,
            "unique_percentage": self.unique_percentage,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "mean_value": self.mean_value,
            "median_value": self.median_value,
            "std_deviation": self.std_deviation,
            "most_common": self.most_common[:10],  # Top 10
            "sample_values": self.sample_values[:5],  # 5 samples
            "last_computed_at": self.last_computed_at.isoformat() if self.last_computed_at else None,
            "total_records": self.total_records,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnStatistics":
        last_computed = data.get("last_computed_at")
        if isinstance(last_computed, str):
            last_computed = datetime.fromisoformat(last_computed)

        return cls(
            null_count=data.get("null_count", 0),
            null_percentage=data.get("null_percentage", 0.0),
            unique_count=data.get("unique_count", 0),
            unique_percentage=data.get("unique_percentage", 0.0),
            min_value=data.get("min_value"),
            max_value=data.get("max_value"),
            mean_value=data.get("mean_value"),
            median_value=data.get("median_value"),
            std_deviation=data.get("std_deviation"),
            most_common=data.get("most_common", []),
            sample_values=data.get("sample_values", []),
            last_computed_at=last_computed,
            total_records=data.get("total_records", 0),
        )


@dataclass
class Column:
    """컬럼 정의"""
    name: str
    data_type: ColumnType
    description: str = ""
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_ref: Optional[str] = None  # dataset_id.column_name
    default_value: Optional[Any] = None
    sensitivity: SensitivityLevel = SensitivityLevel.INTERNAL
    business_name: str = ""         # 비즈니스 용어
    business_definition: str = ""   # 비즈니스 정의
    example_values: List[Any] = field(default_factory=list)
    validation_rules: List[str] = field(default_factory=list)  # 검증 규칙 이름
    tags: List[str] = field(default_factory=list)
    statistics: Optional[ColumnStatistics] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "description": self.description,
            "is_nullable": self.is_nullable,
            "is_primary_key": self.is_primary_key,
            "is_foreign_key": self.is_foreign_key,
            "foreign_key_ref": self.foreign_key_ref,
            "default_value": self.default_value,
            "sensitivity": self.sensitivity.value,
            "business_name": self.business_name,
            "business_definition": self.business_definition,
            "example_values": self.example_values[:5],
            "validation_rules": self.validation_rules,
            "tags": self.tags,
            "statistics": self.statistics.to_dict() if self.statistics else None,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Column":
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.utcnow()

        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)

        statistics = data.get("statistics")
        if statistics:
            statistics = ColumnStatistics.from_dict(statistics)

        return cls(
            name=data["name"],
            data_type=ColumnType(data.get("data_type", "unknown")),
            description=data.get("description", ""),
            is_nullable=data.get("is_nullable", True),
            is_primary_key=data.get("is_primary_key", False),
            is_foreign_key=data.get("is_foreign_key", False),
            foreign_key_ref=data.get("foreign_key_ref"),
            default_value=data.get("default_value"),
            sensitivity=SensitivityLevel(data.get("sensitivity", "internal")),
            business_name=data.get("business_name", ""),
            business_definition=data.get("business_definition", ""),
            example_values=data.get("example_values", []),
            validation_rules=data.get("validation_rules", []),
            tags=data.get("tags", []),
            statistics=statistics,
            metadata=data.get("metadata", {}),
            created_at=created_at,
            updated_at=updated_at,
        )


@dataclass
class DatasetOwner:
    """데이터셋 소유자 정보"""
    user_id: str
    name: str
    email: str = ""
    role: str = "owner"  # owner, steward, contributor
    assigned_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "name": self.name,
            "email": self.email,
            "role": self.role,
            "assigned_at": self.assigned_at.isoformat() if isinstance(self.assigned_at, datetime) else self.assigned_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetOwner":
        assigned_at = data.get("assigned_at")
        if isinstance(assigned_at, str):
            assigned_at = datetime.fromisoformat(assigned_at)
        elif assigned_at is None:
            assigned_at = datetime.utcnow()

        return cls(
            user_id=data["user_id"],
            name=data["name"],
            email=data.get("email", ""),
            role=data.get("role", "owner"),
            assigned_at=assigned_at,
        )


@dataclass
class LineageNode:
    """리니지 노드 (업스트림/다운스트림 참조)"""
    dataset_id: str
    dataset_name: str
    relationship: str  # "upstream", "downstream"
    transformation: str = ""  # 변환 설명
    columns_mapping: Dict[str, str] = field(default_factory=dict)  # source_col -> target_col

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "dataset_name": self.dataset_name,
            "relationship": self.relationship,
            "transformation": self.transformation,
            "columns_mapping": self.columns_mapping,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LineageNode":
        return cls(
            dataset_id=data["dataset_id"],
            dataset_name=data.get("dataset_name", ""),
            relationship=data["relationship"],
            transformation=data.get("transformation", ""),
            columns_mapping=data.get("columns_mapping", {}),
        )


@dataclass
class Dataset:
    """데이터셋 정의"""
    id: str                         # MongoDB ObjectId 또는 고유 식별자
    name: str                       # 데이터셋 이름
    display_name: str = ""          # 표시 이름
    description: str = ""           # 설명
    dataset_type: DatasetType = DatasetType.SOURCE
    status: DatasetStatus = DatasetStatus.DRAFT

    # 위치 정보
    collection_name: str = ""       # MongoDB 컬렉션 이름
    source_id: Optional[str] = None # 원본 소스 ID (sources 컬렉션 참조)

    # 소유자/담당자
    owners: List[DatasetOwner] = field(default_factory=list)

    # 스키마 정보
    columns: List[Column] = field(default_factory=list)

    # 분류 및 태그
    tags: List[str] = field(default_factory=list)
    domain: str = ""                # 비즈니스 도메인
    subdomain: str = ""             # 서브 도메인

    # 품질 정보
    quality_metrics: Optional[QualityMetrics] = None
    sla_freshness_hours: Optional[int] = None  # 데이터 신선도 SLA (시간)

    # 리니지 정보
    upstream: List[LineageNode] = field(default_factory=list)
    downstream: List[LineageNode] = field(default_factory=list)

    # 접근 정보
    sensitivity: SensitivityLevel = SensitivityLevel.INTERNAL
    access_groups: List[str] = field(default_factory=list)

    # 통계
    record_count: int = 0
    size_bytes: int = 0
    last_updated_at: Optional[datetime] = None
    last_accessed_at: Optional[datetime] = None
    access_count: int = 0

    # 메타데이터
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    created_by: str = "system"

    def get_column(self, name: str) -> Optional[Column]:
        """컬럼 조회"""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def get_column_names(self) -> List[str]:
        """컬럼 이름 목록"""
        return [col.name for col in self.columns]

    def get_primary_keys(self) -> List[str]:
        """기본키 컬럼 목록"""
        return [col.name for col in self.columns if col.is_primary_key]

    def get_sensitive_columns(self) -> List[Column]:
        """민감 컬럼 목록"""
        return [col for col in self.columns if col.sensitivity in (SensitivityLevel.CONFIDENTIAL, SensitivityLevel.RESTRICTED)]

    def compute_fingerprint(self) -> str:
        """데이터셋 스키마 지문 계산"""
        schema_data = {
            "name": self.name,
            "columns": [{"name": c.name, "type": c.data_type.value} for c in self.columns],
        }
        schema_str = json.dumps(schema_data, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "_id": self.id,
            "name": self.name,
            "display_name": self.display_name or self.name,
            "description": self.description,
            "dataset_type": self.dataset_type.value,
            "status": self.status.value,
            "collection_name": self.collection_name,
            "source_id": self.source_id,
            "owners": [o.to_dict() for o in self.owners],
            "columns": [c.to_dict() for c in self.columns],
            "tags": self.tags,
            "domain": self.domain,
            "subdomain": self.subdomain,
            "quality_metrics": self.quality_metrics.to_dict() if self.quality_metrics else None,
            "sla_freshness_hours": self.sla_freshness_hours,
            "upstream": [n.to_dict() for n in self.upstream],
            "downstream": [n.to_dict() for n in self.downstream],
            "sensitivity": self.sensitivity.value,
            "access_groups": self.access_groups,
            "record_count": self.record_count,
            "size_bytes": self.size_bytes,
            "last_updated_at": self.last_updated_at.isoformat() if self.last_updated_at else None,
            "last_accessed_at": self.last_accessed_at.isoformat() if self.last_accessed_at else None,
            "access_count": self.access_count,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "created_by": self.created_by,
            "fingerprint": self.compute_fingerprint(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Dataset":
        # 날짜 필드 처리
        def parse_datetime(value):
            if isinstance(value, str):
                return datetime.fromisoformat(value)
            return value

        # 품질 메트릭
        quality_metrics = data.get("quality_metrics")
        if quality_metrics:
            quality_metrics = QualityMetrics.from_dict(quality_metrics)

        return cls(
            id=str(data.get("_id", data.get("id", ""))),
            name=data["name"],
            display_name=data.get("display_name", ""),
            description=data.get("description", ""),
            dataset_type=DatasetType(data.get("dataset_type", "source")),
            status=DatasetStatus(data.get("status", "draft")),
            collection_name=data.get("collection_name", ""),
            source_id=data.get("source_id"),
            owners=[DatasetOwner.from_dict(o) for o in data.get("owners", [])],
            columns=[Column.from_dict(c) for c in data.get("columns", [])],
            tags=data.get("tags", []),
            domain=data.get("domain", ""),
            subdomain=data.get("subdomain", ""),
            quality_metrics=quality_metrics,
            sla_freshness_hours=data.get("sla_freshness_hours"),
            upstream=[LineageNode.from_dict(n) for n in data.get("upstream", [])],
            downstream=[LineageNode.from_dict(n) for n in data.get("downstream", [])],
            sensitivity=SensitivityLevel(data.get("sensitivity", "internal")),
            access_groups=data.get("access_groups", []),
            record_count=data.get("record_count", 0),
            size_bytes=data.get("size_bytes", 0),
            last_updated_at=parse_datetime(data.get("last_updated_at")),
            last_accessed_at=parse_datetime(data.get("last_accessed_at")),
            access_count=data.get("access_count", 0),
            metadata=data.get("metadata", {}),
            created_at=parse_datetime(data.get("created_at")) or datetime.utcnow(),
            updated_at=parse_datetime(data.get("updated_at")),
            created_by=data.get("created_by", "system"),
        )


@dataclass
class CatalogStatistics:
    """카탈로그 통계"""
    total_datasets: int = 0
    active_datasets: int = 0
    total_columns: int = 0
    documented_columns: int = 0
    total_tags: int = 0
    avg_quality_score: float = 0.0
    datasets_by_type: Dict[str, int] = field(default_factory=dict)
    datasets_by_domain: Dict[str, int] = field(default_factory=dict)
    datasets_by_status: Dict[str, int] = field(default_factory=dict)
    computed_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_datasets": self.total_datasets,
            "active_datasets": self.active_datasets,
            "total_columns": self.total_columns,
            "documented_columns": self.documented_columns,
            "documentation_rate": round(self.documented_columns / self.total_columns * 100, 2) if self.total_columns > 0 else 0,
            "total_tags": self.total_tags,
            "avg_quality_score": round(self.avg_quality_score, 2),
            "datasets_by_type": self.datasets_by_type,
            "datasets_by_domain": self.datasets_by_domain,
            "datasets_by_status": self.datasets_by_status,
            "computed_at": self.computed_at.isoformat(),
        }

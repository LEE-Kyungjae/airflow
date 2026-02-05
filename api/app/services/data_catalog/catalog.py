"""
Data Catalog - 데이터 카탈로그 핵심 서비스

주요 기능:
1. 데이터셋 메타데이터 CRUD
2. 컬럼 레벨 문서화
3. 태그 관리
4. 품질 메트릭 연동
5. 기존 컬렉션 자동 등록
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from bson import ObjectId
from bson.errors import InvalidId

from .models import (
    Dataset,
    DatasetType,
    DatasetStatus,
    Column,
    ColumnType,
    ColumnStatistics,
    Tag,
    TagCategory,
    DatasetOwner,
    QualityMetrics,
    LineageNode,
    SensitivityLevel,
    CatalogStatistics,
)

logger = logging.getLogger(__name__)


class DataCatalog:
    """데이터 카탈로그 서비스"""

    # 시스템 컬렉션 목록 (자동 등록 대상)
    SYSTEM_COLLECTIONS = {
        "sources": {
            "display_name": "크롤링 소스",
            "description": "크롤링 대상 웹사이트/파일 소스 정보",
            "dataset_type": DatasetType.SOURCE,
            "domain": "crawler",
        },
        "crawlers": {
            "display_name": "크롤러 코드",
            "description": "자동 생성된 크롤러 코드 및 버전 관리",
            "dataset_type": DatasetType.SOURCE,
            "domain": "crawler",
        },
        "crawl_results": {
            "display_name": "크롤링 결과",
            "description": "크롤링 실행 결과 및 상태",
            "dataset_type": DatasetType.STAGING,
            "domain": "crawler",
        },
        "news_articles": {
            "display_name": "뉴스 기사",
            "description": "수집된 뉴스 기사 데이터",
            "dataset_type": DatasetType.FINAL,
            "domain": "news",
        },
        "financial_data": {
            "display_name": "금융 데이터",
            "description": "수집된 금융 관련 데이터",
            "dataset_type": DatasetType.FINAL,
            "domain": "finance",
        },
        "error_logs": {
            "display_name": "에러 로그",
            "description": "크롤링 에러 및 복구 로그",
            "dataset_type": DatasetType.SOURCE,
            "domain": "monitoring",
        },
        "schema_registry": {
            "display_name": "스키마 레지스트리",
            "description": "데이터 스키마 버전 관리",
            "dataset_type": DatasetType.SOURCE,
            "domain": "governance",
        },
    }

    def __init__(self, mongo_service=None):
        """
        초기화

        Args:
            mongo_service: MongoService 인스턴스
        """
        self.mongo = mongo_service
        self._tag_cache: Dict[str, Tag] = {}

    def _get_catalog_collection(self):
        """데이터 카탈로그 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_catalog
        return None

    def _get_columns_collection(self):
        """컬럼 정보 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_columns
        return None

    def _get_tags_collection(self):
        """태그 컬렉션"""
        if self.mongo:
            return self.mongo.db.data_tags
        return None

    def _serialize_id(self, doc: Dict) -> Dict:
        """ObjectId를 문자열로 변환"""
        if doc and "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    # ==================== Dataset CRUD ====================

    def create_dataset(
        self,
        name: str,
        dataset_type: DatasetType,
        description: str = "",
        collection_name: str = "",
        source_id: str = None,
        columns: List[Column] = None,
        tags: List[str] = None,
        domain: str = "",
        owner: DatasetOwner = None,
        created_by: str = "system",
    ) -> Dataset:
        """
        데이터셋 생성

        Args:
            name: 데이터셋 이름
            dataset_type: 데이터셋 유형
            description: 설명
            collection_name: MongoDB 컬렉션 이름
            source_id: 원본 소스 ID
            columns: 컬럼 목록
            tags: 태그 목록
            domain: 비즈니스 도메인
            owner: 소유자
            created_by: 생성자

        Returns:
            생성된 Dataset
        """
        collection = self._get_catalog_collection()

        # 중복 체크
        if collection:
            existing = collection.find_one({"name": name})
            if existing:
                raise ValueError(f"Dataset with name '{name}' already exists")

        # 객체 생성
        dataset = Dataset(
            id="",  # MongoDB에서 생성
            name=name,
            display_name=name,
            description=description,
            dataset_type=dataset_type,
            status=DatasetStatus.DRAFT,
            collection_name=collection_name or name,
            source_id=source_id,
            owners=[owner] if owner else [],
            columns=columns or [],
            tags=tags or [],
            domain=domain,
            created_at=datetime.utcnow(),
            created_by=created_by,
        )

        # 저장
        if collection:
            doc = dataset.to_dict()
            del doc["_id"]  # 새 ID 생성
            result = collection.insert_one(doc)
            dataset.id = str(result.inserted_id)
            logger.info(f"Created dataset: {dataset.id} ({name})")

            # 컬럼 정보 저장
            if columns:
                self._save_columns(dataset.id, columns)

            # 태그 사용 카운트 증가
            if tags:
                self._increment_tag_usage(tags)

        return dataset

    def get_dataset(self, dataset_id: str) -> Optional[Dataset]:
        """
        데이터셋 조회

        Args:
            dataset_id: 데이터셋 ID

        Returns:
            Dataset 또는 None
        """
        collection = self._get_catalog_collection()
        if not collection:
            return None

        try:
            doc = collection.find_one({"_id": ObjectId(dataset_id)})
            if doc:
                return Dataset.from_dict(self._serialize_id(doc))
        except InvalidId:
            logger.warning(f"Invalid dataset ID: {dataset_id}")

        return None

    def get_dataset_by_name(self, name: str) -> Optional[Dataset]:
        """
        이름으로 데이터셋 조회

        Args:
            name: 데이터셋 이름

        Returns:
            Dataset 또는 None
        """
        collection = self._get_catalog_collection()
        if not collection:
            return None

        doc = collection.find_one({"name": name})
        if doc:
            return Dataset.from_dict(self._serialize_id(doc))

        return None

    def get_dataset_by_collection(self, collection_name: str) -> Optional[Dataset]:
        """
        컬렉션 이름으로 데이터셋 조회

        Args:
            collection_name: MongoDB 컬렉션 이름

        Returns:
            Dataset 또는 None
        """
        collection = self._get_catalog_collection()
        if not collection:
            return None

        doc = collection.find_one({"collection_name": collection_name})
        if doc:
            return Dataset.from_dict(self._serialize_id(doc))

        return None

    def list_datasets(
        self,
        dataset_type: DatasetType = None,
        status: DatasetStatus = None,
        domain: str = None,
        tags: List[str] = None,
        search_query: str = None,
        skip: int = 0,
        limit: int = 100,
    ) -> Tuple[List[Dataset], int]:
        """
        데이터셋 목록 조회

        Args:
            dataset_type: 데이터셋 유형 필터
            status: 상태 필터
            domain: 도메인 필터
            tags: 태그 필터 (AND 조건)
            search_query: 검색어 (이름, 설명에서 검색)
            skip: 건너뛸 개수
            limit: 최대 개수

        Returns:
            (데이터셋 목록, 전체 개수)
        """
        collection = self._get_catalog_collection()
        if not collection:
            return [], 0

        query = {}

        if dataset_type:
            query["dataset_type"] = dataset_type.value

        if status:
            query["status"] = status.value

        if domain:
            query["domain"] = domain

        if tags:
            query["tags"] = {"$all": tags}

        if search_query:
            query["$or"] = [
                {"name": {"$regex": search_query, "$options": "i"}},
                {"display_name": {"$regex": search_query, "$options": "i"}},
                {"description": {"$regex": search_query, "$options": "i"}},
            ]

        total = collection.count_documents(query)
        cursor = collection.find(query).sort("created_at", -1).skip(skip).limit(limit)

        datasets = [Dataset.from_dict(self._serialize_id(doc)) for doc in cursor]

        return datasets, total

    def update_dataset(
        self,
        dataset_id: str,
        updates: Dict[str, Any],
        updated_by: str = "system",
    ) -> bool:
        """
        데이터셋 수정

        Args:
            dataset_id: 데이터셋 ID
            updates: 수정할 필드
            updated_by: 수정자

        Returns:
            수정 성공 여부
        """
        collection = self._get_catalog_collection()
        if not collection:
            return False

        # 수정 불가 필드 제거
        protected_fields = {"_id", "created_at", "created_by"}
        updates = {k: v for k, v in updates.items() if k not in protected_fields}

        updates["updated_at"] = datetime.utcnow()

        try:
            result = collection.update_one(
                {"_id": ObjectId(dataset_id)},
                {"$set": updates}
            )
            if result.modified_count > 0:
                logger.info(f"Updated dataset: {dataset_id}")
                return True
        except InvalidId:
            logger.warning(f"Invalid dataset ID: {dataset_id}")

        return False

    def delete_dataset(self, dataset_id: str) -> bool:
        """
        데이터셋 삭제

        Args:
            dataset_id: 데이터셋 ID

        Returns:
            삭제 성공 여부
        """
        collection = self._get_catalog_collection()
        columns_collection = self._get_columns_collection()

        if not collection:
            return False

        try:
            # 컬럼 정보 삭제
            if columns_collection:
                columns_collection.delete_many({"dataset_id": dataset_id})

            # 데이터셋 삭제
            result = collection.delete_one({"_id": ObjectId(dataset_id)})
            if result.deleted_count > 0:
                logger.info(f"Deleted dataset: {dataset_id}")
                return True
        except InvalidId:
            logger.warning(f"Invalid dataset ID: {dataset_id}")

        return False

    def update_dataset_status(
        self,
        dataset_id: str,
        status: DatasetStatus,
    ) -> bool:
        """
        데이터셋 상태 변경

        Args:
            dataset_id: 데이터셋 ID
            status: 새 상태

        Returns:
            수정 성공 여부
        """
        return self.update_dataset(dataset_id, {"status": status.value})

    # ==================== Column Management ====================

    def _save_columns(self, dataset_id: str, columns: List[Column]):
        """컬럼 정보 저장"""
        collection = self._get_columns_collection()
        if not collection:
            return

        # 기존 컬럼 삭제
        collection.delete_many({"dataset_id": dataset_id})

        # 새 컬럼 저장
        for col in columns:
            doc = col.to_dict()
            doc["dataset_id"] = dataset_id
            collection.insert_one(doc)

    def get_columns(self, dataset_id: str) -> List[Column]:
        """
        데이터셋의 컬럼 목록 조회

        Args:
            dataset_id: 데이터셋 ID

        Returns:
            컬럼 목록
        """
        collection = self._get_columns_collection()
        if not collection:
            return []

        cursor = collection.find({"dataset_id": dataset_id})
        return [Column.from_dict(doc) for doc in cursor]

    def update_column(
        self,
        dataset_id: str,
        column_name: str,
        updates: Dict[str, Any],
    ) -> bool:
        """
        컬럼 정보 수정

        Args:
            dataset_id: 데이터셋 ID
            column_name: 컬럼 이름
            updates: 수정할 필드

        Returns:
            수정 성공 여부
        """
        collection = self._get_columns_collection()
        if not collection:
            return False

        updates["updated_at"] = datetime.utcnow()

        result = collection.update_one(
            {"dataset_id": dataset_id, "name": column_name},
            {"$set": updates}
        )
        return result.modified_count > 0

    def add_column(self, dataset_id: str, column: Column) -> bool:
        """
        컬럼 추가

        Args:
            dataset_id: 데이터셋 ID
            column: 컬럼 정보

        Returns:
            추가 성공 여부
        """
        collection = self._get_columns_collection()
        catalog_collection = self._get_catalog_collection()

        if not collection or not catalog_collection:
            return False

        # 중복 체크
        existing = collection.find_one({
            "dataset_id": dataset_id,
            "name": column.name
        })
        if existing:
            return False

        # 컬럼 저장
        doc = column.to_dict()
        doc["dataset_id"] = dataset_id
        collection.insert_one(doc)

        # 데이터셋의 columns 배열 업데이트
        catalog_collection.update_one(
            {"_id": ObjectId(dataset_id)},
            {
                "$push": {"columns": column.to_dict()},
                "$set": {"updated_at": datetime.utcnow()}
            }
        )

        return True

    def update_column_statistics(
        self,
        dataset_id: str,
        column_name: str,
        statistics: ColumnStatistics,
    ) -> bool:
        """
        컬럼 통계 업데이트

        Args:
            dataset_id: 데이터셋 ID
            column_name: 컬럼 이름
            statistics: 통계 정보

        Returns:
            수정 성공 여부
        """
        return self.update_column(
            dataset_id,
            column_name,
            {"statistics": statistics.to_dict()}
        )

    # ==================== Tag Management ====================

    def create_tag(
        self,
        name: str,
        category: TagCategory,
        description: str = "",
        color: str = "#808080",
        created_by: str = "system",
    ) -> Tag:
        """
        태그 생성

        Args:
            name: 태그 이름
            category: 태그 카테고리
            description: 설명
            color: 표시 색상
            created_by: 생성자

        Returns:
            생성된 Tag
        """
        collection = self._get_tags_collection()

        tag = Tag(
            name=name,
            category=category,
            description=description,
            color=color,
            created_at=datetime.utcnow(),
            created_by=created_by,
            usage_count=0,
        )

        if collection:
            # 중복 체크
            existing = collection.find_one({"name": name})
            if existing:
                raise ValueError(f"Tag '{name}' already exists")

            collection.insert_one(tag.to_dict())
            logger.info(f"Created tag: {name}")

        # 캐시 업데이트
        self._tag_cache[name] = tag

        return tag

    def get_tag(self, name: str) -> Optional[Tag]:
        """
        태그 조회

        Args:
            name: 태그 이름

        Returns:
            Tag 또는 None
        """
        # 캐시 확인
        if name in self._tag_cache:
            return self._tag_cache[name]

        collection = self._get_tags_collection()
        if not collection:
            return None

        doc = collection.find_one({"name": name})
        if doc:
            tag = Tag.from_dict(doc)
            self._tag_cache[name] = tag
            return tag

        return None

    def list_tags(
        self,
        category: TagCategory = None,
        search_query: str = None,
    ) -> List[Tag]:
        """
        태그 목록 조회

        Args:
            category: 카테고리 필터
            search_query: 검색어

        Returns:
            태그 목록
        """
        collection = self._get_tags_collection()
        if not collection:
            return []

        query = {}

        if category:
            query["category"] = category.value

        if search_query:
            query["name"] = {"$regex": search_query, "$options": "i"}

        cursor = collection.find(query).sort("usage_count", -1)
        return [Tag.from_dict(doc) for doc in cursor]

    def _increment_tag_usage(self, tag_names: List[str]):
        """태그 사용 카운트 증가"""
        collection = self._get_tags_collection()
        if collection:
            collection.update_many(
                {"name": {"$in": tag_names}},
                {"$inc": {"usage_count": 1}}
            )

    def add_tags_to_dataset(self, dataset_id: str, tags: List[str]) -> bool:
        """
        데이터셋에 태그 추가

        Args:
            dataset_id: 데이터셋 ID
            tags: 추가할 태그 목록

        Returns:
            수정 성공 여부
        """
        collection = self._get_catalog_collection()
        if not collection:
            return False

        result = collection.update_one(
            {"_id": ObjectId(dataset_id)},
            {
                "$addToSet": {"tags": {"$each": tags}},
                "$set": {"updated_at": datetime.utcnow()}
            }
        )

        if result.modified_count > 0:
            self._increment_tag_usage(tags)
            return True

        return False

    def remove_tags_from_dataset(self, dataset_id: str, tags: List[str]) -> bool:
        """
        데이터셋에서 태그 제거

        Args:
            dataset_id: 데이터셋 ID
            tags: 제거할 태그 목록

        Returns:
            수정 성공 여부
        """
        collection = self._get_catalog_collection()
        if not collection:
            return False

        result = collection.update_one(
            {"_id": ObjectId(dataset_id)},
            {
                "$pull": {"tags": {"$in": tags}},
                "$set": {"updated_at": datetime.utcnow()}
            }
        )

        return result.modified_count > 0

    # ==================== Quality Metrics ====================

    def update_quality_metrics(
        self,
        dataset_id: str,
        metrics: QualityMetrics,
    ) -> bool:
        """
        품질 메트릭 업데이트

        Args:
            dataset_id: 데이터셋 ID
            metrics: 품질 메트릭

        Returns:
            수정 성공 여부
        """
        metrics.calculate_overall()
        return self.update_dataset(
            dataset_id,
            {"quality_metrics": metrics.to_dict()}
        )

    def sync_quality_from_validator(
        self,
        dataset_id: str,
        validation_result: Dict[str, Any],
    ) -> bool:
        """
        데이터 품질 검증 결과 동기화

        Args:
            dataset_id: 데이터셋 ID
            validation_result: DataValidator의 ValidationResult.to_dict()

        Returns:
            수정 성공 여부
        """
        # 검증 결과에서 품질 메트릭 추출
        field_stats = validation_result.get("field_stats", {})
        issue_summary = validation_result.get("issue_summary", {})
        quality_score = validation_result.get("quality_score", 0)

        # 완전성 계산 (null 비율 기반)
        total_null_rate = 0
        field_count = len(field_stats)
        if field_count > 0:
            for stats in field_stats.values():
                total_null_rate += stats.get("null_rate", 0)
            avg_null_rate = total_null_rate / field_count
            completeness = 100 - avg_null_rate
        else:
            completeness = 100.0

        # 유효성 계산 (이슈 기반)
        total_records = validation_result.get("total_records", 0)
        error_count = issue_summary.get("by_severity", {}).get("error", 0) + \
                      issue_summary.get("by_severity", {}).get("critical", 0)
        if total_records > 0:
            validity = max(0, 100 - (error_count / total_records * 100))
        else:
            validity = 100.0

        metrics = QualityMetrics(
            completeness=round(completeness, 2),
            validity=round(validity, 2),
            overall_score=quality_score,
            last_assessed_at=datetime.utcnow(),
            assessed_records=total_records,
            failed_checks=issue_summary.get("total", 0),
        )

        return self.update_quality_metrics(dataset_id, metrics)

    # ==================== Owner Management ====================

    def add_owner(
        self,
        dataset_id: str,
        owner: DatasetOwner,
    ) -> bool:
        """
        소유자 추가

        Args:
            dataset_id: 데이터셋 ID
            owner: 소유자 정보

        Returns:
            추가 성공 여부
        """
        collection = self._get_catalog_collection()
        if not collection:
            return False

        result = collection.update_one(
            {"_id": ObjectId(dataset_id)},
            {
                "$push": {"owners": owner.to_dict()},
                "$set": {"updated_at": datetime.utcnow()}
            }
        )

        return result.modified_count > 0

    def remove_owner(self, dataset_id: str, user_id: str) -> bool:
        """
        소유자 제거

        Args:
            dataset_id: 데이터셋 ID
            user_id: 사용자 ID

        Returns:
            제거 성공 여부
        """
        collection = self._get_catalog_collection()
        if not collection:
            return False

        result = collection.update_one(
            {"_id": ObjectId(dataset_id)},
            {
                "$pull": {"owners": {"user_id": user_id}},
                "$set": {"updated_at": datetime.utcnow()}
            }
        )

        return result.modified_count > 0

    # ==================== Auto Registration ====================

    def register_existing_collections(self) -> List[Dataset]:
        """
        기존 MongoDB 컬렉션 자동 등록

        Returns:
            등록된 데이터셋 목록
        """
        if not self.mongo:
            return []

        registered = []
        db = self.mongo.db

        # 시스템 컬렉션 등록
        for collection_name, config in self.SYSTEM_COLLECTIONS.items():
            # 이미 등록되었는지 확인
            existing = self.get_dataset_by_collection(collection_name)
            if existing:
                continue

            # 컬렉션 존재 확인
            if collection_name not in db.list_collection_names():
                continue

            # 스키마 감지
            columns = self._detect_schema_from_collection(collection_name)

            # 데이터셋 생성
            try:
                dataset = self.create_dataset(
                    name=collection_name,
                    dataset_type=config["dataset_type"],
                    description=config["description"],
                    collection_name=collection_name,
                    columns=columns,
                    domain=config["domain"],
                    created_by="auto_registration",
                )
                dataset.display_name = config["display_name"]
                self.update_dataset(dataset.id, {"display_name": config["display_name"]})
                registered.append(dataset)
                logger.info(f"Auto-registered collection: {collection_name}")
            except Exception as e:
                logger.error(f"Failed to register {collection_name}: {e}")

        # staging_ 프리픽스 컬렉션 자동 등록
        for collection_name in db.list_collection_names():
            if collection_name.startswith("staging_"):
                existing = self.get_dataset_by_collection(collection_name)
                if existing:
                    continue

                columns = self._detect_schema_from_collection(collection_name)
                source_name = collection_name.replace("staging_", "")

                try:
                    dataset = self.create_dataset(
                        name=collection_name,
                        dataset_type=DatasetType.STAGING,
                        description=f"{source_name} 소스의 스테이징 데이터",
                        collection_name=collection_name,
                        columns=columns,
                        domain="staging",
                        created_by="auto_registration",
                    )
                    registered.append(dataset)
                except Exception as e:
                    logger.error(f"Failed to register {collection_name}: {e}")

        return registered

    def _detect_schema_from_collection(
        self,
        collection_name: str,
        sample_size: int = 100,
    ) -> List[Column]:
        """
        컬렉션에서 스키마 감지

        Args:
            collection_name: 컬렉션 이름
            sample_size: 샘플 문서 수

        Returns:
            감지된 컬럼 목록
        """
        if not self.mongo:
            return []

        collection = self.mongo.db[collection_name]
        samples = list(collection.find().limit(sample_size))

        if not samples:
            return []

        # 필드 타입 추론
        field_types: Dict[str, Dict[str, int]] = {}
        field_nulls: Dict[str, int] = {}

        for doc in samples:
            for field_name, value in doc.items():
                if field_name not in field_types:
                    field_types[field_name] = {}
                    field_nulls[field_name] = 0

                inferred_type = self._infer_type(value)
                field_types[field_name][inferred_type] = \
                    field_types[field_name].get(inferred_type, 0) + 1

                if value is None:
                    field_nulls[field_name] += 1

        # 컬럼 객체 생성
        columns = []
        for field_name, type_counts in field_types.items():
            # 가장 빈번한 타입 선택
            most_common = max(type_counts.items(), key=lambda x: x[1])
            col_type = ColumnType(most_common[0])

            # Null 비율로 nullable 결정
            null_ratio = field_nulls[field_name] / len(samples)

            columns.append(Column(
                name=field_name,
                data_type=col_type,
                is_nullable=null_ratio > 0,
                is_primary_key=(field_name == "_id"),
                created_at=datetime.utcnow(),
            ))

        return columns

    def _infer_type(self, value: Any) -> str:
        """값에서 타입 추론"""
        if value is None:
            return ColumnType.UNKNOWN.value
        elif isinstance(value, bool):
            return ColumnType.BOOLEAN.value
        elif isinstance(value, int):
            return ColumnType.INTEGER.value
        elif isinstance(value, float):
            return ColumnType.FLOAT.value
        elif isinstance(value, datetime):
            return ColumnType.DATETIME.value
        elif isinstance(value, list):
            return ColumnType.ARRAY.value
        elif isinstance(value, dict):
            return ColumnType.OBJECT.value
        elif isinstance(value, bytes):
            return ColumnType.BINARY.value
        elif isinstance(value, ObjectId):
            return ColumnType.STRING.value
        else:
            return ColumnType.STRING.value

    # ==================== Statistics ====================

    def get_statistics(self) -> CatalogStatistics:
        """
        카탈로그 통계 조회

        Returns:
            CatalogStatistics
        """
        collection = self._get_catalog_collection()
        tags_collection = self._get_tags_collection()

        if not collection:
            return CatalogStatistics()

        # 데이터셋 통계
        total_datasets = collection.count_documents({})
        active_datasets = collection.count_documents({"status": DatasetStatus.ACTIVE.value})

        # 타입별 통계
        datasets_by_type = {}
        for dtype in DatasetType:
            count = collection.count_documents({"dataset_type": dtype.value})
            if count > 0:
                datasets_by_type[dtype.value] = count

        # 상태별 통계
        datasets_by_status = {}
        for status in DatasetStatus:
            count = collection.count_documents({"status": status.value})
            if count > 0:
                datasets_by_status[status.value] = count

        # 도메인별 통계
        pipeline = [
            {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": ""}}},
        ]
        domain_results = list(collection.aggregate(pipeline))
        datasets_by_domain = {r["_id"]: r["count"] for r in domain_results}

        # 컬럼 통계
        all_datasets = list(collection.find({}, {"columns": 1}))
        total_columns = 0
        documented_columns = 0
        for ds in all_datasets:
            for col in ds.get("columns", []):
                total_columns += 1
                if col.get("description") or col.get("business_definition"):
                    documented_columns += 1

        # 품질 평균
        quality_pipeline = [
            {"$match": {"quality_metrics.overall_score": {"$gt": 0}}},
            {"$group": {"_id": None, "avg_score": {"$avg": "$quality_metrics.overall_score"}}},
        ]
        quality_result = list(collection.aggregate(quality_pipeline))
        avg_quality = quality_result[0]["avg_score"] if quality_result else 0.0

        # 태그 수
        total_tags = tags_collection.count_documents({}) if tags_collection else 0

        return CatalogStatistics(
            total_datasets=total_datasets,
            active_datasets=active_datasets,
            total_columns=total_columns,
            documented_columns=documented_columns,
            total_tags=total_tags,
            avg_quality_score=avg_quality,
            datasets_by_type=datasets_by_type,
            datasets_by_domain=datasets_by_domain,
            datasets_by_status=datasets_by_status,
            computed_at=datetime.utcnow(),
        )

    def update_dataset_stats(self, dataset_id: str) -> bool:
        """
        데이터셋 통계 업데이트 (레코드 수, 사이즈 등)

        Args:
            dataset_id: 데이터셋 ID

        Returns:
            업데이트 성공 여부
        """
        dataset = self.get_dataset(dataset_id)
        if not dataset or not self.mongo:
            return False

        collection_name = dataset.collection_name
        if not collection_name:
            return False

        try:
            collection = self.mongo.db[collection_name]
            stats = self.mongo.db.command("collStats", collection_name)

            record_count = stats.get("count", 0)
            size_bytes = stats.get("size", 0)

            return self.update_dataset(dataset_id, {
                "record_count": record_count,
                "size_bytes": size_bytes,
                "last_updated_at": datetime.utcnow(),
            })
        except Exception as e:
            logger.error(f"Failed to update stats for {dataset_id}: {e}")
            return False

    def record_access(self, dataset_id: str) -> bool:
        """
        데이터셋 접근 기록

        Args:
            dataset_id: 데이터셋 ID

        Returns:
            기록 성공 여부
        """
        collection = self._get_catalog_collection()
        if not collection:
            return False

        try:
            result = collection.update_one(
                {"_id": ObjectId(dataset_id)},
                {
                    "$inc": {"access_count": 1},
                    "$set": {"last_accessed_at": datetime.utcnow()}
                }
            )
            return result.modified_count > 0
        except InvalidId:
            return False

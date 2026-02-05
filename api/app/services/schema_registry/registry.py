"""
Schema Registry - 스키마 레지스트리 핵심 클래스

MongoDB에 스키마 버전을 저장하고 관리하는 중앙 레지스트리

기능:
- 스키마 버전 등록 및 관리
- 버전별 스키마 이력 조회
- 호환성 검증 및 드리프트 감지
- 스키마 진화 추적
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from bson import ObjectId

from .models import (
    Schema,
    SchemaVersion,
    FieldSchema,
    FieldType,
    CompatibilityMode,
    CompatibilityIssue,
    CompatibilityResult,
    DataCategory,
    get_default_schema,
)
from .compatibility import CompatibilityChecker, check_compatibility

logger = logging.getLogger(__name__)


class SchemaRegistry:
    """
    스키마 레지스트리

    MongoDB의 schema_registry 컬렉션에 스키마 버전을 저장하고 관리

    Collection Structure:
    {
        _id: ObjectId,
        source_id: str,           # 소스 ID (또는 컬렉션명)
        version: int,             # 버전 번호
        schema: {...},            # Schema 객체
        fingerprint: str,         # 스키마 지문
        created_at: datetime,
        created_by: str,
        change_description: str,
        is_active: bool,
        compatibility_mode: str,
        tags: [str],
    }
    """

    COLLECTION_NAME = "schema_registry"

    def __init__(self, mongo_service=None):
        """
        Args:
            mongo_service: MongoService 인스턴스 (None이면 인메모리 모드)
        """
        self.mongo = mongo_service
        self._cache: Dict[str, List[SchemaVersion]] = {}
        self._compatibility_checker = CompatibilityChecker()

    def _get_collection(self):
        """MongoDB 컬렉션 반환"""
        if self.mongo:
            return self.mongo.db[self.COLLECTION_NAME]
        return None

    async def ensure_indexes(self):
        """필요한 인덱스 생성"""
        collection = self._get_collection()
        if collection:
            # 소스별 버전 조회용
            collection.create_index([("source_id", 1), ("version", -1)])
            # 활성 스키마 조회용
            collection.create_index([("source_id", 1), ("is_active", 1)])
            # fingerprint 조회용
            collection.create_index("fingerprint")
            # 생성일 기준 조회용
            collection.create_index("created_at")
            logger.info("Schema registry indexes ensured")

    def register_schema(
        self,
        source_id: str,
        schema: Schema,
        created_by: str = "system",
        change_description: str = "",
        compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD,
        tags: List[str] = None
    ) -> Tuple[SchemaVersion, CompatibilityResult]:
        """
        새 스키마 버전 등록

        Args:
            source_id: 소스 ID (또는 컬렉션 이름)
            schema: 등록할 스키마
            created_by: 생성자
            change_description: 변경 설명
            compatibility_mode: 호환성 검사 모드
            tags: 태그 목록

        Returns:
            (SchemaVersion, CompatibilityResult)

        Raises:
            ValueError: 호환성 검사 실패 시
        """
        fingerprint = schema.compute_fingerprint()
        tags = tags or []

        # 기존 버전 조회
        versions = self.get_versions(source_id)
        latest = versions[-1] if versions else None

        # 동일 스키마 체크 (지문 비교)
        if latest and latest.fingerprint == fingerprint:
            logger.info(f"Schema unchanged for source {source_id}, fingerprint={fingerprint}")
            return latest, CompatibilityResult(
                is_compatible=True,
                issues=[],
                mode=compatibility_mode
            )

        # 호환성 검사
        compat_result = CompatibilityResult(is_compatible=True, issues=[], mode=compatibility_mode)
        if latest and compatibility_mode != CompatibilityMode.NONE:
            compat_result = self._compatibility_checker.check_compatibility(
                latest.schema, schema, compatibility_mode
            )

            # 에러가 있으면 예외 발생
            if not compat_result.is_compatible:
                error_messages = [f"{e.field_name}: {e.message}" for e in compat_result.errors]
                raise ValueError(
                    f"Schema incompatible with mode '{compatibility_mode.value}': "
                    f"{'; '.join(error_messages)}"
                )

        # 새 버전 생성
        new_version = SchemaVersion(
            version=(latest.version + 1) if latest else 1,
            schema=schema,
            fingerprint=fingerprint,
            created_at=datetime.utcnow(),
            created_by=created_by,
            change_description=change_description,
            is_active=True,
            compatibility_mode=compatibility_mode,
            tags=tags,
        )

        # 저장
        self._save_version(source_id, new_version)

        logger.info(
            f"Schema registered: source={source_id}, version={new_version.version}, "
            f"fingerprint={fingerprint}, warnings={len(compat_result.warnings)}"
        )

        return new_version, compat_result

    def register_schema_for_category(
        self,
        source_id: str,
        category: DataCategory,
        created_by: str = "system",
        extra_fields: List[FieldSchema] = None
    ) -> Tuple[SchemaVersion, CompatibilityResult]:
        """
        데이터 카테고리 기반으로 스키마 등록

        Args:
            source_id: 소스 ID
            category: 데이터 카테고리
            created_by: 생성자
            extra_fields: 추가할 필드들

        Returns:
            (SchemaVersion, CompatibilityResult)
        """
        schema = get_default_schema(category)

        if extra_fields:
            for field in extra_fields:
                schema.add_field(field)

        return self.register_schema(
            source_id=source_id,
            schema=schema,
            created_by=created_by,
            change_description=f"Initial schema from category: {category.value}",
        )

    def get_schema(
        self,
        source_id: str,
        version: int = None
    ) -> Optional[SchemaVersion]:
        """
        스키마 버전 조회

        Args:
            source_id: 소스 ID
            version: 버전 번호 (None이면 최신)

        Returns:
            SchemaVersion 또는 None
        """
        versions = self.get_versions(source_id)
        if not versions:
            return None

        if version is None:
            # 활성 버전 중 최신
            active_versions = [v for v in versions if v.is_active]
            return active_versions[-1] if active_versions else versions[-1]

        for v in versions:
            if v.version == version:
                return v
        return None

    def get_versions(self, source_id: str, include_inactive: bool = False) -> List[SchemaVersion]:
        """
        소스의 모든 스키마 버전 조회

        Args:
            source_id: 소스 ID
            include_inactive: 비활성 버전 포함 여부

        Returns:
            버전 목록 (오래된 순)
        """
        # 캐시 확인
        cache_key = f"{source_id}:{include_inactive}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # DB에서 로드
        collection = self._get_collection()
        versions = []

        if collection:
            query = {"source_id": source_id}
            if not include_inactive:
                query["is_active"] = True

            docs = list(collection.find(query).sort("version", 1))
            versions = [self._doc_to_version(d) for d in docs]
        else:
            # 인메모리 모드
            if source_id in self._cache:
                versions = self._cache[source_id]

        self._cache[cache_key] = versions
        return versions

    def get_version_history(
        self,
        source_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        스키마 버전 이력 요약 조회

        Args:
            source_id: 소스 ID
            limit: 최대 조회 수

        Returns:
            버전 이력 목록
        """
        versions = self.get_versions(source_id, include_inactive=True)

        history = []
        for v in versions[-limit:]:
            history.append({
                "version": v.version,
                "fingerprint": v.fingerprint,
                "created_at": v.created_at.isoformat(),
                "created_by": v.created_by,
                "change_description": v.change_description,
                "is_active": v.is_active,
                "field_count": len(v.schema.fields),
                "tags": v.tags,
            })

        return history

    def deprecate_version(self, source_id: str, version: int, reason: str = "") -> bool:
        """
        특정 버전 비활성화

        Args:
            source_id: 소스 ID
            version: 버전 번호
            reason: 비활성화 사유

        Returns:
            성공 여부
        """
        collection = self._get_collection()
        if collection:
            result = collection.update_one(
                {"source_id": source_id, "version": version},
                {
                    "$set": {
                        "is_active": False,
                        "deprecated_at": datetime.utcnow(),
                        "deprecated_reason": reason,
                    }
                }
            )
            if result.modified_count > 0:
                self.invalidate_cache(source_id)
                logger.info(f"Schema version deprecated: {source_id} v{version}")
                return True
        return False

    def check_compatibility(
        self,
        source_id: str,
        new_schema: Schema,
        mode: CompatibilityMode = None
    ) -> CompatibilityResult:
        """
        새 스키마와 현재 스키마의 호환성 검사 (등록 없이)

        Args:
            source_id: 소스 ID
            new_schema: 검사할 새 스키마
            mode: 호환성 모드 (None이면 마지막 등록 시 사용된 모드)

        Returns:
            CompatibilityResult
        """
        current = self.get_schema(source_id)
        if not current:
            return CompatibilityResult(
                is_compatible=True,
                issues=[],
                mode=mode or CompatibilityMode.BACKWARD
            )

        check_mode = mode or current.compatibility_mode
        return self._compatibility_checker.check_compatibility(
            current.schema, new_schema, check_mode
        )

    def check_transitive_compatibility(
        self,
        source_id: str,
        new_schema: Schema,
        mode: CompatibilityMode
    ) -> Dict[str, CompatibilityResult]:
        """
        모든 이전 버전과의 호환성 검사 (Transitive 모드용)

        Args:
            source_id: 소스 ID
            new_schema: 검사할 새 스키마
            mode: 호환성 모드

        Returns:
            {version: CompatibilityResult} 딕셔너리
        """
        results = {}
        versions = self.get_versions(source_id, include_inactive=False)

        for v in versions:
            result = self._compatibility_checker.check_compatibility(
                v.schema, new_schema, mode
            )
            results[f"v{v.version}"] = result

        return results

    def detect_drift(
        self,
        source_id: str,
        actual_data: List[Dict[str, Any]]
    ) -> CompatibilityResult:
        """
        실제 데이터와 등록된 스키마 간의 드리프트 감지

        Args:
            source_id: 소스 ID
            actual_data: 실제 데이터 샘플

        Returns:
            CompatibilityResult (드리프트가 있으면 이슈 포함)
        """
        current = self.get_schema(source_id)
        if not current:
            return CompatibilityResult(
                is_compatible=True,
                issues=[CompatibilityIssue(
                    field_name="_",
                    issue_type="no_schema",
                    severity="warning",
                    message=f"No schema registered for source {source_id}",
                )],
                mode=CompatibilityMode.FULL
            )

        from .detector import SchemaDetector

        detector = SchemaDetector()
        detected_schema = detector.detect_from_data(actual_data)

        return self._compatibility_checker.check_compatibility(
            current.schema,
            detected_schema,
            CompatibilityMode.FULL
        )

    def compare_schemas(
        self,
        source_id: str,
        version1: int,
        version2: int
    ) -> Dict[str, Any]:
        """
        두 버전의 스키마 비교

        Args:
            source_id: 소스 ID
            version1: 첫 번째 버전
            version2: 두 번째 버전

        Returns:
            비교 결과
        """
        v1 = self.get_schema(source_id, version1)
        v2 = self.get_schema(source_id, version2)

        if not v1 or not v2:
            return {"error": "Version not found"}

        v1_fields = {f.name: f for f in v1.schema.fields}
        v2_fields = {f.name: f for f in v2.schema.fields}

        v1_names = set(v1_fields.keys())
        v2_names = set(v2_fields.keys())

        comparison = {
            "source_id": source_id,
            "version1": {
                "version": v1.version,
                "fingerprint": v1.fingerprint,
                "field_count": len(v1.schema.fields),
            },
            "version2": {
                "version": v2.version,
                "fingerprint": v2.fingerprint,
                "field_count": len(v2.schema.fields),
            },
            "changes": {
                "added_fields": list(v2_names - v1_names),
                "removed_fields": list(v1_names - v2_names),
                "common_fields": list(v1_names & v2_names),
                "modified_fields": [],
            }
        }

        # 공통 필드 중 변경된 것 찾기
        for name in v1_names & v2_names:
            f1 = v1_fields[name]
            f2 = v2_fields[name]
            if f1.to_dict() != f2.to_dict():
                comparison["changes"]["modified_fields"].append({
                    "field": name,
                    "v1": f1.to_dict(),
                    "v2": f2.to_dict(),
                })

        return comparison

    def list_all_sources(self) -> List[Dict[str, Any]]:
        """
        등록된 모든 소스 목록 조회

        Returns:
            소스 정보 목록
        """
        collection = self._get_collection()
        if not collection:
            return list(self._cache.keys())

        pipeline = [
            {"$group": {
                "_id": "$source_id",
                "version_count": {"$sum": 1},
                "latest_version": {"$max": "$version"},
                "first_registered": {"$min": "$created_at"},
                "last_updated": {"$max": "$created_at"},
            }},
            {"$sort": {"last_updated": -1}}
        ]

        results = list(collection.aggregate(pipeline))
        return [
            {
                "source_id": r["_id"],
                "version_count": r["version_count"],
                "latest_version": r["latest_version"],
                "first_registered": r["first_registered"],
                "last_updated": r["last_updated"],
            }
            for r in results
        ]

    def export_schema(self, source_id: str, version: int = None) -> Optional[Dict[str, Any]]:
        """
        스키마를 내보내기 형식으로 반환

        Args:
            source_id: 소스 ID
            version: 버전 (None이면 최신)

        Returns:
            내보내기 가능한 형식의 스키마
        """
        schema_version = self.get_schema(source_id, version)
        if not schema_version:
            return None

        return {
            "source_id": source_id,
            "version": schema_version.version,
            "fingerprint": schema_version.fingerprint,
            "schema": schema_version.schema.to_dict(),
            "metadata": {
                "created_at": schema_version.created_at.isoformat(),
                "created_by": schema_version.created_by,
                "change_description": schema_version.change_description,
                "compatibility_mode": schema_version.compatibility_mode.value,
                "tags": schema_version.tags,
            },
            "exported_at": datetime.utcnow().isoformat(),
        }

    def import_schema(
        self,
        source_id: str,
        schema_data: Dict[str, Any],
        created_by: str = "import"
    ) -> Tuple[SchemaVersion, CompatibilityResult]:
        """
        외부에서 스키마 가져오기

        Args:
            source_id: 소스 ID
            schema_data: 가져올 스키마 데이터
            created_by: 가져오기 수행자

        Returns:
            (SchemaVersion, CompatibilityResult)
        """
        schema = Schema.from_dict(schema_data["schema"])
        change_description = f"Imported from {schema_data.get('source_id', 'unknown')}"

        return self.register_schema(
            source_id=source_id,
            schema=schema,
            created_by=created_by,
            change_description=change_description,
        )

    def _save_version(self, source_id: str, version: SchemaVersion):
        """버전 저장"""
        collection = self._get_collection()

        if collection:
            doc = version.to_dict()
            doc["source_id"] = source_id
            collection.insert_one(doc)
        else:
            # 인메모리 모드
            if source_id not in self._cache:
                self._cache[source_id] = []
            self._cache[source_id].append(version)

        # 캐시 무효화
        self.invalidate_cache(source_id)

    def _doc_to_version(self, doc: Dict[str, Any]) -> SchemaVersion:
        """MongoDB 문서를 SchemaVersion으로 변환"""
        return SchemaVersion.from_dict(doc)

    def invalidate_cache(self, source_id: str = None):
        """
        캐시 무효화

        Args:
            source_id: 특정 소스만 무효화 (None이면 전체)
        """
        if source_id:
            keys_to_remove = [k for k in self._cache.keys() if k.startswith(source_id)]
            for key in keys_to_remove:
                self._cache.pop(key, None)
        else:
            self._cache.clear()

    def health_check(self) -> Dict[str, Any]:
        """
        레지스트리 상태 확인

        Returns:
            상태 정보
        """
        collection = self._get_collection()

        if not collection:
            return {
                "status": "memory_mode",
                "cached_sources": len(self._cache),
            }

        try:
            total_schemas = collection.count_documents({})
            total_sources = len(collection.distinct("source_id"))

            return {
                "status": "healthy",
                "total_schemas": total_schemas,
                "total_sources": total_sources,
                "cached_sources": len(self._cache),
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
            }


# 기존 호환성을 위한 별칭
from .models import FieldSchema, FieldType, Schema, CompatibilityIssue

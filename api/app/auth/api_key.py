"""
API Key 기반 인증
간단하고 빠른 서비스 간 통신용
"""

import os
import secrets
import hashlib
from datetime import datetime, timedelta
from typing import Optional, List
from dataclasses import dataclass, field

from fastapi import HTTPException, Security, Depends, Request
from fastapi.security import APIKeyHeader, APIKeyQuery
import logging

logger = logging.getLogger(__name__)

# API Key 헤더/쿼리 정의
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
API_KEY_QUERY = APIKeyQuery(name="api_key", auto_error=False)


@dataclass
class APIKeyInfo:
    """API Key 정보"""
    key_id: str
    name: str
    hashed_key: str
    created_at: datetime
    expires_at: Optional[datetime] = None
    scopes: List[str] = field(default_factory=lambda: ["read"])
    is_active: bool = True
    last_used_at: Optional[datetime] = None
    rate_limit: int = 1000  # 시간당 요청 제한

    def is_expired(self) -> bool:
        """만료 여부 확인"""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at

    def has_scope(self, scope: str) -> bool:
        """권한 확인"""
        return scope in self.scopes or "admin" in self.scopes


class APIKeyAuth:
    """API Key 인증 관리자"""

    # 환경 변수에서 마스터 키 로드 (쉼표로 구분된 여러 키 지원)
    MASTER_KEYS: List[str] = []

    # 메모리 내 API 키 저장소 (프로덕션에서는 DB 사용)
    _keys: dict = {}

    @classmethod
    def init(cls):
        """초기화 - 환경 변수에서 API 키 로드"""
        master_keys_env = os.getenv("API_MASTER_KEYS", "")
        if master_keys_env:
            cls.MASTER_KEYS = [k.strip() for k in master_keys_env.split(",") if k.strip()]

        env = os.getenv("ENV", "development")

        # 프로덕션 환경에서 API 키 필수
        if env == "production" and not cls.MASTER_KEYS:
            raise RuntimeError(
                "프로덕션 환경에서는 API_MASTER_KEYS 환경변수가 필수입니다. "
                "쉼표로 구분된 API 키 목록을 설정하세요."
            )

        # 개발 환경에서만 자동 생성된 임시 키 사용
        if not cls.MASTER_KEYS and env == "development":
            # 실행 시마다 새로운 랜덤 키 생성 (하드코딩 방지)
            temp_key = f"dev-{secrets.token_urlsafe(16)}"
            cls.MASTER_KEYS = [temp_key]
            logger.warning(f"개발용 임시 API 키 생성됨: {temp_key[:20]}...")
            logger.warning("프로덕션에서는 API_MASTER_KEYS 환경변수 설정 필요!")

    @classmethod
    def generate_key(cls) -> str:
        """새 API 키 생성"""
        return f"craw_{secrets.token_urlsafe(32)}"

    @classmethod
    def hash_key(cls, key: str) -> str:
        """API 키 해싱"""
        return hashlib.sha256(key.encode()).hexdigest()

    @classmethod
    def register_key(
        cls,
        name: str,
        scopes: Optional[List[str]] = None,
        expires_in_days: Optional[int] = None,
        rate_limit: int = 1000
    ) -> tuple:
        """
        새 API 키 등록

        Returns:
            (key_id, raw_api_key) - raw_api_key는 한 번만 반환됨
        """
        raw_key = cls.generate_key()
        hashed_key = cls.hash_key(raw_key)
        key_id = f"key_{secrets.token_hex(8)}"

        expires_at = None
        if expires_in_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_in_days)

        key_info = APIKeyInfo(
            key_id=key_id,
            name=name,
            hashed_key=hashed_key,
            created_at=datetime.utcnow(),
            expires_at=expires_at,
            scopes=scopes or ["read"],
            rate_limit=rate_limit
        )

        cls._keys[hashed_key] = key_info

        logger.info(f"API 키 등록: {key_id} ({name})")

        return key_id, raw_key

    @classmethod
    def validate_key(cls, api_key: str) -> Optional[APIKeyInfo]:
        """API 키 검증"""
        if not api_key:
            return None

        # 마스터 키 확인
        if api_key in cls.MASTER_KEYS:
            return APIKeyInfo(
                key_id="master",
                name="Master Key",
                hashed_key="",
                created_at=datetime.utcnow(),
                scopes=["admin", "read", "write", "delete"],
                is_active=True
            )

        # 등록된 키 확인
        hashed = cls.hash_key(api_key)
        key_info = cls._keys.get(hashed)

        if not key_info:
            return None

        if not key_info.is_active:
            return None

        if key_info.is_expired():
            return None

        # 마지막 사용 시간 업데이트
        key_info.last_used_at = datetime.utcnow()

        return key_info

    @classmethod
    def revoke_key(cls, key_id: str) -> bool:
        """API 키 폐기"""
        for hashed, info in cls._keys.items():
            if info.key_id == key_id:
                info.is_active = False
                logger.info(f"API 키 폐기: {key_id}")
                return True
        return False

    @classmethod
    def list_keys(cls) -> List[dict]:
        """등록된 API 키 목록 (해시 제외)"""
        return [
            {
                "key_id": info.key_id,
                "name": info.name,
                "scopes": info.scopes,
                "created_at": info.created_at.isoformat(),
                "expires_at": info.expires_at.isoformat() if info.expires_at else None,
                "is_active": info.is_active,
                "last_used_at": info.last_used_at.isoformat() if info.last_used_at else None
            }
            for info in cls._keys.values()
        ]


# 초기화 실행
APIKeyAuth.init()


async def get_api_key(
    api_key_header: str = Security(API_KEY_HEADER),
    api_key_query: str = Security(API_KEY_QUERY),
) -> APIKeyInfo:
    """
    API Key 의존성 주입
    헤더 또는 쿼리 파라미터에서 API 키 추출 및 검증
    """
    api_key = api_key_header or api_key_query

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "API 키 누락",
                "message": "X-API-Key 헤더 또는 api_key 쿼리 파라미터를 제공하세요"
            }
        )

    key_info = APIKeyAuth.validate_key(api_key)

    if not key_info:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "유효하지 않은 API 키",
                "message": "API 키가 유효하지 않거나 만료되었습니다"
            }
        )

    return key_info


def require_scope(required_scope: str):
    """특정 권한이 필요한 엔드포인트용 의존성"""
    async def scope_checker(key_info: APIKeyInfo = Depends(get_api_key)):
        if not key_info.has_scope(required_scope):
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "권한 없음",
                    "message": f"'{required_scope}' 권한이 필요합니다",
                    "your_scopes": key_info.scopes
                }
            )
        return key_info

    return scope_checker

"""
통합 인증 의존성
API Key 또는 JWT 토큰 중 하나로 인증 가능
"""

import os
from typing import Optional, Union
from dataclasses import dataclass

from fastapi import HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader, APIKeyQuery
import logging

from .api_key import APIKeyAuth, APIKeyInfo, API_KEY_HEADER, API_KEY_QUERY
from .jwt_auth import JWTAuth, User, bearer_scheme

logger = logging.getLogger(__name__)


@dataclass
class AuthContext:
    """인증 컨텍스트 (API Key 또는 JWT 둘 다 지원)"""
    auth_type: str  # "api_key" | "jwt" | "none"
    user_id: Optional[str] = None
    role: str = "user"
    scopes: list = None
    api_key_info: Optional[APIKeyInfo] = None
    jwt_user: Optional[User] = None

    def __post_init__(self):
        if self.scopes is None:
            self.scopes = []

    def has_scope(self, scope: str) -> bool:
        """권한 확인"""
        return scope in self.scopes or "admin" in self.scopes or self.role == "admin"

    @property
    def is_authenticated(self) -> bool:
        """인증 여부"""
        return self.auth_type != "none"

    @property
    def is_admin(self) -> bool:
        """관리자 여부"""
        return self.role == "admin" or "admin" in self.scopes


class AuthMode:
    """인증 모드 설정"""
    # 환경 변수로 인증 모드 설정
    # "required": 모든 요청에 인증 필요
    # "optional": 인증 선택 (일부 엔드포인트만 필요)
    # "disabled": 인증 비활성화 (개발용)
    MODE = os.getenv("AUTH_MODE", "required")

    # 인증 제외 경로 (항상 인증 없이 접근 가능)
    EXEMPT_PATHS = [
        "/",
        "/health",
        "/docs",
        "/redoc",
        "/openapi.json",
    ]

    @classmethod
    def is_exempt(cls, path: str) -> bool:
        """인증 제외 경로 확인"""
        return any(path == exempt or path.startswith(exempt + "/") for exempt in cls.EXEMPT_PATHS)


async def get_auth_context(
    request: Request,
    api_key_header: str = Depends(lambda: None),
    api_key_query: str = Depends(lambda: None),
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)
) -> AuthContext:
    """
    통합 인증 컨텍스트 가져오기
    API Key 또는 JWT 토큰 중 하나로 인증
    """
    # API Key 먼저 확인 (헤더)
    api_key = request.headers.get("X-API-Key")

    # 쿼리 파라미터에서도 확인
    if not api_key:
        api_key = request.query_params.get("api_key")

    # API Key 인증 시도
    if api_key:
        key_info = APIKeyAuth.validate_key(api_key)
        if key_info:
            return AuthContext(
                auth_type="api_key",
                user_id=key_info.key_id,
                role="admin" if "admin" in key_info.scopes else "user",
                scopes=key_info.scopes,
                api_key_info=key_info
            )

    # JWT 토큰 인증 시도
    if credentials:
        token_payload = JWTAuth.verify_access_token(credentials.credentials)
        if token_payload:
            user = JWTAuth.get_user(token_payload.sub)
            if user and user.is_active:
                return AuthContext(
                    auth_type="jwt",
                    user_id=user.id,
                    role=user.role,
                    scopes=user.scopes,
                    jwt_user=user
                )

    # 인증 없음
    return AuthContext(auth_type="none")


async def require_auth(
    request: Request,
    auth_context: AuthContext = Depends(get_auth_context)
) -> AuthContext:
    """
    인증 필수 의존성
    인증되지 않은 요청은 401 에러
    """
    # 제외 경로 확인
    if AuthMode.is_exempt(request.url.path):
        return auth_context

    # 인증 모드가 disabled면 개발 환경에서만 통과
    if AuthMode.MODE == "disabled":
        env = os.getenv("ENV", "production")
        if env == "production":
            logger.error("AUTH_MODE=disabled is not allowed in production")
            raise HTTPException(
                status_code=500,
                detail="AUTH_MODE=disabled는 프로덕션 환경에서 사용할 수 없습니다"
            )
        logger.warning("AUTH_MODE=disabled: 인증 우회 (개발 환경)")
        return AuthContext(
            auth_type="none",
            role="admin",
            scopes=["admin", "read", "write", "delete"]
        )

    # 인증 확인
    if not auth_context.is_authenticated:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "인증 필요",
                "message": "API Key(X-API-Key 헤더) 또는 Bearer 토큰이 필요합니다"
            },
            headers={"WWW-Authenticate": "Bearer, ApiKey"}
        )

    return auth_context


async def require_admin(
    auth_context: AuthContext = Depends(require_auth)
) -> AuthContext:
    """관리자 권한 필수 의존성"""
    if not auth_context.is_admin:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "관리자 권한 필요",
                "message": "이 작업은 관리자만 수행할 수 있습니다"
            }
        )
    return auth_context


def require_scope(required_scope: str):
    """특정 권한 필수 의존성 팩토리"""
    async def scope_checker(
        auth_context: AuthContext = Depends(require_auth)
    ) -> AuthContext:
        if not auth_context.has_scope(required_scope):
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "권한 없음",
                    "message": f"'{required_scope}' 권한이 필요합니다",
                    "your_scopes": auth_context.scopes
                }
            )
        return auth_context

    return scope_checker


async def optional_auth(
    auth_context: AuthContext = Depends(get_auth_context)
) -> AuthContext:
    """
    선택적 인증 의존성
    인증 없이도 접근 가능하지만, 인증 시 추가 기능 제공
    """
    return auth_context

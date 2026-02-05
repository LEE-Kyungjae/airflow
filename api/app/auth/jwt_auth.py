"""
JWT 토큰 기반 인증
사용자 인증 및 세션 관리용
"""

import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from dataclasses import dataclass

from fastapi import HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
import logging

logger = logging.getLogger(__name__)

# Bearer 토큰 스키마
bearer_scheme = HTTPBearer(auto_error=False)


@dataclass
class TokenPayload:
    """JWT 토큰 페이로드"""
    sub: str  # 사용자 ID
    exp: datetime  # 만료 시간
    iat: datetime  # 발급 시간
    role: str = "user"
    scopes: list = None

    def __post_init__(self):
        if self.scopes is None:
            self.scopes = ["read"]


@dataclass
class User:
    """사용자 정보"""
    id: str
    username: str
    email: Optional[str] = None
    role: str = "user"
    scopes: list = None
    is_active: bool = True

    def __post_init__(self):
        if self.scopes is None:
            self.scopes = ["read"]

    def has_scope(self, scope: str) -> bool:
        """권한 확인"""
        return scope in self.scopes or self.role == "admin"


class JWTAuth:
    """JWT 인증 관리자"""

    # 설정
    SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("JWT_REFRESH_EXPIRE_DAYS", "7"))

    # 간단한 사용자 저장소 (프로덕션에서는 DB 사용)
    _users: Dict[str, User] = {}
    _initialized: bool = False

    @classmethod
    def init(cls):
        """초기화"""
        import secrets as sec

        env = os.getenv("ENV", "development")

        # 프로덕션 환경에서 JWT 시크릿 키 필수
        if env == "production":
            if not cls.SECRET_KEY:
                raise RuntimeError(
                    "프로덕션 환경에서는 JWT_SECRET_KEY 환경변수가 필수입니다. "
                    "최소 32자 이상의 랜덤 문자열을 설정하세요."
                )
            if len(cls.SECRET_KEY) < 32:
                raise RuntimeError(
                    "JWT_SECRET_KEY는 최소 32자 이상이어야 합니다."
                )

        # 개발 환경에서 시크릿 키가 없으면 임시 생성
        if not cls.SECRET_KEY and env == "development":
            cls.SECRET_KEY = sec.token_urlsafe(32)
            logger.warning("개발용 임시 JWT 시크릿 키 생성됨 (서버 재시작 시 변경)")
            logger.warning("프로덕션에서는 JWT_SECRET_KEY 환경변수 설정 필요!")

        # 기본 관리자 계정 (개발용)
        if env == "development":
            cls._users["admin"] = User(
                id="admin",
                username="admin",
                email="admin@localhost",
                role="admin",
                scopes=["admin", "read", "write", "delete"]
            )

        cls._initialized = True

    @classmethod
    def create_access_token(
        cls,
        user_id: str,
        role: str = "user",
        scopes: Optional[list] = None,
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """액세스 토큰 생성"""
        if expires_delta is None:
            expires_delta = timedelta(minutes=cls.ACCESS_TOKEN_EXPIRE_MINUTES)

        now = datetime.utcnow()
        expire = now + expires_delta

        payload = {
            "sub": user_id,
            "role": role,
            "scopes": scopes or ["read"],
            "iat": now,
            "exp": expire,
            "type": "access"
        }

        token = jwt.encode(payload, cls.SECRET_KEY, algorithm=cls.ALGORITHM)

        logger.debug(f"액세스 토큰 생성: user={user_id}, expires={expire}")

        return token

    @classmethod
    def create_refresh_token(cls, user_id: str) -> str:
        """리프레시 토큰 생성"""
        now = datetime.utcnow()
        expire = now + timedelta(days=cls.REFRESH_TOKEN_EXPIRE_DAYS)

        payload = {
            "sub": user_id,
            "iat": now,
            "exp": expire,
            "type": "refresh"
        }

        return jwt.encode(payload, cls.SECRET_KEY, algorithm=cls.ALGORITHM)

    @classmethod
    def decode_token(cls, token: str) -> Optional[Dict[str, Any]]:
        """토큰 디코딩 및 검증"""
        try:
            payload = jwt.decode(
                token,
                cls.SECRET_KEY,
                algorithms=[cls.ALGORITHM]
            )
            return payload

        except jwt.ExpiredSignatureError:
            logger.debug("토큰 만료")
            return None

        except jwt.InvalidTokenError as e:
            logger.debug(f"유효하지 않은 토큰: {e}")
            return None

    @classmethod
    def verify_access_token(cls, token: str) -> Optional[TokenPayload]:
        """액세스 토큰 검증"""
        payload = cls.decode_token(token)

        if not payload:
            return None

        if payload.get("type") != "access":
            return None

        return TokenPayload(
            sub=payload["sub"],
            exp=datetime.fromtimestamp(payload["exp"]),
            iat=datetime.fromtimestamp(payload["iat"]),
            role=payload.get("role", "user"),
            scopes=payload.get("scopes", ["read"])
        )

    @classmethod
    def refresh_access_token(cls, refresh_token: str) -> Optional[str]:
        """리프레시 토큰으로 새 액세스 토큰 발급"""
        payload = cls.decode_token(refresh_token)

        if not payload:
            return None

        if payload.get("type") != "refresh":
            return None

        user_id = payload["sub"]

        # 사용자 정보 조회
        user = cls._users.get(user_id)
        if not user or not user.is_active:
            return None

        return cls.create_access_token(
            user_id=user.id,
            role=user.role,
            scopes=user.scopes
        )

    @classmethod
    def get_user(cls, user_id: str) -> Optional[User]:
        """사용자 조회"""
        return cls._users.get(user_id)

    @classmethod
    def register_user(
        cls,
        user_id: str,
        username: str,
        email: Optional[str] = None,
        role: str = "user",
        scopes: Optional[list] = None
    ) -> User:
        """사용자 등록"""
        user = User(
            id=user_id,
            username=username,
            email=email,
            role=role,
            scopes=scopes or ["read"]
        )
        cls._users[user_id] = user
        logger.info(f"사용자 등록: {user_id}")
        return user


# 초기화 실행
JWTAuth.init()


# 의존성 함수들
async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)
) -> User:
    """현재 인증된 사용자 가져오기"""
    if not credentials:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "인증 필요",
                "message": "Bearer 토큰을 제공하세요"
            },
            headers={"WWW-Authenticate": "Bearer"}
        )

    token_payload = JWTAuth.verify_access_token(credentials.credentials)

    if not token_payload:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "유효하지 않은 토큰",
                "message": "토큰이 만료되었거나 유효하지 않습니다"
            },
            headers={"WWW-Authenticate": "Bearer"}
        )

    user = JWTAuth.get_user(token_payload.sub)

    if not user:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "사용자 없음",
                "message": "사용자를 찾을 수 없습니다"
            }
        )

    if not user.is_active:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "비활성 계정",
                "message": "계정이 비활성화되었습니다"
            }
        )

    return user


async def get_optional_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)
) -> Optional[User]:
    """선택적 사용자 인증 (인증 없이도 접근 가능)"""
    if not credentials:
        return None

    try:
        return await get_current_user(credentials)
    except HTTPException:
        return None


def create_access_token(
    user_id: str,
    role: str = "user",
    scopes: Optional[list] = None,
    expires_delta: Optional[timedelta] = None
) -> str:
    """액세스 토큰 생성 헬퍼"""
    return JWTAuth.create_access_token(user_id, role, scopes, expires_delta)

"""
인증 관련 API 엔드포인트
로그인, 토큰 갱신, API 키 관리
"""

import os
from datetime import timedelta
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Depends, Body
from pydantic import BaseModel, Field
import logging

from app.auth import (
    APIKeyAuth,
    JWTAuth,
    require_auth,
    require_admin,
)
from app.auth.dependencies import AuthContext

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================
# Request/Response 모델
# ============================================

class LoginRequest(BaseModel):
    """로그인 요청"""
    username: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=1, max_length=100)


class TokenResponse(BaseModel):
    """토큰 응답"""
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: int  # 초 단위


class RefreshTokenRequest(BaseModel):
    """토큰 갱신 요청"""
    refresh_token: str


class CreateAPIKeyRequest(BaseModel):
    """API 키 생성 요청"""
    name: str = Field(..., min_length=1, max_length=100)
    scopes: List[str] = Field(default=["read"])
    expires_in_days: Optional[int] = Field(None, ge=1, le=365)
    rate_limit: int = Field(default=1000, ge=1, le=100000)


class APIKeyResponse(BaseModel):
    """API 키 응답"""
    key_id: str
    api_key: str  # 한 번만 표시
    name: str
    scopes: List[str]
    expires_at: Optional[str]
    message: str = "API 키를 안전하게 보관하세요. 이 키는 다시 표시되지 않습니다."


class APIKeyListItem(BaseModel):
    """API 키 목록 항목"""
    key_id: str
    name: str
    scopes: List[str]
    created_at: str
    expires_at: Optional[str]
    is_active: bool
    last_used_at: Optional[str]


# ============================================
# 로그인/토큰 엔드포인트
# ============================================

@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest):
    """
    로그인하여 JWT 토큰 획득

    개발 환경에서는 admin/admin123으로 로그인 가능
    """
    # 간단한 인증 (프로덕션에서는 DB 조회 및 비밀번호 해싱 필요)
    admin_password = os.getenv("ADMIN_PASSWORD", "admin123")

    if request.username == "admin" and request.password == admin_password:
        access_token = JWTAuth.create_access_token(
            user_id="admin",
            role="admin",
            scopes=["admin", "read", "write", "delete"]
        )
        refresh_token = JWTAuth.create_refresh_token(user_id="admin")

        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            expires_in=JWTAuth.ACCESS_TOKEN_EXPIRE_MINUTES * 60
        )

    raise HTTPException(
        status_code=401,
        detail={
            "error": "인증 실패",
            "message": "사용자명 또는 비밀번호가 잘못되었습니다"
        }
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(request: RefreshTokenRequest):
    """리프레시 토큰으로 새 액세스 토큰 발급"""
    new_access_token = JWTAuth.refresh_access_token(request.refresh_token)

    if not new_access_token:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "토큰 갱신 실패",
                "message": "리프레시 토큰이 만료되었거나 유효하지 않습니다"
            }
        )

    return TokenResponse(
        access_token=new_access_token,
        token_type="bearer",
        expires_in=JWTAuth.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )


@router.get("/me")
async def get_current_user_info(
    auth_context: AuthContext = Depends(require_auth)
):
    """현재 인증된 사용자 정보"""
    return {
        "auth_type": auth_context.auth_type,
        "user_id": auth_context.user_id,
        "role": auth_context.role,
        "scopes": auth_context.scopes,
        "is_admin": auth_context.is_admin
    }


# ============================================
# API 키 관리 엔드포인트
# ============================================

@router.post("/api-keys", response_model=APIKeyResponse)
async def create_api_key(
    request: CreateAPIKeyRequest,
    auth_context: AuthContext = Depends(require_admin)
):
    """
    새 API 키 생성 (관리자 전용)

    생성된 API 키는 한 번만 표시되므로 안전하게 보관하세요.
    """
    key_id, raw_key = APIKeyAuth.register_key(
        name=request.name,
        scopes=request.scopes,
        expires_in_days=request.expires_in_days,
        rate_limit=request.rate_limit
    )

    expires_at = None
    if request.expires_in_days:
        from datetime import datetime, timedelta
        expires_at = (datetime.utcnow() + timedelta(days=request.expires_in_days)).isoformat()

    return APIKeyResponse(
        key_id=key_id,
        api_key=raw_key,
        name=request.name,
        scopes=request.scopes,
        expires_at=expires_at
    )


@router.get("/api-keys", response_model=List[APIKeyListItem])
async def list_api_keys(
    auth_context: AuthContext = Depends(require_admin)
):
    """API 키 목록 조회 (관리자 전용)"""
    return APIKeyAuth.list_keys()


@router.delete("/api-keys/{key_id}")
async def revoke_api_key(
    key_id: str,
    auth_context: AuthContext = Depends(require_admin)
):
    """API 키 폐기 (관리자 전용)"""
    success = APIKeyAuth.revoke_key(key_id)

    if not success:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "API 키 없음",
                "message": f"key_id '{key_id}'를 찾을 수 없습니다"
            }
        )

    return {
        "success": True,
        "message": f"API 키 '{key_id}'가 폐기되었습니다"
    }


# ============================================
# 인증 상태 확인
# ============================================

@router.get("/verify")
async def verify_auth(
    auth_context: AuthContext = Depends(require_auth)
):
    """인증 상태 확인"""
    return {
        "authenticated": True,
        "auth_type": auth_context.auth_type,
        "user_id": auth_context.user_id,
        "scopes": auth_context.scopes
    }
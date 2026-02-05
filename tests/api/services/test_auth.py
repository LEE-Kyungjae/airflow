"""
Tests for authentication system (JWT and API Key).

Covers:
- JWTAuth token creation and validation
- APIKeyAuth key management
- AuthContext and dependencies
- Token expiration handling
- Scope-based authorization
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, AsyncMock
import os


class TestJWTAuthTokenCreation:
    """Tests for JWT token creation."""

    def test_create_access_token(self):
        """Test creating a valid access token."""
        from api.app.auth.jwt_auth import JWTAuth

        token = JWTAuth.create_access_token(
            user_id="test_user",
            role="user",
            scopes=["read", "write"]
        )

        assert token is not None
        assert isinstance(token, str)
        assert len(token) > 50  # JWT tokens are quite long

    def test_create_access_token_with_custom_expiry(self):
        """Test creating access token with custom expiry."""
        from api.app.auth.jwt_auth import JWTAuth

        token = JWTAuth.create_access_token(
            user_id="test_user",
            expires_delta=timedelta(hours=2)
        )

        assert token is not None

    def test_create_refresh_token(self):
        """Test creating a refresh token."""
        from api.app.auth.jwt_auth import JWTAuth

        token = JWTAuth.create_refresh_token(user_id="test_user")

        assert token is not None
        assert isinstance(token, str)


class TestJWTAuthTokenValidation:
    """Tests for JWT token validation."""

    def test_verify_valid_access_token(self, valid_jwt_token):
        """Test verifying a valid access token."""
        from api.app.auth.jwt_auth import JWTAuth

        payload = JWTAuth.verify_access_token(valid_jwt_token)

        assert payload is not None
        assert payload.sub == "test_user"
        assert payload.role == "admin"

    def test_verify_invalid_token(self):
        """Test verifying an invalid token."""
        from api.app.auth.jwt_auth import JWTAuth

        result = JWTAuth.verify_access_token("invalid.token.here")

        assert result is None

    def test_verify_expired_token(self, expired_jwt_token):
        """Test verifying an expired token."""
        from api.app.auth.jwt_auth import JWTAuth

        result = JWTAuth.verify_access_token(expired_jwt_token)

        assert result is None

    def test_decode_token_success(self, valid_jwt_token):
        """Test decoding a valid token."""
        from api.app.auth.jwt_auth import JWTAuth

        payload = JWTAuth.decode_token(valid_jwt_token)

        assert payload is not None
        assert "sub" in payload
        assert "exp" in payload

    def test_decode_token_invalid(self):
        """Test decoding an invalid token returns None."""
        from api.app.auth.jwt_auth import JWTAuth

        result = JWTAuth.decode_token("not.a.valid.jwt")

        assert result is None


class TestJWTAuthRefreshToken:
    """Tests for JWT refresh token functionality."""

    def test_refresh_access_token_valid(self):
        """Test refreshing access token with valid refresh token."""
        from api.app.auth.jwt_auth import JWTAuth

        # Create a refresh token
        refresh_token = JWTAuth.create_refresh_token(user_id="admin")

        # Refresh should work for admin user (which exists)
        new_access = JWTAuth.refresh_access_token(refresh_token)

        assert new_access is not None

    def test_refresh_access_token_invalid(self):
        """Test refreshing with invalid token returns None."""
        from api.app.auth.jwt_auth import JWTAuth

        result = JWTAuth.refresh_access_token("invalid.refresh.token")

        assert result is None

    def test_refresh_with_access_token_fails(self, valid_jwt_token):
        """Test that access token cannot be used as refresh token."""
        from api.app.auth.jwt_auth import JWTAuth

        result = JWTAuth.refresh_access_token(valid_jwt_token)

        assert result is None


class TestJWTAuthUserManagement:
    """Tests for JWT user management."""

    def test_get_user_exists(self):
        """Test getting existing user."""
        from api.app.auth.jwt_auth import JWTAuth

        user = JWTAuth.get_user("admin")

        assert user is not None
        assert user.id == "admin"
        assert user.role == "admin"

    def test_get_user_not_exists(self):
        """Test getting non-existent user."""
        from api.app.auth.jwt_auth import JWTAuth

        user = JWTAuth.get_user("nonexistent_user")

        assert user is None

    def test_register_user(self):
        """Test registering a new user."""
        from api.app.auth.jwt_auth import JWTAuth

        user = JWTAuth.register_user(
            user_id="new_user",
            username="new_user",
            email="new@example.com",
            role="user",
            scopes=["read"]
        )

        assert user.id == "new_user"
        assert user.email == "new@example.com"

        # Clean up
        JWTAuth._users.pop("new_user", None)


class TestTokenPayload:
    """Tests for TokenPayload dataclass."""

    def test_token_payload_creation(self):
        """Test TokenPayload creation."""
        from api.app.auth.jwt_auth import TokenPayload

        payload = TokenPayload(
            sub="user123",
            exp=datetime.utcnow() + timedelta(hours=1),
            iat=datetime.utcnow(),
            role="admin",
            scopes=["read", "write"]
        )

        assert payload.sub == "user123"
        assert payload.role == "admin"
        assert len(payload.scopes) == 2

    def test_token_payload_default_scopes(self):
        """Test TokenPayload default scopes."""
        from api.app.auth.jwt_auth import TokenPayload

        payload = TokenPayload(
            sub="user",
            exp=datetime.utcnow(),
            iat=datetime.utcnow()
        )

        assert payload.scopes == ["read"]


class TestUser:
    """Tests for User dataclass."""

    def test_user_creation(self):
        """Test User creation."""
        from api.app.auth.jwt_auth import User

        user = User(
            id="user1",
            username="testuser",
            email="test@example.com",
            role="admin",
            scopes=["admin", "read", "write"]
        )

        assert user.id == "user1"
        assert user.is_active is True

    def test_user_has_scope(self):
        """Test User scope checking."""
        from api.app.auth.jwt_auth import User

        user = User(
            id="user1",
            username="testuser",
            scopes=["read", "write"]
        )

        assert user.has_scope("read") is True
        assert user.has_scope("write") is True
        assert user.has_scope("admin") is False

    def test_user_admin_has_all_scopes(self):
        """Test that admin role has all scopes."""
        from api.app.auth.jwt_auth import User

        user = User(
            id="admin",
            username="admin",
            role="admin",
            scopes=[]
        )

        assert user.has_scope("anything") is True


class TestAPIKeyAuth:
    """Tests for API Key authentication."""

    def test_generate_key_format(self):
        """Test generated API key format."""
        from api.app.auth.api_key import APIKeyAuth

        key = APIKeyAuth.generate_key()

        assert key.startswith("craw_")
        assert len(key) > 20

    def test_hash_key(self):
        """Test API key hashing."""
        from api.app.auth.api_key import APIKeyAuth

        key = "test_key_123"
        hashed = APIKeyAuth.hash_key(key)

        assert hashed != key
        assert len(hashed) == 64  # SHA256 hash length

    def test_hash_key_consistency(self):
        """Test that hashing is consistent."""
        from api.app.auth.api_key import APIKeyAuth

        key = "test_key"
        hash1 = APIKeyAuth.hash_key(key)
        hash2 = APIKeyAuth.hash_key(key)

        assert hash1 == hash2

    def test_register_key(self):
        """Test registering a new API key."""
        from api.app.auth.api_key import APIKeyAuth

        key_id, raw_key = APIKeyAuth.register_key(
            name="Test Key",
            scopes=["read", "write"],
            expires_in_days=30,
            rate_limit=500
        )

        assert key_id.startswith("key_")
        assert raw_key.startswith("craw_")

        # Clean up
        APIKeyAuth.revoke_key(key_id)

    def test_validate_master_key(self, valid_api_key):
        """Test validating master key."""
        from api.app.auth.api_key import APIKeyAuth

        key_info = APIKeyAuth.validate_key(valid_api_key)

        assert key_info is not None
        assert key_info.key_id == "master"
        assert "admin" in key_info.scopes

    def test_validate_invalid_key(self, invalid_api_key):
        """Test validating invalid key returns None."""
        from api.app.auth.api_key import APIKeyAuth

        result = APIKeyAuth.validate_key(invalid_api_key)

        assert result is None

    def test_validate_empty_key(self):
        """Test validating empty key returns None."""
        from api.app.auth.api_key import APIKeyAuth

        result = APIKeyAuth.validate_key("")

        assert result is None

    def test_validate_none_key(self):
        """Test validating None key returns None."""
        from api.app.auth.api_key import APIKeyAuth

        result = APIKeyAuth.validate_key(None)

        assert result is None

    def test_validate_registered_key(self):
        """Test validating a registered key."""
        from api.app.auth.api_key import APIKeyAuth

        key_id, raw_key = APIKeyAuth.register_key(
            name="Registered Key",
            scopes=["read"]
        )

        key_info = APIKeyAuth.validate_key(raw_key)

        assert key_info is not None
        assert key_info.name == "Registered Key"

        # Clean up
        APIKeyAuth.revoke_key(key_id)

    def test_revoke_key(self):
        """Test revoking an API key."""
        from api.app.auth.api_key import APIKeyAuth

        key_id, raw_key = APIKeyAuth.register_key(name="To Revoke")

        # Verify key works
        assert APIKeyAuth.validate_key(raw_key) is not None

        # Revoke
        result = APIKeyAuth.revoke_key(key_id)
        assert result is True

        # Verify key no longer works
        assert APIKeyAuth.validate_key(raw_key) is None

    def test_revoke_nonexistent_key(self):
        """Test revoking non-existent key returns False."""
        from api.app.auth.api_key import APIKeyAuth

        result = APIKeyAuth.revoke_key("nonexistent_key_id")

        assert result is False

    def test_list_keys(self):
        """Test listing registered keys."""
        from api.app.auth.api_key import APIKeyAuth

        key_id, _ = APIKeyAuth.register_key(name="List Test Key")

        keys = APIKeyAuth.list_keys()

        assert isinstance(keys, list)
        key_ids = [k["key_id"] for k in keys]
        assert key_id in key_ids

        # Clean up
        APIKeyAuth.revoke_key(key_id)


class TestAPIKeyInfo:
    """Tests for APIKeyInfo dataclass."""

    def test_api_key_info_creation(self):
        """Test APIKeyInfo creation."""
        from api.app.auth.api_key import APIKeyInfo

        info = APIKeyInfo(
            key_id="key_123",
            name="Test Key",
            hashed_key="abc123",
            created_at=datetime.utcnow(),
            scopes=["read", "write"],
            rate_limit=1000
        )

        assert info.key_id == "key_123"
        assert info.is_active is True
        assert info.rate_limit == 1000

    def test_api_key_info_is_expired_no_expiry(self):
        """Test is_expired when no expiry set."""
        from api.app.auth.api_key import APIKeyInfo

        info = APIKeyInfo(
            key_id="key",
            name="Test",
            hashed_key="hash",
            created_at=datetime.utcnow(),
            expires_at=None
        )

        assert info.is_expired() is False

    def test_api_key_info_is_expired_future(self):
        """Test is_expired with future expiry."""
        from api.app.auth.api_key import APIKeyInfo

        info = APIKeyInfo(
            key_id="key",
            name="Test",
            hashed_key="hash",
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=30)
        )

        assert info.is_expired() is False

    def test_api_key_info_is_expired_past(self):
        """Test is_expired with past expiry."""
        from api.app.auth.api_key import APIKeyInfo

        info = APIKeyInfo(
            key_id="key",
            name="Test",
            hashed_key="hash",
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() - timedelta(days=1)
        )

        assert info.is_expired() is True

    def test_api_key_info_has_scope(self):
        """Test has_scope checking."""
        from api.app.auth.api_key import APIKeyInfo

        info = APIKeyInfo(
            key_id="key",
            name="Test",
            hashed_key="hash",
            created_at=datetime.utcnow(),
            scopes=["read", "write"]
        )

        assert info.has_scope("read") is True
        assert info.has_scope("write") is True
        assert info.has_scope("admin") is False
        assert info.has_scope("delete") is False

    def test_api_key_info_admin_has_all_scopes(self):
        """Test that admin scope grants all access."""
        from api.app.auth.api_key import APIKeyInfo

        info = APIKeyInfo(
            key_id="key",
            name="Test",
            hashed_key="hash",
            created_at=datetime.utcnow(),
            scopes=["admin"]
        )

        assert info.has_scope("anything") is True


class TestAuthContext:
    """Tests for AuthContext dataclass."""

    def test_auth_context_creation(self):
        """Test AuthContext creation."""
        from api.app.auth.dependencies import AuthContext

        context = AuthContext(
            auth_type="jwt",
            user_id="user123",
            role="user",
            scopes=["read", "write"]
        )

        assert context.auth_type == "jwt"
        assert context.is_authenticated is True
        assert context.is_admin is False

    def test_auth_context_none_auth(self):
        """Test AuthContext with no authentication."""
        from api.app.auth.dependencies import AuthContext

        context = AuthContext(auth_type="none")

        assert context.is_authenticated is False

    def test_auth_context_admin(self):
        """Test AuthContext admin detection."""
        from api.app.auth.dependencies import AuthContext

        context = AuthContext(
            auth_type="jwt",
            role="admin",
            scopes=["admin"]
        )

        assert context.is_admin is True

    def test_auth_context_has_scope(self):
        """Test AuthContext scope checking."""
        from api.app.auth.dependencies import AuthContext

        context = AuthContext(
            auth_type="api_key",
            scopes=["read", "write"]
        )

        assert context.has_scope("read") is True
        assert context.has_scope("delete") is False


class TestAuthMode:
    """Tests for AuthMode configuration."""

    def test_auth_mode_exempt_paths(self):
        """Test exempt path checking."""
        from api.app.auth.dependencies import AuthMode

        assert AuthMode.is_exempt("/") is True
        assert AuthMode.is_exempt("/health") is True
        assert AuthMode.is_exempt("/docs") is True
        assert AuthMode.is_exempt("/api/sources") is False


@pytest.mark.asyncio
class TestAuthDependencies:
    """Tests for authentication dependencies."""

    async def test_get_auth_context_with_api_key(self):
        """Test getting auth context with API key."""
        from api.app.auth.dependencies import get_auth_context
        from fastapi import Request

        request = MagicMock(spec=Request)
        request.headers = {"X-API-Key": "test-api-key-123"}
        request.query_params = {}

        context = await get_auth_context(request, None, None, None)

        assert context.auth_type == "api_key"
        assert context.is_authenticated is True

    async def test_get_auth_context_no_auth(self):
        """Test getting auth context without authentication."""
        from api.app.auth.dependencies import get_auth_context
        from fastapi import Request

        request = MagicMock(spec=Request)
        request.headers = {}
        request.query_params = {}

        context = await get_auth_context(request, None, None, None)

        assert context.auth_type == "none"
        assert context.is_authenticated is False

    async def test_require_auth_exempt_path(self):
        """Test require_auth with exempt path."""
        from api.app.auth.dependencies import require_auth, AuthContext
        from fastapi import Request

        request = MagicMock(spec=Request)
        request.url.path = "/health"

        context = AuthContext(auth_type="none")

        result = await require_auth(request, context)

        # Should not raise, returns context
        assert result == context

    async def test_require_auth_disabled_mode(self):
        """Test require_auth with disabled auth mode."""
        from api.app.auth.dependencies import require_auth, AuthContext, AuthMode
        from fastapi import Request

        original_mode = AuthMode.MODE
        AuthMode.MODE = "disabled"

        try:
            request = MagicMock(spec=Request)
            request.url.path = "/api/protected"

            context = AuthContext(auth_type="none")

            result = await require_auth(request, context)

            assert result.role == "admin"
            assert "admin" in result.scopes
        finally:
            AuthMode.MODE = original_mode

    async def test_require_auth_unauthenticated(self):
        """Test require_auth raises for unauthenticated request."""
        from api.app.auth.dependencies import require_auth, AuthContext, AuthMode
        from fastapi import Request, HTTPException

        original_mode = AuthMode.MODE
        AuthMode.MODE = "required"

        try:
            request = MagicMock(spec=Request)
            request.url.path = "/api/protected"

            context = AuthContext(auth_type="none")

            with pytest.raises(HTTPException) as exc_info:
                await require_auth(request, context)

            assert exc_info.value.status_code == 401
        finally:
            AuthMode.MODE = original_mode

    async def test_require_admin_success(self):
        """Test require_admin with admin context."""
        from api.app.auth.dependencies import require_admin, AuthContext

        context = AuthContext(
            auth_type="jwt",
            role="admin",
            scopes=["admin"]
        )

        result = await require_admin(context)

        assert result == context

    async def test_require_admin_failure(self):
        """Test require_admin raises for non-admin."""
        from api.app.auth.dependencies import require_admin, AuthContext
        from fastapi import HTTPException

        context = AuthContext(
            auth_type="jwt",
            role="user",
            scopes=["read"]
        )

        with pytest.raises(HTTPException) as exc_info:
            await require_admin(context)

        assert exc_info.value.status_code == 403

    async def test_require_scope_success(self):
        """Test require_scope with valid scope."""
        from api.app.auth.dependencies import require_scope, AuthContext

        checker = require_scope("write")

        context = AuthContext(
            auth_type="jwt",
            scopes=["read", "write"]
        )

        result = await checker(context)

        assert result == context

    async def test_require_scope_failure(self):
        """Test require_scope raises for missing scope."""
        from api.app.auth.dependencies import require_scope, AuthContext
        from fastapi import HTTPException

        checker = require_scope("delete")

        context = AuthContext(
            auth_type="jwt",
            scopes=["read"]
        )

        with pytest.raises(HTTPException) as exc_info:
            await checker(context)

        assert exc_info.value.status_code == 403

    async def test_optional_auth_returns_context(self):
        """Test optional_auth returns context without raising."""
        from api.app.auth.dependencies import optional_auth, AuthContext

        context = AuthContext(auth_type="none")

        result = await optional_auth(context)

        assert result == context
        assert result.is_authenticated is False

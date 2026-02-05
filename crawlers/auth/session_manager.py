"""
Session Manager for authenticated crawling.

This module provides secure credential storage and session state management
for crawling sites that require authentication.
"""

import os
import json
import base64
import logging
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from enum import Enum
from pathlib import Path

try:
    from cryptography.fernet import Fernet, InvalidToken
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False
    Fernet = None
    InvalidToken = Exception

logger = logging.getLogger(__name__)


class AuthType(str, Enum):
    """Supported authentication types."""
    FORM = "form"
    OAUTH = "oauth"
    API_KEY = "api_key"
    COOKIE = "cookie"
    BASIC = "basic"
    BEARER = "bearer"
    CUSTOM = "custom"


@dataclass
class AuthCredentials:
    """
    Authentication credentials with encryption support.

    Stores various types of authentication information securely.
    Sensitive fields (password, api_key, oauth_token) should be encrypted
    before storage.
    """
    source_id: str
    auth_type: AuthType
    username: Optional[str] = None
    password: Optional[str] = None  # Should be encrypted
    api_key: Optional[str] = None   # Should be encrypted
    oauth_token: Optional[str] = None  # Should be encrypted
    oauth_refresh_token: Optional[str] = None  # Should be encrypted
    oauth_expires_at: Optional[datetime] = None
    cookies: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    custom_data: Optional[Dict[str, Any]] = None
    login_url: Optional[str] = None
    selectors: Optional[Dict[str, str]] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        data = {
            'source_id': self.source_id,
            'auth_type': self.auth_type.value if isinstance(self.auth_type, AuthType) else self.auth_type,
            'username': self.username,
            'password': self.password,
            'api_key': self.api_key,
            'oauth_token': self.oauth_token,
            'oauth_refresh_token': self.oauth_refresh_token,
            'oauth_expires_at': self.oauth_expires_at.isoformat() if self.oauth_expires_at else None,
            'cookies': self.cookies,
            'headers': self.headers,
            'custom_data': self.custom_data,
            'login_url': self.login_url,
            'selectors': self.selectors,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
        return {k: v for k, v in data.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AuthCredentials':
        """Create from dictionary."""
        auth_type = data.get('auth_type', 'form')
        if isinstance(auth_type, str):
            auth_type = AuthType(auth_type)

        oauth_expires = data.get('oauth_expires_at')
        if oauth_expires and isinstance(oauth_expires, str):
            oauth_expires = datetime.fromisoformat(oauth_expires)

        created_at = data.get('created_at')
        if created_at and isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        else:
            created_at = datetime.utcnow()

        updated_at = data.get('updated_at')
        if updated_at and isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)

        return cls(
            source_id=data['source_id'],
            auth_type=auth_type,
            username=data.get('username'),
            password=data.get('password'),
            api_key=data.get('api_key'),
            oauth_token=data.get('oauth_token'),
            oauth_refresh_token=data.get('oauth_refresh_token'),
            oauth_expires_at=oauth_expires,
            cookies=data.get('cookies'),
            headers=data.get('headers'),
            custom_data=data.get('custom_data'),
            login_url=data.get('login_url'),
            selectors=data.get('selectors'),
            created_at=created_at,
            updated_at=updated_at
        )


@dataclass
class SessionState:
    """
    Browser session state for authenticated crawling.

    Stores cookies, local storage, and other browser state
    that can be restored to maintain authenticated sessions.
    """
    source_id: str
    cookies: Dict[str, Any] = field(default_factory=dict)
    local_storage: Optional[Dict[str, str]] = None
    session_storage: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    user_agent: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
    is_valid: bool = True
    login_verified_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            'source_id': self.source_id,
            'cookies': self.cookies,
            'local_storage': self.local_storage,
            'session_storage': self.session_storage,
            'headers': self.headers,
            'user_agent': self.user_agent,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'is_valid': self.is_valid,
            'login_verified_at': self.login_verified_at.isoformat() if self.login_verified_at else None,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SessionState':
        """Create from dictionary."""
        def parse_datetime(val):
            if val and isinstance(val, str):
                return datetime.fromisoformat(val)
            return val

        return cls(
            source_id=data['source_id'],
            cookies=data.get('cookies', {}),
            local_storage=data.get('local_storage'),
            session_storage=data.get('session_storage'),
            headers=data.get('headers'),
            user_agent=data.get('user_agent'),
            created_at=parse_datetime(data.get('created_at')) or datetime.utcnow(),
            expires_at=parse_datetime(data.get('expires_at')),
            last_used_at=parse_datetime(data.get('last_used_at')),
            is_valid=data.get('is_valid', True),
            login_verified_at=parse_datetime(data.get('login_verified_at')),
            metadata=data.get('metadata', {})
        )

    def is_expired(self) -> bool:
        """Check if session has expired."""
        if not self.expires_at:
            return False
        return datetime.utcnow() > self.expires_at

    def mark_used(self) -> None:
        """Update last used timestamp."""
        self.last_used_at = datetime.utcnow()


class SessionManager:
    """
    Manages authentication credentials and session states for crawling.

    Features:
    - Encrypts sensitive credential data (passwords, API keys, tokens)
    - Stores and retrieves session states (cookies, storage)
    - Validates session expiration
    - Supports both file-based and MongoDB storage

    Security considerations:
    - Encryption key should be stored securely (environment variable)
    - Credentials are encrypted at rest
    - Session states are validated before use
    """

    # Fields that should be encrypted
    SENSITIVE_FIELDS = ['password', 'api_key', 'oauth_token', 'oauth_refresh_token']

    # Default session duration (hours)
    DEFAULT_SESSION_DURATION_HOURS = 24

    def __init__(
        self,
        encryption_key: Optional[str] = None,
        storage_path: Optional[str] = None,
        mongodb_client: Optional[Any] = None,
        mongodb_db_name: str = "crawler_system"
    ):
        """
        Initialize SessionManager.

        Args:
            encryption_key: Fernet-compatible encryption key (base64 encoded).
                          If not provided, attempts to load from SESSION_ENCRYPTION_KEY env var.
            storage_path: Path for file-based storage. If None, uses MongoDB.
            mongodb_client: PyMongo client for MongoDB storage.
            mongodb_db_name: MongoDB database name.
        """
        self._encryption_key = encryption_key or os.getenv("SESSION_ENCRYPTION_KEY")
        self._fernet: Optional[Fernet] = None
        self._storage_path = storage_path
        self._mongodb_client = mongodb_client
        self._mongodb_db_name = mongodb_db_name

        # In-memory cache
        self._sessions: Dict[str, SessionState] = {}
        self._credentials_cache: Dict[str, AuthCredentials] = {}

        # Initialize encryption
        if self._encryption_key:
            self._init_encryption()
        else:
            logger.warning(
                "No encryption key provided. Sensitive data will not be encrypted. "
                "Set SESSION_ENCRYPTION_KEY environment variable for production use."
            )

        # Initialize storage
        if self._storage_path:
            self._init_file_storage()

    def _init_encryption(self) -> None:
        """Initialize Fernet encryption."""
        if not CRYPTOGRAPHY_AVAILABLE:
            logger.error(
                "cryptography package not installed. "
                "Install with: pip install cryptography"
            )
            return

        try:
            # Ensure key is properly formatted
            key = self._encryption_key
            if isinstance(key, str):
                key = key.encode()

            # Validate key format (must be 32 url-safe base64-encoded bytes)
            if len(base64.urlsafe_b64decode(key)) != 32:
                raise ValueError("Invalid key length")

            self._fernet = Fernet(key)
            logger.info("Encryption initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize encryption: {e}")
            self._fernet = None

    def _init_file_storage(self) -> None:
        """Initialize file-based storage directories."""
        storage = Path(self._storage_path)
        (storage / "credentials").mkdir(parents=True, exist_ok=True)
        (storage / "sessions").mkdir(parents=True, exist_ok=True)
        logger.info(f"File storage initialized at: {storage}")

    @staticmethod
    def generate_encryption_key() -> str:
        """
        Generate a new Fernet encryption key.

        Returns:
            Base64-encoded encryption key suitable for SESSION_ENCRYPTION_KEY.
        """
        if not CRYPTOGRAPHY_AVAILABLE:
            raise RuntimeError("cryptography package required for key generation")
        return Fernet.generate_key().decode()

    def _encrypt(self, plaintext: str) -> str:
        """
        Encrypt a string value.

        Args:
            plaintext: Value to encrypt.

        Returns:
            Base64-encoded encrypted value.
        """
        if not self._fernet:
            logger.warning("Encryption not available, storing plaintext")
            return plaintext

        try:
            encrypted = self._fernet.encrypt(plaintext.encode())
            return base64.urlsafe_b64encode(encrypted).decode()
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise

    def _decrypt(self, ciphertext: str) -> str:
        """
        Decrypt an encrypted string value.

        Args:
            ciphertext: Base64-encoded encrypted value.

        Returns:
            Decrypted plaintext value.
        """
        if not self._fernet:
            return ciphertext

        try:
            encrypted = base64.urlsafe_b64decode(ciphertext.encode())
            decrypted = self._fernet.decrypt(encrypted)
            return decrypted.decode()
        except InvalidToken:
            logger.error("Decryption failed: invalid token or corrupted data")
            raise ValueError("Failed to decrypt: invalid encryption key or corrupted data")
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise

    def _encrypt_credentials(self, credentials: AuthCredentials) -> Dict[str, Any]:
        """Encrypt sensitive fields in credentials."""
        data = credentials.to_dict()

        for field_name in self.SENSITIVE_FIELDS:
            if field_name in data and data[field_name]:
                data[field_name] = self._encrypt(data[field_name])

        return data

    def _decrypt_credentials(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt sensitive fields in credentials."""
        decrypted = data.copy()

        for field_name in self.SENSITIVE_FIELDS:
            if field_name in decrypted and decrypted[field_name]:
                try:
                    decrypted[field_name] = self._decrypt(decrypted[field_name])
                except ValueError:
                    logger.warning(f"Could not decrypt {field_name}, may be plaintext")

        return decrypted

    # =========================================================================
    # Credential Management
    # =========================================================================

    async def store_credentials(
        self,
        credentials: AuthCredentials,
        overwrite: bool = True
    ) -> None:
        """
        Store authentication credentials securely.

        Args:
            credentials: AuthCredentials object to store.
            overwrite: If True, overwrites existing credentials for the source.
        """
        source_id = credentials.source_id
        credentials.updated_at = datetime.utcnow()

        # Encrypt sensitive data
        encrypted_data = self._encrypt_credentials(credentials)

        if self._mongodb_client:
            await self._store_credentials_mongodb(source_id, encrypted_data, overwrite)
        elif self._storage_path:
            await self._store_credentials_file(source_id, encrypted_data, overwrite)

        # Update cache (with unencrypted data)
        self._credentials_cache[source_id] = credentials
        logger.info(f"Stored credentials for source: {source_id}")

    async def _store_credentials_mongodb(
        self,
        source_id: str,
        data: Dict[str, Any],
        overwrite: bool
    ) -> None:
        """Store credentials in MongoDB."""
        db = self._mongodb_client[self._mongodb_db_name]
        collection = db.auth_credentials

        if overwrite:
            await collection.update_one(
                {"source_id": source_id},
                {"$set": data},
                upsert=True
            )
        else:
            existing = await collection.find_one({"source_id": source_id})
            if existing:
                raise ValueError(f"Credentials already exist for source: {source_id}")
            await collection.insert_one(data)

    async def _store_credentials_file(
        self,
        source_id: str,
        data: Dict[str, Any],
        overwrite: bool
    ) -> None:
        """Store credentials in file."""
        file_path = Path(self._storage_path) / "credentials" / f"{source_id}.json"

        if not overwrite and file_path.exists():
            raise ValueError(f"Credentials already exist for source: {source_id}")

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)

    async def load_credentials(self, source_id: str) -> Optional[AuthCredentials]:
        """
        Load authentication credentials for a source.

        Args:
            source_id: Source identifier.

        Returns:
            AuthCredentials if found, None otherwise.
        """
        # Check cache first
        if source_id in self._credentials_cache:
            return self._credentials_cache[source_id]

        encrypted_data = None

        if self._mongodb_client:
            encrypted_data = await self._load_credentials_mongodb(source_id)
        elif self._storage_path:
            encrypted_data = await self._load_credentials_file(source_id)

        if not encrypted_data:
            return None

        # Decrypt and create object
        decrypted_data = self._decrypt_credentials(encrypted_data)
        credentials = AuthCredentials.from_dict(decrypted_data)

        # Update cache
        self._credentials_cache[source_id] = credentials

        return credentials

    async def _load_credentials_mongodb(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Load credentials from MongoDB."""
        db = self._mongodb_client[self._mongodb_db_name]
        collection = db.auth_credentials

        result = await collection.find_one({"source_id": source_id})
        if result:
            result.pop('_id', None)
        return result

    async def _load_credentials_file(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Load credentials from file."""
        file_path = Path(self._storage_path) / "credentials" / f"{source_id}.json"

        if not file_path.exists():
            return None

        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    async def delete_credentials(self, source_id: str) -> bool:
        """
        Delete stored credentials for a source.

        Args:
            source_id: Source identifier.

        Returns:
            True if deleted, False if not found.
        """
        # Remove from cache
        self._credentials_cache.pop(source_id, None)

        if self._mongodb_client:
            db = self._mongodb_client[self._mongodb_db_name]
            result = await db.auth_credentials.delete_one({"source_id": source_id})
            return result.deleted_count > 0
        elif self._storage_path:
            file_path = Path(self._storage_path) / "credentials" / f"{source_id}.json"
            if file_path.exists():
                file_path.unlink()
                return True

        return False

    # =========================================================================
    # Session Management
    # =========================================================================

    async def save_session(
        self,
        session: SessionState,
        duration_hours: Optional[int] = None
    ) -> None:
        """
        Save browser session state.

        Args:
            session: SessionState object to save.
            duration_hours: Session duration in hours. Defaults to DEFAULT_SESSION_DURATION_HOURS.
        """
        source_id = session.source_id

        # Set expiration if not already set
        if not session.expires_at:
            duration = duration_hours or self.DEFAULT_SESSION_DURATION_HOURS
            session.expires_at = datetime.utcnow() + timedelta(hours=duration)

        session.last_used_at = datetime.utcnow()
        data = session.to_dict()

        if self._mongodb_client:
            await self._save_session_mongodb(source_id, data)
        elif self._storage_path:
            await self._save_session_file(source_id, data)

        # Update cache
        self._sessions[source_id] = session
        logger.info(f"Saved session for source: {source_id}, expires: {session.expires_at}")

    async def _save_session_mongodb(self, source_id: str, data: Dict[str, Any]) -> None:
        """Save session to MongoDB."""
        db = self._mongodb_client[self._mongodb_db_name]
        collection = db.auth_sessions

        await collection.update_one(
            {"source_id": source_id},
            {"$set": data},
            upsert=True
        )

    async def _save_session_file(self, source_id: str, data: Dict[str, Any]) -> None:
        """Save session to file."""
        file_path = Path(self._storage_path) / "sessions" / f"{source_id}.json"

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)

    async def load_session(self, source_id: str) -> Optional[SessionState]:
        """
        Load session state for a source.

        Args:
            source_id: Source identifier.

        Returns:
            SessionState if found and valid, None otherwise.
        """
        # Check cache first
        if source_id in self._sessions:
            session = self._sessions[source_id]
            if session.is_valid and not session.is_expired():
                return session

        data = None

        if self._mongodb_client:
            data = await self._load_session_mongodb(source_id)
        elif self._storage_path:
            data = await self._load_session_file(source_id)

        if not data:
            return None

        session = SessionState.from_dict(data)

        # Validate session
        if not session.is_valid or session.is_expired():
            logger.info(f"Session expired or invalid for source: {source_id}")
            return None

        # Update cache
        self._sessions[source_id] = session

        return session

    async def _load_session_mongodb(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Load session from MongoDB."""
        db = self._mongodb_client[self._mongodb_db_name]
        collection = db.auth_sessions

        result = await collection.find_one({"source_id": source_id})
        if result:
            result.pop('_id', None)
        return result

    async def _load_session_file(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Load session from file."""
        file_path = Path(self._storage_path) / "sessions" / f"{source_id}.json"

        if not file_path.exists():
            return None

        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    async def is_session_valid(self, source_id: str) -> bool:
        """
        Check if a session is valid and not expired.

        Args:
            source_id: Source identifier.

        Returns:
            True if session is valid and active.
        """
        session = await self.load_session(source_id)
        return session is not None and session.is_valid and not session.is_expired()

    async def refresh_session(
        self,
        source_id: str,
        additional_hours: Optional[int] = None
    ) -> Optional[SessionState]:
        """
        Refresh/extend an existing session.

        Args:
            source_id: Source identifier.
            additional_hours: Hours to extend. Defaults to DEFAULT_SESSION_DURATION_HOURS.

        Returns:
            Updated SessionState or None if session not found.
        """
        session = await self.load_session(source_id)

        if not session:
            logger.warning(f"Cannot refresh: session not found for source {source_id}")
            return None

        # Extend expiration
        duration = additional_hours or self.DEFAULT_SESSION_DURATION_HOURS
        session.expires_at = datetime.utcnow() + timedelta(hours=duration)
        session.last_used_at = datetime.utcnow()

        await self.save_session(session)
        logger.info(f"Refreshed session for source: {source_id}")

        return session

    async def invalidate_session(self, source_id: str) -> None:
        """
        Invalidate a session (mark as invalid without deleting).

        Args:
            source_id: Source identifier.
        """
        session = await self.load_session(source_id)

        if session:
            session.is_valid = False
            await self.save_session(session)
            logger.info(f"Invalidated session for source: {source_id}")

        # Remove from cache
        self._sessions.pop(source_id, None)

    async def delete_session(self, source_id: str) -> bool:
        """
        Delete session data completely.

        Args:
            source_id: Source identifier.

        Returns:
            True if deleted, False if not found.
        """
        # Remove from cache
        self._sessions.pop(source_id, None)

        if self._mongodb_client:
            db = self._mongodb_client[self._mongodb_db_name]
            result = await db.auth_sessions.delete_one({"source_id": source_id})
            return result.deleted_count > 0
        elif self._storage_path:
            file_path = Path(self._storage_path) / "sessions" / f"{source_id}.json"
            if file_path.exists():
                file_path.unlink()
                return True

        return False

    # =========================================================================
    # Utility Methods
    # =========================================================================

    async def get_session_status(self, source_id: str) -> Dict[str, Any]:
        """
        Get comprehensive status for a source's auth configuration.

        Args:
            source_id: Source identifier.

        Returns:
            Dictionary with auth and session status.
        """
        credentials = await self.load_credentials(source_id)
        session = await self.load_session(source_id)

        status = {
            "source_id": source_id,
            "auth_configured": credentials is not None,
            "auth_type": credentials.auth_type.value if credentials else None,
            "has_session": session is not None,
            "session_valid": False,
            "session_expires_at": None,
            "last_login": None,
            "last_used": None
        }

        if session:
            status["session_valid"] = session.is_valid and not session.is_expired()
            status["session_expires_at"] = session.expires_at.isoformat() if session.expires_at else None
            status["last_login"] = session.login_verified_at.isoformat() if session.login_verified_at else None
            status["last_used"] = session.last_used_at.isoformat() if session.last_used_at else None

        return status

    async def cleanup_expired_sessions(self) -> int:
        """
        Remove all expired sessions.

        Returns:
            Number of sessions removed.
        """
        count = 0

        if self._mongodb_client:
            db = self._mongodb_client[self._mongodb_db_name]
            result = await db.auth_sessions.delete_many({
                "$or": [
                    {"expires_at": {"$lt": datetime.utcnow().isoformat()}},
                    {"is_valid": False}
                ]
            })
            count = result.deleted_count
        elif self._storage_path:
            sessions_dir = Path(self._storage_path) / "sessions"
            for file_path in sessions_dir.glob("*.json"):
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    session = SessionState.from_dict(data)
                    if session.is_expired() or not session.is_valid:
                        file_path.unlink()
                        count += 1
                except Exception as e:
                    logger.error(f"Error checking session file {file_path}: {e}")

        # Clear cache
        self._sessions = {
            k: v for k, v in self._sessions.items()
            if v.is_valid and not v.is_expired()
        }

        logger.info(f"Cleaned up {count} expired sessions")
        return count

    def clear_cache(self) -> None:
        """Clear in-memory caches."""
        self._sessions.clear()
        self._credentials_cache.clear()
        logger.info("Cleared session and credentials cache")

"""
Local Database Authentication for stock-web-v3.
Uses SHA256 (compatible with existing database).
"""

import hashlib
import secrets
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

from ..database import fetchone, execute


# ─── Django-compatible password verification ────────────────────────────────
# Falls back to SHA256 to support legacy tokens/both formats.


def _constant_time_compare(val1: str, val2: str) -> bool:
    """Constant time comparison, mimics hmac.compare_digest for strings."""
    if len(val1) != len(val2):
        return False
    result = 0
    for a, b in zip(val1, val2):
        result |= ord(a) ^ ord(b)
    return result == 0


def verify_password(password: str, hashed: str) -> bool:
    """Verify password against SHA256 or Django PBKDF2 hash."""
    # ── 1. Django PBKDF2-SHA256 (pbkdf2_sha256$<iter>$<salt>$<hash>)
    if hashed.startswith("pbkdf2_sha256$"):
        try:
            _, iterations_str, salt, hash_val = hashed.split("$")
            iterations = int(iterations_str)
            dk = hashlib.pbkdf2_hmac(
                "sha256",
                password.encode("utf-8"),
                salt.encode("utf-8"),
                iterations,
                dklen=32
            )
            computed = dk.hex()
            return _constant_time_compare(computed, hash_val)
        except Exception:
            return False

    # ── 2. Plain SHA256 (legacy / simple installations)
    sha_hash = hashlib.sha256(password.encode()).hexdigest()
    return _constant_time_compare(sha_hash, hashed)


def hash_password(password: str) -> str:
    """Hash password with SHA256 (for new users)."""
    return hashlib.sha256(password.encode()).hexdigest()


# User management

async def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    """Get user from database."""
    query = """
        SELECT id, username, email, password_hash, is_active, stockbit_token, 
               token_expires_at, created_at, is_admin
        FROM users 
        WHERE username = $1 AND is_active = true
    """
    return await fetchone(query, username)


async def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Authenticate user with username/password."""
    user = await get_user_by_username(username)
    if not user:
        return None
    
    if not verify_password(password, user['password_hash']):
        return None
    
    return user


async def update_user_token(username: str, token: str, expires_at: Optional[datetime] = None):
    """Update user's Stockbit token."""
    await execute("""
        UPDATE users 
        SET stockbit_token = $1, 
            token_expires_at = $2,
            updated_at = NOW()
        WHERE username = $3
    """, token, expires_at, username)


async def check_token_status(username: str) -> Dict[str, Any]:
    """
    Check Stockbit token status for user.
    Now with Chrome Extension proxy fallback — system Redis key takes priority.
    """
    # ── Priority 1: System key from Chrome Extension proxy ──────────────────
    from ..redis_client import get_redis
    redis = get_redis()
    sys_token = await redis.get("stockbit_token:system:primary")
    
    if sys_token and len(sys_token) > 500:
        # JWT decode only — no API call (proxy token is actively maintained)
        try:
            import base64, json, time
            parts = sys_token.split(".")
            payload_b64 = parts[1]
            padding = 4 - len(payload_b64) % 4
            if padding != 4:
                payload_b64 += "=" * padding
            decoded = json.loads(base64.urlsafe_b64decode(payload_b64))
            exp = decoded.get("exp", 0)
            if exp > time.time():
                return {
                    "has_token": True,
                    "valid": True,
                    "message": "Token active (Chrome Extension proxy)",
                    "requires_token": False,
                    "source": "proxy_extension",
                    "user_info": decoded.get("data")
                }
        except Exception:
            pass  # fall through to DB check
    
    # ── Fallback: User DB token ───────────────────────────────────────────
    user = await get_user_by_username(username)
    if not user:
        return {"has_token": False, "valid": False, "message": "User not found"}
    
    # Check if user has stockbit_token column (may need migration)
    token = user.get('stockbit_token')
    expires_at = user.get('token_expires_at')
    
    if not token:
        return {
            "has_token": False, 
            "valid": False, 
            "message": "Stockbit token not set. Please input token.",
            "requires_token": True
        }
    
    # Check if expired by timestamp
    if expires_at and isinstance(expires_at, datetime):
        if datetime.utcnow() > expires_at:
            return {
                "has_token": True,
                "valid": False,
                "message": "Stockbit token expired. Please input new token.",
                "requires_token": True,
                "expired_at": expires_at.isoformat()
            }
    
    # Validate with Stockbit API
    from ..ingestion.stockbit_client import validate_token
    is_valid, user_info = await validate_token(token)
    
    if not is_valid:
        return {
            "has_token": True,
            "valid": False,
            "message": "Stockbit token invalid or revoked. Please input new token.",
            "requires_token": True
        }
    
    return {
        "has_token": True,
        "valid": True,
        "message": "Token valid",
        "requires_token": False,
        "user_info": user_info
    }


async def create_session_with_user(username: str, email: str, tier: str = "basic") -> str:
    """Create Redis session for logged-in user."""
    from ..redis_client import create_session
    
    session_id = secrets.token_urlsafe(32)
    user_data = {
        "username": username,
        "email": email,
        "tier": tier,
        "login_at": datetime.utcnow().isoformat()
    }
    
    await create_session(session_id, user_data, ttl=86400)
    return session_id
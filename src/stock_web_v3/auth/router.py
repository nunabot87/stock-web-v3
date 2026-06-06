"""Authentication API - Login with local user/pass + Stockbit token validation"""

import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel

from ..config import get_settings
from ..redis_client import (
    create_session, get_session, delete_session,
    set_stockbit_token, get_stockbit_token as get_redis_token
)
from ..auth.local_auth import (
    authenticate_user, check_token_status, 
    create_session_with_user, update_user_token, get_user_by_username
)

router = APIRouter(prefix="/auth", tags=["authentication"])


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenInputRequest(BaseModel):
    stockbit_token: str


class LoginResponse(BaseModel):
    success: bool
    session_id: Optional[str] = None
    username: Optional[str] = None
    message: str
    requires_stockbit_token: bool = False
    next_step: str  # "input_token" or "dashboard"


class TokenStatusResponse(BaseModel):
    has_token: bool
    valid: bool
    message: str
    requires_token: bool = False


class LogoutResponse(BaseModel):
    message: str


class SessionResponse(BaseModel):
    valid: bool
    username: Optional[str] = None
    email: Optional[str] = None
    stockbit_token_valid: bool = False
    tier_access: str


async def require_session(x_session_id: Optional[str] = Header(None)) -> Dict[str, Any]:
    """Dependency that requires valid session."""
    if not x_session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    session = await get_session(x_session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    
    return session


@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    """
    Step 1: Login with local username/password.
    After successful auth, system checks Stockbit token status.
    """
    # Step 1: Authenticate against local database
    user = await authenticate_user(request.username, request.password)
    
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    
    # Step 2: Check Stockbit token status
    token_status = await check_token_status(request.username)
    
    # Step 3: Create session
    session_id = await create_session_with_user(
        username=user['username'],
        email=user['email'],
        tier="premium" if user.get('is_admin') else "basic"
    )
    
    # If token expired/missing, require input
    if token_status.get("requires_token"):
        # Store session but flag as needing token
        return LoginResponse(
            success=True,
            session_id=session_id,
            username=user['username'],
            message=token_status['message'],
            requires_stockbit_token=True,
            next_step="input_token"
        )
    
    # Token is valid, store in Redis for quick access
    if user.get('stockbit_token'):
        await set_stockbit_token(session_id, user['stockbit_token'])
    
    return LoginResponse(
        success=True,
        session_id=session_id,
        username=user['username'],
        message="Login successful",
        requires_stockbit_token=False,
        next_step="dashboard"
    )


@router.post("/token", response_model=LoginResponse)
async def input_token(
    request: TokenInputRequest,
    session: Dict[str, Any] = Depends(require_session)
):
    """
    Step 2: Input/Refresh Stockbit token after local login.
    Validates token against Stockbit API before storing.
    """
    from ..ingestion.stockbit_client import validate_token
    
    username = session.get('username')
    if not username:
        raise HTTPException(status_code=400, detail="Invalid session")
    
    # Validate token with Stockbit
    is_valid, user_info = await validate_token(request.stockbit_token)
    
    if not is_valid:
        raise HTTPException(status_code=401, detail="Invalid Stockbit token. Please copy fresh token from browser.")
    
    # Calculate expiry (default 24 hours if not in JWT)
    exp_timestamp = None
    if user_info and 'exp' in user_info:
        try:
            exp_timestamp = datetime.fromtimestamp(user_info['exp'])
        except:
            pass
    
    if not exp_timestamp:
        exp_timestamp = datetime.utcnow() + timedelta(days=1)
    
    # Save to database
    await update_user_token(username, request.stockbit_token, exp_timestamp)
    
    # Save to Redis for quick access
    session_id = None
    for header, value in [("x-session-id", None)]:  # How to get header here?
        pass
    
    # Get session ID from context
    # We need to store in Redis for this session
    # This is handled by the caller providing session
    
    return LoginResponse(
        success=True,
        session_id=None,  # Same session
        username=username,
        message="Stockbit token saved successfully",
        requires_stockbit_token=False,
        next_step="dashboard"
    )


@router.post("/refresh-token", response_model=LoginResponse)
async def refresh_token(
    request: TokenInputRequest,
    session: Dict[str, Any] = Depends(require_session)
):
    """Refresh Stockbit token for existing session."""
    return await input_token(request, session)


@router.get("/token/status", response_model=TokenStatusResponse)
async def token_status(session: Dict[str, Any] = Depends(require_session)):
    """Check current Stockbit token status."""
    username = session.get('username')
    status = await check_token_status(username)
    return TokenStatusResponse(**status)


@router.post("/logout", response_model=LogoutResponse)
async def logout(x_session_id: Optional[str] = Header(None)):
    """Logout and invalidate session."""
    if x_session_id:
        await delete_session(x_session_id)
    
    return LogoutResponse(message="Logged out successfully")


@router.get("/validate", response_model=SessionResponse)
async def validate_session(x_session_id: Optional[str] = Header(None)):
    """Validate current session and token status."""
    if not x_session_id:
        return SessionResponse(valid=False, stockbit_token_valid=False, tier_access="none")
    
    session = await get_session(x_session_id)
    if not session:
        return SessionResponse(valid=False, stockbit_token_valid=False, tier_access="none")
    
    # Check token status
    token_status = await check_token_status(session.get('username'))
    
    return SessionResponse(
        valid=True,
        username=session.get('username'),
        email=session.get('email'),
        stockbit_token_valid=token_status.get('valid', False),
        tier_access=session.get('tier', 'basic')
    )


# Dependency exports
async def get_current_user(session: Dict[str, Any] = Depends(require_session)) -> Dict[str, Any]:
    """Get current authenticated user."""
    return session


async def require_stockbit_token(session: Dict[str, Any] = Depends(require_session)) -> str:
    """Get valid Stockbit token — priority: system Redis key (Chrome Extension) > user DB token."""
    # Priority 1: System key from Chrome Extension proxy
    from ..redis_client import get_redis
    redis = get_redis()
    sys_token = await redis.get("stockbit_token:system:primary")
    if sys_token and len(sys_token) > 500:
        return sys_token
    
    # Fallback: User DB token
    username = session.get('username')
    user = await get_user_by_username(username)
    
    if user and user.get('stockbit_token'):
        return user['stockbit_token']
    
    raise HTTPException(
        status_code=401,
        detail="Stockbit token not available. Please ensure Chrome Extension is active or input token."
    )
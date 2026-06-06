from fastapi import APIRouter, Depends, HTTPException, Header
from typing import Optional, Dict, Any
from pydantic import BaseModel

from ..redis_client import get_stockbit_token
from .router import require_session

# Stockbit token dependency for protected endpoints
async def require_stockbit_token(x_session_id: Optional[str] = Header(None)) -> str:
    """Dependency to get valid Stockbit token from session."""
    if not x_session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    token = await get_stockbit_token(x_session_id)
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Stockbit token expired. Please login with a valid token."
        )
    return token


class TokenStatus(BaseModel):
    valid: bool
    expires_soon: bool = False
    message: str


# Sub-router untuk dependency re-exports
router = APIRouter()


@router.get("/token/status", response_model=TokenStatus)
async def check_token_status(token: str = Depends(require_stockbit_token)):
    """Check if current token is valid."""
    from ..ingestion.stockbit_client import validate_token
    
    is_valid, _ = await validate_token(token)
    
    return TokenStatus(
        valid=is_valid,
        expires_soon=not is_valid,  # Simplified - token invalid considered "expiring"
        message="Token valid" if is_valid else "Token expired or invalid"
    )
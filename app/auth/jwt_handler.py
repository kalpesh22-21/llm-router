"""
JWT creation and validation.

Tokens are signed with HMAC-SHA256 (HS256) and contain the payload:
  - sub                      : user ID (as string)
  - username                 : login name
  - is_admin                 : boolean
  - max_concurrent_requests  : int (user's concurrent request limit)
  - accessible_models        : list[str] (models user can access)
  - exp                      : expiry timestamp (UTC)

Note: is_active is NOT in the token - we check DB randomly once in 10 requests
      to catch deactivated users while avoiding DB calls on every request.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import HTTPException, status
from jose import ExpiredSignatureError, JWTError, jwt

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_CREDENTIALS_EXCEPTION = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)

_EXPIRED_EXCEPTION = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Token has expired",
    headers={"WWW-Authenticate": "Bearer"},
)


def create_access_token(
    user_id: int,
    username: str,
    is_admin: bool,
    max_concurrent_requests: int,
    accessible_models: List[str],
) -> str:
    """
    Create a signed JWT for the given user.

    The token expires after JWT_EXPIRE_MINUTES minutes (configured via env).
    All user permissions and limits are embedded in the token to avoid DB calls.
    """
    now = datetime.now(tz=timezone.utc)
    expire = now + timedelta(minutes=settings.JWT_EXPIRE_MINUTES)

    payload: Dict[str, Any] = {
        "sub": str(user_id),
        "username": username,
        "is_admin": is_admin,
        "max_concurrent_requests": max_concurrent_requests,
        "accessible_models": accessible_models,
        "iat": now,
        "exp": expire,
    }

    token = jwt.encode(
        payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM
    )
    return token


def decode_token(token: str) -> Dict[str, Any]:
    """
    Decode and validate a JWT.

    Raises:
        HTTPException(401): if the token is invalid or expired.

    Returns:
        The decoded payload dict with keys: sub, username, is_admin.
    """
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )
    except ExpiredSignatureError:
        raise _EXPIRED_EXCEPTION
    except JWTError as exc:
        logger.debug("JWT validation error: %s", exc)
        raise _CREDENTIALS_EXCEPTION

    # Validate required fields
    if payload.get("sub") is None or payload.get("username") is None:
        raise _CREDENTIALS_EXCEPTION

    return payload

"""
JWT creation and validation.

Tokens are signed with HMAC-SHA256 (HS256) and contain a minimal payload:
  - sub      : user ID (as string)
  - username : login name
  - is_admin : boolean
  - exp      : expiry timestamp (UTC)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

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
) -> str:
    """
    Create a signed JWT for the given user.

    The token expires after JWT_EXPIRE_MINUTES minutes (configured via env).
    """
    now = datetime.now(tz=timezone.utc)
    expire = now + timedelta(minutes=settings.JWT_EXPIRE_MINUTES)

    payload: Dict[str, Any] = {
        "sub": str(user_id),
        "username": username,
        "is_admin": is_admin,
        "iat": now,
        "exp": expire,
    }

    token = jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
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
            settings.SECRET_KEY,
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

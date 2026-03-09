"""
Authentication endpoints.

POST /auth/login  — LDAP credential verification → JWT
GET  /auth/me     — return info about the current authenticated user
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.jwt_handler import create_access_token, decode_token
from app.auth.ldap_auth import authenticate_ldap, get_ldap_user_info
from app.core.config import get_settings
from app.db.mysql import User, get_db
from app.services.permission import permission_service

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/auth", tags=["auth"])
_bearer = HTTPBearer(auto_error=True)

# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds


class UserResponse(BaseModel):
    id: int
    username: str
    email: str | None
    is_active: bool
    is_admin: bool
    max_concurrent_requests: int

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Dependency: extract and validate JWT from Authorization header
# ---------------------------------------------------------------------------


async def get_current_user_payload(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(_bearer)],
) -> dict:
    """Extract and decode the Bearer JWT. Raises 401 on failure."""
    return decode_token(credentials.credentials)


async def get_current_user(
    payload: Annotated[dict, Depends(get_current_user_payload)],
    db: AsyncSession = Depends(get_db),
) -> User:
    """Resolve JWT payload to a User ORM object. Raises 401 if inactive."""
    user_id = int(payload["sub"])
    user = await permission_service.get_user(db, user_id)
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is deactivated",
        )
    return user


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    """
    Authenticate via LDAP and return a signed JWT.

    Flow:
      1. Verify credentials against LDAP.
      2. Look up (or create) the user in the database.
      3. On very first login ever, auto-promote to admin if username matches
         SUPERUSER_USERNAME config (bootstrap admin).
      4. Return JWT.
    """
    # 1. LDAP authentication
    if not authenticate_ldap(body.username, body.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )

    # 2. Upsert user in DB
    user = await permission_service.get_user_by_username(db, body.username)

    if user is None:
        # Fetch extra info from LDAP (email, etc.)
        ldap_info = get_ldap_user_info(body.username)
        email = ldap_info.get("email")

        # First user ever with the bootstrap username gets admin privileges
        user_count = await permission_service.count_users(db)
        is_admin = user_count == 0 and body.username == settings.SUPERUSER_USERNAME

        user = await permission_service.create_user(
            db,
            username=body.username,
            email=email,
            is_admin=is_admin,
        )
        logger.info(
            "Created new user %r (id=%d, admin=%s)", body.username, user.id, is_admin
        )

    # 3. Guard against deactivated accounts
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated. Contact an administrator.",
        )

    # 4. Issue JWT
    token = create_access_token(
        user_id=user.id,
        username=user.username,
        is_admin=user.is_admin,
    )

    return TokenResponse(
        access_token=token,
        expires_in=settings.JWT_EXPIRE_MINUTES * 60,
    )


@router.get("/me", response_model=UserResponse)
async def get_me(
    current_user: Annotated[User, Depends(get_current_user)],
) -> UserResponse:
    """Return information about the currently authenticated user."""
    return UserResponse.model_validate(current_user)

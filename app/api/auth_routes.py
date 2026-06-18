"""
Authentication endpoints.

POST /auth/login  — LDAP credential verification → JWT
GET  /auth/me     — return info about the current authenticated user
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.jwt_handler import create_access_token, decode_token
from app.auth.ldap_auth import authenticate_ldap, get_ldap_user_info
from app.backends.base import registry as backend_registry
from app.core.config import get_settings
from app.db.mysql import User, get_db
from app.services.permission import permission_service

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/auth", tags=["auth"])
_bearer = HTTPBearer(auto_error=True)
_bearer_optional = HTTPBearer(auto_error=False)

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


async def get_current_user_payload_from_request(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer_optional)] = None,
) -> dict:
    """
    Extract and decode JWT from x-api-key header (Anthropic SDK compatible),
    falling back to Authorization: Bearer header.
    """
    api_key = request.headers.get("x-api-key")
    if api_key:
        return decode_token(api_key)
    if credentials:
        return decode_token(credentials.credentials)
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing authentication. Provide x-api-key or Authorization: Bearer header.",
    )


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
    Authenticate via LDAP or env-based admin fallback and return a signed JWT.

    Flow:
      1. Try LDAP authentication first.
      2. If LDAP fails AND username is admin AND ADMIN_INITIAL_PASSWORD is set,
         check password against environment variable.
      3. Look up (or create) the user in the database.
      4. On very first login ever, auto-promote to admin if username matches
         SUPERUSER_USERNAME config (bootstrap admin).
      5. Return JWT.
    """
    # 1. Try LDAP authentication first
    ldap_authenticated = authenticate_ldap(body.username, body.password)

    # 2. Get or create user in DB
    user = await permission_service.get_user_by_username(db, body.username)

    # If LDAP failed, try env-based admin authentication
    if not ldap_authenticated:
        # Check if this is the admin user with env password configured
        is_admin_user = body.username == settings.SUPERUSER_USERNAME
        has_env_password = bool(settings.ADMIN_INITIAL_PASSWORD)

        if not is_admin_user or not has_env_password:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password",
            )

        # Verify against environment password
        if body.password != settings.ADMIN_INITIAL_PASSWORD:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password",
            )

    # At this point, either LDAP auth succeeded or env-based admin auth succeeded
    auth_method = "LDAP" if ldap_authenticated else "admin-env"
    logger.debug("User %r authenticated via %s", body.username, auth_method)

    # 3. Upsert user in DB (for LDAP users)
    if (
        user is not None
        and body.username == settings.SUPERUSER_USERNAME
        and not user.is_admin
    ):
        user.is_admin = True

    if user is None:
        # Fetch extra info from LDAP (email, etc.)
        ldap_info = get_ldap_user_info(body.username, body.password)
        email = ldap_info.get("email")

        # First user ever with the bootstrap username gets admin privileges
        user_count = await permission_service.count_users(db)
        is_admin = user_count == 0 or body.username == settings.SUPERUSER_USERNAME

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

    # 4. Get user's accessible models for the token
    available_model_ids = set(backend_registry.list_model_ids())
    accessible_models = await permission_service.get_user_accessible_models(
        db, user.id, available_model_ids=available_model_ids
    )

    # 5. Issue JWT with embedded user info
    token = create_access_token(
        user_id=user.id,
        username=user.username,
        is_admin=user.is_admin,
        max_concurrent_requests=user.max_concurrent_requests,
        accessible_models=accessible_models,
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


@router.get("/me/models")
async def get_my_models(
    current_user: Annotated[User, Depends(get_current_user)],
    db: AsyncSession = Depends(get_db),
) -> list[str]:
    """Return all model_ids accessible to the current user via their bucket grants."""
    # Only return models from models where ALL models are available in backend
    available_model_ids = set(backend_registry.list_model_ids())
    models = await permission_service.get_user_accessible_models(
        db, current_user.id, available_model_ids=available_model_ids
    )
    return sorted(models)


@router.get("/me/opencode-config")
async def get_opencode_config(
    current_user: Annotated[User, Depends(get_current_user)],
    db: AsyncSession = Depends(get_db),
) -> JSONResponse:
    """
    Return OpenCode/AI SDK configuration for all models the user has access to.

    Returns a configuration object compatible with the AI SDK's provider format,
    including baseURL and apiKey for each accessible model.
    """
    # Get accessible models for the current user
    available_model_ids = set(backend_registry.list_model_ids())
    accessible_models = await permission_service.get_user_accessible_models(
        db, current_user.id, available_model_ids=available_model_ids
    )

    # Get model configurations from settings
    current_settings = get_settings()
    model_configs = await current_settings.load_model_configs()

    # Build a mapping from model_id to its config
    config_map: dict[str, any] = {}
    for config in model_configs:
        if hasattr(config, "model_id"):
            config_map[config.model_id] = config

    # Build the provider configuration
    provider_config: dict[str, any] = {}

    for model_id in accessible_models:
        config = config_map.get(model_id)
        if not config:
            continue

        # Format the model name nicely
        # e.g., "moonshotai/Kimi-K2.5-v3" -> "Moonshotai Kimi K2.5 V3"
        name_parts = (
            model_id.replace("/", " ").replace("-", " ").replace("_", " ").split()
        )
        formatted_name = " ".join(part.capitalize() for part in name_parts)

        provider_config[model_id] = {
            "name": "Local-On-Prem",
            "npm": "@ai-sdk/openai-compatible",
            "models": {model_id: {"name": formatted_name}},
            "options": {"baseURL": "PLACE_HOLDER_URL", "apiKey": "YOUR_TOKEN"},
        }

    return JSONResponse(content={"provider": provider_config})


@router.post("/refresh-token", response_model=TokenResponse)
async def refresh_token(
    payload: Annotated[dict, Depends(get_current_user_payload)],
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    """
    Refresh the JWT token for the current user.

    Validates the current token and user status, then issues a new token
    with fresh claims from the database and a fresh expiration time.
    """
    user_id = int(payload["sub"])

    # Verify user still exists in database
    user = await permission_service.get_user(db, user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )

    # Verify user is active
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is deactivated. Contact an administrator.",
        )

    # Get user's accessible models for the new token
    available_model_ids = set(backend_registry.list_model_ids())
    accessible_models = await permission_service.get_user_accessible_models(
        db, user.id, available_model_ids=available_model_ids
    )

    # Issue new JWT with fresh values from database
    new_token = create_access_token(
        user_id=user.id,
        username=user.username,
        is_admin=user.is_admin,
        max_concurrent_requests=user.max_concurrent_requests,
        accessible_models=accessible_models,
    )

    logger.info("Token refreshed for user %r (id=%d)", user.username, user.id)

    return TokenResponse(
        access_token=new_token,
        expires_in=settings.JWT_EXPIRE_MINUTES * 60,
    )

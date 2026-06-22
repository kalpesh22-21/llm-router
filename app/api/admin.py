"""
Admin-only management endpoints.

All routes require is_admin=True:
  GET    /admin/users
  POST   /admin/users
  PATCH  /admin/users/{user_id}/active
  PATCH  /admin/users/{user_id}/admin
  PATCH  /admin/users/{user_id}/max-concurrent
  POST   /admin/users/{user_id}/buckets/{bucket_id}
  DELETE /admin/users/{user_id}/buckets/{bucket_id}
  GET    /admin/buckets
  POST   /admin/buckets
  PATCH  /admin/buckets/{bucket_id}
  PATCH  /admin/buckets/{bucket_id}/default
  GET    /admin/models

Cluster management routes (admin or multicluster admin):
  GET    /admin/models/{model_id}/clusters
  POST   /admin/models/{model_id}/clusters
  GET    /admin/models/{model_id}/clusters/{cluster_id}
  PATCH  /admin/models/{model_id}/clusters/{cluster_id}
  DELETE /admin/models/{model_id}/clusters/{cluster_id}
  POST   /admin/models/{model_id}/clusters/{cluster_id}/enable
  POST   /admin/models/{model_id}/clusters/{cluster_id}/disable
"""

from __future__ import annotations

import datetime
import logging
import re
from typing import Annotated, Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from app.api.auth_routes import get_current_user
from app.auth.ldap_auth import verify_ldap_user_exists
from app.db.redis_client import get_redis
from app.services.api_key import api_key_service
from app.services.service_account import service_account_service
from app.backends.base import registry as backend_registry
from app.db.cluster_drain import (
    DrainState,
    get_drain_state,
    get_inflight_tokens,
    is_drain_complete,
    resume_cluster,
)
from app.db.cluster_operational_state import cluster_metrics_store
from app.db.mysql import Bucket, ContentFilterRule, User, get_db
from app.db.redis_client import (
    get_redis,
    mark_user_deactivated,
    unmark_user_deactivated,
)
from app.services.cluster_manager import ClusterCreate, ClusterUpdate, cluster_manager
from app.services.content_filter import content_filter_service
from app.services.permission import permission_service
from app.services.model_backend_service import model_backend_service
from app.services.database_backend_source import database_backend_source, publish_models_changed
from app.core.config import get_settings
from app.core.httpx import new_http_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


# ---------------------------------------------------------------------------
# Auth guard dependencies
# ---------------------------------------------------------------------------


async def require_admin(
    current_user: Annotated[User, Depends(get_current_user)],
) -> User:
    """Require the caller to be an admin."""
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return current_user


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class UserResponse(BaseModel):
    id: int
    username: str
    email: Optional[str]
    is_active: bool
    is_admin: bool
    max_concurrent_requests: int

    model_config = {"from_attributes": True}


class UsersListResponse(BaseModel):
    data: List[UserResponse]
    total: int
    skip: int
    limit: int

    model_config = {"from_attributes": True}


class CreateUserRequest(BaseModel):
    username: str
    email: Optional[str] = None

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str) -> str:
        """Validate username format - alphanumeric, underscore, hyphen only."""
        if not v:
            raise ValueError("Username cannot be empty")
        if len(v) < 3:
            raise ValueError("Username must be at least 3 characters long")
        if len(v) > 50:
            raise ValueError("Username must be at most 50 characters long")
        # Allow alphanumeric, underscore, hyphen, and dot
        if not re.match(r"^[a-zA-Z0-9_.-]+$", v):
            raise ValueError(
                "Username can only contain letters, numbers, underscores, hyphens, and dots"
            )
        return v

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        """Validate email format if provided."""
        if v is None:
            return v
        if len(v) > 254:
            raise ValueError("Email must be at most 254 characters long")
        # Basic email validation pattern
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if not re.match(email_pattern, v):
            raise ValueError("Invalid email format")
        return v


class SetActiveRequest(BaseModel):
    is_active: bool


class SetAdminRequest(BaseModel):
    is_admin: bool


class SetMaxConcurrentRequest(BaseModel):
    max_concurrent_requests: int


class BucketModelItem(BaseModel):
    model_id: str


class BucketResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    is_default: bool
    default_model: Optional[str]
    models: List[BucketModelItem]

    model_config = {"from_attributes": True}


class CreateBucketRequest(BaseModel):
    name: str
    description: Optional[str] = None
    model_ids: List[str]

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate bucket name format."""
        if not v:
            raise ValueError("Bucket name cannot be empty")
        if len(v) < 3:
            raise ValueError("Bucket name must be at least 3 characters long")
        if len(v) > 50:
            raise ValueError("Bucket name must be at most 50 characters long")
        # Allow alphanumeric, underscore, hyphen
        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(
                "Bucket name can only contain letters, numbers, underscores, and hyphens"
            )
        return v

    @field_validator("description")
    @classmethod
    def validate_description(cls, v: Optional[str]) -> Optional[str]:
        """Validate description length if provided."""
        if v is None:
            return v
        if len(v) > 255:
            raise ValueError("Description must be at most 255 characters long")
        return v

    @field_validator("model_ids")
    @classmethod
    def validate_model_ids(cls, v: List[str]) -> List[str]:
        """Validate that at least one model is selected."""
        if not v:
            raise ValueError("At least one model must be selected")
        # Validate each model_id is not empty and has reasonable length
        for model_id in v:
            if not model_id:
                raise ValueError("Model ID cannot be empty")
            if len(model_id) > 255:
                raise ValueError("Model ID must be at most 255 characters long")
        return v


class UpdateBucketRequest(BaseModel):
    """Request model for updating a bucket. All fields are optional."""

    name: Optional[str] = None
    description: Optional[str] = None
    model_ids: Optional[List[str]] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        """Validate bucket name format if provided."""
        if v is None:
            return v
        if not v:
            raise ValueError("Bucket name cannot be empty")
        if len(v) < 3:
            raise ValueError("Bucket name must be at least 3 characters long")
        if len(v) > 50:
            raise ValueError("Bucket name must be at most 50 characters long")
        # Allow alphanumeric, underscore, hyphen
        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(
                "Bucket name can only contain letters, numbers, underscores, and hyphens"
            )
        return v

    @field_validator("description")
    @classmethod
    def validate_description(cls, v: Optional[str]) -> Optional[str]:
        """Validate description length if provided."""
        if v is None:
            return v
        if len(v) > 255:
            raise ValueError("Description must be at most 255 characters long")
        return v

    @field_validator("model_ids")
    @classmethod
    def validate_model_ids(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate model IDs if provided."""
        if v is None:
            return v
        if not v:
            raise ValueError("At least one model must be selected")
        # Validate each model_id is not empty and has reasonable length
        for model_id in v:
            if not model_id:
                raise ValueError("Model ID cannot be empty")
            if len(model_id) > 255:
                raise ValueError("Model ID must be at most 255 characters long")
        return v


class SetDefaultModelRequest(BaseModel):
    model_id: str


class UserMinItem(BaseModel):
    id: int
    username: str
    email: Optional[str]
    is_active: bool

    model_config = {"from_attributes": True}


class UsersAllResponse(BaseModel):
    data: List[UserMinItem]
    total: int


class BatchBucketUsersRequest(BaseModel):
    user_ids: List[int]

    @field_validator("user_ids")
    @classmethod
    def validate_user_ids(cls, v: List[int]) -> List[int]:
        """Dedupe, enforce positive ints, cap size defensively."""
        if len(v) > 100000:
            raise ValueError("Too many users in one request")
        seen: set[int] = set()
        result: List[int] = []
        for uid in v:
            if uid < 1:
                raise ValueError("user_ids must be positive integers")
            if uid not in seen:
                seen.add(uid)
                result.append(uid)
        return result


class BucketUsersResponse(BaseModel):
    bucket_id: int
    user_ids: List[int]
    is_default: bool


class SetBucketUsersResponse(BaseModel):
    bucket_id: int
    granted: int
    revoked: int
    unchanged: int
    user_ids: List[int]


# ---------------------------------------------------------------------------
# User management routes (admin+)
# ---------------------------------------------------------------------------


@router.get("/users", response_model=UsersListResponse)
async def list_users(
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(20, ge=1, le=100, description="Maximum records to return"),
    search: Optional[str] = Query(None, description="Search by username or email"),
) -> UsersListResponse:
    """List users with pagination and optional search."""
    users, total = await permission_service.list_users(
        db, skip=skip, limit=limit, search=search
    )
    return UsersListResponse(
        data=[UserResponse.model_validate(u) for u in users],
        total=total,
        skip=skip,
        limit=limit,
    )


@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    body: CreateUserRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    """
    Manually create a user account.

    The user must already exist in LDAP (we fetch their email from there).
    If email is not in LDAP and not provided, it is left null.
    """
    # Verify the user exists in LDAP before creating
    ldap_exists = verify_ldap_user_exists(body.username)
    if not ldap_exists:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"User '{body.username}' does not exist in LDAP. "
            "Only LDAP users can be added to the system.",
        )

    # Email comes from the request if provided, otherwise left null. We cannot
    # look it up here: get_ldap_user_info() binds *as the user*, and the admin
    # does not have the user's password. The email is backfilled from LDAP on the
    # user's first login (that flow has the real password).
    email = body.email

    user = await permission_service.create_user(
        db,
        username=body.username,
        email=email,
    )
    return UserResponse.model_validate(user)


# NOTE: this static `/users/all` route MUST stay registered before the
# parameterized `/users/{user_id}/...` routes below, or `all` would be
# captured as a user_id. Do not reorder.
@router.get("/users/all", response_model=UsersAllResponse)
async def list_all_users(
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> UsersAllResponse:
    """
    Return every user (no pagination), ordered by username.

    Intended for selection UIs that need the full user list in one call.
    Response fields: id, username, email, is_active.
    """
    users = await permission_service.list_users_min(db)
    return UsersAllResponse(
        data=[UserMinItem.model_validate(u) for u in users],
        total=len(users),
    )


@router.get("/users/{user_id}/buckets", response_model=List[int])
async def list_user_bucket_ids(
    user_id: int,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> List[int]:
    """List bucket IDs the user currently has access to."""
    # Only return buckets where ALL models are available in backend
    available_model_ids = set(backend_registry.list_model_ids())
    return await permission_service.get_user_bucket_ids(
        db, user_id, available_model_ids=available_model_ids
    )


@router.patch("/users/{user_id}/active", response_model=UserResponse)
async def set_user_active(
    user_id: int,
    body: SetActiveRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> UserResponse:
    """Activate or deactivate a user account."""
    user = await permission_service.set_user_active(
        db, user_id=user_id, is_active=body.is_active, by_user_id=admin.id
    )

    # Tier 1 revocation: publish the status change to Redis immediately. Every
    # proxied request already checks is_user_marked_deactivated(), so a
    # deactivated user is rejected on their very next request instead of waiting
    # for the ~1% random DB poll to discover it; on reactivation we clear the
    # flag. The DB stays the source of truth: if the commit (in get_db, after
    # this returns) fails, _resolve_user reconciles the flag against the DB on
    # the next flagged request. A Redis error here must not fail the admin
    # action — the random poll remains a backstop.
    try:
        if body.is_active:
            await unmark_user_deactivated(redis, user_id)
        else:
            await mark_user_deactivated(redis, user_id, user.username)
    except Exception:
        logger.exception(
            "Failed to update Redis deactivation flag for user_id=%s "
            "(DB change still applied; random poll remains a backstop)",
            user_id,
        )

    return UserResponse.model_validate(user)


@router.patch("/users/{user_id}/max-concurrent", response_model=UserResponse)
async def set_max_concurrent(
    user_id: int,
    body: SetMaxConcurrentRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    """Set the maximum number of simultaneous in-flight requests for a user."""
    user = await permission_service.set_user_max_concurrent(
        db,
        user_id=user_id,
        max_concurrent=body.max_concurrent_requests,
        by_user_id=admin.id,
    )
    return UserResponse.model_validate(user)


@router.post(
    "/users/{user_id}/buckets/{bucket_id}",
    status_code=status.HTTP_201_CREATED,
)
async def grant_bucket_access(
    user_id: int,
    bucket_id: int,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Grant a user access to a bucket."""
    await permission_service.grant_bucket_access(
        db, user_id=user_id, bucket_id=bucket_id, by_user_id=admin.id
    )
    return {"detail": f"Access granted: user {user_id} → bucket {bucket_id}"}


@router.delete(
    "/users/{user_id}/buckets/{bucket_id}",
    status_code=status.HTTP_200_OK,
)
async def revoke_bucket_access(
    user_id: int,
    bucket_id: int,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Revoke a user's access to a bucket."""
    await permission_service.revoke_bucket_access(
        db, user_id=user_id, bucket_id=bucket_id, by_user_id=admin.id
    )
    return {"detail": f"Access revoked: user {user_id} ↛ bucket {bucket_id}"}


# ---------------------------------------------------------------------------
# Superuser-only routes
# ---------------------------------------------------------------------------


@router.patch("/users/{user_id}/admin", response_model=UserResponse)
async def set_admin_status(
    user_id: int,
    body: SetAdminRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    """Grant or revoke admin privileges for a user."""
    user = await permission_service.set_admin(
        db, user_id=user_id, is_admin=body.is_admin, by_user_id=admin.id
    )
    return UserResponse.model_validate(user)


@router.get("/buckets", response_model=List[BucketResponse])
async def list_buckets(
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> List[BucketResponse]:
    """List all buckets with their model assignments."""
    buckets = await permission_service.list_buckets(db)
    return [
        BucketResponse(
            id=b.id,
            name=b.name,
            description=b.description,
            is_default=b.is_default,
            default_model=b.default_model,
            models=[BucketModelItem(model_id=m.model_id) for m in b.models],
        )
        for b in buckets
    ]


@router.post(
    "/buckets", response_model=BucketResponse, status_code=status.HTTP_201_CREATED
)
async def create_bucket(
    body: CreateBucketRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> BucketResponse:
    """Create a new bucket with the specified model_ids."""
    # Validate that all model_ids are valid backend models
    available_models = set(backend_registry.list_model_ids())
    invalid_models = [mid for mid in body.model_ids if mid not in available_models]
    if invalid_models:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid model IDs: {', '.join(invalid_models)}. "
            f"Available models: {', '.join(sorted(available_models))}",
        )

    bucket = await permission_service.create_bucket(
        db,
        name=body.name,
        description=body.description,
        model_ids=body.model_ids,
        by_user_id=admin.id,
    )
    # Reload with models eagerly
    buckets = await permission_service.list_buckets(db)
    created = next(b for b in buckets if b.id == bucket.id)
    return BucketResponse(
        id=created.id,
        name=created.name,
        description=created.description,
        is_default=created.is_default,
        default_model=created.default_model,
        models=[BucketModelItem(model_id=m.model_id) for m in created.models],
    )


@router.patch("/buckets/{bucket_id}/default", response_model=BucketResponse)
async def set_default_bucket(
    bucket_id: int,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> BucketResponse:
    """Set a bucket as the default bucket assigned to new users on creation."""
    bucket = await permission_service.set_default_bucket(
        db, bucket_id=bucket_id, by_user_id=admin.id
    )
    # Reload with models
    buckets = await permission_service.list_buckets(db)
    updated = next(b for b in buckets if b.id == bucket.id)
    return BucketResponse(
        id=updated.id,
        name=updated.name,
        description=updated.description,
        is_default=updated.is_default,
        default_model=updated.default_model,
        models=[BucketModelItem(model_id=m.model_id) for m in updated.models],
    )


@router.patch("/buckets/{bucket_id}/default-model", response_model=BucketResponse)
async def set_bucket_default_model(
    bucket_id: int,
    body: SetDefaultModelRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> BucketResponse:
    """Set a model as the default model for the bucket. """
    bucket = await permission_service.set_default_model(
        db, bucket_id=bucket_id, model_id=body.model_id, by_user_id=admin.id
    )
    # Reload with models
    buckets = await permission_service.list_buckets(db)
    updated = next(b for b in buckets if b.id == bucket.id)
    return BucketResponse(
        id=updated.id,
        name=updated.name,
        description=updated.description,
        is_default=updated.is_default,
        default_model=updated.default_model,
        models=[BucketModelItem(model_id=m.model_id) for m in updated.models],
    )


@router.patch("/buckets/{bucket_id}", response_model=BucketResponse)
async def update_bucket(
    bucket_id: int,
    body: UpdateBucketRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> BucketResponse:
    """
    Update a bucket's name, description, and/or model assignments.

    - **name**: New bucket name (optional)
    - **description**: New bucket description (optional)
    - **model_ids**: Complete list of model IDs to assign (optional, replaces all existing)

    If model_ids is provided, it will replace all existing model assignments for this bucket.
    """
    # Validate model_ids if provided
    if body.model_ids is not None:
        available_models = set(backend_registry.list_model_ids())
        invalid_models = [mid for mid in body.model_ids if mid not in available_models]
        if invalid_models:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid model IDs: {', '.join(invalid_models)}. "
                f"Available models: {', '.join(sorted(available_models))}",
            )

    # Update the bucket
    bucket = await permission_service.update_bucket(
        db,
        bucket_id=bucket_id,
        name=body.name,
        description=body.description,
        model_ids=body.model_ids,
        by_user_id=admin.id,
    )

    # Reload with models to get the complete response
    buckets = await permission_service.list_buckets(db)
    updated = next(b for b in buckets if b.id == bucket.id)
    return BucketResponse(
        id=updated.id,
        name=updated.name,
        description=updated.description,
        is_default=updated.is_default,
        default_model=updated.default_model,
        models=[BucketModelItem(model_id=m.model_id) for m in updated.models],
    )


@router.delete("/buckets/{bucket_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_bucket(
    bucket_id: int,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Delete a bucket along with all of its model assignments and access grants.

    The default bucket cannot be deleted (409 Conflict); set another bucket as
    the default first.
    """
    await permission_service.delete_bucket(
        db, bucket_id=bucket_id, by_user_id=admin.id
    )


@router.get("/buckets/{bucket_id}/users", response_model=BucketUsersResponse)
async def get_bucket_users(
    bucket_id: int,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> BucketUsersResponse:
    """
    Return the explicit user membership of a bucket.

    ``user_ids`` contains only users with an explicit UserBucketAccess row;
    the implicit grant that the default bucket gives to every user is NOT
    reflected here.  ``is_default`` signals that this bucket is the default
    so the UI can warn that explicit grants on it are redundant.
    """
    user_ids = await permission_service.get_bucket_user_ids(db, bucket_id)
    bucket = await permission_service.get_bucket(db, bucket_id)
    return BucketUsersResponse(
        bucket_id=bucket_id,
        user_ids=sorted(user_ids),
        is_default=bucket.is_default,
    )


@router.post("/buckets/{bucket_id}/users", response_model=SetBucketUsersResponse)
async def set_bucket_users(
    bucket_id: int,
    body: BatchBucketUsersRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Batch set the explicit membership of a bucket (set-semantics).

    The explicit UserBucketAccess rows for *bucket_id* are made to equal
    exactly the deduplicated ``user_ids`` in the request body.  Users
    currently granted access who are absent from the list are revoked;
    users in the list who are not yet granted are inserted.

    An empty ``user_ids`` list is valid and revokes all explicit grants.

    Returns:
        JSON with ``bucket_id``, ``granted``, ``revoked``, ``unchanged``,
        and ``user_ids`` (sorted final membership).
    """
    result = await permission_service.set_bucket_users(
        db,
        bucket_id=bucket_id,
        user_ids=body.user_ids,
        by_user_id=admin.id,
    )
    return {"bucket_id": bucket_id, **result}


@router.get("/models")
async def list_all_models(
    _admin: Annotated[User, Depends(require_admin)],
) -> JSONResponse:
    """Return all model IDs registered in models.yaml (regardless of bucket access)."""
    data = [
        {"id": mid, "object": "model", "owned_by": "llm-router"}
        for mid in sorted(backend_registry.list_model_ids())
    ]
    return JSONResponse(content={"object": "list", "data": data})


@router.get("/models/status")
async def models_status(
    _admin: Annotated[User, Depends(require_admin)],
) -> JSONResponse:
    """
    Per-model availability as tracked by the DiscoveryService (this pod's view).

    For static backends `available` reflects the most recent health probe;
    backends with their own health machinery (multi-cluster) report available
    here and manage routing internally.
    """
    data = []
    for mid in sorted(backend_registry.list_model_ids()):
        backend = backend_registry.get(mid)
        if backend is None:
            continue
        data.append(
            {
                "model_id": mid,
                "type": type(backend).__name__,
                "available": backend.is_available,
            }
        )
    return JSONResponse(content={"object": "list", "data": data})


class ClusterStatusResponse(BaseModel):
    """Response schema for cluster status including drain state."""

    cluster_id: str
    model_id: str
    state: str  # ENABLED, DRAINING, or DISABLED
    is_enabled: bool
    inflight_tokens: int
    is_drain_complete: bool
    message: str

    model_config = {"from_attributes": True}


@router.get(
    "/models/{model_id:path}/clusters/{cluster_id}/status",
    response_model=ClusterStatusResponse,
)
async def get_cluster_status(
    model_id: str,
    cluster_id: str,
    _admin: Annotated[User, Depends(require_admin_or_multicluster)],
) -> ClusterStatusResponse:
    """Get cluster status including drain state and in-flight count.

    Returns:
        - Current state (ENABLED/DRAINING/DISABLED)
        - In-flight request count
        - Whether drain is complete
    """
    drain_state = await get_drain_state(model_id, cluster_id)
    inflight = await get_inflight_tokens(model_id, cluster_id)
    drain_complete = await is_drain_complete(model_id, cluster_id)

    # Determine message based on state
    if drain_state == DrainState.DRAINING:
        message = f"Cluster is draining. In-flight tokens: {inflight}"
    elif drain_state == DrainState.DISABLED:
        message = "Cluster is disabled"
    else:
        message = "Cluster is enabled"

    return ClusterStatusResponse(
        cluster_id=cluster_id,
        model_id=model_id,
        state=drain_state.value,
        is_enabled=(drain_state == DrainState.ENABLED),
        inflight_tokens=inflight,
        is_drain_complete=drain_complete,
        message=message,
    )


# ---------------------------------------------------------------------------
# Content Filter Rule Helpers
# ---------------------------------------------------------------------------


def _rule_to_response(rule: ContentFilterRule) -> ContentFilterRuleResponse:
    """Convert a ContentFilterRule ORM model to a response DTO."""
    return ContentFilterRuleResponse(
        id=rule.id,
        name=rule.name,
        description=rule.description,
        rule_type=rule.rule_type,
        pattern=rule.pattern,
        external_url=rule.external_url,
        extra_params=rule.extra_params,
        fields_to_check=rule.fields_to_check,
        is_active=rule.is_active,
        is_blocking=rule.is_blocking,
        violation_message=rule.violation_message,
    )


# ---------------------------------------------------------------------------
# Content Filter Rule Management Routes
# ---------------------------------------------------------------------------


class ContentFilterRuleResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    rule_type: str
    pattern: Optional[str]
    external_url: Optional[str]
    extra_params: Optional[dict]
    fields_to_check: Optional[str]
    is_active: bool
    is_blocking: bool
    violation_message: Optional[str]

    model_config = {"from_attributes": True}


class CreateContentFilterRuleRequest(BaseModel):
    name: str
    description: Optional[str] = None
    rule_type: str
    pattern: Optional[str] = None
    external_url: Optional[str] = None
    extra_params: Optional[dict] = None
    fields_to_check: Optional[str] = "content"
    is_active: bool = True
    is_blocking: bool = True
    violation_message: Optional[str] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate rule name format."""
        if not v:
            raise ValueError("Rule name cannot be empty")
        if len(v) < 3:
            raise ValueError("Rule name must be at least 3 characters long")
        if len(v) > 100:
            raise ValueError("Rule name must be at most 100 characters long")
        # Allow alphanumeric, underscore, hyphen, and space
        if not re.match(r"^[a-zA-Z0-9_\-\s]+$", v):
            raise ValueError(
                "Rule name can only contain letters, numbers, underscores, hyphens, and spaces"
            )
        return v

    @field_validator("description")
    @classmethod
    def validate_description(cls, v: Optional[str]) -> Optional[str]:
        """Validate description length if provided."""
        if v is None:
            return v
        if len(v) > 500:
            raise ValueError("Description must be at most 500 characters long")
        return v

    @field_validator("rule_type")
    @classmethod
    def validate_rule_type(cls, v: str) -> str:
        """Validate rule_type is one of the supported types."""
        valid_types = {"REGEX", "KEYWORD", "EXTERNAL_URL"}
        if v not in valid_types:
            raise ValueError(f"rule_type must be one of: {', '.join(valid_types)}")
        return v

    @model_validator(mode="after")
    def validate_rule_fields(self) -> "CreateContentFilterRuleRequest":
        """Validate that pattern and external_url are used correctly based on rule_type."""
        if self.rule_type == "EXTERNAL_URL":
            if self.pattern is not None:
                raise ValueError(
                    "pattern cannot be provided when rule_type is EXTERNAL_URL"
                )
            if not self.external_url:
                raise ValueError(
                    "external_url is required when rule_type is EXTERNAL_URL"
                )
        elif self.rule_type in ("REGEX", "KEYWORD"):
            if not self.pattern:
                raise ValueError(
                    f"pattern is required when rule_type is {self.rule_type}"
                )
            if self.external_url is not None:
                raise ValueError(
                    f"external_url cannot be provided when rule_type is {self.rule_type}"
                )
        return self


class UpdateContentFilterRuleRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    rule_type: Optional[str] = None
    pattern: Optional[str] = None
    external_url: Optional[str] = None
    extra_params: Optional[dict] = None
    fields_to_check: Optional[str] = None
    is_active: Optional[bool] = None
    is_blocking: Optional[bool] = None
    violation_message: Optional[str] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        """Validate rule name format if provided."""
        if v is None:
            return v
        if not v:
            raise ValueError("Rule name cannot be empty")
        if len(v) < 3:
            raise ValueError("Rule name must be at least 3 characters long")
        if len(v) > 100:
            raise ValueError("Rule name must be at most 100 characters long")
        # Allow alphanumeric, underscore, hyphen, and space
        if not re.match(r"^[a-zA-Z0-9_\-\s]+$", v):
            raise ValueError(
                "Rule name can only contain letters, numbers, underscores, hyphens, and spaces"
            )
        return v

    @field_validator("description")
    @classmethod
    def validate_description(cls, v: Optional[str]) -> Optional[str]:
        """Validate description length if provided."""
        if v is None:
            return v
        if len(v) > 500:
            raise ValueError("Description must be at most 500 characters long")
        return v

    @field_validator("rule_type")
    @classmethod
    def validate_rule_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate rule_type is one of the supported types."""
        if v is None:
            return v
        valid_types = {"REGEX", "KEYWORD", "EXTERNAL_URL"}
        if v not in valid_types:
            raise ValueError(f"rule_type must be one of: {', '.join(valid_types)}")
        return v

    @model_validator(mode="after")
    def validate_rule_fields(self) -> "UpdateContentFilterRuleRequest":
        """Validate that pattern and external_url are used correctly based on rule_type."""
        if self.rule_type == "EXTERNAL_URL":
            if self.pattern is not None:
                raise ValueError(
                    "pattern cannot be provided when rule_type is EXTERNAL_URL"
                )
            if self.external_url is not None and not self.external_url:
                raise ValueError(
                    "external_url cannot be empty when rule_type is EXTERNAL_URL"
                )
        elif self.rule_type in ("REGEX", "KEYWORD"):
            if self.pattern is not None and not self.pattern:
                raise ValueError(
                    f"pattern cannot be empty when rule_type is {self.rule_type}"
                )
            if self.external_url is not None:
                raise ValueError(
                    f"external_url cannot be provided when rule_type is {self.rule_type}"
                )
        return self


class TestContentFilterRequest(BaseModel):
    content: str


class TestContentFilterResponse(BaseModel):
    is_violation: bool
    rule_name: Optional[str] = None
    rule_type: Optional[str] = None
    message: Optional[str] = None
    matched_text: Optional[str] = None


@router.get("/content-filters", response_model=List[ContentFilterRuleResponse])
async def list_content_filter_rules(
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> List[ContentFilterRuleResponse]:
    """List all content filter rules."""
    rules = await content_filter_service.list_rules(db)
    return [ContentFilterRuleResponse.model_validate(r) for r in rules]


@router.post(
    "/content-filters",
    response_model=ContentFilterRuleResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_content_filter_rule(
    body: CreateContentFilterRuleRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> ContentFilterRuleResponse:
    """Create a new content filter rule."""
    # Check if name already exists
    existing = await content_filter_service.get_rule_by_name(db, body.name)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Rule with name '{body.name}' already exists",
        )

    # Validate rule type
    valid_types = {"REGEX", "KEYWORD", "EXTERNAL_URL"}
    if body.rule_type not in valid_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid rule_type. Must be one of: {', '.join(valid_types)}",
        )

    # Validate regex pattern if provided
    if body.rule_type == "REGEX" and body.pattern:
        import re

        try:
            re.compile(body.pattern)
        except re.error as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid regex pattern: {str(e)}",
            )
    try:
        rule = await content_filter_service.create_rule(
            db,
            name=body.name,
            description=body.description,
            rule_type=body.rule_type,
            pattern=body.pattern,
            external_url=body.external_url,
            extra_params=body.extra_params,
            fields_to_check=body.fields_to_check,
            is_active=body.is_active,
            is_blocking=body.is_blocking,
            violation_message=body.violation_message,
            created_by=admin.id,
        )
        return _rule_to_response(rule)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid rule: {str(e)}",
        )


@router.get("/content-filters/{rule_id}", response_model=ContentFilterRuleResponse)
async def get_content_filter_rule(
    rule_id: int,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> ContentFilterRuleResponse:
    """Get a content filter rule by ID."""
    rule = await content_filter_service.get_rule(db, rule_id)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule with ID {rule_id} not found",
        )
    return _rule_to_response(rule)


@router.patch("/content-filters/{rule_id}", response_model=ContentFilterRuleResponse)
async def update_content_filter_rule(
    rule_id: int,
    body: UpdateContentFilterRuleRequest,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> ContentFilterRuleResponse:
    """Update a content filter rule."""
    rule = await content_filter_service.get_rule(db, rule_id)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule with ID {rule_id} not found",
        )

    # Validate regex pattern if being updated
    if body.rule_type == "REGEX" and body.pattern:
        import re

        try:
            re.compile(body.pattern)
        except re.error as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid regex pattern: {str(e)}",
            )

    # Filter out None values
    update_data = {k: v for k, v in body.model_dump().items() if v is not None}

    if update_data:
        rule = await content_filter_service.update_rule(db, rule, **update_data)

    return _rule_to_response(rule)


@router.delete("/content-filters/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_content_filter_rule(
    rule_id: int,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> None:
    """Delete a content filter rule."""
    rule = await content_filter_service.get_rule(db, rule_id)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule with ID {rule_id} not found",
        )
    await content_filter_service.delete_rule(db, rule)


@router.post("/content-filters/test", response_model=TestContentFilterResponse)
async def test_content_filter(
    body: TestContentFilterRequest,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> TestContentFilterResponse:
    """Test content against active filter rules without applying changes."""
    result = await content_filter_service.check_content(body.content)
    return TestContentFilterResponse(
        is_violation=result.is_violation,
        rule_name=result.rule_name,
        rule_type=result.rule_type,
        message=result.message,
        matched_text=result.matched_text,
    )


# ---------------------------------------------------------------------------
# Cluster Management Routes
# ---------------------------------------------------------------------------


class ClusterResponse(BaseModel):
    """Response schema for cluster information including runtime stats."""

    cluster_id: str
    model_id: str
    base_url: Optional[str]
    number_of_replicas: Optional[int]
    seed_capacity_tokens_per_pod: Optional[int]
    inferred_capacity: int
    is_enabled: bool
    auto_adapt_capacity: bool
    priority: int
    load_ratio: float
    inflight_tokens: int
    inflight_requests: int
    is_healthy: bool
    is_local: bool
    last_updated: Optional[datetime.datetime]

    # Performance tuning knobs
    ttft_budget_ms: float
    itl_budget_ms: float
    ttft_alpha: float
    itl_alpha: float
    capacity_alpha: float

    model_config = {"from_attributes": True}


class ClusterListResponse(BaseModel):
    """Response schema for list of clusters."""

    data: List[ClusterResponse]
    model_id: str


async def require_admin_or_multicluster(
    current_user: Annotated[User, Depends(get_current_user)],
) -> User:
    """Require the caller to be an admin or multicluster admin."""
    # For now, only full admins can manage clusters
    # TODO: Add multicluster_admin role check when implemented
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return current_user


def _get_local_cluster_id() -> str:
    """Get the local cluster ID from settings."""
    from app.core.config import settings

    return settings.ROUTER_CLUSTER_ID or "incluster"


async def _get_runtime_stats(model_id: str, cluster_id: str) -> dict:
    """Get runtime stats for a cluster from Redis-backed metrics."""
    from app.backends.multi_cluster import MultiClusterBackend

    backend = backend_registry.get(model_id)
    if not backend or not isinstance(backend, MultiClusterBackend):
        # Try to get metrics directly from Redis
        metrics = await cluster_metrics_store.get_all_metrics(model_id, cluster_id)
        return {
            "load_ratio": 0.0,
            "inflight_tokens": metrics.get("inflight_tokens", 0),
            "inflight_requests": 0,
            "is_healthy": True,
            "inferred_capacity": 0,
        }

    cluster_endpoint = backend._clusters.get(cluster_id)
    if not cluster_endpoint:
        # Try to get metrics directly from Redis
        metrics = await cluster_metrics_store.get_all_metrics(model_id, cluster_id)
        return {
            "load_ratio": 0.0,
            "inflight_tokens": metrics.get("inflight_tokens", 0),
            "inflight_requests": 0,
            "is_healthy": True,
            "inferred_capacity": 0,
        }

    # Get metrics from Redis
    metrics = await cluster_metrics_store.get_all_metrics(model_id, cluster_id)
    inflight_tokens = metrics.get("inflight_tokens", 0)
    adaptive_capacity = metrics.get("adaptive_capacity", 0)

    # Calculate load ratio
    if adaptive_capacity > 0:
        load_ratio = inflight_tokens / adaptive_capacity
    else:
        load_ratio = 0.0

    return {
        "load_ratio": load_ratio,
        "inflight_tokens": inflight_tokens,
        "inflight_requests": 0,  # Not tracked separately in Redis
        "is_healthy": True,  # Could be enhanced with health checks
        "inferred_capacity": cluster_endpoint.inferred_capacity,
    }


def _cluster_to_response(
    cluster: Any, model_id: str, runtime_stats: dict, local_cluster_id: str
) -> ClusterResponse:
    """Convert a ModelCluster ORM object to a ClusterResponse DTO."""
    # Use runtime stats inferred_capacity if it's meaningful (> 0), otherwise calculate from DB values
    # Runtime returns 0 when the cluster hasn't been initialized or the endpoint doesn't exist
    if runtime_stats.get("inferred_capacity", 0) > 0:
        inferred_capacity = runtime_stats["inferred_capacity"]
    else:
        replicas = cluster.number_of_replicas or 1
        capacity_per_pod = cluster.seed_capacity_tokens_per_pod or 40000
        inferred_capacity = replicas * capacity_per_pod

    return ClusterResponse(
        cluster_id=cluster.cluster_id,
        model_id=model_id,
        base_url=cluster.base_url,
        number_of_replicas=cluster.number_of_replicas,
        seed_capacity_tokens_per_pod=cluster.seed_capacity_tokens_per_pod,
        inferred_capacity=inferred_capacity,
        is_enabled=cluster.is_enabled,
        auto_adapt_capacity=cluster.auto_adapt_capacity,
        priority=cluster.priority,
        load_ratio=runtime_stats.get("load_ratio", 0.0),
        inflight_tokens=runtime_stats.get("inflight_tokens", 0),
        inflight_requests=runtime_stats.get("inflight_requests", 0),
        is_healthy=runtime_stats.get("is_healthy", True),
        is_local=(cluster.cluster_id == local_cluster_id),
        last_updated=cluster.updated_at,
        # Performance tuning knobs
        ttft_budget_ms=cluster.ttft_budget_ms,
        itl_budget_ms=cluster.itl_budget_ms,
        ttft_alpha=cluster.ttft_alpha,
        itl_alpha=cluster.itl_alpha,
        capacity_alpha=cluster.capacity_alpha,
    )


@router.get(
    "/models/{model_id:path}/clusters",
    response_model=ClusterListResponse,
)
async def list_clusters(
    model_id: str,
    _admin: Annotated[User, Depends(require_admin_or_multicluster)],
    db: AsyncSession = Depends(get_db),
) -> ClusterListResponse:
    """List all clusters for a model with runtime stats."""
    clusters = await cluster_manager.get_clusters(db, model_id)
    local_cluster_id = _get_local_cluster_id()

    data = []
    for cluster in clusters:
        runtime_stats = await _get_runtime_stats(model_id, cluster.cluster_id)
        data.append(
            _cluster_to_response(cluster, model_id, runtime_stats, local_cluster_id)
        )

    return ClusterListResponse(data=data, model_id=model_id)


@router.get(
    "/models/{model_id:path}/clusters/{cluster_id}",
    response_model=ClusterResponse,
)
async def get_cluster(
    model_id: str,
    cluster_id: str,
    _admin: Annotated[User, Depends(require_admin_or_multicluster)],
    db: AsyncSession = Depends(get_db),
) -> ClusterResponse:
    """Get details for a specific cluster including runtime stats."""
    cluster = await cluster_manager.get_cluster(db, model_id, cluster_id)
    if not cluster:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster '{cluster_id}' not found for model '{model_id}'",
        )

    local_cluster_id = _get_local_cluster_id()
    runtime_stats = await _get_runtime_stats(model_id, cluster_id)

    return _cluster_to_response(cluster, model_id, runtime_stats, local_cluster_id)


@router.post(
    "/models/{model_id:path}/clusters",
    response_model=ClusterResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_cluster(
    model_id: str,
    body: ClusterCreate,
    admin: Annotated[User, Depends(require_admin_or_multicluster)],
    db: AsyncSession = Depends(get_db),
) -> ClusterResponse:
    """Create a new cluster for a model.

    The cluster will be created in the database, synced to Redis for cross-pod
    propagation via ConfigSyncService, and immediately added to the local runtime.
    """
    cluster = await cluster_manager.create_cluster(
        db=db,
        model_id=model_id,
        data=body,
        updated_by=admin.id,
    )

    # Note: create_cluster() internally calls sync_to_runtime_and_redis()
    # which handles both local runtime update and Redis propagation

    local_cluster_id = _get_local_cluster_id()
    runtime_stats = await _get_runtime_stats(model_id, cluster.cluster_id)

    return _cluster_to_response(cluster, model_id, runtime_stats, local_cluster_id)


@router.patch(
    "/models/{model_id:path}/clusters/{cluster_id}",
    response_model=ClusterResponse,
)
async def update_cluster(
    model_id: str,
    cluster_id: str,
    body: ClusterUpdate,
    admin: Annotated[User, Depends(require_admin_or_multicluster)],
    db: AsyncSession = Depends(get_db),
) -> ClusterResponse:
    """Update cluster configuration."""
    cluster = await cluster_manager.update_cluster(
        db=db,
        model_id=model_id,
        cluster_id=cluster_id,
        data=body,
        updated_by=admin.id,
    )

    # Sync changes to runtime and Redis (propagates to other pods via ConfigSyncService)
    await cluster_manager.sync_to_runtime_and_redis(db, model_id, cluster_id)

    local_cluster_id = _get_local_cluster_id()
    runtime_stats = await _get_runtime_stats(model_id, cluster_id)

    return _cluster_to_response(cluster, model_id, runtime_stats, local_cluster_id)


@router.post(
    "/models/{model_id:path}/clusters/{cluster_id}/enable",
    response_model=ClusterResponse,
)
async def enable_cluster(
    model_id: str,
    cluster_id: str,
    admin: Annotated[User, Depends(require_admin_or_multicluster)],
    db: AsyncSession = Depends(get_db),
) -> ClusterResponse:
    """Enable a cluster for routing.

    Resumes the cluster from DRAINING or DISABLED state if needed.
    """
    cluster = await cluster_manager.enable_cluster(
        db=db,
        model_id=model_id,
        cluster_id=cluster_id,
        updated_by=admin.id,
    )

    # Sync changes to runtime and Redis (adds cluster back to routing pool)
    await cluster_manager.sync_to_runtime_and_redis(db, model_id, cluster_id)

    local_cluster_id = _get_local_cluster_id()
    runtime_stats = await _get_runtime_stats(model_id, cluster_id)

    return _cluster_to_response(cluster, model_id, runtime_stats, local_cluster_id)


@router.post(
    "/models/{model_id:path}/clusters/{cluster_id}/disable",
    response_model=ClusterResponse,
)
async def disable_cluster(
    model_id: str,
    cluster_id: str,
    admin: Annotated[User, Depends(require_admin_or_multicluster)],
    db: AsyncSession = Depends(get_db),
) -> ClusterResponse:
    """Disable a cluster from routing with mandatory drain.

    Always drains the cluster first to allow in-flight requests to complete.
    The cluster transitions: ENABLED -> DRAINING -> DISABLED (automatically).
    """
    cluster = await cluster_manager.disable_cluster(
        db=db,
        model_id=model_id,
        cluster_id=cluster_id,
        updated_by=admin.id,
    )

    # Sync changes to runtime and Redis (propagates to other pods via ConfigSyncService)
    await cluster_manager.sync_to_runtime_and_redis(db, model_id, cluster_id)

    local_cluster_id = _get_local_cluster_id()
    runtime_stats = await _get_runtime_stats(model_id, cluster_id)

    return _cluster_to_response(cluster, model_id, runtime_stats, local_cluster_id)


# ---------------------------------------------------------------------------
# Service account management routes (admin+)
# ---------------------------------------------------------------------------


class ServiceAccountResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    owner_team: Optional[str]
    is_active: bool
    max_concurrent_requests: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    created_by: Optional[int]

    model_config = {"from_attributes": True}


class CreateServiceAccountRequest(BaseModel):
    name: str
    description: Optional[str] = None
    owner_team: Optional[str] = None
    max_concurrent_requests: Optional[int] = None
    bucket_ids: Optional[List[int]] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v:
            raise ValueError("Name cannot be empty")
        if len(v) < 3:
            raise ValueError("Name must be at least 3 characters long")
        if len(v) > 255:
            raise ValueError("Name must be at most 255 characters long")
        if not re.match(r"^[a-zA-Z0-9_.-]+$", v):
            raise ValueError(
                "Name can only contain letters, numbers, underscores, hyphens, and dots"
            )
        return v

    @field_validator("max_concurrent_requests")
    @classmethod
    def validate_max_concurrent(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 1:
            raise ValueError("max_concurrent_requests must be >= 1")
        return v


class SetServiceActiveRequest(BaseModel):
    is_active: bool


class IssueKeyRequest(BaseModel):
    name: str
    scopes: Optional[List[str]] = None
    model_ids: Optional[List[str]] = None
    max_concurrent_requests: Optional[int] = None
    expires_in_days: Optional[int] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v:
            raise ValueError("Key name cannot be empty")
        if len(v) > 100:
            raise ValueError("Key name must be at most 100 characters long")
        return v

    @field_validator("expires_in_days")
    @classmethod
    def validate_expires(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 1:
            raise ValueError("expires_in_days must be >= 1")
        return v

    @field_validator("max_concurrent_requests")
    @classmethod
    def validate_max_concurrent(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 1:
            raise ValueError("max_concurrent_requests must be >= 1")
        return v


class IssueKeyResponse(BaseModel):
    id: int
    name: str
    prefix: str
    scopes: Optional[List[str]]
    model_ids: Optional[List[str]]
    max_concurrent_requests: Optional[int]
    expires_at: Optional[datetime.datetime]
    created_at: datetime.datetime
    api_key: str = Field(
        description="Plaintext API key — shown exactly once. Store it now; it cannot be retrieved later."
    )

    model_config = {"from_attributes": True}


class ApiKeyMetaResponse(BaseModel):
    """Key metadata returned on list — NEVER includes the secret or hash."""
    id: int
    name: str
    prefix: str
    scopes: Optional[List[str]]
    model_ids: Optional[List[str]]
    max_concurrent_requests: Optional[int]
    created_at: datetime.datetime
    expires_at: Optional[datetime.datetime]
    last_used_at: Optional[datetime.datetime]
    revoked_at: Optional[datetime.datetime]

    model_config = {"from_attributes": True}


@router.post(
    "/service-accounts",
    response_model=ServiceAccountResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_service_account(
    body: CreateServiceAccountRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> ServiceAccountResponse:
    """
    Create a service account.

    Service accounts are not LDAP users — they authenticate via opaque API keys
    issued at POST /admin/service-accounts/{service_id}/keys.
    Optionally grant immediate bucket access via bucket_ids.
    """
    account = await service_account_service.create_service_account(
        db,
        name=body.name,
        description=body.description,
        owner_team=body.owner_team,
        max_concurrent_requests=body.max_concurrent_requests,
        bucket_ids=body.bucket_ids,
        by_user_id=admin.id,
    )
    return ServiceAccountResponse.model_validate(account)


@router.get("/service-accounts", response_model=List[ServiceAccountResponse])
async def list_service_accounts(
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> List[ServiceAccountResponse]:
    """List all service accounts (metadata only)."""
    accounts = await service_account_service.list_service_accounts(db)
    return [ServiceAccountResponse.model_validate(a) for a in accounts]


@router.get(
    "/service-accounts/{service_id}",
    response_model=ServiceAccountResponse,
)
async def get_service_account(
    service_id: int,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> ServiceAccountResponse:
    """Get a single service account by ID."""
    account = await service_account_service.get_service_account(db, service_id)
    return ServiceAccountResponse.model_validate(account)


@router.patch(
    "/service-accounts/{service_id}/active",
    response_model=ServiceAccountResponse,
)
async def set_service_account_active(
    service_id: int,
    body: SetServiceActiveRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> ServiceAccountResponse:
    """Activate or deactivate a service account.

    On deactivation all active key cache entries are evicted immediately so
    the account is rejected on the next request rather than after the cache TTL.
    """
    account = await service_account_service.set_active(
        db, service_id=service_id, is_active=body.is_active, by_user_id=admin.id
    )
    if not body.is_active:
        await api_key_service.evict_service_cache(db, redis, service_id)
    return ServiceAccountResponse.model_validate(account)


@router.post(
    "/service-accounts/{service_id}/buckets/{bucket_id}",
    status_code=status.HTTP_201_CREATED,
)
async def grant_service_bucket_access(
    service_id: int,
    bucket_id: int,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Grant a service account access to a bucket."""
    # Verify the service account exists first
    await service_account_service.get_service_account(db, service_id)
    await service_account_service.grant_bucket_access(
        db, service_id=service_id, bucket_id=bucket_id, by_user_id=admin.id
    )
    return {"detail": f"Access granted: service {service_id} -> bucket {bucket_id}"}


@router.delete(
    "/service-accounts/{service_id}/buckets/{bucket_id}",
    status_code=status.HTTP_200_OK,
)
async def revoke_service_bucket_access(
    service_id: int,
    bucket_id: int,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Revoke a service account's access to a bucket."""
    await service_account_service.get_service_account(db, service_id)
    await service_account_service.revoke_bucket_access(
        db, service_id=service_id, bucket_id=bucket_id, by_user_id=admin.id
    )
    return {"detail": f"Access revoked: service {service_id} -/-> bucket {bucket_id}"}


@router.get(
    "/service-accounts/{service_id}/buckets",
    response_model=List[int],
)
async def list_service_bucket_ids(
    service_id: int,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> List[int]:
    """List bucket IDs explicitly granted to a service account.

    Only buckets that have at least one currently-available backend model are
    returned, matching the availability filter applied by
    ``GET /admin/users/{user_id}/buckets``.
    """
    await service_account_service.get_service_account(db, service_id)
    available_model_ids = set(backend_registry.list_model_ids())
    return await service_account_service.list_bucket_ids(
        db, service_id, available_model_ids=available_model_ids
    )


@router.post(
    "/service-accounts/{service_id}/keys",
    response_model=IssueKeyResponse,
    status_code=status.HTTP_201_CREATED,
)
async def issue_key(
    service_id: int,
    body: IssueKeyRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> IssueKeyResponse:
    """
    Issue a new API key for a service account.

    The plaintext key is returned in the ``api_key`` field and shown EXACTLY
    ONCE. It is not stored anywhere in the system. Store it securely immediately.
    """
    # Verify the service account exists
    account = await service_account_service.get_service_account(db, service_id)

    # Fix 3: block key issuance for inactive accounts
    if not account.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot issue a key for an inactive service account",
        )

    # Fix 4: validate requested model_ids against the account's actual access
    if body.model_ids:
        available_model_ids = set(backend_registry.list_model_ids())
        account_accessible = set(
            await service_account_service.get_accessible_models(
                db, service_id, available_model_ids=available_model_ids
            )
        )
        bad = set(body.model_ids) - account_accessible
        if bad:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"model_ids not accessible to this service account: {sorted(bad)}",
            )

    expires_at: Optional[datetime.datetime] = None
    if body.expires_in_days is not None:
        expires_at = datetime.datetime.utcnow() + datetime.timedelta(days=body.expires_in_days)

    row, plaintext_key = await api_key_service.create_key(
        db,
        service_id=service_id,
        name=body.name,
        scopes=body.scopes,
        model_ids=body.model_ids,
        max_concurrent_requests=body.max_concurrent_requests,
        expires_at=expires_at,
        created_by=admin.id,
    )

    return IssueKeyResponse(
        id=row.id,
        name=row.name,
        prefix=row.prefix,
        scopes=row.scopes,
        model_ids=row.model_ids,
        max_concurrent_requests=row.max_concurrent_requests,
        expires_at=row.expires_at,
        created_at=row.created_at,
        api_key=plaintext_key,
    )


@router.get(
    "/service-accounts/{service_id}/keys",
    response_model=List[ApiKeyMetaResponse],
)
async def list_keys(
    service_id: int,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> List[ApiKeyMetaResponse]:
    """
    List API keys for a service account.

    Returns metadata only — the secret and hash are NEVER returned.
    """
    await service_account_service.get_service_account(db, service_id)
    keys = await api_key_service.list_keys(db, service_id)
    return [ApiKeyMetaResponse.model_validate(k) for k in keys]


@router.delete(
    "/service-accounts/{service_id}/keys/{key_id}",
    status_code=status.HTTP_200_OK,
)
async def revoke_key(
    service_id: int,
    key_id: int,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> dict:
    """
    Revoke a service account API key (soft delete).

    The key is rejected on the next request. The Redis cache entry is also
    evicted immediately so rejection is near-instant rather than waiting for
    the cache TTL.
    """
    await service_account_service.get_service_account(db, service_id)
    await api_key_service.revoke_key(
        db, redis, key_id=key_id, service_id=service_id
    )
    return {"detail": f"API key {key_id} revoked"}


# ---------------------------------------------------------------------------
# Model Backend Management (ADR-001: Dynamic Model Backend Management)
# ---------------------------------------------------------------------------


# -- Pydantic schemas --------------------------------------------------------


class CreateModelBackendRequest(BaseModel):
    """Request body for POST /admin/model-backends."""

    model_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        pattern=r"^[a-zA-Z0-9_\-./]+$",
        description="Unique model identifier (alphanumeric, underscores, hyphens, dots, slashes).",
    )
    base_url: str = Field(
        ...,
        description="Base URL of the inference server (trailing slash is stripped).",
    )
    api_key: Optional[str] = Field(
        default=None,
        description=(
            "Plaintext API key. Stored encrypted at rest. "
            "Omit or pass null to fall back to the global API_KEY env var."
        ),
    )

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        """Require http:// or https:// and strip trailing slash."""
        stripped = v.strip()
        if not stripped.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return stripped.rstrip("/")


class PatchModelBackendRequest(BaseModel):
    """Request body for PATCH /admin/model-backends/{model_id}.

    All fields are optional.  For api_key:
      - absent from JSON body  → api_key_enc untouched (model_dump(exclude_unset=True)
                                 omits the field, so the service layer never touches it)
      - null or ""             → clear api_key_enc to NULL (falls back to global API_KEY env var)
      - non-empty string       → re-encrypt and store as new ciphertext
    Note: because the model uses Optional[str] = None, passing null and omitting the field
    are distinguishable ONLY via exclude_unset=True in the patch handler.
    """

    base_url: Optional[str] = None
    api_key: Optional[str] = None
    is_enabled: Optional[bool] = None

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        stripped = v.strip()
        if not stripped.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return stripped.rstrip("/")


class ModelBackendResponse(BaseModel):
    """Response shape for model_backend rows.

    api_key is always null — the plaintext and ciphertext are never returned.
    registered and available are derived from the in-process BackendRegistry.
    """

    model_id: str
    backend_type: str
    base_url: str
    is_enabled: bool
    source: str
    version: int
    created_at: Optional[datetime.datetime]
    updated_at: Optional[datetime.datetime]
    created_by: Optional[int]
    updated_by: Optional[int]
    # Hard-coded null — key material MUST NOT leak through serialization.
    api_key: None = None
    # Derived from the in-process BackendRegistry (this pod's view).
    registered: bool
    available: bool

    model_config = {"from_attributes": True}


class TestConnectionRequest(BaseModel):
    """Request body for POST /admin/model-backends/test-connection."""

    base_url: str
    api_key: Optional[str] = None

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return stripped.rstrip("/")


class TestConnectionResponse(BaseModel):
    """Response shape for POST /admin/model-backends/test-connection."""

    reachable: bool
    status_code: Optional[int] = None
    error: Optional[str] = None


# -- Helper ------------------------------------------------------------------


def _model_backend_to_response(row) -> ModelBackendResponse:
    """
    Build a ModelBackendResponse from a ModelBackendRow.

    registered = the model_id is present in the in-process BackendRegistry.
    available  = registered AND backend.is_available is True.
    api_key    = always None (hard-coded; key material never leaves the service layer).
    """
    backend = backend_registry.get(row.model_id)
    registered = backend is not None
    available = registered and backend.is_available
    return ModelBackendResponse(
        model_id=row.model_id,
        backend_type=row.backend_type,
        base_url=row.base_url,
        is_enabled=row.is_enabled,
        source=row.source,
        version=row.version,
        created_at=row.created_at,
        updated_at=row.updated_at,
        created_by=row.created_by,
        updated_by=row.updated_by,
        api_key=None,
        registered=registered,
        available=available,
    )


# -- Route handlers ----------------------------------------------------------

# IMPORTANT: /test-connection must be declared BEFORE /{model_id} routes so
# FastAPI does not interpret "test-connection" as a model_id path parameter.


@router.post(
    "/model-backends/test-connection",
    response_model=TestConnectionResponse,
)
async def test_model_backend_connection(
    body: TestConnectionRequest,
    _admin: Annotated[User, Depends(require_admin)],
) -> TestConnectionResponse:
    """
    Test reachability of a model backend by hitting GET {base_url}/models.

    base_url is expected to already include the API-version path segment (e.g.
    http://host:9000/v1), consistent with how backends are configured in
    config/models.yaml and how the health probe (validate_api_key) and proxy
    (StaticBackend.forward) build upstream URLs.

    The api_key is never logged.  Connection failures return reachable=false
    with an error string rather than raising HTTP 500.

    Security note: this endpoint lets an admin probe arbitrary URLs.  That is
    intentional for an admin-only reachability tool; it is guarded by require_admin.
    """
    url = f"{body.base_url.rstrip('/')}/models"
    headers: dict = {}
    if body.api_key:
        headers["Authorization"] = f"Bearer {body.api_key}"

    try:
        async with new_http_client() as client:
            response = await client.get(url, headers=headers, timeout=5.0)
        # Note: raise_for_status() is intentionally NOT called — a 4xx/5xx from
        # the backend still means it is reachable; only connection-level errors
        # (ConnectError, TimeoutException, etc.) set reachable=False.
        return TestConnectionResponse(
            reachable=True,
            status_code=response.status_code,
            error=None,
        )
    except Exception as exc:
        return TestConnectionResponse(
            reachable=False,
            status_code=None,
            error=str(exc),
        )


@router.post(
    "/model-backends",
    response_model=ModelBackendResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_model_backend(
    body: CreateModelBackendRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> ModelBackendResponse:
    """Create a new DB-managed model backend."""
    row = await model_backend_service.create_backend(
        db,
        model_id=body.model_id,
        base_url=body.base_url,
        api_key=body.api_key,
        created_by=admin.id,
    )
    try:
        await database_backend_source.reconcile()
    except Exception as exc:
        logger.warning(
            "model-backends write: inline reconcile failed (%s); poll loop will converge within %ds",
            exc,
            get_settings().DB_BACKEND_POLL_INTERVAL_SECONDS,
        )
    await publish_models_changed()
    return _model_backend_to_response(row)


@router.get(
    "/model-backends",
    response_model=List[ModelBackendResponse],
)
async def list_model_backends(
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> List[ModelBackendResponse]:
    """List all DB-managed model backends."""
    rows = await model_backend_service.list_backends(db)
    return [_model_backend_to_response(r) for r in rows]


@router.get(
    "/model-backends/{model_id:path}",
    response_model=ModelBackendResponse,
)
async def get_model_backend(
    model_id: str,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> ModelBackendResponse:
    """Get a single DB-managed model backend by model_id."""
    row = await model_backend_service.get_backend(db, model_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Model backend {model_id!r} not found",
        )
    return _model_backend_to_response(row)


@router.patch(
    "/model-backends/{model_id:path}",
    response_model=ModelBackendResponse,
)
async def patch_model_backend(
    model_id: str,
    body: PatchModelBackendRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> ModelBackendResponse:
    """Partially update a DB-managed model backend."""
    # Build patch dict from the request body.
    # Use model_dump(exclude_unset=True) to distinguish "field absent" from
    # "field present with value None / empty string".
    patch = body.model_dump(exclude_unset=True)
    row = await model_backend_service.update_backend(
        db,
        model_id=model_id,
        patch=patch,
        updated_by=admin.id,
    )
    try:
        await database_backend_source.reconcile()
    except Exception as exc:
        logger.warning(
            "model-backends write: inline reconcile failed (%s); poll loop will converge within %ds",
            exc,
            get_settings().DB_BACKEND_POLL_INTERVAL_SECONDS,
        )
    await publish_models_changed()
    return _model_backend_to_response(row)


@router.post(
    "/model-backends/{model_id:path}/enable",
    response_model=ModelBackendResponse,
)
async def enable_model_backend(
    model_id: str,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> ModelBackendResponse:
    """Enable a DB-managed model backend."""
    row = await model_backend_service.enable_backend(db, model_id, updated_by=admin.id)
    try:
        await database_backend_source.reconcile()
    except Exception as exc:
        logger.warning(
            "model-backends write: inline reconcile failed (%s); poll loop will converge within %ds",
            exc,
            get_settings().DB_BACKEND_POLL_INTERVAL_SECONDS,
        )
    await publish_models_changed()
    return _model_backend_to_response(row)


@router.post(
    "/model-backends/{model_id:path}/disable",
    response_model=ModelBackendResponse,
)
async def disable_model_backend(
    model_id: str,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> ModelBackendResponse:
    """Disable a DB-managed model backend."""
    row = await model_backend_service.disable_backend(db, model_id, updated_by=admin.id)
    try:
        await database_backend_source.reconcile()
    except Exception as exc:
        logger.warning(
            "model-backends write: inline reconcile failed (%s); poll loop will converge within %ds",
            exc,
            get_settings().DB_BACKEND_POLL_INTERVAL_SECONDS,
        )
    await publish_models_changed()
    return _model_backend_to_response(row)


@router.delete(
    "/model-backends/{model_id:path}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_model_backend(
    model_id: str,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> None:
    """Delete a DB-managed model backend (rejected if referenced by any bucket)."""
    await model_backend_service.delete_backend(db, model_id)
    try:
        await database_backend_source.reconcile()
    except Exception as exc:
        logger.warning(
            "model-backends write: inline reconcile failed (%s); poll loop will converge within %ds",
            exc,
            get_settings().DB_BACKEND_POLL_INTERVAL_SECONDS,
        )
    await publish_models_changed()
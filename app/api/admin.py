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
  PATCH  /admin/buckets/{bucket_id}/default
  GET    /admin/models
"""

from __future__ import annotations

import logging
from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.auth_routes import get_current_user
from app.auth.ldap_auth import get_ldap_user_info
from app.backends.base import registry as backend_registry
from app.db.mysql import Bucket, User, get_db
from app.services.permission import permission_service

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


class CreateUserRequest(BaseModel):
    username: str
    email: Optional[str] = None


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
    models: List[BucketModelItem]

    model_config = {"from_attributes": True}


class CreateBucketRequest(BaseModel):
    name: str
    description: Optional[str] = None
    model_ids: List[str]


# ---------------------------------------------------------------------------
# User management routes (admin+)
# ---------------------------------------------------------------------------


@router.get("/users", response_model=List[UserResponse])
async def list_users(
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> List[UserResponse]:
    """List all users in the system."""
    users = await permission_service.list_users(db)
    return [UserResponse.model_validate(u) for u in users]


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
    email = body.email
    if email is None:
        ldap_info = get_ldap_user_info(body.username)
        email = ldap_info.get("email")

    user = await permission_service.create_user(
        db,
        username=body.username,
        email=email,
    )
    return UserResponse.model_validate(user)


@router.get("/users/{user_id}/buckets", response_model=List[int])
async def list_user_bucket_ids(
    user_id: int,
    _admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> List[int]:
    """List bucket IDs the user currently has access to."""
    return await permission_service.get_user_bucket_ids(db, user_id)


@router.patch("/users/{user_id}/active", response_model=UserResponse)
async def set_user_active(
    user_id: int,
    body: SetActiveRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    """Activate or deactivate a user account."""
    user = await permission_service.set_user_active(
        db, user_id=user_id, is_active=body.is_active, by_user_id=admin.id
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
            models=[BucketModelItem(model_id=m.model_id) for m in b.models],
        )
        for b in buckets
    ]


@router.post("/buckets", response_model=BucketResponse, status_code=status.HTTP_201_CREATED)
async def create_bucket(
    body: CreateBucketRequest,
    admin: Annotated[User, Depends(require_admin)],
    db: AsyncSession = Depends(get_db),
) -> BucketResponse:
    """Create a new bucket with the specified model_ids."""
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
        models=[BucketModelItem(model_id=m.model_id) for m in updated.models],
    )


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

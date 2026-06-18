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
from pydantic import BaseModel, field_validator, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.auth_routes import get_current_user
from app.auth.ldap_auth import get_ldap_user_info, verify_ldap_user_exists
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
from app.services.cluster_manager import ClusterCreate, ClusterUpdate, cluster_manager
from app.services.content_filter import content_filter_service
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

    email = body.email
    if email is None:
        ldap_info = get_ldap_user_info(body.username)
        if ldap_info and isinstance(ldap_info, dict):
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
    available_model_ids = set(backend_registry.list_model_ids())
    valid_buckets = list()
    for bucket in buckets:
        # Only include buckets that have at least one available model
        bucket_model_ids = {model.model_id for model in bucket.models}
        if bucket_model_ids and bucket_model_ids.intersection(available_model_ids):
            valid_buckets.append(bucket)
    return [
        BucketResponse(
            id=b.id,
            name=b.name,
            description=b.description,
            is_default=b.is_default,
            default_model=b.default_model,
            models=[BucketModelItem(model_id=m.model_id) for m in b.models],
        )
        for b in valid_buckets
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
    result = await content_filter_service.check_content(db, body.content)
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
"""
ServiceAccountService — create, list, and manage service accounts + their bucket access.

Service accounts live in the dedicated `service_accounts` table (NOT the users table).
They have no LDAP identity — they authenticate exclusively via opaque API keys
issued through ApiKeyService.

Model access follows the same bucket-union logic as PermissionService for users:
  accessible_models = union of all bucket models across granted buckets,
  intersected with available_model_ids from the backend registry.
The default bucket + default model are included on the same terms as for users.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.mysql import Bucket, ServiceAccount, ServiceBucketAccess

logger = logging.getLogger(__name__)


class ServiceAccountService:
    """Async service for managing service accounts and their bucket access."""

    # ------------------------------------------------------------------ #
    # Account CRUD
    # ------------------------------------------------------------------ #

    async def create_service_account(
        self,
        db: AsyncSession,
        *,
        name: str,
        description: Optional[str] = None,
        owner_team: Optional[str] = None,
        max_concurrent_requests: Optional[int] = None,
        bucket_ids: Optional[List[int]] = None,
        by_user_id: int,
    ) -> ServiceAccount:
        """
        Create a new service account.

        Raises:
            HTTPException(409) if a service account with *name* already exists.
            HTTPException(404) if any bucket_id in *bucket_ids* does not exist.
        """
        kwargs: dict = dict(
            name=name,
            description=description,
            owner_team=owner_team,
            created_by=by_user_id,
        )
        if max_concurrent_requests is not None:
            if max_concurrent_requests < 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="max_concurrent_requests must be >= 1",
                )
            kwargs["max_concurrent_requests"] = max_concurrent_requests

        account = ServiceAccount(**kwargs)
        db.add(account)
        try:
            await db.flush()
        except IntegrityError:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Service account with name {name!r} already exists",
            )

        if bucket_ids:
            for bucket_id in bucket_ids:
                await self.grant_bucket_access(
                    db, service_id=account.id, bucket_id=bucket_id, by_user_id=by_user_id
                )

        logger.info(
            "Service account %r (id=%d) created by user_id=%d",
            name,
            account.id,
            by_user_id,
        )
        return account

    async def get_service_account(
        self, db: AsyncSession, service_id: int
    ) -> ServiceAccount:
        """
        Fetch a service account by primary key.

        Raises:
            HTTPException(404) if not found.
        """
        result = await db.execute(
            select(ServiceAccount).where(ServiceAccount.id == service_id)
        )
        account = result.scalar_one_or_none()
        if account is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Service account {service_id} not found",
            )
        return account

    async def get_by_name(
        self, db: AsyncSession, name: str
    ) -> Optional[ServiceAccount]:
        """Return a ServiceAccount by name, or None if not found."""
        result = await db.execute(
            select(ServiceAccount).where(ServiceAccount.name == name)
        )
        return result.scalar_one_or_none()

    async def list_service_accounts(
        self, db: AsyncSession
    ) -> List[ServiceAccount]:
        """Return all service accounts ordered by id."""
        result = await db.execute(
            select(ServiceAccount).order_by(ServiceAccount.id)
        )
        return list(result.scalars().all())

    async def set_active(
        self,
        db: AsyncSession,
        service_id: int,
        is_active: bool,
        by_user_id: int,
    ) -> ServiceAccount:
        """Activate or deactivate a service account."""
        account = await self.get_service_account(db, service_id)
        account.is_active = is_active
        await db.flush()
        logger.info(
            "Service account %r (id=%d) set is_active=%s by user_id=%d",
            account.name,
            service_id,
            is_active,
            by_user_id,
        )
        return account

    async def is_active(self, db: AsyncSession, service_id: int) -> bool:
        """Return True if the service account exists and is active."""
        result = await db.execute(
            select(ServiceAccount.is_active).where(ServiceAccount.id == service_id)
        )
        row = result.one_or_none()
        if row is None:
            return False
        return bool(row[0])

    # ------------------------------------------------------------------ #
    # Bucket access helpers
    # ------------------------------------------------------------------ #

    async def grant_bucket_access(
        self,
        db: AsyncSession,
        service_id: int,
        bucket_id: int,
        by_user_id: int,
    ) -> ServiceBucketAccess:
        """Grant *service_id* access to *bucket_id*. Idempotent."""
        # Verify bucket exists
        bucket_result = await db.execute(
            select(Bucket).where(Bucket.id == bucket_id)
        )
        if bucket_result.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bucket {bucket_id} not found",
            )

        # Check for existing grant
        existing_result = await db.execute(
            select(ServiceBucketAccess).where(
                ServiceBucketAccess.service_id == service_id,
                ServiceBucketAccess.bucket_id == bucket_id,
            )
        )
        existing = existing_result.scalar_one_or_none()
        if existing is not None:
            return existing

        access = ServiceBucketAccess(
            service_id=service_id,
            bucket_id=bucket_id,
            granted_by=by_user_id,
        )
        db.add(access)
        await db.flush()
        logger.info(
            "Granted service_id=%d access to bucket %d by user_id=%d",
            service_id,
            bucket_id,
            by_user_id,
        )
        return access

    async def list_bucket_ids(
        self,
        db: AsyncSession,
        service_id: int,
        available_model_ids: Optional[set] = None,
    ) -> List[int]:
        """Return the explicit bucket IDs granted to *service_id* via service_bucket_access.

        When *available_model_ids* is provided, only buckets that have at least
        one model in that set are returned — mirroring
        ``permission_service.get_user_bucket_ids``'s filtering logic.
        """
        result = await db.execute(
            select(ServiceBucketAccess.bucket_id).where(
                ServiceBucketAccess.service_id == service_id
            )
        )
        bucket_ids = {row[0] for row in result.all()}

        if available_model_ids is not None:
            # Keep only buckets that have at least one currently-available model
            buckets_result = await db.execute(
                select(Bucket)
                .options(selectinload(Bucket.models))
                .where(Bucket.id.in_(bucket_ids))
            )
            buckets = buckets_result.scalars().all()
            valid_bucket_ids: set[int] = set()
            for bucket in buckets:
                bucket_model_ids = {model.model_id for model in bucket.models}
                if bucket_model_ids and bucket_model_ids.intersection(available_model_ids):
                    valid_bucket_ids.add(bucket.id)
            return list(valid_bucket_ids)

        return list(bucket_ids)

    async def revoke_bucket_access(
        self,
        db: AsyncSession,
        service_id: int,
        bucket_id: int,
        by_user_id: int,
    ) -> None:
        """Revoke *service_id*'s access to *bucket_id*. No-op if not granted."""
        result = await db.execute(
            select(ServiceBucketAccess).where(
                ServiceBucketAccess.service_id == service_id,
                ServiceBucketAccess.bucket_id == bucket_id,
            )
        )
        access = result.scalar_one_or_none()
        if access is not None:
            await db.delete(access)
            await db.flush()
            logger.info(
                "Revoked service_id=%d access to bucket %d by user_id=%d",
                service_id,
                bucket_id,
                by_user_id,
            )

    # ------------------------------------------------------------------ #
    # Model access
    # ------------------------------------------------------------------ #

    async def get_accessible_models(
        self,
        db: AsyncSession,
        service_id: int,
        available_model_ids: set,
    ) -> List[str]:
        """
        Return all model_ids accessible to *service_id* via their bucket grants.

        Mirrors PermissionService.get_user_accessible_models:
          - Includes the default bucket for every service (same as users).
          - Includes the default model from the default bucket (same as users).
          - Intersects each bucket's models with *available_model_ids*.
        """
        # Lazy import to avoid circular dependency at module load time
        from app.services.permission import permission_service

        default_model_id = await permission_service.get_default_model(db=db)
        default_bucket = await permission_service.get_default_bucket(db=db)

        # Collect bucket IDs this service has explicit access to
        result = await db.execute(
            select(ServiceBucketAccess.bucket_id).where(
                ServiceBucketAccess.service_id == service_id
            )
        )
        bucket_ids: set[int] = {row[0] for row in result.all()}
        if default_bucket:
            bucket_ids.add(default_bucket.id)

        if not bucket_ids:
            valid_model_ids: set = {default_model_id} if default_model_id else set()
            return list(valid_model_ids)

        # Load buckets with their models
        buckets_result = await db.execute(
            select(Bucket)
            .options(selectinload(Bucket.models))
            .where(Bucket.id.in_(bucket_ids))
        )
        buckets = buckets_result.scalars().all()

        valid_model_ids = {default_model_id} if default_model_id else set()
        for bucket in buckets:
            bucket_model_ids = {model.model_id for model in bucket.models}
            if bucket_model_ids:
                valid_model_ids.update(bucket_model_ids.intersection(available_model_ids))

        return list(valid_model_ids)


# Module-level singleton
service_account_service = ServiceAccountService()

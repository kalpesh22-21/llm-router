"""
PermissionService — single source of truth for all user/bucket/access DB operations.

All DB mutations go through this service. Routes and other services must not
issue raw SQL or ORM queries directly against the permission tables.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import List, Optional

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth.ldap_auth import ldap_user_exists
from app.db.mysql import Bucket, BucketModel, User, UserBucketAccess

logger = logging.getLogger(__name__)


class PermissionService:
    """Async service for managing users, buckets, and access control."""

    # ------------------------------------------------------------------ #
    # User helpers
    # ------------------------------------------------------------------ #

    async def get_user(self, db: AsyncSession, user_id: int) -> User:
        """
        Fetch a user by primary key.

        Raises:
            HTTPException(404) if not found.
        """
        result = await db.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User {user_id} not found",
            )
        return user

    async def get_user_by_username(
        self, db: AsyncSession, username: str
    ) -> Optional[User]:
        """Return a User by username, or None if not found."""
        result = await db.execute(select(User).where(User.username == username))
        return result.scalar_one_or_none()

    async def list_users(self, db: AsyncSession) -> List[User]:
        """Return all users ordered by id."""
        result = await db.execute(select(User).order_by(User.id))
        return list(result.scalars().all())

    async def create_user(
        self,
        db: AsyncSession,
        username: str,
        email: Optional[str],
        *,
        is_admin: bool = False,
    ) -> User:
        """
        Create a new user and optionally auto-grant the default bucket.

        The caller is responsible for committing the session.
        """
        user = User(
            username=username,
            email=email,
            is_admin=is_admin,
        )
        db.add(user)
        try:
            await db.flush()  # so user.id is populated
        except IntegrityError:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"User with username {username!r} already exists",
            )

        # Auto-grant the default bucket if one exists
        default_bucket = await self.get_default_bucket(db)
        if default_bucket is not None:
            access = UserBucketAccess(
                user_id=user.id,
                bucket_id=default_bucket.id,
                granted_by=None,
            )
            db.add(access)
            await db.flush()

        return user

    async def set_user_active(
        self,
        db: AsyncSession,
        user_id: int,
        is_active: bool,
        by_user_id: int,
    ) -> User:
        """
        Activate or deactivate a user.

        Requires the caller to be an admin (checked by route layer).
        """
        user = await self.get_user(db, user_id)
        user.is_active = is_active
        await db.flush()
        logger.info(
            "User %d set is_active=%s by user %d", user_id, is_active, by_user_id
        )
        return user

    async def set_admin(
        self,
        db: AsyncSession,
        user_id: int,
        is_admin: bool,
        by_user_id: int,
    ) -> User:
        """
        Grant or revoke admin status.

        Requires the caller to be an admin (enforced by the route layer).
        """
        user = await self.get_user(db, user_id)
        user.is_admin = is_admin
        await db.flush()
        logger.info(
            "User %d set is_admin=%s by admin %d", user_id, is_admin, by_user_id
        )
        return user

    async def set_user_max_concurrent(
        self,
        db: AsyncSession,
        user_id: int,
        max_concurrent: int,
        by_user_id: int,
    ) -> User:
        """Set the maximum concurrent in-flight requests for a user."""
        if max_concurrent < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="max_concurrent_requests must be >= 1",
            )
        user = await self.get_user(db, user_id)
        user.max_concurrent_requests = max_concurrent
        await db.flush()
        logger.info(
            "User %d max_concurrent set to %d by user %d",
            user_id,
            max_concurrent,
            by_user_id,
        )
        return user

    async def is_user_active(self, db: AsyncSession, user_id: int) -> bool:
        """
        Check whether a user is active.

        Called on every proxied request to catch deactivated users who still
        hold a valid JWT. Randomly (~10% of calls) also verifies the user
        still exists in LDAP; if not, the user is auto-deactivated.
        """
        result = await db.execute(
            select(User.is_active, User.username).where(User.id == user_id)
        )
        row = result.one_or_none()
        if row is None:
            return False
        is_active, username = row

        if not is_active:
            return False

        # Probabilistic LDAP existence check (~10% of requests)
        if random.random() < 0.1:
            loop = asyncio.get_event_loop()
            exists = await loop.run_in_executor(None, ldap_user_exists, username)
            if exists is False:
                # User was deprovisioned from LDAP — auto-deactivate
                logger.warning(
                    "User %r (id=%d) no longer exists in LDAP; deactivating",
                    username,
                    user_id,
                )
                user = await self.get_user(db, user_id)
                user.is_active = False
                await db.flush()
                return False
            elif exists is None:
                # LDAP unreachable — fail open, log a warning
                logger.warning(
                    "LDAP unreachable during existence check for user %r (id=%d); skipping",
                    username,
                    user_id,
                )

        return True

    # ------------------------------------------------------------------ #
    # Model access helpers
    # ------------------------------------------------------------------ #

    async def get_user_bucket_ids(self, db: AsyncSession, user_id: int) -> List[int]:
        """Return the list of bucket IDs the user currently has access to."""
        result = await db.execute(
            select(UserBucketAccess.bucket_id).where(UserBucketAccess.user_id == user_id)
        )
        return [row[0] for row in result.all()]

    async def get_user_accessible_models(
        self, db: AsyncSession, user_id: int
    ) -> List[str]:
        """
        Return all model_ids accessible to *user_id* via their bucket grants.
        """
        result = await db.execute(
            select(BucketModel.model_id)
            .join(UserBucketAccess, UserBucketAccess.bucket_id == BucketModel.bucket_id)
            .where(UserBucketAccess.user_id == user_id)
            .distinct()
        )
        return [row[0] for row in result.all()]

    # ------------------------------------------------------------------ #
    # Bucket access helpers
    # ------------------------------------------------------------------ #

    async def grant_bucket_access(
        self,
        db: AsyncSession,
        user_id: int,
        bucket_id: int,
        by_user_id: int,
    ) -> UserBucketAccess:
        """Grant *user_id* access to *bucket_id*. Idempotent."""
        # Verify both user and bucket exist
        await self.get_user(db, user_id)
        await self._get_bucket(db, bucket_id)

        # Check for existing grant
        result = await db.execute(
            select(UserBucketAccess).where(
                UserBucketAccess.user_id == user_id,
                UserBucketAccess.bucket_id == bucket_id,
            )
        )
        existing = result.scalar_one_or_none()
        if existing is not None:
            return existing

        access = UserBucketAccess(
            user_id=user_id,
            bucket_id=bucket_id,
            granted_by=by_user_id,
        )
        db.add(access)
        await db.flush()
        logger.info(
            "Granted user %d access to bucket %d by user %d",
            user_id,
            bucket_id,
            by_user_id,
        )
        return access

    async def revoke_bucket_access(
        self,
        db: AsyncSession,
        user_id: int,
        bucket_id: int,
        by_user_id: int,
    ) -> None:
        """Revoke *user_id*'s access to *bucket_id*. No-op if not granted."""
        result = await db.execute(
            select(UserBucketAccess).where(
                UserBucketAccess.user_id == user_id,
                UserBucketAccess.bucket_id == bucket_id,
            )
        )
        access = result.scalar_one_or_none()
        if access is not None:
            await db.delete(access)
            await db.flush()
            logger.info(
                "Revoked user %d access to bucket %d by user %d",
                user_id,
                bucket_id,
                by_user_id,
            )

    # ------------------------------------------------------------------ #
    # Bucket management helpers
    # ------------------------------------------------------------------ #

    async def create_bucket(
        self,
        db: AsyncSession,
        name: str,
        description: Optional[str],
        model_ids: List[str],
        by_user_id: int,
    ) -> Bucket:
        """Create a new bucket and populate its model list."""
        bucket = Bucket(name=name, description=description)
        db.add(bucket)
        try:
            await db.flush()
        except IntegrityError:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Bucket with name {name!r} already exists",
            )

        for model_id in model_ids:
            db.add(BucketModel(bucket_id=bucket.id, model_id=model_id))
        await db.flush()

        logger.info("Bucket %r (id=%d) created by user %d", name, bucket.id, by_user_id)
        return bucket

    async def list_buckets(self, db: AsyncSession) -> List[Bucket]:
        """Return all buckets with their model lists eagerly loaded."""
        result = await db.execute(
            select(Bucket)
            .options(selectinload(Bucket.models))
            .order_by(Bucket.id)
        )
        return list(result.scalars().all())

    async def get_default_bucket(self, db: AsyncSession) -> Optional[Bucket]:
        """Return the bucket marked is_default=True, or None."""
        result = await db.execute(
            select(Bucket).where(Bucket.is_default == True)  # noqa: E712
        )
        return result.scalar_one_or_none()

    async def set_default_bucket(
        self,
        db: AsyncSession,
        bucket_id: int,
        by_user_id: int,
    ) -> Bucket:
        """
        Mark *bucket_id* as the default and clear the flag on all others.

        Only one bucket may be the default at any time.
        """
        # Clear existing default(s)
        result = await db.execute(select(Bucket).where(Bucket.is_default == True))  # noqa: E712
        for existing in result.scalars().all():
            existing.is_default = False

        # Set new default
        new_default = await self._get_bucket(db, bucket_id)
        new_default.is_default = True
        await db.flush()

        logger.info(
            "Bucket %d set as default by user %d", bucket_id, by_user_id
        )
        return new_default

    async def count_users(self, db: AsyncSession) -> int:
        """Return total number of users in the database."""
        result = await db.execute(select(func.count()).select_from(User))
        return result.scalar_one()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    async def _get_bucket(self, db: AsyncSession, bucket_id: int) -> Bucket:
        result = await db.execute(select(Bucket).where(Bucket.id == bucket_id))
        bucket = result.scalar_one_or_none()
        if bucket is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bucket {bucket_id} not found",
            )
        return bucket


# Module-level singleton
permission_service = PermissionService()

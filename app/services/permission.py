"""
PermissionService — single source of truth for all user/bucket/access DB operations.

All DB mutations go through this service. Routes and other services must not
issue raw SQL or ORM queries directly against the permission tables.
"""

from __future__ import annotations

import json
import logging
from typing import List, Optional, Tuple

from fastapi import HTTPException, status
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from app.core.config import get_settings
from app.db.mysql import Bucket, BucketModel, User, UserBucketAccess
from app.db.redis_client import get_redis_pool

logger = logging.getLogger(__name__)
settings = get_settings()

# Redis cache key for default model (no TTL — cache only invalidated on explicit write)
DEFAULT_MODEL_CACHE_KEY = "default_model"


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

    async def list_users(
        self,
        db: AsyncSession,
        skip: int = 0,
        limit: int = 20,
        search: Optional[str] = None,
    ) -> Tuple[List[User], int]:
        """
        Return paginated users ordered by id.

        Args:
            db: Database session
            skip: Number of records to skip (offset)
            limit: Maximum number of records to return
            search: Optional search query for username/email

        Returns:
            Tuple of (users list, total count)
        """
        # Build base query
        query = select(User)
        count_query = select(func.count()).select_from(User)

        # Apply search filter if provided
        if search:
            search_pattern = f"%{search}%"
            filter_condition = User.username.ilike(search_pattern) | User.email.ilike(
                search_pattern
            )
            query = query.where(filter_condition)
            count_query = count_query.where(filter_condition)

        # Get total count
        count_result = await db.execute(count_query)
        total = count_result.scalar_one()

        # Execute paginated query
        query = query.order_by(User.id).offset(skip).limit(limit)
        result = await db.execute(query)
        users = list(result.scalars().all())

        return users, total

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
        by_user = await self.get_user(db, by_user_id)
        logger.info(
            "User %s set is_active=%s by user %s", user.username, is_active, by_user.username
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
        Only the superuser can remove admin status from a user.
        """
        user = await self.get_user(db, user_id)

        # Check if we're trying to remove admin status
        if user.is_admin and not is_admin:
            # Only superuser can remove admin status
            by_user = await self.get_user(db, by_user_id)
            if by_user.username != settings.SUPERUSER_USERNAME:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Only the superuser can remove admin privileges",
                )

        user.is_admin = is_admin
        await db.flush()
        by_user = await self.get_user(db, by_user_id)
        logger.info(
            "User %s set is_admin=%s by admin %s", user.username, is_admin, by_user.username
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
        by_user = await self.get_user(db, by_user_id)
        logger.info(
            "User %s max_concurrent set to %d by user %s",
            user.username,
            max_concurrent,
            by_user.username,
        )
        return user

    async def is_user_active(self, db: AsyncSession, user_id: int) -> bool:
        """
        Check whether a user is active.

        Called on every proxied request to catch deactivated users who still
        hold a valid JWT. Randomly (~10% of calls) also verifies the user
        still exists in LDAP; if not, the user is auto-deactivated.
        """
        result = await db.execute(select(User.is_active).where(User.id == user_id))
        row = result.one_or_none()
        if row is None:
            return False
        is_active = row

        if not is_active:
            return False
        return True

    # ------------------------------------------------------------------ #
    # Model access helpers
    # ------------------------------------------------------------------ #

    async def get_user_bucket_ids(
        self, db: AsyncSession, user_id: int, available_model_ids: Optional[set] = None
    ) -> List[int]:
        """Return the list of bucket IDs the user currently has access to."""
        default_bucket: Optional[Bucket] = await self.get_default_bucket(db=db)
        result = await db.execute(
            select(UserBucketAccess.bucket_id).where(
                UserBucketAccess.user_id == user_id
            )
        )
        bucket_ids_within_user_access = set([row[0] for row in result.all()])
        if default_bucket:
            bucket_ids_within_user_access.add(default_bucket.id)

        # If available_model_ids provided, keep buckets that have at least one available model
        if available_model_ids is not None:
            # Get all buckets with their models
            result = await db.execute(
                select(Bucket)
                .options(selectinload(Bucket.models))
                .where(Bucket.id.in_(bucket_ids_within_user_access))
            )
            buckets = result.scalars().all()

            valid_bucket_ids = set()
            for bucket in buckets:
                bucket_model_ids = {model.model_id for model in bucket.models}
                if bucket_model_ids and bucket_model_ids.intersection(
                    available_model_ids
                ):
                    valid_bucket_ids.add(bucket.id)

            return list(valid_bucket_ids)

        return list(bucket_ids_within_user_access)

    async def get_user_accessible_models(
        self, db: AsyncSession, user_id: int, available_model_ids: Optional[set] = None
    ) -> List[str]:
        """
        Return all model_ids accessible to *user_id* via their bucket grants.

        For each accessible bucket, returns the subset of its models that are
        currently available in the backend — i.e. the bucket's models intersected
        with ``available_model_ids``. This is a per-model filter, NOT an
        all-or-nothing-per-bucket rule. The default bucket is included for every
        user.
        """
        default_model_id = await self.get_default_model(db=db)
        default_bucket = await self.get_default_bucket(db=db)
        result = await db.execute(
            select(UserBucketAccess.bucket_id).where(
                UserBucketAccess.user_id == user_id
            )
        )
        bucket_ids = set([row[0] for row in result.all()])
        if default_bucket:
            bucket_ids.add(default_bucket.id)

        result = await db.execute(
            select(Bucket)
            .options(selectinload(Bucket.models))
            .where(Bucket.id.in_(bucket_ids))
        )
        buckets = result.scalars().all()
        valid_model_ids: set = {default_model_id} if default_model_id else set()
        for bucket in buckets:
            bucket_model_ids = {model.model_id for model in bucket.models}
            if bucket_model_ids:
                valid_model_ids.update(
                    bucket_model_ids.intersection(available_model_ids)
                )
        return list(valid_model_ids)

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
        user = await self.get_user(db, user_id)
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
        by_user = await self.get_user(db, by_user_id)
        logger.info(
            "Granted user %s access to bucket %d by user %s",
            user.username,
            bucket_id,
            by_user.username,
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
            user = await self.get_user(db, user_id)
            by_user = await self.get_user(db, by_user_id)
            logger.info(
                "Revoked user %s access to bucket %d by user %s",
                user.username,
                bucket_id,
                by_user.username,
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

        by_user = await self.get_user(db, by_user_id)
        logger.info("Bucket %r (id=%d) created by user %s", name, bucket.id, by_user.username)
        return bucket

    async def get_default_model(self, db: AsyncSession) -> Optional[str]:
        """Return the default model ID from the default bucket, or None.

        Cached in Redis with no TTL — only invalidated when explicitly set/cleared.
        """
        try:
            redis = get_redis_pool()
            cached = await redis.get(DEFAULT_MODEL_CACHE_KEY)
            if cached is not None:
                return json.loads(cached) if cached != 'null' else None
        except Exception:
            pass  # Redis unavailable, fall through to DB

        result = await db.execute(
            select(Bucket)
            .options(joinedload(Bucket.models))
            .where(Bucket.is_default == True)  # noqa: E712
        )
        bucket = result.unique().scalar_one_or_none()
        if bucket is None:
            model = None
        else:
            model = bucket.default_model

        # Cache result (including None) with no TTL so it persists until explicit invalidation
        try:
            redis = get_redis_pool()
            await redis.set(DEFAULT_MODEL_CACHE_KEY, json.dumps(model))
        except Exception:
            pass

        return model

    async def set_default_model(
        self,
        db: AsyncSession,
        bucket_id: int,
        model_id: str,
        by_user_id: int,
    ) -> Bucket:
        """Set a specific model as the default model on the bucket."""
        bucket = await self._get_bucket(db, bucket_id)

        # Validate the model_id belongs to this bucket
        bucket_model_ids = {m.model_id for m in bucket.models}
        if bucket_model_ids and model_id not in bucket_model_ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Model {model_id!r} is not in bucket {bucket_id!r}. Available models: {sorted(bucket_model_ids)}",
            )

        bucket.default_model = model_id
        await db.flush()

        # Invalidate cache so next /v1/models fetches fresh value
        try:
            redis = get_redis_pool()
            await redis.delete(DEFAULT_MODEL_CACHE_KEY)
        except Exception:
            pass

        by_user = await self.get_user(db, by_user_id)
        logger.info(
            "Model %r set as default model on bucket %d by user %s",
            model_id, bucket_id, by_user.username
        )
        return bucket

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
        result = await db.execute(
            select(Bucket).where(Bucket.is_default == True)
        )  # noqa: E712
        for existing in result.scalars().all():
            existing.is_default = False

        # Set new default
        new_default = await self._get_bucket(db, bucket_id)
        new_default.is_default = True
        await db.flush()

        # Invalidate default model cache - the default bucket changed
        try:
            redis = get_redis_pool()
            await redis.delete(DEFAULT_MODEL_CACHE_KEY)
        except Exception:
            pass

        by_user = await self.get_user(db, by_user_id)
        logger.info("Bucket %d set as default by user %s", bucket_id, by_user.username)
        return new_default

    async def update_bucket(
        self,
        db: AsyncSession,
        bucket_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        model_ids: Optional[List[str]] = None,
        by_user_id: int = None,
    ) -> Bucket:
        """
        Update a bucket's name, description, and/or model assignments.

        Args:
            db: Database session
            bucket_id: ID of the bucket to update
            name: New bucket name (optional)
            description: New bucket description (optional)
            model_ids: Complete list of model IDs to assign (optional, replaces all existing)
            by_user_id: ID of the user making the change (for audit logging)

        Returns:
            Updated Bucket object

        Raises:
            HTTPException(404) if bucket not found
            HTTPException(409) if name conflicts with another bucket
        """
        # Fetch the bucket to update
        bucket = await self._get_bucket(db, bucket_id)

        # Track changes for audit log
        changes = []

        # Update name if provided
        if name is not None and name != bucket.name:
            # Check for duplicate name (excluding current bucket)
            result = await db.execute(
                select(Bucket).where(
                    Bucket.name == name,
                    Bucket.id != bucket_id
                )
            )
            existing = result.scalar_one_or_none()
            if existing is not None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Bucket with name {name!r} already exists",
                )
            old_name = bucket.name
            bucket.name = name
            changes.append(f"name: {old_name!r} -> {name!r}")

        # Update description if provided
        if description is not None and description != bucket.description:
            old_desc = bucket.description
            bucket.description = description
            changes.append(f"description: {old_desc!r} -> {description!r}")

        # Update model assignments if provided (replaces all existing)
        if model_ids is not None:
            # Fetch current models for comparison
            result = await db.execute(
                select(BucketModel).where(BucketModel.bucket_id == bucket_id)
            )
            current_models = {m.model_id for m in result.scalars().all()}
            new_models = set(model_ids)

            if current_models != new_models:
                # Delete all existing model assignments
                await db.execute(
                    BucketModel.__table__.delete().where(
                        BucketModel.bucket_id == bucket_id
                    )
                )
                # Add new model assignments
                for model_id in model_ids:
                    db.add(BucketModel(bucket_id=bucket_id, model_id=model_id))

                changes.append(
                    f"models: {sorted(current_models)} -> {sorted(new_models)}"
                )

        # Flush changes to the database
        if changes:
            try:
                await db.flush()
            except IntegrityError:
                await db.rollback()
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Bucket update failed due to constraint violation",
                )

            # Log the changes
            by_user = await self.get_user(db, by_user_id)
            logger.info(
                "Bucket %d updated by user %s: %s",
                bucket_id,
                by_user.username,
                "; ".join(changes),
            )

        return bucket

    async def delete_bucket(
        self,
        db: AsyncSession,
        bucket_id: int,
        by_user_id: int,
    ) -> None:
        """
        Delete a bucket.

        The bucket's model assignments (bucket_models) and access grants
        (user_bucket_access, service_bucket_access) are removed automatically
        via ON DELETE CASCADE at the database level.

        Args:
            db: Database session
            bucket_id: ID of the bucket to delete
            by_user_id: ID of the user making the change (for audit logging)

        Raises:
            HTTPException(404) if the bucket does not exist.
            HTTPException(409) if the bucket is the default bucket.
        """
        bucket = await self._get_bucket(db, bucket_id)
        if bucket.is_default:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "The default bucket cannot be deleted. "
                    "Set another bucket as default first."
                ),
            )

        name = bucket.name
        # Core DELETE so the database's ON DELETE CASCADE removes the related
        # bucket_models / user_bucket_access / service_bucket_access rows. A Core
        # statement avoids the ORM trying to NULL the NOT NULL bucket_id FKs on
        # the (non-cascaded) access relationships.
        await db.execute(delete(Bucket).where(Bucket.id == bucket_id))
        # Drop the now-deleted instance from the identity map so nothing in this
        # session lazy-loads a relationship off the zombie object.
        db.expunge(bucket)
        await db.flush()

        by_user = await self.get_user(db, by_user_id)
        logger.info(
            "Bucket %d (%r) deleted by user %s", bucket_id, name, by_user.username
        )

    async def get_default_bucket(self, db: AsyncSession) -> Optional[Bucket]:
        """Return the bucket marked as default, or None if no default exists."""
        result = await db.execute(
            select(Bucket).options(selectinload(Bucket.models)).where(Bucket.is_default == True)
        )  # noqa: E712
        return result.unique().scalar_one_or_none()

    async def list_buckets(self, db: AsyncSession) -> List[Bucket]:
        """Return all buckets with their models eager-loaded."""
        result = await db.execute(
            select(Bucket).options(selectinload(Bucket.models))
        )
        return result.unique().scalars().all()

    async def count_users(self, db: AsyncSession) -> int:
        """Return total number of users in the database."""
        result = await db.execute(select(func.count()).select_from(User))
        return result.scalar_one()

    async def list_users_min(self, db: AsyncSession) -> List[User]:
        """
        Return ALL users ordered by username, with no pagination.

        Intended for selection UIs that need the full user list at once.
        """
        result = await db.execute(select(User).order_by(User.username))
        users = list(result.scalars().all())
        if len(users) > 5000:
            logger.warning(
                "list_users_min returned %d users — selection UIs may be slow; "
                "consider server-side search/pagination",
                len(users),
            )
        return users

    async def get_bucket(self, db: AsyncSession, bucket_id: int) -> Bucket:
        """
        Public wrapper around _get_bucket.

        Raises:
            HTTPException(404) if the bucket does not exist.
        """
        return await self._get_bucket(db, bucket_id)

    async def get_bucket_user_ids(self, db: AsyncSession, bucket_id: int) -> List[int]:
        """
        Return the list of user IDs that have an explicit UserBucketAccess row
        for *bucket_id*.

        Does not include the implicit grant that the default bucket gives to
        every user — only rows that exist in the user_bucket_access table.

        Raises:
            HTTPException(404) if the bucket does not exist.
        """
        await self._get_bucket(db, bucket_id)
        result = await db.execute(
            select(UserBucketAccess.user_id).where(
                UserBucketAccess.bucket_id == bucket_id
            )
        )
        return [row[0] for row in result.all()]

    async def set_bucket_users(
        self,
        db: AsyncSession,
        bucket_id: int,
        user_ids: List[int],
        by_user_id: int,
    ) -> dict:
        """
        Set-semantics batch membership update for a bucket.

        Makes the explicit UserBucketAccess membership of *bucket_id* exactly
        equal to the deduplicated *user_ids* list.  Users not in *user_ids* but
        currently granted access have their grant revoked; users in *user_ids*
        but not yet granted are inserted.  An empty *user_ids* list is valid and
        revokes all explicit grants.

        Args:
            db: Database session.
            bucket_id: The bucket whose membership is being set.
            user_ids: Desired complete set of user IDs (duplicates ignored).
            by_user_id: Admin user performing the operation (for audit logging).

        Returns:
            Dict with keys ``granted``, ``revoked``, ``unchanged``, and
            ``user_ids`` (sorted final membership list).

        Raises:
            HTTPException(404) if bucket or by_user not found.
            HTTPException(400) if any requested user_id does not exist.

        Concurrency: no row-level lock is held during the read-modify-write
        sequence; concurrent calls for the same bucket_id are last-write-wins.
        The unique (user_id, bucket_id) constraint in the DB prevents duplicate
        grant rows from being inserted.
        """
        await self._get_bucket(db, bucket_id)

        desired: set[int] = set(user_ids)

        # Validate all requested user IDs exist in a single query
        if desired:
            result = await db.execute(
                select(User.id).where(User.id.in_(desired))
            )
            found_ids = {row[0] for row in result.all()}
            missing = desired - found_ids
            if missing:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unknown user id(s): {sorted(missing)}",
                )

        # Fetch current explicit membership
        result = await db.execute(
            select(UserBucketAccess.user_id).where(
                UserBucketAccess.bucket_id == bucket_id
            )
        )
        current: set[int] = {row[0] for row in result.all()}

        to_grant = desired - current
        to_revoke = current - desired

        # Insert new grants
        for uid in to_grant:
            db.add(
                UserBucketAccess(
                    user_id=uid,
                    bucket_id=bucket_id,
                    granted_by=by_user_id,
                )
            )

        # Bulk-delete revoked grants (mirrors update_bucket's delete pattern)
        if to_revoke:
            await db.execute(
                UserBucketAccess.__table__.delete().where(
                    UserBucketAccess.__table__.c.bucket_id == bucket_id,
                    UserBucketAccess.__table__.c.user_id.in_(to_revoke),
                )
            )

        await db.flush()

        by_user = await self.get_user(db, by_user_id)
        logger.info(
            "Bucket %d membership set by %s: +%d granted, -%d revoked",
            bucket_id,
            by_user.username,
            len(to_grant),
            len(to_revoke),
        )

        return {
            "granted": len(to_grant),
            "revoked": len(to_revoke),
            "unchanged": len(desired & current),
            "user_ids": sorted(desired),
        }

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    async def _get_bucket(self, db: AsyncSession, bucket_id: int) -> Bucket:
        result = await db.execute(select(Bucket).options(selectinload(Bucket.models)).where(Bucket.id == bucket_id))
        bucket = result.scalar_one_or_none()
        if bucket is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bucket {bucket_id} not found",
            )
        return bucket


# Module-level singleton
permission_service = PermissionService()

"""
ModelBackendService — all DB operations for the model_backends table.

All mutations and reads for model_backends go through this service.
Route handlers must not issue raw SQL or ORM queries directly.

Security contract:
  - api_key_enc is NEVER returned.  All public-facing row objects use
    ModelBackendRow which has no key-material fields.
  - decrypt_api_key is NOT called here; only encrypt_api_key is used (at write).
    Decryption is the exclusive domain of DatabaseBackendSource / _build_backend.
  - Nothing in this module logs api_key or api_key_enc.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from typing import Optional

from fastapi import HTTPException, status
from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.mysql import Bucket, BucketModel, ModelBackend
from app.services.model_backend_crypto import encrypt_api_key

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public data-transfer object — never contains key material
# ---------------------------------------------------------------------------


@dataclass
class ModelBackendRow:
    """
    Safe projection of a ModelBackend ORM row.

    api_key_enc is deliberately absent.  The route layer uses this object to
    build responses; the encryption key and ciphertext never leave the service
    layer boundary.
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


def _to_row(orm: ModelBackend) -> ModelBackendRow:
    """Convert an ORM instance to a ModelBackendRow (strips api_key_enc)."""
    return ModelBackendRow(
        model_id=orm.model_id,
        backend_type=orm.backend_type,
        base_url=orm.base_url,
        is_enabled=orm.is_enabled,
        source=orm.source,
        version=orm.version,
        created_at=orm.created_at,
        updated_at=orm.updated_at,
        created_by=orm.created_by,
        updated_by=orm.updated_by,
    )


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class ModelBackendService:
    """Async service for managing model_backends table rows."""

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #

    async def list_backends(self, session: AsyncSession) -> list[ModelBackendRow]:
        """Return all model_backend rows ordered by model_id."""
        result = await session.execute(
            select(ModelBackend).order_by(ModelBackend.model_id)
        )
        return [_to_row(row) for row in result.scalars().all()]

    async def get_backend(
        self, session: AsyncSession, model_id: str
    ) -> ModelBackendRow | None:
        """Return the ModelBackendRow for *model_id*, or None if absent."""
        result = await session.execute(
            select(ModelBackend).where(ModelBackend.model_id == model_id)
        )
        orm = result.scalar_one_or_none()
        return _to_row(orm) if orm is not None else None

    # ------------------------------------------------------------------ #
    # Create
    # ------------------------------------------------------------------ #

    async def create_backend(
        self,
        session: AsyncSession,
        model_id: str,
        base_url: str,
        api_key: Optional[str],
        created_by: Optional[int],
    ) -> ModelBackendRow:
        """
        Insert a new model_backend row.

        Raises:
            HTTPException(409) if model_id already exists.
        """
        # Encrypt api_key if provided and non-empty; otherwise store NULL.
        api_key_enc: Optional[str] = None
        if api_key:
            api_key_enc = encrypt_api_key(api_key)

        orm = ModelBackend(
            model_id=model_id,
            base_url=base_url.rstrip("/"),
            api_key_enc=api_key_enc,
            is_enabled=True,
            source="admin_ui",
            version=1,
            created_by=created_by,
            updated_by=created_by,
        )
        session.add(orm)
        try:
            await session.flush()
        except IntegrityError:
            await session.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Model backend with model_id {model_id!r} already exists",
            )

        await session.commit()
        # Re-fetch to populate server-side defaults (created_at, updated_at).
        result = await session.execute(
            select(ModelBackend).where(ModelBackend.model_id == model_id)
        )
        refreshed = result.scalar_one()
        logger.info(
            "ModelBackendService: created model_id=%r by user_id=%s",
            model_id,
            created_by,
        )
        return _to_row(refreshed)

    # ------------------------------------------------------------------ #
    # Update (patch)
    # ------------------------------------------------------------------ #

    async def update_backend(
        self,
        session: AsyncSession,
        model_id: str,
        patch: dict,
        updated_by: Optional[int],
    ) -> ModelBackendRow:
        """
        Apply a partial update to a model_backend row.

        patch keys:
          base_url    — replace base_url (trailing slash stripped)
          api_key     — non-empty str → re-encrypt; empty str → clear to NULL;
                        key absent from patch → leave api_key_enc untouched
          is_enabled  — bool

        version is incremented atomically via an UPDATE ... SET version = version + 1
        to prevent concurrent writers clobbering the change-detection counter.

        Raises:
            HTTPException(404) if model_id is not found.
        """
        # Verify existence first.
        result = await session.execute(
            select(ModelBackend).where(ModelBackend.model_id == model_id)
        )
        if result.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model backend {model_id!r} not found",
            )

        # Build the SET clause for the UPDATE statement.
        values: dict = {
            "version": ModelBackend.version + 1,
            "updated_by": updated_by,
        }

        if "base_url" in patch and patch["base_url"] is not None:
            values["base_url"] = str(patch["base_url"]).rstrip("/")

        if "is_enabled" in patch and patch["is_enabled"] is not None:
            values["is_enabled"] = patch["is_enabled"]

        # api_key semantics (three-way):
        #   absent from patch        → do not touch api_key_enc
        #   present + non-empty str  → re-encrypt new plaintext
        #   present + empty str ""   → clear to NULL (global API_KEY fallback)
        if "api_key" in patch:
            raw_key = patch["api_key"]
            if raw_key is None or raw_key == "":
                # Explicit clear
                values["api_key_enc"] = None
            else:
                values["api_key_enc"] = encrypt_api_key(raw_key)

        stmt = (
            update(ModelBackend)
            .where(ModelBackend.model_id == model_id)
            .values(**values)
        )
        await session.execute(stmt)
        await session.commit()

        result = await session.execute(
            select(ModelBackend).where(ModelBackend.model_id == model_id)
        )
        updated = result.scalar_one_or_none()
        if updated is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model backend {model_id!r} was deleted concurrently",
            )
        logger.info(
            "ModelBackendService: updated model_id=%r version=%d by user_id=%s",
            model_id,
            updated.version,
            updated_by,
        )
        return _to_row(updated)

    # ------------------------------------------------------------------ #
    # Enable / disable
    # ------------------------------------------------------------------ #

    async def enable_backend(
        self,
        session: AsyncSession,
        model_id: str,
        updated_by: Optional[int],
    ) -> ModelBackendRow:
        """Enable a model_backend row (is_enabled=True) and bump version."""
        return await self._set_enabled(session, model_id, True, updated_by)

    async def disable_backend(
        self,
        session: AsyncSession,
        model_id: str,
        updated_by: Optional[int],
    ) -> ModelBackendRow:
        """Disable a model_backend row (is_enabled=False) and bump version."""
        return await self._set_enabled(session, model_id, False, updated_by)

    async def _set_enabled(
        self,
        session: AsyncSession,
        model_id: str,
        is_enabled: bool,
        updated_by: Optional[int],
    ) -> ModelBackendRow:
        result = await session.execute(
            select(ModelBackend).where(ModelBackend.model_id == model_id)
        )
        if result.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model backend {model_id!r} not found",
            )

        stmt = (
            update(ModelBackend)
            .where(ModelBackend.model_id == model_id)
            .values(
                is_enabled=is_enabled,
                version=ModelBackend.version + 1,
                updated_by=updated_by,
            )
        )
        await session.execute(stmt)
        await session.commit()

        result = await session.execute(
            select(ModelBackend).where(ModelBackend.model_id == model_id)
        )
        updated = result.scalar_one_or_none()
        if updated is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model backend {model_id!r} was deleted concurrently",
            )
        logger.info(
            "ModelBackendService: model_id=%r is_enabled=%s version=%d by user_id=%s",
            model_id,
            is_enabled,
            updated.version,
            updated_by,
        )
        return _to_row(updated)

    # ------------------------------------------------------------------ #
    # Delete
    # ------------------------------------------------------------------ #

    async def delete_backend(
        self,
        session: AsyncSession,
        model_id: str,
    ) -> None:
        """
        Delete a model_backend row.

        Raises:
            HTTPException(404) if model_id is not found.
            HTTPException(409) if the model_id is referenced by one or more buckets
                              (lists the bucket names in detail).
        """
        result = await session.execute(
            select(ModelBackend).where(ModelBackend.model_id == model_id)
        )
        if result.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model backend {model_id!r} not found",
            )

        bucket_names = await self.is_referenced_by_bucket(session, model_id)
        if bucket_names:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Cannot delete model_id {model_id!r}: it is referenced by "
                    f"bucket(s): {sorted(bucket_names)}"
                ),
            )

        await session.execute(
            delete(ModelBackend).where(ModelBackend.model_id == model_id)
        )
        await session.commit()
        logger.info("ModelBackendService: deleted model_id=%r", model_id)

    # ------------------------------------------------------------------ #
    # Reference check
    # ------------------------------------------------------------------ #

    async def is_referenced_by_bucket(
        self, session: AsyncSession, model_id: str
    ) -> list[str]:
        """
        Return the names of all buckets that reference *model_id* in bucket_models.

        Returns an empty list if the model is not referenced.
        """
        result = await session.execute(
            select(Bucket.name)
            .join(BucketModel, BucketModel.bucket_id == Bucket.id)
            .where(BucketModel.model_id == model_id)
        )
        return [row[0] for row in result.all()]


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

model_backend_service = ModelBackendService()

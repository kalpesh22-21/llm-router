"""
ApiKeyService — create, resolve, revoke, and list service-account API keys.

Key design:
  - Keys are opaque: ``llmr_sk_<random>``. Only the SHA-256 hex digest
    (key_hash) and a non-secret prefix are stored in the DB.
  - The hot path is cache-first: a Redis positive cache holds the resolved
    identity payload for SERVICE_API_KEY_CACHE_TTL_SECONDS seconds so that
    repeated requests are DB-free.
  - Negative results (unknown / revoked / expired) are NOT cached — they are
    always re-checked against the DB. This keeps revocation instantaneous
    on the next non-cached request.
  - Redis errors fall through to the DB (fail-open for availability), matching
    the pattern used throughout permission_service.

Per-key authorization model:
  1. Account-level bucket grants determine the base set of accessible models
     (same logic as PermissionService for users).
  2. key.model_ids (if non-empty) further restricts to the intersection of
     account models and the key's allowed model list.
  3. key.scopes (if non-empty) restricts which operations the key may call.
  4. key.max_concurrent_requests overrides the account-level default when set.
  5. Concurrency is tracked under ``concurrent:svckey:{key_id}`` so per-key
     limits never collide with user-id namespaced counters.
"""

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import logging
import secrets
from typing import List, Optional

from fastapi import HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.mysql import ApiKey, ServiceAccount

logger = logging.getLogger(__name__)
settings = get_settings()

# Fixed set of valid operation scopes
VALID_SCOPES: frozenset[str] = frozenset({"chat", "embeddings", "messages", "models"})


async def _touch_last_used(key_id: int) -> None:
    """Fire-and-forget: update last_used_at on a separate session so a failure
    never poisons or rolls back the request transaction."""
    from app.db.mysql import AsyncSessionLocal
    try:
        async with AsyncSessionLocal() as s:
            await s.execute(
                update(ApiKey)
                .where(ApiKey.id == key_id)
                .values(last_used_at=datetime.datetime.utcnow())
            )
            await s.commit()
    except Exception:
        logger.warning("Failed to update last_used_at for key id=%d", key_id, exc_info=True)

# Prefix that distinguishes service keys from JWT tokens and placeholder strings.
KEY_PREFIX = "llmr_sk_"


def _cache_key(key_hash: str) -> str:
    """Redis key for the positive identity cache of a given key_hash."""
    return f"apikey:{key_hash}"


class ApiKeyService:
    """Async service for managing service-account API keys."""

    # Exposed on the class/instance so callers (e.g. the auth resolver) can do
    # ``api_key_service.KEY_PREFIX`` without importing the module-level constant.
    KEY_PREFIX = KEY_PREFIX

    # ------------------------------------------------------------------ #
    # Key generation
    # ------------------------------------------------------------------ #

    def generate_key(self) -> tuple[str, str, str]:
        """
        Generate a new opaque API key.

        Returns:
            (full_key, prefix, key_hash) where:
              - full_key is shown to the admin exactly once and NEVER stored.
              - prefix is the first 16 characters — stored for UI display.
              - key_hash is the SHA-256 hex digest — stored as the lookup key.
        """
        full_key = KEY_PREFIX + secrets.token_urlsafe(32)
        prefix = full_key[:16]
        key_hash = hashlib.sha256(full_key.encode()).hexdigest()
        return full_key, prefix, key_hash

    # ------------------------------------------------------------------ #
    # Persistence
    # ------------------------------------------------------------------ #

    async def create_key(
        self,
        db: AsyncSession,
        *,
        service_id: int,
        name: str,
        scopes: Optional[List[str]] = None,
        model_ids: Optional[List[str]] = None,
        max_concurrent_requests: Optional[int] = None,
        expires_at: Optional[datetime.datetime] = None,
        created_by: int,
    ) -> tuple[ApiKey, str]:
        """
        Generate and persist an API key for a service account.

        The plaintext ``full_key`` is returned here and must be shown to the
        caller exactly once. It is NOT stored anywhere in the system.

        Args:
            service_id: The service account this key belongs to.
            name: Human-readable label for the key.
            scopes: Optional list from VALID_SCOPES restricting allowed operations.
            model_ids: Optional list of model_id strings restricting model access.
            max_concurrent_requests: Optional per-key concurrency cap.
            expires_at: Optional expiry datetime (naive UTC).
            created_by: User ID of the admin creating this key.

        Returns:
            (ApiKey ORM row, full_key_plaintext)

        Raises:
            HTTPException(400) if any scope is not in VALID_SCOPES.
        """
        if scopes:
            invalid = set(scopes) - VALID_SCOPES
            if invalid:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid scopes: {sorted(invalid)}. Must be from {sorted(VALID_SCOPES)}.",
                )

        full_key, prefix, key_hash = self.generate_key()
        row = ApiKey(
            service_id=service_id,
            name=name,
            prefix=prefix,
            key_hash=key_hash,
            created_by=created_by,
            expires_at=expires_at,
            scopes=scopes or None,
            model_ids=model_ids or None,
            max_concurrent_requests=max_concurrent_requests,
        )
        db.add(row)
        try:
            await db.flush()
        except IntegrityError:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Key hash collision — please retry (extremely rare).",
            )
        # Refresh so that server-default columns (created_at) are loaded before
        # the caller reads them.  get_db() owns the commit; we only need the
        # attributes populated within this transaction.
        await db.refresh(row)
        logger.info(
            "API key %r (id=%d) created for service_id=%d by user_id=%d",
            name,
            row.id,
            service_id,
            created_by,
        )
        return row, full_key

    # ------------------------------------------------------------------ #
    # Hot path: resolve
    # ------------------------------------------------------------------ #

    async def resolve_key(
        self,
        db: AsyncSession,
        redis,
        presented_key: str,
    ) -> Optional[dict]:
        """
        Resolve a presented service API key to a JWT-shaped identity payload.

        Hot path — positive cache in Redis; DB only on cache miss or Redis error.

        Authorization model (applied on every cache miss):
          1. Look up ApiKey by hash; reject if revoked or expired.
          2. Load owning ServiceAccount; reject if missing or inactive.
          3. Compute account_models via service_account_service.get_accessible_models.
          4. Intersect with key.model_ids if non-empty.
          5. Effective concurrency = key.max_concurrent_requests ?? account.max_concurrent_requests.
          6. Scopes = key.scopes (empty list = all ops allowed).

        Returns:
            A payload dict (same shape the proxy consumes) or None if the key
            is unknown, revoked, expired, or belongs to an inactive account.
        """
        if not presented_key or len(presented_key) > 128:
            return None

        from app.backends.base import registry as backend_registry
        from app.services.service_account import service_account_service

        key_hash = hashlib.sha256(presented_key.encode()).hexdigest()
        cache_redis_key = _cache_key(key_hash)

        # --- Redis positive cache ---
        try:
            cached_raw = await redis.get(cache_redis_key)
            if cached_raw is not None:
                payload = json.loads(cached_raw)
                # Honour an embedded expiry epoch (set when key has expires_at)
                exp = payload.get("exp")
                if exp is not None and datetime.datetime.now(datetime.timezone.utc).timestamp() > exp:
                    # Key has expired since it was cached — treat as miss
                    try:
                        await redis.delete(cache_redis_key)
                    except Exception:
                        pass
                    return None
                return payload
        except Exception:
            logger.warning(
                "Redis error during api-key cache lookup; falling through to DB",
                exc_info=True,
            )

        # --- DB lookup (cache miss or Redis error) ---
        result = await db.execute(
            select(ApiKey).where(
                ApiKey.key_hash == key_hash,
                ApiKey.revoked_at.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None

        # Check expiry
        if row.expires_at is not None and row.expires_at < datetime.datetime.utcnow():
            return None

        # Load owning service account
        account_result = await db.execute(
            select(ServiceAccount).where(ServiceAccount.id == row.service_id)
        )
        account = account_result.scalar_one_or_none()
        if account is None or not account.is_active:
            return None

        # Compute accessible models for this account
        available_model_ids = set(backend_registry.list_model_ids())
        account_models = await service_account_service.get_accessible_models(
            db, account.id, available_model_ids=available_model_ids
        )

        # Apply per-key model subset restriction
        key_model_ids: list = row.model_ids if row.model_ids else []
        if key_model_ids:
            accessible_models = list(set(account_models) & set(key_model_ids))
        else:
            accessible_models = account_models

        # Effective concurrency: key override takes precedence over account default
        effective_max_concurrent = (
            row.max_concurrent_requests
            if row.max_concurrent_requests is not None
            else account.max_concurrent_requests
        )

        scopes: list = row.scopes or []

        payload: dict = {
            "sub": str(account.id),
            "username": account.name,
            "is_admin": False,
            "auth": "service",
            "service_id": account.id,
            "key_id": row.id,
            "accessible_models": accessible_models,
            "scopes": scopes,
            "max_concurrent_requests": effective_max_concurrent,
            # Per-key concurrency namespace — never collides with user ids
            "rate_principal": f"svckey:{row.id}",
        }

        # Embed expiry epoch so the cache can self-invalidate on key expiry.
        # The stored naive datetime is UTC by convention; make it tz-aware before
        # converting to a POSIX timestamp so the result is unambiguous.
        if row.expires_at is not None:
            payload["exp"] = row.expires_at.replace(tzinfo=datetime.timezone.utc).timestamp()

        # Write positive cache
        ttl = settings.SERVICE_API_KEY_CACHE_TTL_SECONDS
        try:
            await redis.set(cache_redis_key, json.dumps(payload), ex=ttl)
        except Exception:
            logger.warning("Redis error writing api-key cache; continuing without cache")

        # Update last_used_at on DB hit (once per TTL window — writes happen only
        # on cache misses, so this fires at most once every TTL seconds per key).
        # Use a separate session so a failure cannot poison the request transaction.
        asyncio.create_task(_touch_last_used(row.id))

        return payload

    # ------------------------------------------------------------------ #
    # Revocation
    # ------------------------------------------------------------------ #

    async def revoke_key(
        self,
        db: AsyncSession,
        redis,
        *,
        key_id: int,
        service_id: int,
    ) -> None:
        """
        Soft-revoke a key by setting revoked_at = now().

        Ownership-scoped: only revokes a key that belongs to *service_id*.
        Also best-effort deletes the Redis cache entry so the key is rejected
        on the next request rather than waiting for the TTL to expire.
        """
        result = await db.execute(
            select(ApiKey).where(
                ApiKey.id == key_id,
                ApiKey.service_id == service_id,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"API key {key_id} not found",
            )
        if row.revoked_at is not None:
            # Already revoked — idempotent
            return

        row.revoked_at = datetime.datetime.utcnow()
        await db.flush()

        # Best-effort cache eviction
        try:
            await redis.delete(_cache_key(row.key_hash))
        except Exception:
            logger.warning(
                "Redis error evicting api-key cache for key id=%d; "
                "will be rejected after TTL expires",
                key_id,
            )

        logger.info("API key id=%d revoked for service_id=%d", key_id, service_id)

    # ------------------------------------------------------------------ #
    # Listing
    # ------------------------------------------------------------------ #

    async def list_keys(self, db: AsyncSession, service_id: int) -> list[ApiKey]:
        """Return all API key metadata rows for a service account. Never returns secrets."""
        result = await db.execute(
            select(ApiKey).where(ApiKey.service_id == service_id).order_by(ApiKey.created_at)
        )
        return list(result.scalars().all())

    async def evict_service_cache(
        self, db: AsyncSession, redis, service_id: int
    ) -> None:
        """Best-effort: drop all cached identities for a service account's active keys.

        Called when a service account is deactivated so all in-flight cached
        payloads are immediately invalidated.
        """
        try:
            result = await db.execute(
                select(ApiKey.key_hash).where(
                    ApiKey.service_id == service_id,
                    ApiKey.revoked_at.is_(None),
                )
            )
            hashes = [r[0] for r in result.all()]
            if hashes:
                await redis.delete(*[_cache_key(h) for h in hashes])
        except Exception:
            logger.warning(
                "Failed to evict api-key cache for service_id=%d", service_id, exc_info=True
            )


# Module-level singleton
api_key_service = ApiKeyService()

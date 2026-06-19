"""
Tests for the batch-assign-bucket-to-users feature.

Covers:
  Service layer  — PermissionService.list_users_min, .get_bucket_user_ids,
                   .set_bucket_users
  API layer      — GET /admin/users/all
                   GET /admin/buckets/{id}/users
                   POST /admin/buckets/{id}/users
                   AuthZ guard (non-admin / unauthenticated rejected)

Approach (mirrors test_discovery.py):
  Pure / mocked — no real MySQL, Redis, or network.
  SQLite in-memory via SQLAlchemy async (aiosqlite) is used for the DB.
  asyncio.run() drives all async helpers; only core pytest is required.
  No pytest-asyncio dependency needed.

Run inside the app image (all deps installed):
    VAULT_ENABLED=false pytest tests/test_bucket_users.py -v
"""

from __future__ import annotations

import asyncio
import os
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

# ---------------------------------------------------------------------------
# Import the ORM models and the service under test.
# The conftest sets env vars before any app.* import, so VAULT_ENABLED=false
# is already applied.  We also need to prevent the module-level engine in
# app.db.mysql from trying to connect to a real MySQL server on import.
# We do this by patching the engine before importing mysql.
# ---------------------------------------------------------------------------

from app.db.mysql import Base, Bucket, BucketModel, User, UserBucketAccess
from app.services.permission import PermissionService

# ---------------------------------------------------------------------------
# Shared async SQLite engine + session factory (per test-module, not per test,
# because engine creation is cheap and we tear down tables between tests via
# the _db fixture below).
# ---------------------------------------------------------------------------

_SQLITE_URL = "sqlite+aiosqlite:///:memory:"

# StaticPool + check_same_thread=False are required for SQLite in async tests.
_engine = create_async_engine(
    _SQLITE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
    echo=False,
)

_AsyncSession = async_sessionmaker(
    _engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


# ---------------------------------------------------------------------------
# Helper: create tables, yield a session, drop tables.
# Used by every async test helper via asyncio.run().
# ---------------------------------------------------------------------------


async def _setup_db() -> None:
    """Create all ORM tables in the in-memory SQLite DB."""
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def _teardown_db() -> None:
    """Drop all ORM tables so each test starts clean."""
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


async def _get_session() -> AsyncSession:
    """Return a new raw session (caller must close it)."""
    return _AsyncSession()


# ---------------------------------------------------------------------------
# Per-test setup/teardown using pytest fixtures (sync wrappers around async).
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _db_tables():
    """Create tables before each test and drop them afterwards."""
    asyncio.run(_setup_db())
    yield
    asyncio.run(_teardown_db())


# ---------------------------------------------------------------------------
# Factory helpers — create ORM objects directly (no service layer needed).
# ---------------------------------------------------------------------------


async def _make_user(
    db: AsyncSession,
    username: str,
    *,
    email: str | None = None,
    is_admin: bool = False,
) -> User:
    u = User(username=username, email=email, is_admin=is_admin)
    db.add(u)
    await db.flush()
    return u


async def _make_bucket(
    db: AsyncSession,
    name: str,
    *,
    is_default: bool = False,
) -> Bucket:
    b = Bucket(name=name, is_default=is_default)
    db.add(b)
    await db.flush()
    return b


async def _grant(db: AsyncSession, user_id: int, bucket_id: int, by: int) -> None:
    db.add(
        UserBucketAccess(
            user_id=user_id,
            bucket_id=bucket_id,
            granted_by=by,
        )
    )
    await db.flush()


# ---------------------------------------------------------------------------
# Redis patch helpers.
# get_default_model() calls get_redis_pool() which would raise if Redis is not
# initialised.  We patch it for every test.
# ---------------------------------------------------------------------------


def _patch_redis():
    """Return a context-manager that stubs out get_redis_pool with a fake."""
    fake_redis = AsyncMock()
    fake_redis.get = AsyncMock(return_value=None)
    fake_redis.set = AsyncMock(return_value=None)
    fake_redis.delete = AsyncMock(return_value=None)
    return patch(
        "app.services.permission.get_redis_pool",
        return_value=fake_redis,
    )


# ===========================================================================
# SERVICE LAYER TESTS
# ===========================================================================

# ---------------------------------------------------------------------------
# list_users_min
# ---------------------------------------------------------------------------


def test_list_users_min_empty():
    """Returns an empty list when no users exist."""

    async def run():
        async with _AsyncSession() as db:
            svc = PermissionService()
            users = await svc.list_users_min(db)
            assert users == []

    asyncio.run(run())


def test_list_users_min_ordered_by_username():
    """Returns all users ordered alphabetically by username, no pagination."""

    async def run():
        async with _AsyncSession() as db:
            await _make_user(db, "zara")
            await _make_user(db, "alice")
            await _make_user(db, "bob")
            await db.commit()

        async with _AsyncSession() as db:
            svc = PermissionService()
            users = await svc.list_users_min(db)
            assert [u.username for u in users] == ["alice", "bob", "zara"]

    asyncio.run(run())


def test_list_users_min_returns_all_without_pagination():
    """list_users_min returns ALL users (no limit)."""

    async def run():
        async with _AsyncSession() as db:
            for i in range(25):
                await _make_user(db, f"user{i:03d}")
            await db.commit()

        async with _AsyncSession() as db:
            svc = PermissionService()
            users = await svc.list_users_min(db)
            assert len(users) == 25

    asyncio.run(run())


# ---------------------------------------------------------------------------
# get_bucket_user_ids
# ---------------------------------------------------------------------------


def test_get_bucket_user_ids_missing_bucket_raises_404():
    """Raises HTTPException(404) when the bucket does not exist."""

    async def run():
        async with _AsyncSession() as db:
            svc = PermissionService()
            with pytest.raises(HTTPException) as exc:
                await svc.get_bucket_user_ids(db, bucket_id=9999)
            assert exc.value.status_code == 404

    asyncio.run(run())


def test_get_bucket_user_ids_empty_bucket():
    """Returns [] for a bucket that exists but has no explicit grants."""

    async def run():
        async with _AsyncSession() as db:
            b = await _make_bucket(db, "empty-bucket")
            await db.commit()
            bid = b.id

        async with _AsyncSession() as db:
            svc = PermissionService()
            ids = await svc.get_bucket_user_ids(db, bucket_id=bid)
            assert ids == []

    asyncio.run(run())


def test_get_bucket_user_ids_returns_explicit_members_only():
    """
    Returns only user_ids with an explicit UserBucketAccess row.

    The test uses two buckets.  Users are granted to bucket A only.
    get_bucket_user_ids for bucket B (which has no explicit grants but is
    the default) must still return [].  The implicit "default bucket →
    everyone" contract is tested structurally here: the service never inserts
    a row on its own, so the returned list is empty.
    """

    async def run():
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            u1 = await _make_user(db, "alice")
            u2 = await _make_user(db, "bob")
            bucket_a = await _make_bucket(db, "bucket-a")
            bucket_b = await _make_bucket(db, "bucket-b", is_default=True)
            await _grant(db, u1.id, bucket_a.id, admin.id)
            await _grant(db, u2.id, bucket_a.id, admin.id)
            await db.commit()
            bid_a = bucket_a.id
            bid_b = bucket_b.id

        async with _AsyncSession() as db:
            svc = PermissionService()
            ids_a = await svc.get_bucket_user_ids(db, bucket_id=bid_a)
            ids_b = await svc.get_bucket_user_ids(db, bucket_id=bid_b)

        assert sorted(ids_a) == sorted([u1.id, u2.id])
        assert ids_b == []

    asyncio.run(run())


# ---------------------------------------------------------------------------
# set_bucket_users — core logic
# ---------------------------------------------------------------------------


def test_set_bucket_users_grant_from_empty():
    """
    Grant several users to a bucket that has no prior members.
    Expect granted=N, revoked=0, unchanged=0, user_ids sorted.
    """

    async def run():
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            u1 = await _make_user(db, "alice")
            u2 = await _make_user(db, "bob")
            u3 = await _make_user(db, "charlie")
            b = await _make_bucket(db, "test-bucket")
            await db.commit()
            uid1, uid2, uid3, bid, admin_id = u1.id, u2.id, u3.id, b.id, admin.id

        with _patch_redis():
            async with _AsyncSession() as db:
                svc = PermissionService()
                result = await svc.set_bucket_users(
                    db,
                    bucket_id=bid,
                    user_ids=[uid1, uid2, uid3],
                    by_user_id=admin_id,
                )
                await db.commit()

        assert result["granted"] == 3
        assert result["revoked"] == 0
        assert result["unchanged"] == 0
        assert result["user_ids"] == sorted([uid1, uid2, uid3])

    asyncio.run(run())


def test_set_bucket_users_set_semantics_grant_revoke_unchanged():
    """
    Starting from {A, B}, calling with {B, C}:
    → A revoked, C granted, B unchanged.
    Result: granted=1, revoked=1, unchanged=1.
    """

    async def run():
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            ua = await _make_user(db, "user-a")
            ub = await _make_user(db, "user-b")
            uc = await _make_user(db, "user-c")
            b = await _make_bucket(db, "test-bucket")
            # Pre-grant A and B
            await _grant(db, ua.id, b.id, admin.id)
            await _grant(db, ub.id, b.id, admin.id)
            await db.commit()
            uid_a, uid_b, uid_c = ua.id, ub.id, uc.id
            bid, admin_id = b.id, admin.id

        with _patch_redis():
            async with _AsyncSession() as db:
                svc = PermissionService()
                result = await svc.set_bucket_users(
                    db,
                    bucket_id=bid,
                    user_ids=[uid_b, uid_c],
                    by_user_id=admin_id,
                )
                await db.commit()

        assert result["granted"] == 1
        assert result["revoked"] == 1
        assert result["unchanged"] == 1
        assert result["user_ids"] == sorted([uid_b, uid_c])

        # Verify DB state: A gone, B and C present
        async with _AsyncSession() as db:
            svc = PermissionService()
            ids = await svc.get_bucket_user_ids(db, bucket_id=bid)
        assert sorted(ids) == sorted([uid_b, uid_c])
        assert uid_a not in ids

    asyncio.run(run())


def test_set_bucket_users_empty_list_revokes_all():
    """Passing an empty user_ids list revokes all existing explicit grants."""

    async def run():
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            u1 = await _make_user(db, "alice")
            u2 = await _make_user(db, "bob")
            b = await _make_bucket(db, "test-bucket")
            await _grant(db, u1.id, b.id, admin.id)
            await _grant(db, u2.id, b.id, admin.id)
            await db.commit()
            bid, admin_id = b.id, admin.id

        with _patch_redis():
            async with _AsyncSession() as db:
                svc = PermissionService()
                result = await svc.set_bucket_users(
                    db,
                    bucket_id=bid,
                    user_ids=[],
                    by_user_id=admin_id,
                )
                await db.commit()

        assert result["granted"] == 0
        assert result["revoked"] == 2
        assert result["unchanged"] == 0
        assert result["user_ids"] == []

        async with _AsyncSession() as db:
            svc = PermissionService()
            ids = await svc.get_bucket_user_ids(db, bucket_id=bid)
        assert ids == []

    asyncio.run(run())


def test_set_bucket_users_idempotent():
    """Calling set_bucket_users twice with the same set yields granted=0, revoked=0 the second time."""

    async def run():
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            u1 = await _make_user(db, "alice")
            u2 = await _make_user(db, "bob")
            b = await _make_bucket(db, "test-bucket")
            await db.commit()
            uid1, uid2, bid, admin_id = u1.id, u2.id, b.id, admin.id

        with _patch_redis():
            async with _AsyncSession() as db:
                svc = PermissionService()
                await svc.set_bucket_users(
                    db, bucket_id=bid, user_ids=[uid1, uid2], by_user_id=admin_id
                )
                await db.commit()

            async with _AsyncSession() as db:
                svc = PermissionService()
                result = await svc.set_bucket_users(
                    db, bucket_id=bid, user_ids=[uid1, uid2], by_user_id=admin_id
                )
                await db.commit()

        assert result["granted"] == 0
        assert result["revoked"] == 0
        assert result["unchanged"] == 2

    asyncio.run(run())


def test_set_bucket_users_deduplicates_input():
    """Duplicate user_ids in the input are deduplicated — no IntegrityError, correct count."""

    async def run():
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            u1 = await _make_user(db, "alice")
            b = await _make_bucket(db, "test-bucket")
            await db.commit()
            uid1, bid, admin_id = u1.id, b.id, admin.id

        with _patch_redis():
            async with _AsyncSession() as db:
                svc = PermissionService()
                # Pass the same user_id three times
                result = await svc.set_bucket_users(
                    db,
                    bucket_id=bid,
                    user_ids=[uid1, uid1, uid1],
                    by_user_id=admin_id,
                )
                await db.commit()

        # Only one row should exist
        assert result["granted"] == 1
        assert result["user_ids"] == [uid1]

        async with _AsyncSession() as db:
            svc = PermissionService()
            ids = await svc.get_bucket_user_ids(db, bucket_id=bid)
        assert ids == [uid1]

    asyncio.run(run())


def test_set_bucket_users_unknown_user_raises_400_no_mutation():
    """
    Providing an unknown user_id raises HTTPException(400) and leaves
    membership unchanged (no partial mutation).
    """

    async def run():
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            u1 = await _make_user(db, "alice")
            b = await _make_bucket(db, "test-bucket")
            await _grant(db, u1.id, b.id, admin.id)
            await db.commit()
            uid1, bid, admin_id = u1.id, b.id, admin.id

        unknown_id = 99999
        raised = False

        with _patch_redis():
            async with _AsyncSession() as db:
                svc = PermissionService()
                try:
                    await svc.set_bucket_users(
                        db,
                        bucket_id=bid,
                        user_ids=[uid1, unknown_id],
                        by_user_id=admin_id,
                    )
                except HTTPException as exc:
                    raised = True
                    assert exc.status_code == 400
                    assert str(unknown_id) in str(exc.detail)
                # Do NOT commit — the service raises before flush completes
                await db.rollback()

        assert raised, "Expected HTTPException(400) was not raised"

        # Membership must be unchanged (still just u1)
        async with _AsyncSession() as db:
            svc = PermissionService()
            ids = await svc.get_bucket_user_ids(db, bucket_id=bid)
        assert sorted(ids) == [uid1]

    asyncio.run(run())


def test_set_bucket_users_missing_bucket_raises_404():
    """Passing a non-existent bucket_id raises HTTPException(404)."""

    async def run():
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            await db.commit()
            admin_id = admin.id

        with _patch_redis():
            async with _AsyncSession() as db:
                svc = PermissionService()
                with pytest.raises(HTTPException) as exc:
                    await svc.set_bucket_users(
                        db,
                        bucket_id=9999,
                        user_ids=[],
                        by_user_id=admin_id,
                    )
                assert exc.value.status_code == 404

    asyncio.run(run())


# ===========================================================================
# API LAYER TESTS
# ===========================================================================
#
# We use FastAPI's synchronous TestClient (wraps ASGI in a thread) and
# override the three FastAPI dependencies that would otherwise require real
# infrastructure:
#   get_db        → yield our in-memory SQLite session
#   require_admin → return a pre-built admin User object
#   get_redis     → return a fake AsyncMock redis client
#
# We also patch get_redis_pool (used by permission_service internally).
# ---------------------------------------------------------------------------


def _make_test_app():
    """
    Build a minimal FastAPI app with only the admin router mounted, wiring in
    dependency overrides.  A new SQLite DB is created per call.
    """
    from fastapi import FastAPI
    from app.api.admin import router as admin_router, require_admin
    from app.db.mysql import get_db

    app = FastAPI()
    app.include_router(admin_router)
    return app, admin_router, require_admin, get_db


# ---------------------------------------------------------------------------
# Helper: run a complete API test scenario inside asyncio.run() with a fresh
# SQLite DB and inject async session into TestClient via dependency override.
# ---------------------------------------------------------------------------


def _build_client_with_session(
    app,
    admin_router,
    require_admin_dep,
    get_db_dep,
    admin_user: User,
    session_factory,
):
    """
    Wire dependency overrides onto *app* and return a synchronous TestClient.

    - get_db is overridden to yield sessions from *session_factory*.
    - require_admin is overridden to return *admin_user* directly.
    - get_redis is overridden to return a fake redis client.
    """
    from app.db.redis_client import get_redis as get_redis_dep

    async def _override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    fake_redis = AsyncMock()
    fake_redis.get = AsyncMock(return_value=None)
    fake_redis.set = AsyncMock(return_value=None)
    fake_redis.delete = AsyncMock(return_value=None)

    app.dependency_overrides[get_db_dep] = _override_get_db
    app.dependency_overrides[require_admin_dep] = lambda: admin_user
    app.dependency_overrides[get_redis_dep] = lambda: fake_redis

    return TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# GET /admin/users/all
# ---------------------------------------------------------------------------


def test_api_list_all_users_shape():
    """
    GET /admin/users/all returns 200 with {data:[{id,username,email,is_active}], total}
    ordered by username.
    """

    async def setup():
        await _setup_db()
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            await _make_user(db, "zara")
            await _make_user(db, "alice")
            await db.commit()
            return admin

    admin = asyncio.run(setup())

    app, admin_router, require_admin_dep, get_db_dep = _make_test_app()

    # We need a sync session factory wrapper for the override
    def _session_factory():
        return _AsyncSession()

    client = _build_client_with_session(
        app, admin_router, require_admin_dep, get_db_dep, admin, _AsyncSession
    )

    with _patch_redis():
        response = client.get("/admin/users/all")

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "total" in body
    # admin-actor, alice, zara = 3 users
    assert body["total"] == 3
    usernames = [u["username"] for u in body["data"]]
    assert usernames == sorted(usernames), "users must be ordered by username"
    # Each item must have the required fields
    for item in body["data"]:
        assert set(item.keys()) >= {"id", "username", "email", "is_active"}

    asyncio.run(_teardown_db())


# ---------------------------------------------------------------------------
# GET /admin/buckets/{bucket_id}/users
# ---------------------------------------------------------------------------


def test_api_get_bucket_users_returns_correct_shape():
    """
    GET /admin/buckets/{id}/users returns
    {bucket_id, user_ids (sorted), is_default}.
    """

    async def setup():
        await _setup_db()
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            u1 = await _make_user(db, "alice")
            u2 = await _make_user(db, "bob")
            b = await _make_bucket(db, "test-bucket")
            await _grant(db, u1.id, b.id, admin.id)
            await _grant(db, u2.id, b.id, admin.id)
            await db.commit()
            return admin, b.id, sorted([u1.id, u2.id])

    admin, bid, expected_uids = asyncio.run(setup())

    app, admin_router, require_admin_dep, get_db_dep = _make_test_app()
    client = _build_client_with_session(
        app, admin_router, require_admin_dep, get_db_dep, admin, _AsyncSession
    )

    with _patch_redis():
        response = client.get(f"/admin/buckets/{bid}/users")

    assert response.status_code == 200
    body = response.json()
    assert body["bucket_id"] == bid
    assert sorted(body["user_ids"]) == expected_uids
    assert body["is_default"] is False

    asyncio.run(_teardown_db())


def test_api_get_bucket_users_is_default_true():
    """is_default=True is returned for the default bucket."""

    async def setup():
        await _setup_db()
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            b = await _make_bucket(db, "default-bucket", is_default=True)
            await db.commit()
            return admin, b.id

    admin, bid = asyncio.run(setup())

    app, admin_router, require_admin_dep, get_db_dep = _make_test_app()
    client = _build_client_with_session(
        app, admin_router, require_admin_dep, get_db_dep, admin, _AsyncSession
    )

    with _patch_redis():
        response = client.get(f"/admin/buckets/{bid}/users")

    assert response.status_code == 200
    body = response.json()
    assert body["is_default"] is True
    assert body["user_ids"] == []

    asyncio.run(_teardown_db())


def test_api_get_bucket_users_404_for_missing_bucket():
    """GET /admin/buckets/{id}/users returns 404 for a non-existent bucket."""

    async def setup():
        await _setup_db()
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            await db.commit()
            return admin

    admin = asyncio.run(setup())

    app, admin_router, require_admin_dep, get_db_dep = _make_test_app()
    client = _build_client_with_session(
        app, admin_router, require_admin_dep, get_db_dep, admin, _AsyncSession
    )

    with _patch_redis():
        response = client.get("/admin/buckets/9999/users")

    assert response.status_code == 404

    asyncio.run(_teardown_db())


# ---------------------------------------------------------------------------
# POST /admin/buckets/{bucket_id}/users
# ---------------------------------------------------------------------------


def test_api_set_bucket_users_returns_correct_shape():
    """
    POST /admin/buckets/{id}/users returns
    {bucket_id, granted, revoked, unchanged, user_ids}.
    """

    async def setup():
        await _setup_db()
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            u1 = await _make_user(db, "alice")
            u2 = await _make_user(db, "bob")
            b = await _make_bucket(db, "test-bucket")
            await db.commit()
            return admin, b.id, [u1.id, u2.id]

    admin, bid, uids = asyncio.run(setup())

    app, admin_router, require_admin_dep, get_db_dep = _make_test_app()
    client = _build_client_with_session(
        app, admin_router, require_admin_dep, get_db_dep, admin, _AsyncSession
    )

    with _patch_redis():
        response = client.post(
            f"/admin/buckets/{bid}/users",
            json={"user_ids": uids},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["bucket_id"] == bid
    assert body["granted"] == 2
    assert body["revoked"] == 0
    assert body["unchanged"] == 0
    assert sorted(body["user_ids"]) == sorted(uids)

    asyncio.run(_teardown_db())


def test_api_set_bucket_users_empty_list_is_valid():
    """POST with empty user_ids is valid (revokes all) and returns 200."""

    async def setup():
        await _setup_db()
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            u1 = await _make_user(db, "alice")
            b = await _make_bucket(db, "test-bucket")
            await _grant(db, u1.id, b.id, admin.id)
            await db.commit()
            return admin, b.id

    admin, bid = asyncio.run(setup())

    app, admin_router, require_admin_dep, get_db_dep = _make_test_app()
    client = _build_client_with_session(
        app, admin_router, require_admin_dep, get_db_dep, admin, _AsyncSession
    )

    with _patch_redis():
        response = client.post(
            f"/admin/buckets/{bid}/users",
            json={"user_ids": []},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["revoked"] == 1
    assert body["user_ids"] == []

    asyncio.run(_teardown_db())


def test_api_set_bucket_users_unknown_user_returns_400():
    """POST with an unknown user_id returns 400."""

    async def setup():
        await _setup_db()
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            b = await _make_bucket(db, "test-bucket")
            await db.commit()
            return admin, b.id

    admin, bid = asyncio.run(setup())

    app, admin_router, require_admin_dep, get_db_dep = _make_test_app()
    client = _build_client_with_session(
        app, admin_router, require_admin_dep, get_db_dep, admin, _AsyncSession
    )

    with _patch_redis():
        response = client.post(
            f"/admin/buckets/{bid}/users",
            json={"user_ids": [99999]},
        )

    assert response.status_code == 400

    asyncio.run(_teardown_db())


def test_api_set_bucket_users_missing_bucket_returns_404():
    """POST /admin/buckets/{id}/users with a missing bucket returns 404."""

    async def setup():
        await _setup_db()
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            await db.commit()
            return admin

    admin = asyncio.run(setup())

    app, admin_router, require_admin_dep, get_db_dep = _make_test_app()
    client = _build_client_with_session(
        app, admin_router, require_admin_dep, get_db_dep, admin, _AsyncSession
    )

    with _patch_redis():
        response = client.post(
            "/admin/buckets/9999/users",
            json={"user_ids": []},
        )

    assert response.status_code == 404

    asyncio.run(_teardown_db())


# ---------------------------------------------------------------------------
# AuthZ guard — non-admin / unauthenticated are rejected
# ---------------------------------------------------------------------------


def test_api_all_routes_require_admin_auth():
    """
    Without overriding require_admin, all three new routes return 403/401
    when no token is provided, confirming the guard is in place.

    We override get_db and get_redis so that the only remaining guard is
    require_admin itself — confirming it rejects unauthenticated callers.
    A request without an Authorization header hits HTTPBearer(auto_error=True)
    which raises 403 before any business logic runs.
    """
    from fastapi import FastAPI
    from app.api.admin import router as admin_router
    from app.db.mysql import get_db
    from app.db.redis_client import get_redis as get_redis_dep

    async def setup():
        await _setup_db()
        async with _AsyncSession() as db:
            b = await _make_bucket(db, "test-bucket")
            await db.commit()
            return b.id

    bid = asyncio.run(setup())

    # Do NOT override require_admin — let the real guard run.
    # Override get_db and get_redis so they don't try to reach real infra.
    app = FastAPI()
    app.include_router(admin_router)

    async def _override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with _AsyncSession() as session:
            yield session

    fake_redis = AsyncMock()

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_redis_dep] = lambda: fake_redis

    # TestClient raises an exception by default when there is a server error,
    # so we use raise_server_exceptions=False and only check status codes.
    client = TestClient(app, raise_server_exceptions=False)

    r1 = client.get("/admin/users/all")
    r2 = client.get(f"/admin/buckets/{bid}/users")
    r3 = client.post(f"/admin/buckets/{bid}/users", json={"user_ids": []})

    # All three must be rejected (401 or 403 — exact code depends on whether
    # a token was sent; here we send none, so 403 from HTTPBearer auto_error).
    for r in (r1, r2, r3):
        assert r.status_code in (401, 403), (
            f"Expected 401 or 403, got {r.status_code} for {r.url}"
        )

    asyncio.run(_teardown_db())

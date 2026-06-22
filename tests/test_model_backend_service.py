"""
Tests for app.services.model_backend_service (Phase 3, Task 3.5).

Covers:
  ModelBackendService:
    - create + 409 on duplicate model_id
    - create stores ciphertext (not plaintext) in DB; round-trip via decrypt
    - patch increments version atomically; leaves api_key_enc untouched when
      api_key absent from patch; re-encrypts when provided; clears to NULL
      on empty string
    - enable / disable bump version
    - delete 409 when bucket-referenced (asserts bucket names in detail);
      delete succeeds when not referenced
    - list / get never expose api_key_enc (ModelBackendRow has no such field)

Bootstrap note:
  Follows the exact same stub-injection pattern as test_service_api_keys.py
  and test_model_backend_crypto.py to avoid Py3.9 X|Y collection errors.
  conftest.py sets VAULT_ENABLED=false before this file is imported.

Route-level tests:
  The FastAPI TestClient requires the full app with all startup lifespan hooks
  (Redis, DB engine, DatabaseBackendSource polling tasks, etc.) which cannot
  be reliably mocked in this isolated environment.  Service-layer tests cover
  all business-logic paths; the api_key=null invariant is exercised by
  inspecting ModelBackendRow directly.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Bootstrap — must happen BEFORE any app.* import.
# ---------------------------------------------------------------------------

import asyncio
import os
import pathlib
import sys
import types
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cryptography.fernet import Fernet

_ROOT = pathlib.Path(__file__).parents[1]

# ---- set MODEL_BACKEND_ENCRYPTION_KEY before the crypto module is imported ---
_TEST_FERNET_KEY = Fernet.generate_key().decode()
os.environ.setdefault("MODEL_BACKEND_ENCRYPTION_KEY", _TEST_FERNET_KEY)


def _stub_if_absent(name: str, attrs: dict) -> None:
    if name not in sys.modules:
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m


def _make_pkg_stub(*parts: str) -> types.ModuleType:
    pkg_path = _ROOT.joinpath(*parts)
    stub = types.ModuleType(".".join(parts))
    stub.__path__ = [str(pkg_path)]  # type: ignore[attr-defined]
    stub.__package__ = ".".join(parts)  # type: ignore[attr-defined]
    return stub


async def _async_noop(*a, **kw) -> None:
    pass


def _bootstrap_stubs() -> None:
    # app.core.httpx uses X | None syntax (Python 3.10+).
    _stub_if_absent("app.core.httpx", {
        "new_http_client": lambda: None,
        "close_http_client": _async_noop,
        "BACKEND_TIMEOUT": None,
    })

    # app.utils.api_key_validator makes outbound HTTP calls.
    _stub_if_absent("app.utils.api_key_validator", {
        "find_working_api_key": _async_noop,
    })

    # app.auth.ldap_auth may not have an LDAP server.
    _stub_if_absent("app.auth.ldap_auth", {
        "authenticate_ldap": lambda u, p: False,
        "get_ldap_user_info": lambda u, p: {},
    })

    # app/services/__init__.py eagerly imports cluster_manager → Redis/DB chain.
    if "app.services" not in sys.modules:
        stub_svc_pkg = types.ModuleType("app.services")
        stub_svc_pkg.__path__ = [str(_ROOT / "app" / "services")]  # type: ignore[attr-defined]
        stub_svc_pkg.__package__ = "app.services"  # type: ignore[attr-defined]
        sys.modules["app.services"] = stub_svc_pkg

    # app.backends — needed for model_backend_service (import chain)
    if "app.backends" not in sys.modules:
        stub_backends_pkg = _make_pkg_stub("app", "backends")
        sys.modules["app.backends"] = stub_backends_pkg

    if "app.backends.base" not in sys.modules:
        stub_base = types.ModuleType("app.backends.base")

        class _FakeRegistry:
            def list_model_ids(self):
                return []
            def get(self, mid):
                return None

        stub_base.registry = _FakeRegistry()  # type: ignore[attr-defined]
        stub_base.Backend = object  # type: ignore[attr-defined]
        sys.modules["app.backends.base"] = stub_base

    # app.db.redis_client — referenced by database_backend_source
    _stub_if_absent("app.db.redis_client", {
        "get_redis": MagicMock(),
        "get_redis_pool": MagicMock(),
        "mark_user_deactivated": AsyncMock(),
        "unmark_user_deactivated": AsyncMock(),
        "is_user_marked_deactivated": AsyncMock(return_value=False),
    })


_bootstrap_stubs()

# ---------------------------------------------------------------------------
# Standalone ORM backed by SQLite (aiosqlite) — mirrors production schema.
# ---------------------------------------------------------------------------

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.pool import StaticPool

_SQLITE_URL = "sqlite+aiosqlite:///:memory:"

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


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    max_concurrent_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    email = mapped_column(String(255), nullable=True)
    created_at = mapped_column(DateTime, nullable=True)
    updated_at = mapped_column(DateTime, nullable=True)


class Bucket(Base):
    __tablename__ = "buckets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description = mapped_column(Text, nullable=True)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    default_model = mapped_column(String(255), nullable=True)
    created_at = mapped_column(DateTime, nullable=True)
    models = relationship(
        "BucketModel",
        back_populates="bucket",
        lazy="select",
        cascade="all, delete-orphan",
    )


class BucketModel(Base):
    __tablename__ = "bucket_models"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bucket_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("buckets.id", ondelete="CASCADE"), nullable=False
    )
    model_id: Mapped[str] = mapped_column(String(255), nullable=False)

    __table_args__ = (
        UniqueConstraint("bucket_id", "model_id", name="uq_bucket_model"),
    )

    bucket = relationship("Bucket", back_populates="models")


class ModelBackend(Base):
    __tablename__ = "model_backends"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_id: Mapped[str] = mapped_column(String(255), nullable=False)
    backend_type: Mapped[str] = mapped_column(String(64), nullable=False, default="static")
    base_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    api_key_enc = mapped_column(Text, nullable=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False, default="admin_ui")
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at = mapped_column(DateTime, nullable=True)
    updated_at = mapped_column(DateTime, nullable=True)
    created_by = mapped_column(Integer, nullable=True)
    updated_by = mapped_column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("model_id", name="uq_model_backends_model_id"),
        Index("ix_model_backends_is_enabled", "is_enabled"),
    )


# ---------------------------------------------------------------------------
# Inject the fake mysql module so app.services.* see our SQLite ORM.
# ---------------------------------------------------------------------------

_fake_mysql = types.ModuleType("app.db.mysql")
_fake_mysql.Base = Base  # type: ignore[attr-defined]
_fake_mysql.User = User  # type: ignore[attr-defined]
_fake_mysql.Bucket = Bucket  # type: ignore[attr-defined]
_fake_mysql.BucketModel = BucketModel  # type: ignore[attr-defined]
_fake_mysql.ModelBackend = ModelBackend  # type: ignore[attr-defined]
# Provide a minimal AsyncSessionLocal for any code that calls it directly.
_fake_mysql.AsyncSessionLocal = _AsyncSession  # type: ignore[attr-defined]


async def _get_db_dep() -> AsyncGenerator[AsyncSession, None]:
    async with _AsyncSession() as s:
        yield s


_fake_mysql.get_db = _get_db_dep  # type: ignore[attr-defined]
sys.modules["app.db.mysql"] = _fake_mysql

# ---------------------------------------------------------------------------
# Now it is safe to import the production service + crypto module.
# ---------------------------------------------------------------------------

import app.services.model_backend_service as _svc_mod  # noqa: E402
from app.services.model_backend_service import ModelBackendService  # noqa: E402
from app.services.model_backend_crypto import decrypt_api_key, encrypt_api_key  # noqa: E402
import app.services.model_backend_crypto as _crypto_mod  # noqa: E402

# Ensure the crypto module uses our test key.
_crypto_mod._fernet = None  # force lazy rebuild with the env var we set above

# ---------------------------------------------------------------------------
# DB lifecycle helpers
# ---------------------------------------------------------------------------


async def _setup_db() -> None:
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def _teardown_db() -> None:
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture(autouse=True)
def _db_tables():
    """Create all tables before each test; drop them afterwards."""
    asyncio.run(_setup_db())
    yield
    asyncio.run(_teardown_db())


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


async def _make_user(db: AsyncSession, username: str, *, is_admin: bool = False) -> User:
    u = User(username=username, is_admin=is_admin, is_active=True)
    db.add(u)
    await db.flush()
    return u


async def _make_bucket(db: AsyncSession, name: str) -> Bucket:
    b = Bucket(name=name)
    db.add(b)
    await db.flush()
    return b


async def _make_bucket_model(db: AsyncSession, bucket_id: int, model_id: str) -> BucketModel:
    bm = BucketModel(bucket_id=bucket_id, model_id=model_id)
    db.add(bm)
    await db.flush()
    return bm


async def _make_model_backend(
    db: AsyncSession,
    model_id: str = "test/model",
    base_url: str = "http://localhost:8000",
    api_key: str | None = None,
    created_by: int | None = None,
) -> _svc_mod.ModelBackendRow:
    svc = ModelBackendService()
    return await svc.create_backend(
        db,
        model_id=model_id,
        base_url=base_url,
        api_key=api_key,
        created_by=created_by,
    )


# ---------------------------------------------------------------------------
# Helper: raw ORM fetch (to inspect api_key_enc without going through service)
# ---------------------------------------------------------------------------

from sqlalchemy import select as _select


async def _raw_fetch(db: AsyncSession, model_id: str) -> ModelBackend | None:
    result = await db.execute(
        _select(ModelBackend).where(ModelBackend.model_id == model_id)
    )
    return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# Tests: create
# ---------------------------------------------------------------------------


def test_create_backend_stores_ciphertext_not_plaintext():
    """The stored api_key_enc must be different from the plaintext."""

    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(
                db, model_id="m1", api_key="sk-supersecret-12345"
            )
        async with _AsyncSession() as db:
            orm = await _raw_fetch(db, "m1")
            assert orm is not None
            # Ciphertext must not equal plaintext
            assert orm.api_key_enc != "sk-supersecret-12345"
            # Ciphertext must be decryptable back to original
            assert decrypt_api_key(orm.api_key_enc) == "sk-supersecret-12345"

    asyncio.run(_run())


def test_create_backend_no_api_key_stores_null():
    """Omitting api_key stores NULL in api_key_enc."""

    async def _run():
        async with _AsyncSession() as db:
            row = await _make_model_backend(db, model_id="m2", api_key=None)
        async with _AsyncSession() as db:
            orm = await _raw_fetch(db, "m2")
            assert orm is not None
            assert orm.api_key_enc is None

    asyncio.run(_run())


def test_create_backend_returns_row_without_api_key_enc():
    """ModelBackendRow must not expose api_key_enc or any key material."""

    async def _run():
        async with _AsyncSession() as db:
            row = await _make_model_backend(
                db, model_id="m3", api_key="secret"
            )
        # ModelBackendRow is a dataclass — it has no api_key_enc field at all.
        assert not hasattr(row, "api_key_enc")
        assert not hasattr(row, "api_key")

    asyncio.run(_run())


def test_create_backend_409_on_duplicate():
    """Creating two backends with the same model_id raises HTTP 409."""
    from fastapi import HTTPException

    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="dup/model")

        # Second create with the same model_id must conflict.
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            with pytest.raises(HTTPException) as exc_info:
                await svc.create_backend(
                    db,
                    model_id="dup/model",
                    base_url="http://other:8000",
                    api_key=None,
                    created_by=None,
                )
        assert exc_info.value.status_code == 409

    asyncio.run(_run())


def test_create_backend_strips_trailing_slash():
    """base_url trailing slash is stripped at create time."""

    async def _run():
        async with _AsyncSession() as db:
            row = await _make_model_backend(
                db, model_id="slash/model", base_url="http://server:8000/"
            )
        assert row.base_url == "http://server:8000"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Tests: get / list
# ---------------------------------------------------------------------------


def test_get_backend_returns_none_when_absent():
    async def _run():
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            result = await svc.get_backend(db, "nonexistent-model")
        assert result is None

    asyncio.run(_run())


def test_get_backend_returns_row():
    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="get/test")
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            row = await svc.get_backend(db, "get/test")
        assert row is not None
        assert row.model_id == "get/test"
        assert not hasattr(row, "api_key_enc")

    asyncio.run(_run())


def test_list_backends_returns_all():
    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="list/a")
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="list/b")
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            rows = await svc.list_backends(db)
        model_ids = [r.model_id for r in rows]
        assert "list/a" in model_ids
        assert "list/b" in model_ids
        # Verify no row leaks api_key_enc
        for row in rows:
            assert not hasattr(row, "api_key_enc")

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Tests: patch / update
# ---------------------------------------------------------------------------


def test_patch_increments_version():
    """PATCH must increment version, even when only base_url changes."""

    async def _run():
        async with _AsyncSession() as db:
            row = await _make_model_backend(db, model_id="ver/model")
            assert row.version == 1

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            updated = await svc.update_backend(
                db,
                model_id="ver/model",
                patch={"base_url": "http://new-host:9000"},
                updated_by=None,
            )
        assert updated.version == 2

    asyncio.run(_run())


def test_patch_version_increments_again_on_second_patch():
    """Each successive PATCH increments version independently."""

    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="ver2/model")

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            r1 = await svc.update_backend(
                db, "ver2/model", patch={"is_enabled": False}, updated_by=None
            )
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            r2 = await svc.update_backend(
                db, "ver2/model", patch={"is_enabled": True}, updated_by=None
            )
        assert r1.version == 2
        assert r2.version == 3

    asyncio.run(_run())


def test_patch_api_key_absent_leaves_ciphertext_unchanged():
    """When api_key is not in the patch dict, api_key_enc must be untouched."""

    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(
                db, model_id="key-unchanged/model", api_key="original-secret"
            )

        # Read the ciphertext before PATCH.
        async with _AsyncSession() as db:
            before = await _raw_fetch(db, "key-unchanged/model")
            original_enc = before.api_key_enc

        # PATCH base_url only — api_key not in patch dict.
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            await svc.update_backend(
                db,
                "key-unchanged/model",
                patch={"base_url": "http://changed:9000"},
                updated_by=None,
            )

        # api_key_enc must be the same ciphertext.
        async with _AsyncSession() as db:
            after = await _raw_fetch(db, "key-unchanged/model")
        assert after.api_key_enc == original_enc
        # And it still decrypts to the original plaintext.
        assert decrypt_api_key(after.api_key_enc) == "original-secret"

    asyncio.run(_run())


def test_patch_api_key_provided_re_encrypts():
    """When api_key is in the patch dict (non-empty), the ciphertext is updated."""

    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(
                db, model_id="key-reenc/model", api_key="old-key"
            )
        async with _AsyncSession() as db:
            before = await _raw_fetch(db, "key-reenc/model")
            enc_before = before.api_key_enc

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            await svc.update_backend(
                db,
                "key-reenc/model",
                patch={"api_key": "new-key"},
                updated_by=None,
            )

        async with _AsyncSession() as db:
            after = await _raw_fetch(db, "key-reenc/model")

        # Ciphertext must differ (new key, and Fernet uses random IV).
        assert after.api_key_enc != enc_before
        # Must decrypt to the new plaintext.
        assert decrypt_api_key(after.api_key_enc) == "new-key"

    asyncio.run(_run())


def test_patch_api_key_empty_string_clears_to_null():
    """When api_key is an empty string in the patch, api_key_enc is set to NULL."""

    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(
                db, model_id="key-clear/model", api_key="some-key"
            )

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            await svc.update_backend(
                db,
                "key-clear/model",
                patch={"api_key": ""},
                updated_by=None,
            )

        async with _AsyncSession() as db:
            after = await _raw_fetch(db, "key-clear/model")
        assert after.api_key_enc is None

    asyncio.run(_run())


def test_patch_404_when_absent():
    from fastapi import HTTPException

    async def _run():
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            with pytest.raises(HTTPException) as exc_info:
                await svc.update_backend(
                    db, "ghost/model", patch={}, updated_by=None
                )
        assert exc_info.value.status_code == 404

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Tests: enable / disable
# ---------------------------------------------------------------------------


def test_enable_bumps_version():
    async def _run():
        async with _AsyncSession() as db:
            row = await _make_model_backend(db, model_id="en/model")

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            # Disable first so we have something to re-enable.
            r1 = await svc.disable_backend(db, "en/model", updated_by=None)
        assert r1.is_enabled is False
        assert r1.version == 2

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            r2 = await svc.enable_backend(db, "en/model", updated_by=None)
        assert r2.is_enabled is True
        assert r2.version == 3

    asyncio.run(_run())


def test_disable_bumps_version():
    async def _run():
        async with _AsyncSession() as db:
            row = await _make_model_backend(db, model_id="dis/model")

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            r = await svc.disable_backend(db, "dis/model", updated_by=None)
        assert r.is_enabled is False
        assert r.version == 2

    asyncio.run(_run())


def test_enable_404_when_absent():
    from fastapi import HTTPException

    async def _run():
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            with pytest.raises(HTTPException) as exc_info:
                await svc.enable_backend(db, "ghost/model", updated_by=None)
        assert exc_info.value.status_code == 404

    asyncio.run(_run())


def test_disable_404_when_absent():
    from fastapi import HTTPException

    async def _run():
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            with pytest.raises(HTTPException) as exc_info:
                await svc.disable_backend(db, "ghost/model", updated_by=None)
        assert exc_info.value.status_code == 404

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Tests: delete
# ---------------------------------------------------------------------------


def test_delete_succeeds_when_not_referenced():
    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="del/model")

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            await svc.delete_backend(db, "del/model")

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            row = await svc.get_backend(db, "del/model")
        assert row is None

    asyncio.run(_run())


def test_delete_409_when_bucket_references_model():
    """Delete must fail with 409 and the bucket names listed in detail."""
    from fastapi import HTTPException

    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="ref/model")
            b = await _make_bucket(db, "my-bucket")
            await _make_bucket_model(db, b.id, "ref/model")
            await db.commit()

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            with pytest.raises(HTTPException) as exc_info:
                await svc.delete_backend(db, "ref/model")

        exc = exc_info.value
        assert exc.status_code == 409
        # The bucket name must appear in the error detail.
        assert "my-bucket" in exc.detail

    asyncio.run(_run())


def test_delete_409_lists_all_referencing_buckets():
    """All referencing bucket names must appear in the 409 detail."""
    from fastapi import HTTPException

    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="multi-ref/model")
            b1 = await _make_bucket(db, "bucket-alpha")
            b2 = await _make_bucket(db, "bucket-beta")
            await _make_bucket_model(db, b1.id, "multi-ref/model")
            await _make_bucket_model(db, b2.id, "multi-ref/model")
            await db.commit()

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            with pytest.raises(HTTPException) as exc_info:
                await svc.delete_backend(db, "multi-ref/model")

        exc = exc_info.value
        assert exc.status_code == 409
        assert "bucket-alpha" in exc.detail
        assert "bucket-beta" in exc.detail

    asyncio.run(_run())


def test_delete_404_when_absent():
    from fastapi import HTTPException

    async def _run():
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            with pytest.raises(HTTPException) as exc_info:
                await svc.delete_backend(db, "ghost/model")
        assert exc_info.value.status_code == 404

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Tests: create → get round-trip (key storage verification)
# ---------------------------------------------------------------------------


def test_create_get_round_trip_key_stored_as_ciphertext():
    """
    After create, get_backend returns a row with no key material.
    The raw DB row's api_key_enc is the ciphertext (not plaintext) and
    decrypts back to the original value.
    """

    async def _run():
        plaintext = "my-real-api-key"
        async with _AsyncSession() as db:
            await _make_model_backend(
                db, model_id="rt/model", api_key=plaintext
            )

        # Service layer: get returns row without api_key_enc.
        async with _AsyncSession() as db:
            svc = ModelBackendService()
            row = await svc.get_backend(db, "rt/model")
        assert row is not None
        assert not hasattr(row, "api_key_enc")

        # ORM layer: ciphertext != plaintext; decrypt recovers original.
        async with _AsyncSession() as db:
            orm = await _raw_fetch(db, "rt/model")
        assert orm.api_key_enc != plaintext
        assert decrypt_api_key(orm.api_key_enc) == plaintext

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Tests: is_referenced_by_bucket helper
# ---------------------------------------------------------------------------


def test_is_referenced_by_bucket_empty_when_no_refs():
    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="noref/model")
            svc = ModelBackendService()
            names = await svc.is_referenced_by_bucket(db, "noref/model")
        assert names == []

    asyncio.run(_run())


def test_is_referenced_by_bucket_returns_names():
    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="ref2/model")
            b = await _make_bucket(db, "ref-bucket")
            await _make_bucket_model(db, b.id, "ref2/model")
            svc = ModelBackendService()
            names = await svc.is_referenced_by_bucket(db, "ref2/model")
        assert "ref-bucket" in names

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Tests: ModelBackendRow never exposes key material
# ---------------------------------------------------------------------------


def test_model_backend_row_has_no_key_material_fields():
    """ModelBackendRow dataclass must not have api_key_enc or api_key attributes."""
    from app.services.model_backend_service import ModelBackendRow
    import dataclasses

    field_names = {f.name for f in dataclasses.fields(ModelBackendRow)}
    assert "api_key_enc" not in field_names
    assert "api_key" not in field_names


def test_list_backends_never_exposes_key_material():
    """list_backends must return rows with no key material for any entry."""

    async def _run():
        async with _AsyncSession() as db:
            await _make_model_backend(db, model_id="safe/a", api_key="key1")
            await _make_model_backend(db, model_id="safe/b", api_key="key2")

        async with _AsyncSession() as db:
            svc = ModelBackendService()
            rows = await svc.list_backends(db)

        for row in rows:
            assert not hasattr(row, "api_key_enc")
            assert not hasattr(row, "api_key")

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Tests: concurrent-delete TOCTOU (SUGGESTION 1)
#
# The race window is: row found by the initial SELECT → concurrent DELETE on
# another connection → UPDATE executes (0 rows affected, no error in SQL) →
# re-fetch returns None → must raise 404 "deleted concurrently".
#
# Because both the initial SELECT and the re-fetch happen inside the same
# AsyncSession call (update_backend / _set_enabled), we cannot interleave a
# real DELETE between them without coroutine-level patching.  We use
# unittest.mock.patch to replace the session's execute() so that:
#   - call 1 (existence SELECT)  → returns a real row  (row exists)
#   - call 2 (UPDATE stmt)       → no-op result
#   - call 3 (re-fetch SELECT)   → returns an empty result (row gone)
# This precisely mirrors the production race without needing two real DB
# connections open simultaneously.
# ---------------------------------------------------------------------------

from unittest.mock import AsyncMock, MagicMock, patch as _patch  # noqa: E402 (already imported above, re-import for clarity)


def _make_orm_row(model_id: str = "race/model") -> ModelBackend:
    """Build a detached ModelBackend ORM instance for use as a mock return value."""
    row = ModelBackend()
    row.model_id = model_id
    row.backend_type = "static"
    row.base_url = "http://original:8000"
    row.is_enabled = True
    row.source = "admin_ui"
    row.version = 1
    row.created_at = None
    row.updated_at = None
    row.created_by = None
    row.updated_by = None
    row.api_key_enc = None
    return row


def _scalar_result(value):
    """Return a MagicMock that mimics CursorResult.scalar_one_or_none()."""
    r = MagicMock()
    r.scalar_one_or_none.return_value = value
    return r


def test_update_backend_404_on_concurrent_delete():
    """
    update_backend must raise HTTP 404 (not NoResultFound / 500) when the row
    is deleted between the internal UPDATE and the subsequent re-fetch.
    """
    from fastapi import HTTPException

    async def _run():
        fake_row = _make_orm_row("race/model")

        call_count = 0

        async def _fake_execute(stmt, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Existence SELECT — row found.
                return _scalar_result(fake_row)
            elif call_count == 2:
                # UPDATE — succeeds (rowcount irrelevant in service layer).
                return MagicMock()
            else:
                # Re-fetch SELECT — row gone (concurrent delete).
                return _scalar_result(None)

        async with _AsyncSession() as db:
            db.execute = _fake_execute  # type: ignore[method-assign]
            db.commit = AsyncMock()     # skip real commit
            svc = ModelBackendService()
            with pytest.raises(HTTPException) as exc_info:
                await svc.update_backend(
                    db,
                    model_id="race/model",
                    patch={"base_url": "http://another:9000"},
                    updated_by=None,
                )

        assert exc_info.value.status_code == 404
        assert "concurrently" in exc_info.value.detail

    asyncio.run(_run())


def test_enable_backend_404_on_concurrent_delete():
    """
    _set_enabled (called by enable_backend) must raise HTTP 404 when the row
    is deleted between the UPDATE and the subsequent re-fetch.
    """
    from fastapi import HTTPException

    async def _run():
        fake_row = _make_orm_row("race2/model")

        call_count = 0

        async def _fake_execute(stmt, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Existence SELECT — row found.
                return _scalar_result(fake_row)
            elif call_count == 2:
                # UPDATE — no error.
                return MagicMock()
            else:
                # Re-fetch SELECT — row gone.
                return _scalar_result(None)

        async with _AsyncSession() as db:
            db.execute = _fake_execute  # type: ignore[method-assign]
            db.commit = AsyncMock()
            svc = ModelBackendService()
            with pytest.raises(HTTPException) as exc_info:
                await svc.enable_backend(db, "race2/model", updated_by=None)

        assert exc_info.value.status_code == 404
        assert "concurrently" in exc_info.value.detail

    asyncio.run(_run())

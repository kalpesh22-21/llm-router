"""
Tests for the service-account API key feature.

Covers:
  ApiKeyService      — generate_key, create_key (hash stored + per-key fields,
                       invalid-scope 400), resolve_key (cache hit/miss,
                       negative cases, model-subset, concurrency override),
                       revoke_key (ownership guard), evict_service_cache
  ServiceAccountService — create_service_account, get_accessible_models,
                          set_active / is_active, grant/revoke_bucket_access
                          (idempotent), list_service_accounts
  Auth layer         — get_current_user (CRITICAL-2: service payload → 403),
                       refresh_token (service payload → 403),
                       _assert_scope, unified resolver (service key / JWT / 401)
  Proxy invariant    — CRITICAL-1: backend resolved BEFORE rate-limit increment

Approach (mirrors test_bucket_users.py):
  Pure / mocked — no real MySQL, Redis, or network.
  SQLite in-memory via SQLAlchemy async (aiosqlite) is used for the DB.
  asyncio.run() drives all async helpers; only core pytest is required.
  No pytest-asyncio dependency needed.

Environment note:
  This suite runs on Python 3.9 in the developer's local environment.  The
  production codebase targets Python 3.10+ and uses X | Y union syntax in
  several modules (app/core/httpx.py, app/api/auth_routes.py Pydantic models).
  We work around this by:
    1. Injecting stub modules for app.core.httpx, app.utils.api_key_validator,
       and app.auth.ldap_auth before any app.* imports.
    2. Blocking app/services/__init__.py (which eagerly imports cluster_manager)
       by registering a package stub with __path__ but without running __init__.
    3. Injecting a fake app.db.mysql backed by a real SQLite/aiosqlite engine
       so ORM operations behave exactly as in production.
  The conftest already sets VAULT_ENABLED=false before any app.* import.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Bootstrap — must happen BEFORE any app.* import.
# conftest.py sets VAULT_ENABLED=false, JWT_SECRET_KEY, API_KEY, and
# PHOENIX_TRACING_ENABLED.  We add the remaining stubs needed on Python 3.9.
# ---------------------------------------------------------------------------

import asyncio
import datetime
import hashlib
import json
import os
import pathlib
import sys
import types
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

# Stub modules that use Python-3.10-only syntax or unavailable packages.
# These must be registered BEFORE any app.* import triggers the import chain.

def _stub_if_absent(name: str, attrs: dict) -> None:
    """Register a stub module with the given attrs if not already in sys.modules."""
    if name not in sys.modules:
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m


def _make_pkg_stub(root: pathlib.Path, *parts: str) -> types.ModuleType:
    """Create a package stub with __path__ set to the real directory."""
    pkg_path = root.joinpath(*parts)
    stub = types.ModuleType(".".join(parts))
    stub.__path__ = [str(pkg_path)]  # type: ignore[attr-defined]
    stub.__package__ = ".".join(parts)  # type: ignore[attr-defined]
    return stub


def _make_counter_class():
    """Return a MagicMock class that can be called to produce a metric object."""
    return MagicMock


def _bootstrap_stubs() -> None:
    """Register all required module stubs into sys.modules."""
    _root = pathlib.Path(__file__).parents[1]

    # app.core.httpx uses `X | None` type-union syntax (Python 3.10+).
    stub_httpx = types.ModuleType("app.core.httpx")
    stub_httpx.get_http_client = lambda: None  # type: ignore[attr-defined]
    stub_httpx.close_http_client = _async_noop  # type: ignore[attr-defined]
    sys.modules.setdefault("app.core.httpx", stub_httpx)

    # app.utils.api_key_validator makes outbound HTTP calls at import time.
    stub_validator = types.ModuleType("app.utils.api_key_validator")
    stub_validator.find_working_api_key = _async_return_empty  # type: ignore[attr-defined]
    sys.modules.setdefault("app.utils.api_key_validator", stub_validator)

    # app.auth.ldap_auth may not have the LDAP server available.
    stub_ldap = types.ModuleType("app.auth.ldap_auth")
    stub_ldap.authenticate_ldap = lambda u, p: False  # type: ignore[attr-defined]
    stub_ldap.get_ldap_user_info = lambda u, p: {}  # type: ignore[attr-defined]
    sys.modules.setdefault("app.auth.ldap_auth", stub_ldap)

    # app/services/__init__.py eagerly imports cluster_manager which needs
    # psycopg and dozens of other things.  Expose a package stub so
    # sub-module imports (app.services.api_key, app.services.service_account,
    # app.services.permission) still resolve to the real files.
    if "app.services" not in sys.modules:
        stub_svc_pkg = types.ModuleType("app.services")
        stub_svc_pkg.__path__ = [  # type: ignore[attr-defined]
            str(_root / "app" / "services")
        ]
        stub_svc_pkg.__package__ = "app.services"  # type: ignore[attr-defined]
        sys.modules["app.services"] = stub_svc_pkg

    # app.backends.base is imported lazily inside resolve_key().  Provide a
    # stub with a controllable FakeRegistry so that lazy import succeeds and
    # tests can replace it per-test via patch.object.
    if "app.backends" not in sys.modules:
        stub_backends_pkg = types.ModuleType("app.backends")
        stub_backends_pkg.__path__ = [str(_root / "app" / "backends")]  # type: ignore[attr-defined]
        stub_backends_pkg.__package__ = "app.backends"  # type: ignore[attr-defined]
        sys.modules["app.backends"] = stub_backends_pkg

    if "app.backends.base" not in sys.modules:
        stub_backends_base = types.ModuleType("app.backends.base")

        class _EmptyRegistry:
            def list_model_ids(self):
                return []

        stub_backends_base.registry = _EmptyRegistry()  # type: ignore[attr-defined]
        # Also expose a Backend base class and a registry object for proxy.py imports
        stub_backends_base.Backend = object  # type: ignore[attr-defined]
        sys.modules["app.backends.base"] = stub_backends_base

    # Stubs needed to import app.api.proxy without the full infrastructure.
    # These are only used for _assert_scope and the CRITICAL-1 test.
    _stub_if_absent("prometheus_client", {
        "Counter": _make_counter_class(),
        "Histogram": _make_counter_class(),
        "Gauge": _make_counter_class(),
        "REGISTRY": MagicMock(),
    })

    # app.metrics.collector — proxy.py imports metrics_collector at module level
    if "app.metrics" not in sys.modules:
        sys.modules["app.metrics"] = _make_pkg_stub(_root, "app", "metrics")
    _stub_if_absent("app.metrics.collector", {"metrics_collector": MagicMock()})

    # app.routing — proxy.py imports model_router at module level
    if "app.routing" not in sys.modules:
        sys.modules["app.routing"] = _make_pkg_stub(_root, "app", "routing")
    _router_stub = MagicMock()
    _router_stub.resolve = MagicMock(return_value=MagicMock())
    _stub_if_absent("app.routing.router", {"model_router": _router_stub})

    # app.services.rate_limit — proxy.py imports rate_limit_service at module level
    _stub_if_absent("app.services.rate_limit", {"rate_limit_service": AsyncMock()})

    # app.services.content_filter — proxy.py imports content_filter_service at module level
    _stub_if_absent("app.services.content_filter", {"content_filter_service": AsyncMock()})

    # app.db.redis_client — proxy.py imports several helpers from it
    _redis_client_stub = types.ModuleType("app.db.redis_client")
    _redis_client_stub.get_redis = MagicMock()  # type: ignore[attr-defined]
    _redis_client_stub.get_redis_pool = MagicMock()  # type: ignore[attr-defined]
    _redis_client_stub.is_user_marked_deactivated = AsyncMock(return_value=False)  # type: ignore[attr-defined]
    _redis_client_stub.mark_user_deactivated = AsyncMock()  # type: ignore[attr-defined]
    _redis_client_stub.unmark_user_deactivated = AsyncMock()  # type: ignore[attr-defined]
    sys.modules.setdefault("app.db.redis_client", _redis_client_stub)

    # app.logging.pheonix — TracingHelper used as decorator in proxy.py
    _logging_pkg = types.ModuleType("app.logging")
    _logging_pkg.__path__ = [str(_root / "app" / "logging")]  # type: ignore[attr-defined]
    _logging_pkg.__package__ = "app.logging"  # type: ignore[attr-defined]
    sys.modules.setdefault("app.logging", _logging_pkg)
    _pheonix_stub = types.ModuleType("app.logging.pheonix")
    _pheonix_stub.SpanKind = MagicMock()  # type: ignore[attr-defined]

    # TracingHelper.trace_async is used as a decorator — it must return a pass-through
    class _FakeTracingHelper:
        @staticmethod
        def trace_async(*args, **kwargs):
            def decorator(fn):
                return fn
            return decorator

    _pheonix_stub.TracingHelper = _FakeTracingHelper  # type: ignore[attr-defined]
    sys.modules.setdefault("app.logging.pheonix", _pheonix_stub)


async def _async_noop(*a, **kw) -> None:
    pass


async def _async_return_empty(*a, **kw) -> str:
    return ""


_bootstrap_stubs()

# ---------------------------------------------------------------------------
# Standalone ORM — mirrors production models, built against a fresh
# SQLite engine so we never touch the real MySQL database.
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
    JSON,
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
    email = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    max_concurrent_requests: Mapped[int] = mapped_column(
        Integer, nullable=False, default=10
    )
    created_at = mapped_column(DateTime, nullable=True)
    updated_at = mapped_column(DateTime, nullable=True)
    bucket_accesses = relationship(
        "UserBucketAccess",
        back_populates="user",
        foreign_keys="UserBucketAccess.user_id",
        lazy="select",
    )


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
    user_accesses = relationship(
        "UserBucketAccess", back_populates="bucket", lazy="select"
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


class UserBucketAccess(Base):
    __tablename__ = "user_bucket_access"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    bucket_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("buckets.id", ondelete="CASCADE"), nullable=False
    )
    granted_by = mapped_column(Integer, nullable=True)
    granted_at = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("user_id", "bucket_id", name="uq_user_bucket"),
    )
    user = relationship(
        "User", back_populates="bucket_accesses", foreign_keys=[user_id]
    )
    bucket = relationship("Bucket", back_populates="user_accesses")


class ServiceAccount(Base):
    __tablename__ = "service_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description = mapped_column(Text, nullable=True)
    owner_team = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    max_concurrent_requests: Mapped[int] = mapped_column(
        Integer, nullable=False, default=10
    )
    created_at = mapped_column(DateTime, nullable=True)
    updated_at = mapped_column(DateTime, nullable=True)
    created_by = mapped_column(Integer, nullable=True)

    bucket_accesses = relationship(
        "ServiceBucketAccess",
        back_populates="service_account",
        lazy="select",
        cascade="all, delete-orphan",
    )
    api_keys = relationship(
        "ApiKey",
        back_populates="service_account",
        lazy="select",
        cascade="all, delete-orphan",
    )


class ServiceBucketAccess(Base):
    __tablename__ = "service_bucket_access"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    service_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("service_accounts.id", ondelete="CASCADE"), nullable=False
    )
    bucket_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("buckets.id", ondelete="CASCADE"), nullable=False
    )
    granted_by = mapped_column(Integer, nullable=True)
    granted_at = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("service_id", "bucket_id", name="uq_service_bucket"),
        Index("ix_service_bucket_access_service_id", "service_id"),
    )

    service_account = relationship("ServiceAccount", back_populates="bucket_accesses")
    bucket = relationship("Bucket")


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    service_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("service_accounts.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    prefix: Mapped[str] = mapped_column(String(32), nullable=False)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    created_by = mapped_column(Integer, nullable=True)
    created_at = mapped_column(DateTime, nullable=True)
    expires_at = mapped_column(DateTime, nullable=True)
    last_used_at = mapped_column(DateTime, nullable=True)
    revoked_at = mapped_column(DateTime, nullable=True)
    scopes = mapped_column(JSON, nullable=True)
    model_ids = mapped_column(JSON, nullable=True)
    max_concurrent_requests = mapped_column(Integer, nullable=True)

    service_account = relationship("ServiceAccount", back_populates="api_keys")


# ---------------------------------------------------------------------------
# Inject the fake mysql module so app.services.* see our SQLite ORM.
# ---------------------------------------------------------------------------

_fake_mysql = types.ModuleType("app.db.mysql")
_fake_mysql.Base = Base  # type: ignore[attr-defined]
_fake_mysql.User = User  # type: ignore[attr-defined]
_fake_mysql.ApiKey = ApiKey  # type: ignore[attr-defined]
_fake_mysql.Bucket = Bucket  # type: ignore[attr-defined]
_fake_mysql.BucketModel = BucketModel  # type: ignore[attr-defined]
_fake_mysql.UserBucketAccess = UserBucketAccess  # type: ignore[attr-defined]
_fake_mysql.ServiceAccount = ServiceAccount  # type: ignore[attr-defined]
_fake_mysql.ServiceBucketAccess = ServiceBucketAccess  # type: ignore[attr-defined]
_fake_mysql.AsyncSessionLocal = _AsyncSession  # type: ignore[attr-defined]


async def _get_db_dep() -> AsyncGenerator[AsyncSession, None]:
    async with _AsyncSession() as s:
        yield s


_fake_mysql.get_db = _get_db_dep  # type: ignore[attr-defined]
sys.modules["app.db.mysql"] = _fake_mysql

# ---------------------------------------------------------------------------
# Now it is safe to import the production services.
# ---------------------------------------------------------------------------

import app.services.api_key as _api_key_mod  # noqa: E402
import app.services.service_account as _svc_acct_mod  # noqa: E402
import app.services.permission as _perm_mod  # noqa: E402

from app.services.api_key import ApiKeyService  # noqa: E402
from app.services.service_account import ServiceAccountService  # noqa: E402
from app.services.permission import PermissionService  # noqa: E402

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


async def _make_user(
    db: AsyncSession,
    username: str,
    *,
    is_admin: bool = False,
    is_active: bool = True,
) -> User:
    u = User(
        username=username,
        is_admin=is_admin,
        is_active=is_active,
    )
    db.add(u)
    await db.flush()
    return u


async def _make_service_account(
    db: AsyncSession,
    name: str,
    *,
    is_active: bool = True,
    max_concurrent_requests: int = 10,
    by_user_id: int = 1,
) -> ServiceAccount:
    """Create and persist a ServiceAccount row directly (bypasses service layer)."""
    sa = ServiceAccount(
        name=name,
        is_active=is_active,
        max_concurrent_requests=max_concurrent_requests,
        created_by=by_user_id,
    )
    db.add(sa)
    await db.flush()
    return sa


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


async def _make_bucket_model(
    db: AsyncSession,
    bucket_id: int,
    model_id: str,
) -> BucketModel:
    bm = BucketModel(bucket_id=bucket_id, model_id=model_id)
    db.add(bm)
    await db.flush()
    return bm


async def _make_api_key(
    db: AsyncSession,
    service_id: int,
    *,
    name: str = "test-key",
    expires_at: datetime.datetime | None = None,
    revoked_at: datetime.datetime | None = None,
    scopes: list | None = None,
    model_ids: list | None = None,
    max_concurrent_requests: int | None = None,
) -> tuple[ApiKey, str]:
    """Create and persist an API key; returns (row, plaintext)."""
    svc = ApiKeyService()
    row, plaintext = await svc.create_key(
        db,
        service_id=service_id,
        name=name,
        expires_at=expires_at,
        created_by=1,
        scopes=scopes,
        model_ids=model_ids,
        max_concurrent_requests=max_concurrent_requests,
    )
    if revoked_at is not None:
        row.revoked_at = revoked_at
        await db.flush()
    return row, plaintext


# ---------------------------------------------------------------------------
# Redis mock helpers
# ---------------------------------------------------------------------------


def _make_redis_miss() -> AsyncMock:
    """Redis mock that always returns a cache miss (get → None)."""
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.set = AsyncMock(return_value=True)
    r.delete = AsyncMock(return_value=1)
    return r


def _make_redis_hit(payload: dict) -> AsyncMock:
    """Redis mock that always returns a cache hit with *payload*."""
    r = AsyncMock()
    r.get = AsyncMock(return_value=json.dumps(payload))
    r.set = AsyncMock(return_value=True)
    r.delete = AsyncMock(return_value=1)
    return r


# ---------------------------------------------------------------------------
# Patch helpers
# ---------------------------------------------------------------------------


def _patch_redis_pool(fake_redis):
    """
    Patch get_redis_pool in the permission module directly.
    """
    return patch.object(_perm_mod, "get_redis_pool", return_value=fake_redis)


def _patch_backend_registry(model_ids=None):
    """
    Patch the backend registry used inside resolve_key.

    resolve_key() does ``from app.backends.base import registry as backend_registry``
    lazily at call time. We patch the ``registry`` attribute on the already-stubbed
    ``app.backends.base`` module, and separately patch
    ServiceAccountService.get_accessible_models so bucket lookups return a
    predictable list.

    Returns a pair of context-managers; use both together:
        reg_patch, svc_patch = _patch_backend_registry()
        with reg_patch, svc_patch:
            ...
    """
    import app.backends.base as _backends_base_mod  # already stubbed above

    model_ids = model_ids or []

    class FakeRegistry:
        def list_model_ids(self):
            return model_ids

    reg_patch = patch.object(_backends_base_mod, "registry", FakeRegistry())
    svc_patch = patch.object(
        ServiceAccountService,
        "get_accessible_models",
        new=AsyncMock(return_value=model_ids),
    )
    return reg_patch, svc_patch


# ---------------------------------------------------------------------------
# Silence the asyncio.create_task() call inside resolve_key so it does not
# spawn background DB writes that would fail because AsyncSessionLocal points
# to the test engine, which is torn down between tests.
# ---------------------------------------------------------------------------

_TOUCH_PATCH = patch.object(_api_key_mod.asyncio, "create_task", return_value=None)


# ===========================================================================
# TEST 1  generate_key — shape invariants
# ===========================================================================


def test_generate_key_shape():
    """
    generate_key() returns (full_key, prefix, key_hash) where:
      - full_key starts with 'llmr_sk_'
      - prefix == full_key[:16]
      - key_hash == sha256(full_key).hexdigest()
    """
    svc = ApiKeyService()
    full_key, prefix, key_hash = svc.generate_key()

    assert full_key.startswith("llmr_sk_"), (
        f"expected full_key to start with 'llmr_sk_', got {full_key!r}"
    )
    assert prefix == full_key[:16], (
        f"expected prefix={full_key[:16]!r}, got {prefix!r}"
    )
    expected_hash = hashlib.sha256(full_key.encode()).hexdigest()
    assert key_hash == expected_hash, "key_hash must be sha256(full_key)"


def test_generate_key_unique():
    """Two consecutive generate_key() calls must return different keys."""
    svc = ApiKeyService()
    k1, _, _ = svc.generate_key()
    k2, _, _ = svc.generate_key()
    assert k1 != k2, "generate_key must produce unique keys"


def test_key_prefix_exposed_as_class_attribute():
    """
    KEY_PREFIX must be accessible on both the class and an instance.
    The auth resolver accesses ``api_key_service.KEY_PREFIX`` on every request.
    """
    assert ApiKeyService.KEY_PREFIX == "llmr_sk_"
    assert ApiKeyService().KEY_PREFIX == "llmr_sk_"
    import app.services.api_key as _ak
    assert _ak.api_key_service.KEY_PREFIX == "llmr_sk_"


# ===========================================================================
# TEST 2  create_key — hash stored, plaintext never stored, per-key fields
# ===========================================================================


def test_create_key_stores_hash_not_plaintext():
    """
    create_key() must persist a row whose key_hash == sha256(returned_plaintext)
    and must NOT store the plaintext anywhere in the ApiKey row.
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-account-a")
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            svc = ApiKeyService()
            row, plaintext = await svc.create_key(
                db, service_id=sid, name="ci-prod", created_by=1
            )
            await db.commit()
            stored_hash = row.key_hash
            stored_prefix = row.prefix

        expected_hash = hashlib.sha256(plaintext.encode()).hexdigest()
        assert stored_hash == expected_hash, "key_hash in DB must be sha256(plaintext)"
        assert plaintext != stored_hash, "plaintext and hash must differ"
        assert plaintext != stored_prefix, "plaintext must not be stored as prefix"
        assert plaintext.startswith("llmr_sk_"), "plaintext must start with KEY_PREFIX"

    asyncio.run(run())


def test_create_key_stores_per_key_fields():
    """
    create_key() must persist scopes, model_ids, and max_concurrent_requests.
    Empty scopes/model_ids must normalize to NULL (not an empty list).
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-perfield")
            await db.commit()
            sid = sa.id

        # --- key with all per-key fields set ---
        async with _AsyncSession() as db:
            svc = ApiKeyService()
            row, _ = await svc.create_key(
                db,
                service_id=sid,
                name="full-key",
                scopes=["chat", "embeddings"],
                model_ids=["model-a", "model-b"],
                max_concurrent_requests=3,
                created_by=1,
            )
            await db.commit()
            assert set(row.scopes) == {"chat", "embeddings"}
            assert set(row.model_ids) == {"model-a", "model-b"}
            assert row.max_concurrent_requests == 3

        # --- key with empty scopes/model_ids — must normalize to NULL ---
        async with _AsyncSession() as db:
            svc = ApiKeyService()
            row_empty, _ = await svc.create_key(
                db,
                service_id=sid,
                name="empty-fields",
                scopes=[],
                model_ids=[],
                created_by=1,
            )
            await db.commit()
            assert row_empty.scopes is None, (
                f"empty scopes must be stored as NULL, got {row_empty.scopes!r}"
            )
            assert row_empty.model_ids is None, (
                f"empty model_ids must be stored as NULL, got {row_empty.model_ids!r}"
            )
            assert row_empty.max_concurrent_requests is None

    asyncio.run(run())


def test_create_key_invalid_scope_raises_400():
    """
    create_key() with an invalid scope must raise HTTPException(400).
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-badscope")
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with pytest.raises(HTTPException) as exc_info:
                await svc.create_key(
                    db,
                    service_id=sid,
                    name="bad-key",
                    scopes=["chat", "bogus"],
                    created_by=1,
                )
            assert exc_info.value.status_code == 400, (
                f"expected 400 for invalid scope, got {exc_info.value.status_code}"
            )
            assert "bogus" in str(exc_info.value.detail)

    asyncio.run(run())


# ===========================================================================
# TEST 4  resolve_key — cache MISS happy path
# ===========================================================================


def test_resolve_key_cache_miss_returns_payload_and_calls_redis_set():
    """
    resolve_key() on cache miss:
      - queries the DB
      - returns a payload with sub==str(service_id), auth=='service',
        rate_principal=='svckey:{key_id}', accessible_models, scopes,
        max_concurrent_requests
      - calls redis.set exactly once to prime the cache
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-b", max_concurrent_requests=7)
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            row, plaintext = await _make_api_key(db, sid, name="ci-key")
            await db.commit()
            kid = row.id

        fake_redis = _make_redis_miss()
        reg_patch, svc_patch = _patch_backend_registry(["model-x"])

        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with reg_patch, svc_patch, _TOUCH_PATCH:
                payload = await svc.resolve_key(db, fake_redis, plaintext)

        assert payload is not None, "resolve_key should return a payload on valid key"
        assert payload["sub"] == str(sid)
        assert payload["auth"] == "service"
        assert payload["key_id"] == kid
        assert payload["rate_principal"] == f"svckey:{kid}"
        assert "accessible_models" in payload
        assert "scopes" in payload
        assert "max_concurrent_requests" in payload

        fake_redis.set.assert_awaited_once()

    asyncio.run(run())


# ===========================================================================
# TEST 5  resolve_key — cache HIT is DB-free
# ===========================================================================


def test_resolve_key_cache_hit_does_not_touch_db():
    """
    resolve_key() on cache hit must return the cached payload without
    executing any DB query.  We verify by passing a DB session whose
    execute() method raises if called.
    """
    cached_payload = {
        "sub": "42",
        "username": "svc-cached",
        "is_admin": False,
        "max_concurrent_requests": 5,
        "accessible_models": [],
        "auth": "service",
        "key_id": 7,
        "service_id": 42,
        "scopes": [],
        "rate_principal": "svckey:7",
    }

    import secrets
    fake_key = "llmr_sk_" + secrets.token_urlsafe(32)

    fake_redis = _make_redis_hit(cached_payload)

    bad_db = AsyncMock()
    bad_db.execute = AsyncMock(side_effect=AssertionError("DB must not be queried on cache hit"))

    async def run():
        svc = ApiKeyService()
        result = await svc.resolve_key(bad_db, fake_redis, fake_key)
        assert result == cached_payload, (
            f"cache hit should return cached payload, got {result!r}"
        )
        bad_db.execute.assert_not_awaited()

    asyncio.run(run())


# ===========================================================================
# TEST 6  resolve_key — negative cases
# ===========================================================================


def test_resolve_key_unknown_key_returns_none():
    """resolve_key returns None for a key that has no row in the DB."""

    async def run():
        fake_redis = _make_redis_miss()
        import secrets
        unknown = "llmr_sk_" + secrets.token_urlsafe(32)

        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with _TOUCH_PATCH:
                result = await svc.resolve_key(db, fake_redis, unknown)
        assert result is None

    asyncio.run(run())


def test_resolve_key_revoked_key_returns_none():
    """resolve_key returns None for a key whose revoked_at is set."""

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-revoked")
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            row, plaintext = await _make_api_key(
                db, sid, name="revoked-key",
                revoked_at=datetime.datetime.utcnow() - datetime.timedelta(seconds=1),
            )
            await db.commit()

        fake_redis = _make_redis_miss()
        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with _TOUCH_PATCH:
                result = await svc.resolve_key(db, fake_redis, plaintext)
        assert result is None, "revoked key must resolve to None"

    asyncio.run(run())


def test_resolve_key_expired_key_returns_none():
    """resolve_key returns None for a key whose expires_at is in the past."""

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-expired")
            await db.commit()
            sid = sa.id

        past = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        async with _AsyncSession() as db:
            svc_key = ApiKeyService()
            row, plaintext = await svc_key.create_key(
                db, service_id=sid, name="expired-key", expires_at=past, created_by=1
            )
            await db.commit()

        fake_redis = _make_redis_miss()
        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with _TOUCH_PATCH:
                result = await svc.resolve_key(db, fake_redis, plaintext)
        assert result is None, "expired key must resolve to None"

    asyncio.run(run())


def test_resolve_key_inactive_service_account_returns_none():
    """resolve_key returns None when the owning service account is inactive."""

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-inactive", is_active=False)
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            row, plaintext = await _make_api_key(db, sid, name="inactive-sa-key")
            await db.commit()

        fake_redis = _make_redis_miss()
        reg_patch, svc_patch = _patch_backend_registry()
        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with reg_patch, svc_patch, _TOUCH_PATCH:
                result = await svc.resolve_key(db, fake_redis, plaintext)
        assert result is None, "key for inactive service account must resolve to None"

    asyncio.run(run())


def test_resolve_key_oversized_key_returns_none_without_db_work():
    """
    resolve_key returns None immediately for keys longer than 128 chars
    without hashing or hitting the DB or Redis.
    """
    oversized = "x" * 200

    bad_db = AsyncMock()
    bad_db.execute = AsyncMock(
        side_effect=AssertionError("DB must not be queried for oversized key")
    )
    fake_redis = AsyncMock()
    fake_redis.get = AsyncMock(
        side_effect=AssertionError("Redis must not be queried for oversized key")
    )

    async def run():
        svc = ApiKeyService()
        result = await svc.resolve_key(bad_db, fake_redis, oversized)
        assert result is None, "oversized key must be rejected without any I/O"
        bad_db.execute.assert_not_awaited()
        fake_redis.get.assert_not_awaited()

    asyncio.run(run())


# ===========================================================================
# TEST 7  resolve_key — model-subset intersection
# ===========================================================================


def test_resolve_key_model_subset_intersection():
    """
    When key.model_ids is set, resolved accessible_models is the intersection
    of account models and key.model_ids (not a superset or subset alone).

    Account has access to {A, B, C}.
    Key restricts to ["A", "Z"].
    Expected: accessible_models == {"A"}  (Z dropped because account lacks it;
    B,C dropped because key restricts).
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-subset")
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            row, plaintext = await _make_api_key(
                db, sid, name="subset-key", model_ids=["A", "Z"]
            )
            await db.commit()
            kid = row.id

        fake_redis = _make_redis_miss()

        import app.backends.base as _backends_base_mod

        class FakeRegistry:
            def list_model_ids(self):
                return ["A", "B", "C", "Z"]

        # Service account has access to A, B, C
        reg_patch = patch.object(_backends_base_mod, "registry", FakeRegistry())
        svc_patch = patch.object(
            ServiceAccountService,
            "get_accessible_models",
            new=AsyncMock(return_value=["A", "B", "C"]),
        )

        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with reg_patch, svc_patch, _TOUCH_PATCH:
                payload = await svc.resolve_key(db, fake_redis, plaintext)

        assert payload is not None
        assert set(payload["accessible_models"]) == {"A"}, (
            f"expected {{'A'}}, got {set(payload['accessible_models'])}"
        )

    asyncio.run(run())


def test_resolve_key_no_model_restriction_returns_all_account_models():
    """
    When key.model_ids is None, resolved accessible_models == all account models.
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-all-models")
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            row, plaintext = await _make_api_key(
                db, sid, name="no-restriction", model_ids=None
            )
            await db.commit()

        fake_redis = _make_redis_miss()
        reg_patch, svc_patch = _patch_backend_registry(["model-x", "model-y"])
        # Override get_accessible_models to return specific list
        svc_patch2 = patch.object(
            ServiceAccountService,
            "get_accessible_models",
            new=AsyncMock(return_value=["model-x", "model-y"]),
        )

        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with reg_patch, svc_patch2, _TOUCH_PATCH:
                payload = await svc.resolve_key(db, fake_redis, plaintext)

        assert payload is not None
        assert set(payload["accessible_models"]) == {"model-x", "model-y"}, (
            f"expected all account models, got {payload['accessible_models']!r}"
        )

    asyncio.run(run())


# ===========================================================================
# TEST 8  resolve_key — effective concurrency override
# ===========================================================================


def test_resolve_key_key_max_concurrent_overrides_account_default():
    """
    When key.max_concurrent_requests=3 and account.max_concurrent_requests=10,
    the resolved payload must have max_concurrent_requests==3.
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-conc", max_concurrent_requests=10)
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            row, plaintext = await _make_api_key(
                db, sid, name="conc-key", max_concurrent_requests=3
            )
            await db.commit()

        fake_redis = _make_redis_miss()
        reg_patch, svc_patch = _patch_backend_registry()

        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with reg_patch, svc_patch, _TOUCH_PATCH:
                payload = await svc.resolve_key(db, fake_redis, plaintext)

        assert payload is not None
        assert payload["max_concurrent_requests"] == 3, (
            f"key override should be 3, got {payload['max_concurrent_requests']}"
        )

    asyncio.run(run())


def test_resolve_key_no_key_override_uses_account_default():
    """
    When key.max_concurrent_requests is None, resolved max_concurrent_requests
    falls back to account.max_concurrent_requests.
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-fallback", max_concurrent_requests=10)
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            row, plaintext = await _make_api_key(
                db, sid, name="fallback-key", max_concurrent_requests=None
            )
            await db.commit()

        fake_redis = _make_redis_miss()
        reg_patch, svc_patch = _patch_backend_registry()

        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with reg_patch, svc_patch, _TOUCH_PATCH:
                payload = await svc.resolve_key(db, fake_redis, plaintext)

        assert payload is not None
        assert payload["max_concurrent_requests"] == 10, (
            f"account default should be 10, got {payload['max_concurrent_requests']}"
        )

    asyncio.run(run())


# ===========================================================================
# TEST 9  revoke_key — ownership-scoped
# ===========================================================================


def test_revoke_key_cross_owner_raises_404_and_key_remains_valid():
    """
    Regression: revoke_key(key_id=A_key, service_id=B) where B != A must raise
    HTTPException(404) and must NOT revoke A's key.

    This is the critical IDOR fix — service B cannot revoke service A's key.
    """

    async def run():
        async with _AsyncSession() as db:
            sa_a = await _make_service_account(db, "svc-owner-a")
            sa_b = await _make_service_account(db, "svc-owner-b")
            await db.commit()
            sid_a = sa_a.id
            sid_b = sa_b.id

        async with _AsyncSession() as db:
            row_a, plaintext_a = await _make_api_key(db, sid_a, name="key-a")
            await db.commit()
            kid_a = row_a.id

        fake_redis = _make_redis_miss()

        # Attempt to revoke A's key as B — must raise 404
        raised = False
        async with _AsyncSession() as db:
            svc = ApiKeyService()
            try:
                await svc.revoke_key(
                    db, fake_redis, key_id=kid_a, service_id=sid_b
                )
            except HTTPException as exc:
                raised = True
                assert exc.status_code == 404, (
                    f"cross-owner revoke must raise 404, got {exc.status_code}"
                )
            await db.rollback()

        assert raised, "HTTPException(404) must be raised for cross-owner revoke"

        # A's key must still resolve successfully
        reg_patch, svc_patch = _patch_backend_registry()
        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with reg_patch, svc_patch, _TOUCH_PATCH:
                payload = await svc.resolve_key(db, _make_redis_miss(), plaintext_a)

        assert payload is not None, (
            "A's key must still be valid after failed cross-owner revoke"
        )
        assert payload["key_id"] == kid_a

        # Now revoke A's key as A — must succeed
        async with _AsyncSession() as db:
            svc = ApiKeyService()
            await svc.revoke_key(
                db, fake_redis, key_id=kid_a, service_id=sid_a
            )
            await db.commit()

        # A's key must now resolve to None
        async with _AsyncSession() as db:
            svc = ApiKeyService()
            with _TOUCH_PATCH:
                payload_after = await svc.resolve_key(
                    db, _make_redis_miss(), plaintext_a
                )
        assert payload_after is None, "A's key must be None after proper revocation"

    asyncio.run(run())


# ===========================================================================
# TEST 10  evict_service_cache
# ===========================================================================


def test_evict_service_cache_deletes_all_non_revoked_key_cache_entries():
    """
    evict_service_cache() must call redis.delete with the cache keys for all
    non-revoked keys belonging to the service account, and only those.
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-evict")
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            svc_key = ApiKeyService()
            row1, _ = await svc_key.create_key(
                db, service_id=sid, name="active-key-1", created_by=1
            )
            row2, _ = await svc_key.create_key(
                db, service_id=sid, name="active-key-2", created_by=1
            )
            # Third key is already revoked — must NOT be in the delete call
            row3, _ = await svc_key.create_key(
                db, service_id=sid, name="revoked-key", created_by=1
            )
            row3.revoked_at = datetime.datetime.utcnow()
            await db.flush()
            await db.commit()
            hash1 = row1.key_hash
            hash2 = row2.key_hash

        fake_redis = _make_redis_miss()
        async with _AsyncSession() as db:
            svc = ApiKeyService()
            await svc.evict_service_cache(db, fake_redis, sid)

        fake_redis.delete.assert_awaited_once()
        delete_args = fake_redis.delete.call_args[0]
        expected_cache_keys = {f"apikey:{hash1}", f"apikey:{hash2}"}
        assert set(delete_args) == expected_cache_keys, (
            f"expected delete args {expected_cache_keys!r}, got {set(delete_args)!r}"
        )

    asyncio.run(run())


# ===========================================================================
# TEST 11  ServiceAccountService
# ===========================================================================


def test_create_service_account_with_bucket_grants():
    """
    create_service_account() with bucket_ids grants access to those buckets.
    get_accessible_models() reflects the granted buckets.
    """

    async def run():
        async with _AsyncSession() as db:
            admin = await _make_user(db, "admin-actor", is_admin=True)
            bucket = await _make_bucket(db, "ml-models")
            await _make_bucket_model(db, bucket.id, "model-llama")
            await _make_bucket_model(db, bucket.id, "model-mistral")
            await db.commit()
            admin_id = admin.id
            bucket_id = bucket.id

        fake_redis = AsyncMock()
        fake_redis.get = AsyncMock(return_value=None)
        fake_redis.set = AsyncMock(return_value=True)
        fake_redis.delete = AsyncMock(return_value=1)

        with _patch_redis_pool(fake_redis):
            async with _AsyncSession() as db:
                svc_svc = ServiceAccountService()
                account = await svc_svc.create_service_account(
                    db,
                    name="ci-runner",
                    by_user_id=admin_id,
                    bucket_ids=[bucket_id],
                )
                await db.commit()
                account_id = account.id

        # Verify accessible models reflect the granted bucket
        with _patch_redis_pool(fake_redis):
            async with _AsyncSession() as db:
                svc_svc = ServiceAccountService()
                models = await svc_svc.get_accessible_models(
                    db, account_id, available_model_ids={"model-llama", "model-mistral", "model-other"}
                )

        assert "model-llama" in models
        assert "model-mistral" in models
        assert "model-other" not in models

    asyncio.run(run())


def test_set_active_and_is_active():
    """
    set_active(False) must make is_active return False; set_active(True) reverts it.
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-toggle")
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            svc_svc = ServiceAccountService()
            assert await svc_svc.is_active(db, sid) is True

        async with _AsyncSession() as db:
            svc_svc = ServiceAccountService()
            await svc_svc.set_active(db, sid, is_active=False, by_user_id=1)
            await db.commit()

        async with _AsyncSession() as db:
            svc_svc = ServiceAccountService()
            assert await svc_svc.is_active(db, sid) is False

        async with _AsyncSession() as db:
            svc_svc = ServiceAccountService()
            await svc_svc.set_active(db, sid, is_active=True, by_user_id=1)
            await db.commit()

        async with _AsyncSession() as db:
            svc_svc = ServiceAccountService()
            assert await svc_svc.is_active(db, sid) is True

    asyncio.run(run())


def test_grant_bucket_access_is_idempotent():
    """grant_bucket_access called twice must not raise and must not create duplicates."""

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-idempotent")
            bucket = await _make_bucket(db, "bucket-x")
            await db.commit()
            sid = sa.id
            bid = bucket.id

        async with _AsyncSession() as db:
            svc_svc = ServiceAccountService()
            await svc_svc.grant_bucket_access(db, service_id=sid, bucket_id=bid, by_user_id=1)
            await db.commit()

        # Second call must succeed without raising
        async with _AsyncSession() as db:
            svc_svc = ServiceAccountService()
            await svc_svc.grant_bucket_access(db, service_id=sid, bucket_id=bid, by_user_id=1)
            await db.commit()

        # Check only one row exists
        from sqlalchemy import select as sa_select
        async with _AsyncSession() as db:
            result = await db.execute(
                sa_select(ServiceBucketAccess).where(
                    ServiceBucketAccess.service_id == sid,
                    ServiceBucketAccess.bucket_id == bid,
                )
            )
            rows = result.scalars().all()
        assert len(rows) == 1, f"Expected 1 grant row, got {len(rows)}"

    asyncio.run(run())


def test_revoke_bucket_access_is_idempotent():
    """revoke_bucket_access when not granted must be a no-op (no raise)."""

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "svc-revoke-idem")
            bucket = await _make_bucket(db, "bucket-z")
            await db.commit()
            sid = sa.id
            bid = bucket.id

        # Revoke without prior grant — should not raise
        async with _AsyncSession() as db:
            svc_svc = ServiceAccountService()
            await svc_svc.revoke_bucket_access(db, service_id=sid, bucket_id=bid, by_user_id=1)
            await db.commit()

    asyncio.run(run())


def test_list_service_accounts_returns_only_service_accounts():
    """
    list_service_accounts() must return only ServiceAccount rows, not User rows.
    """

    async def run():
        async with _AsyncSession() as db:
            # Create some users (should not appear in list_service_accounts)
            await _make_user(db, "human-charlie")
            # Create service accounts
            await _make_service_account(db, "svc-deploy")
            await _make_service_account(db, "svc-monitor")
            await db.commit()

        async with _AsyncSession() as db:
            svc_svc = ServiceAccountService()
            accounts = await svc_svc.list_service_accounts(db)

        assert len(accounts) == 2, f"expected 2 service accounts, got {len(accounts)}"
        names = {a.name for a in accounts}
        assert names == {"svc-deploy", "svc-monitor"}
        assert "human-charlie" not in names
        for acct in accounts:
            assert isinstance(acct, ServiceAccount)

    asyncio.run(run())


# ===========================================================================
# TEST 12  CRITICAL-2: get_current_user rejects service payload with 403
# ===========================================================================


def test_get_current_user_rejects_service_payload_with_403():
    """
    CRITICAL regression guard — privilege escalation prevention.

    get_current_user() with a service payload (auth=='service') must raise
    HTTPException(403) BEFORE any users-table query.

    A service API key must NEVER resolve to a User ORM object or pass
    require_admin. Without this guard, a service key holder could access
    admin endpoints.
    """
    from app.api.auth_routes import get_current_user

    service_payload = {
        "sub": "1",
        "username": "svc-account",
        "is_admin": False,
        "max_concurrent_requests": 5,
        "accessible_models": [],
        "auth": "service",
        "service_id": 1,
        "key_id": 1,
        "scopes": [],
        "rate_principal": "svckey:1",
    }

    # DB mock that raises if execute() is called — guards that no DB query happens
    bad_db = AsyncMock()
    bad_db.execute = AsyncMock(
        side_effect=AssertionError(
            "DB must NOT be queried before the auth=='service' guard fires"
        )
    )

    async def run():
        with pytest.raises(HTTPException) as exc_info:
            await get_current_user(payload=service_payload, db=bad_db)

        assert exc_info.value.status_code == 403, (
            f"expected 403 for service payload, got {exc_info.value.status_code}"
        )
        bad_db.execute.assert_not_awaited(), (
            "DB must not be queried — the guard must fire before any users-table access"
        )

    asyncio.run(run())


# ===========================================================================
# TEST 13  refresh_token guard — service payload must be rejected with 403
# ===========================================================================


def test_refresh_token_rejects_service_payload_with_403():
    """
    refresh_token called with a payload where auth=='service' must raise 403.
    """
    from app.api.auth_routes import refresh_token

    service_payload = {
        "sub": "1",
        "username": "svc-account",
        "is_admin": False,
        "max_concurrent_requests": 5,
        "accessible_models": [],
        "auth": "service",
        "key_id": 1,
    }

    async def run():
        async with _AsyncSession() as db:
            with pytest.raises(HTTPException) as exc:
                await refresh_token(payload=service_payload, db=db)
            assert exc.value.status_code == 403, (
                f"expected 403 for service payload refresh, got {exc.value.status_code}"
            )

    asyncio.run(run())


# ===========================================================================
# TEST 14  _assert_scope
# ===========================================================================


def test_assert_scope_service_key_missing_scope_raises_403():
    """
    _assert_scope with a service payload that has scopes=["chat"] and
    required="embeddings" must raise HTTPException(403).
    """
    from app.api.proxy import _assert_scope

    payload = {"auth": "service", "scopes": ["chat"]}
    with pytest.raises(HTTPException) as exc_info:
        _assert_scope(payload, "embeddings")
    assert exc_info.value.status_code == 403


def test_assert_scope_service_key_empty_scopes_allows_all():
    """
    _assert_scope with scopes=[] (or missing scopes) must NOT raise — empty
    scopes means all operations allowed.
    """
    from app.api.proxy import _assert_scope

    # Empty list
    _assert_scope({"auth": "service", "scopes": []}, "embeddings")
    # Missing scopes key
    _assert_scope({"auth": "service"}, "chat")
    # None scopes
    _assert_scope({"auth": "service", "scopes": None}, "models")


def test_assert_scope_non_service_payload_is_noop():
    """
    _assert_scope for non-service payloads (JWT users) must never raise,
    regardless of the required scope.
    """
    from app.api.proxy import _assert_scope

    # No auth key
    _assert_scope({}, "chat")
    _assert_scope({"sub": "1", "username": "alice"}, "embeddings")
    _assert_scope({"auth": "ldap", "scopes": []}, "models")


def test_assert_scope_service_key_with_required_scope_passes():
    """
    _assert_scope must NOT raise when the required scope IS in scopes.
    """
    from app.api.proxy import _assert_scope

    _assert_scope({"auth": "service", "scopes": ["chat", "embeddings"]}, "chat")
    _assert_scope({"auth": "service", "scopes": ["models"]}, "models")


# ===========================================================================
# TEST 15  Unified auth resolver
# ===========================================================================


def test_auth_resolver_service_key_path_resolves():
    """
    The unified auth resolver accepts a valid service API key in x-api-key
    and returns the service payload.
    """

    async def run():
        async with _AsyncSession() as db:
            sa = await _make_service_account(db, "resolver-svc")
            await db.commit()
            sid = sa.id

        async with _AsyncSession() as db:
            row, plaintext = await _make_api_key(db, sid, name="resolver-key")
            await db.commit()
            kid = row.id

        from app.api.auth_routes import get_current_user_payload_from_request

        fake_request = MagicMock()
        fake_request.headers = {"x-api-key": plaintext}
        fake_credentials = None

        fake_redis = _make_redis_miss()
        reg_patch, svc_patch = _patch_backend_registry()

        async with _AsyncSession() as db:
            with reg_patch, svc_patch, _TOUCH_PATCH:
                payload = await get_current_user_payload_from_request(
                    fake_request, fake_credentials, db, fake_redis
                )

        assert payload is not None
        assert payload["auth"] == "service"
        assert payload["key_id"] == kid
        assert payload["sub"] == str(sid)

    asyncio.run(run())


def test_auth_resolver_jwt_path_still_works():
    """
    The unified auth resolver still accepts a valid JWT in the Bearer header.
    """
    from app.auth.jwt_handler import create_access_token
    from app.api.auth_routes import get_current_user_payload_from_request
    from fastapi.security import HTTPAuthorizationCredentials

    token = create_access_token(
        user_id=99,
        username="jwt-user",
        is_admin=False,
        max_concurrent_requests=10,
        accessible_models=["model-a"],
    )

    fake_request = MagicMock()
    fake_request.headers = {}

    fake_creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
    fake_redis = _make_redis_miss()

    async def run():
        async with _AsyncSession() as db:
            payload = await get_current_user_payload_from_request(
                fake_request, fake_creds, db, fake_redis
            )
        assert payload["sub"] == "99"
        assert payload["username"] == "jwt-user"
        assert payload.get("auth") != "service"

    asyncio.run(run())


def test_auth_resolver_invalid_service_key_returns_401():
    """
    A key that looks like a service key (starts with llmr_sk_) but doesn't
    exist in the DB must cause the resolver to raise 401.
    """
    import secrets
    from app.api.auth_routes import get_current_user_payload_from_request

    nonexistent_key = "llmr_sk_" + secrets.token_urlsafe(32)

    fake_request = MagicMock()
    fake_request.headers = {"x-api-key": nonexistent_key}
    fake_credentials = None
    fake_redis = _make_redis_miss()

    async def run():
        async with _AsyncSession() as db:
            with pytest.raises(HTTPException) as exc:
                await get_current_user_payload_from_request(
                    fake_request, fake_credentials, db, fake_redis
                )
            assert exc.value.status_code == 401

    asyncio.run(run())


# ===========================================================================
# TEST 16  CRITICAL-1: backend resolved BEFORE rate-limit increment
# ===========================================================================


def test_backend_resolved_before_rate_limit_increment():
    """
    CRITICAL regression — concurrency counter leak prevention.

    chat_completions must call model_router.resolve() BEFORE
    rate_limit_service.assert_within_limit(). If resolve() raises (503 on
    no backend), assert_within_limit must never be called, ensuring the
    concurrency counter is never incremented and thus can never leak.

    Strategy: patch model_router.resolve to raise HTTPException(503) and
    patch rate_limit_service.assert_within_limit with a MagicMock that records
    calls. Then call the endpoint function directly with a minimal service
    payload. Expect the 503 to propagate; assert assert_within_limit was NOT
    called.

    If full endpoint wiring is impractical in the test harness, we fall back
    to assertion-by-inspection: verify the ordering in proxy.py source.
    """
    import inspect
    import app.api.proxy as _proxy_mod

    # --- Assertion-by-inspection: verify source ordering ---
    source = inspect.getsource(_proxy_mod.chat_completions)
    resolve_pos = source.find("model_router.resolve(")
    rate_limit_pos = source.find("rate_limit_service.assert_within_limit(")

    assert resolve_pos != -1, (
        "model_router.resolve() call not found in chat_completions source"
    )
    assert rate_limit_pos != -1, (
        "rate_limit_service.assert_within_limit() call not found in chat_completions source"
    )
    assert resolve_pos < rate_limit_pos, (
        f"CRITICAL-1 invariant violated: model_router.resolve() (pos={resolve_pos}) "
        f"must appear BEFORE rate_limit_service.assert_within_limit() "
        f"(pos={rate_limit_pos}) in chat_completions."
    )

    # --- Runtime verification: patch resolve to raise 503, confirm no increment ---
    from unittest.mock import patch as _patch, MagicMock as _MM, AsyncMock as _AM

    # Build a minimal valid payload and request
    service_payload = {
        "sub": "1",
        "username": "svc-test",
        "is_admin": False,
        "auth": "service",
        "service_id": 1,
        "key_id": 1,
        "accessible_models": ["test-model"],
        "scopes": [],
        "max_concurrent_requests": 5,
        "rate_principal": "svckey:1",
    }

    fake_request = _MM()
    fake_request.headers = {}

    async def _fake_json():
        return {"model": "test-model", "messages": [{"role": "user", "content": "hi"}]}

    fake_request.json = _fake_json

    fake_redis = _make_redis_miss()

    mock_limit = _AM()  # tracks calls to assert_within_limit

    async def run():
        async with _AsyncSession() as db:
            # Patch resolve to raise 503.
            # rate_limit_service in the proxy module is an AsyncMock stub (from
            # the bootstrap stub), so we use create=True to add assert_within_limit.
            with (
                _patch.object(_proxy_mod.model_router, "resolve",
                               side_effect=HTTPException(status_code=503, detail="no backend")),
                _patch.object(_proxy_mod.rate_limit_service, "assert_within_limit",
                               mock_limit, create=True),
                # Patch _resolve_user to return quickly without hitting Redis
                _patch.object(_proxy_mod, "_resolve_user",
                               new=_AM(return_value=("svc-test", "svckey:1", 5))),
                # Patch content_filter to pass
                _patch.object(_proxy_mod.content_filter_service, "check_request_body",
                               new=_AM(return_value=None)),
                # Patch _assert_scope to no-op
                _patch.object(_proxy_mod, "_assert_scope", return_value=None),
            ):
                with pytest.raises(HTTPException) as exc_info:
                    await _proxy_mod.chat_completions(
                        request=fake_request,
                        payload=service_payload,
                        db=db,
                        redis=fake_redis,
                    )
                assert exc_info.value.status_code == 503

        mock_limit.assert_not_awaited(), (
            "assert_within_limit must NOT be called when model_router.resolve raises — "
            "no counter increment means no counter leak"
        )

    asyncio.run(run())

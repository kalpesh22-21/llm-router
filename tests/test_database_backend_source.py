"""
Unit tests for app.services.database_backend_source.

Covers reconcile() logic using a fake in-memory registry and a stubbed DB
session, plus publish_models_changed() Redis-failure swallowing.

Bootstrap note:
  This file follows the same stub-injection pattern as test_model_backend_crypto.py
  and test_service_api_keys.py.  Several production modules use Python 3.10+
  ``X | Y`` union syntax (app.core.httpx, app.db.mysql ORM relationships,
  app/services/__init__.py cluster_manager chain).  We register stubs for those
  before any app.* import.  The new module under test (database_backend_source)
  uses only Optional[...] and Dict[...] from typing, so it runs on Python 3.9+.

  conftest.py already sets VAULT_ENABLED=false / JWT_SECRET_KEY / API_KEY /
  PHOENIX_TRACING_ENABLED via os.environ.setdefault before this file is imported.
"""

from __future__ import annotations

import asyncio
import pathlib
import sys
import types
from typing import Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Bootstrap — must happen BEFORE any app.* import.
# ---------------------------------------------------------------------------

_ROOT = pathlib.Path(__file__).parents[1]


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


def _bootstrap() -> None:
    # app.core.httpx — uses `X | None` syntax on Python 3.9
    _stub_if_absent("app.core.httpx", {
        "new_http_client": lambda: None,
        "close_http_client": lambda: None,
    })

    # app.utils.api_key_validator — makes HTTP calls at import time
    async def _noop(*a, **kw):
        return ""

    _stub_if_absent("app.utils.api_key_validator", {
        "find_working_api_key": _noop,
    })

    # app.auth.ldap_auth — may not have LDAP available
    _stub_if_absent("app.auth.ldap_auth", {
        "authenticate_ldap": lambda u, p: False,
        "get_ldap_user_info": lambda u, p: {},
    })

    # app/services/__init__.py eagerly imports cluster_manager which pulls in
    # psycopg, Redis, and many other things.  Expose a package stub so that
    # sub-module imports (app.services.database_backend_source) still resolve
    # to the real files while skipping __init__.py entirely.
    if "app.services" not in sys.modules:
        stub_svc = _make_pkg_stub("app", "services")
        sys.modules["app.services"] = stub_svc

    # app.db.redis_client — database_backend_source imports get_redis_pool.
    # We inject a controllable stub here; individual tests can replace it via
    # patch() or monkeypatch.
    if "app.db" not in sys.modules:
        stub_db_pkg = _make_pkg_stub("app", "db")
        sys.modules["app.db"] = stub_db_pkg

    _stub_if_absent("app.db.redis_client", {
        "get_redis_pool": MagicMock(),
        "get_redis": MagicMock(),
    })

    # app.db.mysql — database_backend_source imports AsyncSessionLocal and
    # ModelBackend.  We stub out the session factory; tests inject a fake
    # session directly via patch.
    if "app.db.mysql" not in sys.modules:
        stub_mysql = types.ModuleType("app.db.mysql")
        stub_mysql.AsyncSessionLocal = MagicMock()  # type: ignore[attr-defined]

        # Minimal ModelBackend stand-in — tests build real instances of _FakeRow
        # rather than using this class directly.
        stub_mysql.ModelBackend = MagicMock()  # type: ignore[attr-defined]
        sys.modules["app.db.mysql"] = stub_mysql

    # app.backends package
    if "app.backends" not in sys.modules:
        sys.modules["app.backends"] = _make_pkg_stub("app", "backends")


_bootstrap()

# ---------------------------------------------------------------------------
# Safe to import app.* now.
# ---------------------------------------------------------------------------

import app.services.database_backend_source as _dbs_mod  # noqa: E402
from app.services.database_backend_source import (  # noqa: E402
    DatabaseBackendSource,
    _build_backend,
    publish_models_changed,
)


# ---------------------------------------------------------------------------
# Helpers / fakes
# ---------------------------------------------------------------------------


class _FakeRow:
    """Lightweight stand-in for a ModelBackend ORM row."""

    def __init__(
        self,
        model_id: str,
        base_url: str = "http://fake-server/v1",
        api_key_enc: Optional[str] = None,
        is_enabled: bool = True,
        version: int = 1,
        backend_type: str = "static",
    ) -> None:
        self.model_id = model_id
        self.base_url = base_url
        self.api_key_enc = api_key_enc
        self.is_enabled = is_enabled
        self.version = version
        self.backend_type = backend_type


class _FakeRegistry:
    """In-memory drop-in for BackendRegistry."""

    def __init__(self) -> None:
        self._backends: Dict[str, object] = {}

    def contains(self, model_id: str) -> bool:
        return model_id in self._backends

    def upsert(self, backend) -> None:
        self._backends[backend.model_id] = backend

    def unregister(self, model_id: str) -> Optional[object]:
        return self._backends.pop(model_id, None)

    def list_model_ids(self) -> List[str]:
        return list(self._backends.keys())

    # Helpers for test assertions
    def __contains__(self, model_id: str) -> bool:
        return model_id in self._backends

    def get(self, model_id: str) -> Optional[object]:
        return self._backends.get(model_id)

    def _register_external(self, model_id: str, backend: object) -> None:
        """Simulate a higher-priority source (YAML/k8s) owning a model_id."""
        self._backends[model_id] = backend


def _run(coro):
    """Drive a coroutine to completion synchronously."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Fixture: fresh service + patched registry for each test
# ---------------------------------------------------------------------------


@pytest.fixture()
def fake_registry():
    return _FakeRegistry()


@pytest.fixture()
def svc(fake_registry):
    """A fresh DatabaseBackendSource with its internal _managed dict cleared."""
    s = DatabaseBackendSource()
    return s


# ---------------------------------------------------------------------------
# Helper to run reconcile with a given set of rows and a given registry
# ---------------------------------------------------------------------------


def _reconcile(svc: DatabaseBackendSource, fake_reg: _FakeRegistry, rows: List[_FakeRow]):
    """Patch _fetch_rows + registry, then call reconcile()."""
    async def _fake_fetch():
        return rows

    with (
        patch.object(svc, "_fetch_rows", side_effect=_fake_fetch),
        patch.object(_dbs_mod, "registry", fake_reg),
        patch.object(_dbs_mod, "decrypt_api_key", side_effect=lambda t: f"plain:{t}"),
    ):
        _run(svc.reconcile())


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


class TestReconcileNewEnabledRow:
    """Case 1: New enabled row not owned by anyone → registered + tracked."""

    def test_registered_and_tracked(self, svc, fake_registry):
        row = _FakeRow(model_id="model-a", api_key_enc="tok1", is_enabled=True, version=1)
        _reconcile(svc, fake_registry, [row])

        assert "model-a" in fake_registry
        assert svc._managed["model-a"] == 1

    def test_backend_has_correct_model_id(self, svc, fake_registry):
        row = _FakeRow(model_id="model-a", api_key_enc="tok1", is_enabled=True, version=1)
        _reconcile(svc, fake_registry, [row])

        backend = fake_registry.get("model-a")
        assert backend is not None
        assert backend.model_id == "model-a"

    def test_backend_has_correct_base_url(self, svc, fake_registry):
        row = _FakeRow(model_id="model-a", base_url="http://llm-server/v1",
                       api_key_enc="tok1", is_enabled=True, version=1)
        _reconcile(svc, fake_registry, [row])

        backend = fake_registry.get("model-a")
        # StaticBackend strips trailing slash
        assert backend.base_url == "http://llm-server/v1"


class TestReconcileHigherPriorityOwner:
    """Case 2: model_id already in registry (owned by YAML/k8s) → skipped."""

    def test_not_overwritten(self, svc, fake_registry):
        sentinel = object()
        fake_registry._backends["model-b"] = sentinel

        row = _FakeRow(model_id="model-b", api_key_enc="tok1", is_enabled=True, version=1)
        _reconcile(svc, fake_registry, [row])

        # Original sentinel must still be there
        assert fake_registry._backends["model-b"] is sentinel

    def test_not_added_to_managed(self, svc, fake_registry):
        fake_registry._backends["model-b"] = object()

        row = _FakeRow(model_id="model-b", api_key_enc="tok1", is_enabled=True, version=1)
        _reconcile(svc, fake_registry, [row])

        assert "model-b" not in svc._managed


class TestReconcileVersionBump:
    """Case 3: Managed row with bumped version → backend rebuilt and upserted."""

    def test_backend_upserted_on_version_change(self, svc, fake_registry):
        # Simulate the service already managing model-c at version 1
        svc._managed["model-c"] = 1

        row_v1 = _FakeRow(model_id="model-c", base_url="http://old/v1",
                          api_key_enc="tok1", is_enabled=True, version=1)
        _reconcile(svc, fake_registry, [row_v1])

        # Now version bumps to 2 with a new base_url
        row_v2 = _FakeRow(model_id="model-c", base_url="http://new/v1",
                          api_key_enc="tok1", is_enabled=True, version=2)
        _reconcile(svc, fake_registry, [row_v2])

        assert svc._managed["model-c"] == 2
        backend = fake_registry.get("model-c")
        assert backend is not None
        assert backend.base_url == "http://new/v1"

    def test_same_version_not_upserted(self, svc, fake_registry):
        svc._managed["model-c"] = 1

        row_v1 = _FakeRow(model_id="model-c", base_url="http://old/v1",
                          api_key_enc="tok1", is_enabled=True, version=1)
        # Seed the registry with the first reconcile
        _reconcile(svc, fake_registry, [row_v1])

        # Replace backend in registry with a sentinel to detect whether upsert happens
        sentinel = object()
        fake_registry._backends["model-c"] = sentinel

        # Same version row — should NOT upsert
        _reconcile(svc, fake_registry, [row_v1])

        assert fake_registry._backends["model-c"] is sentinel


class TestReconcileDisableManaged:
    """Case 4: Managed row flipped to is_enabled=False → unregistered."""

    def test_unregistered_when_disabled(self, svc, fake_registry):
        # First, register and track it
        svc._managed["model-d"] = 1
        fake_registry._backends["model-d"] = MagicMock(model_id="model-d")

        row_disabled = _FakeRow(model_id="model-d", api_key_enc="tok1",
                                is_enabled=False, version=2)
        _reconcile(svc, fake_registry, [row_disabled])

        assert "model-d" not in fake_registry
        assert "model-d" not in svc._managed


class TestReconcileRowDeleted:
    """Case 5: Managed row deleted from DB → unregistered."""

    def test_unregistered_when_row_gone(self, svc, fake_registry):
        svc._managed["model-e"] = 1
        fake_registry._backends["model-e"] = MagicMock(model_id="model-e")

        # Reconcile with an empty DB (row deleted)
        _reconcile(svc, fake_registry, [])

        assert "model-e" not in fake_registry
        assert "model-e" not in svc._managed


class TestReconcileBadApiKeyEncryption:
    """Case 6: Bad/undecryptable api_key_enc → that backend skipped, rest complete."""

    def test_bad_key_skipped_others_registered(self, svc, fake_registry):
        bad_row = _FakeRow(model_id="bad-model", api_key_enc="corrupted", is_enabled=True, version=1)
        good_row = _FakeRow(model_id="good-model", api_key_enc="good-tok", is_enabled=True, version=1)

        def _selective_decrypt(token: str) -> str:
            if token == "corrupted":
                raise ValueError("api_key decryption failed")
            return f"plain:{token}"

        async def _fake_fetch():
            return [bad_row, good_row]

        with (
            patch.object(svc, "_fetch_rows", side_effect=_fake_fetch),
            patch.object(_dbs_mod, "registry", fake_registry),
            patch.object(_dbs_mod, "decrypt_api_key", side_effect=_selective_decrypt),
        ):
            # Must NOT raise
            _run(svc.reconcile())

        assert "bad-model" not in fake_registry
        assert "bad-model" not in svc._managed
        assert "good-model" in fake_registry
        assert svc._managed.get("good-model") == 1

    def test_reconcile_still_completes_with_bad_key(self, svc, fake_registry):
        """reconcile() must return normally even if a decryption fails."""
        bad_row = _FakeRow(model_id="bad-only", api_key_enc="bad", is_enabled=True, version=1)

        async def _fake_fetch():
            return [bad_row]

        with (
            patch.object(svc, "_fetch_rows", side_effect=_fake_fetch),
            patch.object(_dbs_mod, "registry", fake_registry),
            patch.object(_dbs_mod, "decrypt_api_key",
                         side_effect=ValueError("api_key decryption failed")),
        ):
            # Should not raise
            _run(svc.reconcile())

        assert len(svc._managed) == 0


class TestPublishModelsChanged:
    """Case 7: publish_models_changed() swallows Redis failure without raising."""

    def test_swallows_redis_failure(self):
        failing_redis = AsyncMock()
        failing_redis.incr = AsyncMock(side_effect=ConnectionError("redis down"))
        failing_redis.publish = AsyncMock()

        with patch.object(_dbs_mod, "get_redis_pool", return_value=failing_redis):
            # Must NOT raise
            _run(publish_models_changed())

    def test_swallows_publish_failure(self):
        partial_redis = AsyncMock()
        partial_redis.incr = AsyncMock(return_value=1)
        partial_redis.publish = AsyncMock(side_effect=ConnectionError("redis down"))

        with patch.object(_dbs_mod, "get_redis_pool", return_value=partial_redis):
            _run(publish_models_changed())

    def test_calls_incr_and_publish_on_success(self):
        ok_redis = AsyncMock()
        ok_redis.incr = AsyncMock(return_value=1)
        ok_redis.publish = AsyncMock(return_value=1)

        with patch.object(_dbs_mod, "get_redis_pool", return_value=ok_redis):
            _run(publish_models_changed())

        ok_redis.incr.assert_awaited_once_with("llmr:models_version")
        ok_redis.publish.assert_awaited_once_with("llmr:models_changed", "1")


class TestBuildBackendApiKeyFallback:
    """Verify _build_backend falls back to API_KEY when api_key_enc is None/empty."""

    def test_none_api_key_enc_uses_settings_api_key(self):
        row = _FakeRow(model_id="no-key-model", api_key_enc=None, is_enabled=True, version=1)
        fake_settings = MagicMock()
        fake_settings.API_KEY = "global-key"

        with patch.object(_dbs_mod, "get_settings", return_value=fake_settings):
            backend = _build_backend(row)

        assert backend is not None
        assert backend.api_key == "global-key"

    def test_empty_sentinel_api_key_yields_empty_string(self):
        row = _FakeRow(model_id="no-key-model", api_key_enc=None, is_enabled=True, version=1)
        fake_settings = MagicMock()
        fake_settings.API_KEY = "EMPTY"

        with patch.object(_dbs_mod, "get_settings", return_value=fake_settings):
            backend = _build_backend(row)

        assert backend is not None
        assert backend.api_key == ""

    def test_decrypt_called_when_api_key_enc_present(self):
        row = _FakeRow(model_id="key-model", api_key_enc="encrypted-tok",
                       is_enabled=True, version=1)
        fake_settings = MagicMock()
        fake_settings.API_KEY = "global-key"

        with (
            patch.object(_dbs_mod, "get_settings", return_value=fake_settings),
            patch.object(_dbs_mod, "decrypt_api_key",
                         return_value="decrypted-key") as mock_decrypt,
        ):
            backend = _build_backend(row)

        mock_decrypt.assert_called_once_with("encrypted-tok")
        assert backend is not None
        assert backend.api_key == "decrypted-key"

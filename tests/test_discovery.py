"""
Unit tests for the DiscoveryService availability logic + the proxy resolve gate.

Pure/mocked — no real network, DB, or Redis. Async cases use asyncio.run() so
only core pytest is required (no pytest-asyncio).

Run inside the app image (deps installed, paycompy stub on PYTHONPATH):
    VAULT_ENABLED=false pytest tests/test_discovery.py -v
"""

import asyncio

import pytest
from fastapi import HTTPException

from app.backends.base import BackendRegistry
from app.backends.static import StaticBackend
from app.routing.router import ModelRouter
import app.routing.router as router_mod
import app.services.discovery_service as disc


def _make_backend() -> StaticBackend:
    return StaticBackend(model_id="m", base_url="http://upstream/v1", api_key="k")


# --------------------------------------------------------------------------- #
# Hysteresis (StaticBackend.record_probe_result)
# --------------------------------------------------------------------------- #


def test_starts_available():
    assert _make_backend().is_available is True


def test_disable_requires_threshold_consecutive_failures():
    b = _make_backend()
    b.record_probe_result(False, failure_threshold=2)
    assert b.is_available is True  # 1 failure < threshold → still up
    b.record_probe_result(False, failure_threshold=2)
    assert b.is_available is False  # 2nd consecutive failure → down


def test_single_success_reenables_and_resets_counter():
    b = _make_backend()
    b.record_probe_result(False, 2)
    b.record_probe_result(False, 2)
    assert b.is_available is False

    b.record_probe_result(True, 2)
    assert b.is_available is True

    # One failure right after recovery must NOT immediately disable (counter reset).
    b.record_probe_result(False, 2)
    assert b.is_available is True


# --------------------------------------------------------------------------- #
# probe_once (network mocked at the discovery_service module boundary)
# --------------------------------------------------------------------------- #


async def _run_probe(validate, find=None) -> StaticBackend:
    b = _make_backend()
    svc = disc.DiscoveryService()
    orig_v, orig_f = disc.validate_api_key, disc.find_working_api_key
    disc.validate_api_key = validate
    if find is not None:
        disc.find_working_api_key = find
    try:
        await svc.probe_once(b, failure_threshold=1)
    finally:
        disc.validate_api_key = orig_v
        disc.find_working_api_key = orig_f
    return b


def test_probe_reachable_marks_available():
    async def ok(*a, **k):
        return True

    assert asyncio.run(_run_probe(ok)).is_available is True


def test_probe_connection_error_marks_down():
    async def boom(*a, **k):
        raise ConnectionError("connection refused")

    assert asyncio.run(_run_probe(boom)).is_available is False


def test_probe_bad_key_reresolves_and_stores_new_key():
    async def bad_key(*a, **k):  # reachable but 401
        return False

    async def find(*a, **k):
        return "good-key"

    b = asyncio.run(_run_probe(bad_key, find))
    assert b.is_available is True
    assert b.api_key == "good-key"


def test_probe_bad_key_no_working_key_marks_down():
    async def bad_key(*a, **k):
        return False

    async def find(*a, **k):
        return ""

    assert asyncio.run(_run_probe(bad_key, find)).is_available is False


# --------------------------------------------------------------------------- #
# ModelRouter.resolve gate: 200 / 503 / 404
# --------------------------------------------------------------------------- #


def _router_with(reg: BackendRegistry):
    """Swap the module-level registry the router resolves against."""
    orig = router_mod._global_registry
    router_mod._global_registry = reg
    return orig


def test_resolve_available_returns_backend():
    reg = BackendRegistry()
    b = _make_backend()
    reg.register(b)
    orig = _router_with(reg)
    try:
        assert ModelRouter().resolve("m") is b
    finally:
        router_mod._global_registry = orig


def test_resolve_unavailable_raises_503():
    reg = BackendRegistry()
    b = _make_backend()
    b.record_probe_result(False, 1)  # mark down
    reg.register(b)
    orig = _router_with(reg)
    try:
        with pytest.raises(HTTPException) as exc:
            ModelRouter().resolve("m")
        assert exc.value.status_code == 503
        assert exc.value.headers.get("Retry-After")
    finally:
        router_mod._global_registry = orig


def test_resolve_unknown_model_raises_404():
    reg = BackendRegistry()
    orig = _router_with(reg)
    try:
        with pytest.raises(HTTPException) as exc:
            ModelRouter().resolve("does-not-exist")
        assert exc.value.status_code == 404
    finally:
        router_mod._global_registry = orig

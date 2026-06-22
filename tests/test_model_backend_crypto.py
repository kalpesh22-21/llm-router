"""
Unit tests for app.services.model_backend_crypto.

Covers:
  - encrypt_api_key / decrypt_api_key round-trip produces the original plaintext.
  - decrypt_api_key raises ValueError (not InvalidToken) on a tampered token.
  - Both functions raise RuntimeError when MODEL_BACKEND_ENCRYPTION_KEY is unset.
  - The module-level Fernet singleton is rebuilt when the env var changes
    (tested by resetting _fernet between test cases).

No real DB, Redis, or network required.

Bootstrap note:
  This file follows the same stub-injection pattern as test_service_api_keys.py.
  app/core/httpx.py uses `X | None` union syntax (Python 3.10+) which causes a
  TypeError on Python 3.9.  We register a stub before any app.* import occurs.
  app/services/__init__.py eagerly imports cluster_manager; we expose a package
  stub so app.services.model_backend_crypto can be imported without triggering
  the full service chain.
  conftest.py already sets VAULT_ENABLED=false before this file is imported.
"""

from __future__ import annotations

import pathlib
import sys
import types

import pytest
from cryptography.fernet import Fernet

# ---------------------------------------------------------------------------
# Bootstrap — must happen before any app.* import.
# conftest.py has already set VAULT_ENABLED / JWT_SECRET_KEY / API_KEY /
# PHOENIX_TRACING_ENABLED via os.environ.setdefault.
# ---------------------------------------------------------------------------

_ROOT = pathlib.Path(__file__).parents[1]


def _bootstrap_stubs() -> None:
    """Register the minimum stubs needed to import app.services.model_backend_crypto."""

    # app.core.httpx uses `X | None` type-union syntax (Python 3.10+).
    # Stub it before anything in app.core.config triggers its import.
    if "app.core.httpx" not in sys.modules:
        stub_httpx = types.ModuleType("app.core.httpx")
        stub_httpx.new_http_client = lambda: None  # type: ignore[attr-defined]
        stub_httpx.close_http_client = lambda: None  # type: ignore[attr-defined]
        sys.modules["app.core.httpx"] = stub_httpx

    # app.utils.api_key_validator imports app.core.httpx and makes HTTP calls.
    if "app.utils.api_key_validator" not in sys.modules:
        stub_validator = types.ModuleType("app.utils.api_key_validator")

        async def _noop(*a, **kw):
            return ""

        stub_validator.find_working_api_key = _noop  # type: ignore[attr-defined]
        sys.modules["app.utils.api_key_validator"] = stub_validator

    # app/services/__init__.py eagerly imports cluster_manager, which pulls in
    # Redis, DB, and many other things.  Register a package stub so that
    # "from app.services.model_backend_crypto import ..." still resolves to the
    # real file while skipping __init__.py entirely.
    if "app.services" not in sys.modules:
        stub_svc_pkg = types.ModuleType("app.services")
        stub_svc_pkg.__path__ = [str(_ROOT / "app" / "services")]  # type: ignore[attr-defined]
        stub_svc_pkg.__package__ = "app.services"  # type: ignore[attr-defined]
        sys.modules["app.services"] = stub_svc_pkg


_bootstrap_stubs()

# Safe to import app.* now.
from app.services.model_backend_crypto import decrypt_api_key, encrypt_api_key  # noqa: E402
import app.services.model_backend_crypto as _crypto_mod  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset_singleton() -> None:
    """Force the module to rebuild its Fernet singleton on the next call."""
    _crypto_mod._fernet = None


def _fresh_key() -> str:
    return Fernet.generate_key().decode()


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


def test_round_trip(monkeypatch):
    """encrypt then decrypt returns the original plaintext."""
    key = _fresh_key()
    monkeypatch.setenv("MODEL_BACKEND_ENCRYPTION_KEY", key)
    _reset_singleton()

    from app.core.config import get_settings
    get_settings.cache_clear()

    plaintext = "sk-supersecret-api-key-12345"
    token = encrypt_api_key(plaintext)

    # Token must differ from plaintext
    assert token != plaintext
    # Round-trip must be lossless
    assert decrypt_api_key(token) == plaintext


def test_round_trip_empty_string(monkeypatch):
    """An empty-string api_key round-trips correctly."""
    key = _fresh_key()
    monkeypatch.setenv("MODEL_BACKEND_ENCRYPTION_KEY", key)
    _reset_singleton()

    from app.core.config import get_settings
    get_settings.cache_clear()

    assert decrypt_api_key(encrypt_api_key("")) == ""


def test_different_plaintexts_produce_different_tokens(monkeypatch):
    """Fernet uses random IV, so two encryptions of the same plaintext differ."""
    key = _fresh_key()
    monkeypatch.setenv("MODEL_BACKEND_ENCRYPTION_KEY", key)
    _reset_singleton()

    from app.core.config import get_settings
    get_settings.cache_clear()

    t1 = encrypt_api_key("same-key")
    t2 = encrypt_api_key("same-key")
    assert t1 != t2


# ---------------------------------------------------------------------------
# Wrong / tampered token
# ---------------------------------------------------------------------------


def test_decrypt_tampered_token_raises_value_error(monkeypatch):
    """A corrupted token raises ValueError with the expected message."""
    key = _fresh_key()
    monkeypatch.setenv("MODEL_BACKEND_ENCRYPTION_KEY", key)
    _reset_singleton()

    from app.core.config import get_settings
    get_settings.cache_clear()

    with pytest.raises(ValueError, match="api_key decryption failed"):
        decrypt_api_key("this-is-not-a-valid-fernet-token")


def test_decrypt_wrong_key_raises_value_error(monkeypatch):
    """A token encrypted with one key cannot be decrypted with a different key."""
    key1 = _fresh_key()
    monkeypatch.setenv("MODEL_BACKEND_ENCRYPTION_KEY", key1)
    _reset_singleton()

    from app.core.config import get_settings
    get_settings.cache_clear()

    token = encrypt_api_key("original-plaintext")

    # Switch to a different key
    key2 = _fresh_key()
    monkeypatch.setenv("MODEL_BACKEND_ENCRYPTION_KEY", key2)
    _reset_singleton()
    get_settings.cache_clear()

    with pytest.raises(ValueError, match="api_key decryption failed"):
        decrypt_api_key(token)


# ---------------------------------------------------------------------------
# Missing key
# ---------------------------------------------------------------------------


def test_encrypt_raises_runtime_error_when_key_unset(monkeypatch):
    """encrypt_api_key raises RuntimeError when MODEL_BACKEND_ENCRYPTION_KEY is empty."""
    monkeypatch.setenv("MODEL_BACKEND_ENCRYPTION_KEY", "")
    _reset_singleton()

    from app.core.config import get_settings
    get_settings.cache_clear()

    with pytest.raises(RuntimeError, match="MODEL_BACKEND_ENCRYPTION_KEY"):
        encrypt_api_key("some-key")


def test_decrypt_raises_runtime_error_when_key_unset(monkeypatch):
    """decrypt_api_key raises RuntimeError when MODEL_BACKEND_ENCRYPTION_KEY is empty."""
    monkeypatch.setenv("MODEL_BACKEND_ENCRYPTION_KEY", "")
    _reset_singleton()

    from app.core.config import get_settings
    get_settings.cache_clear()

    with pytest.raises(RuntimeError, match="MODEL_BACKEND_ENCRYPTION_KEY"):
        decrypt_api_key("some-token")


def test_runtime_error_message_contains_generation_hint(monkeypatch):
    """The RuntimeError message tells the operator exactly how to generate a key."""
    monkeypatch.setenv("MODEL_BACKEND_ENCRYPTION_KEY", "")
    _reset_singleton()

    from app.core.config import get_settings
    get_settings.cache_clear()

    with pytest.raises(RuntimeError) as exc_info:
        encrypt_api_key("anything")

    assert "Fernet.generate_key" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Exception chain suppression (from None)
# ---------------------------------------------------------------------------


def test_decrypt_bad_token_does_not_chain_invalid_token(monkeypatch):
    """ValueError raised on a bad token must suppress the internal InvalidToken chain.

    `raise ValueError(...) from None` sets __suppress_context__ = True, which
    is what prevents the InvalidToken from appearing in logger.exception output.
    __context__ still holds the original exception object (Python's design), but
    __suppress_context__ = True is the flag that matters for traceback rendering.
    __cause__ must be None (no explicit `raise ... from <exc>` chaining).
    """
    from cryptography.fernet import InvalidToken as _InvalidToken

    key = _fresh_key()
    monkeypatch.setenv("MODEL_BACKEND_ENCRYPTION_KEY", key)
    _reset_singleton()

    from app.core.config import get_settings
    get_settings.cache_clear()

    with pytest.raises(ValueError) as exc_info:
        decrypt_api_key("not-a-valid-fernet-token")

    raised = exc_info.value
    # __cause__ must be None — no explicit `raise ... from <exc>` chain
    assert raised.__cause__ is None
    # __suppress_context__ = True is set by `from None`, suppressing traceback display
    assert raised.__suppress_context__ is True
    # The context, if present, must be an InvalidToken (not some other leakage)
    if raised.__context__ is not None:
        assert isinstance(raised.__context__, _InvalidToken)

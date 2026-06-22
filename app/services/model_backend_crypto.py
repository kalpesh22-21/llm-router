"""
Encryption utility for model backend API keys at rest (ADR-001).

Provides Fernet symmetric encryption/decryption for the `api_key_enc` column
in the `model_backends` table. The Fernet key is sourced from the
MODEL_BACKEND_ENCRYPTION_KEY setting (or env var).

Generate a new key with:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

SECURITY NOTES:
  - The encryption key is never logged and must never appear in API responses.
  - Plaintext API keys are never logged.
  - api_key_enc column values are never returned by any API endpoint.
  - If the key is unset (empty string), any call to encrypt_api_key or
    decrypt_api_key raises RuntimeError immediately, preventing silent data
    corruption or key-less encryption.
"""

from __future__ import annotations

from cryptography.fernet import Fernet, InvalidToken

# Module-level singleton — built lazily on first use so that processes that
# never touch model_backend crypto do not fail on a missing key.
# Double-init is harmless under asyncio (same key, no threads racing), so no
# lock is needed here.
_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    """Return the module-level Fernet singleton, building it on first call."""
    global _fernet
    if _fernet is not None:
        return _fernet

    # Import here to avoid a circular import at module load time
    # (config imports nothing from services; this is safe).
    from app.core.config import get_settings

    key = get_settings().MODEL_BACKEND_ENCRYPTION_KEY
    if not key:
        raise RuntimeError(
            "MODEL_BACKEND_ENCRYPTION_KEY is not set. "
            "Generate one with: "
            "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        )

    _fernet = Fernet(key.encode() if isinstance(key, str) else key)
    return _fernet


def encrypt_api_key(plaintext: str) -> str:
    """
    Encrypt a plaintext API key with Fernet and return the token as a string.

    The returned value is safe to store in the `api_key_enc` TEXT column.
    Raises RuntimeError if MODEL_BACKEND_ENCRYPTION_KEY is not configured.
    """
    f = _get_fernet()
    token: bytes = f.encrypt(plaintext.encode())
    return token.decode()


def decrypt_api_key(token: str) -> str:
    """
    Decrypt a Fernet token previously produced by encrypt_api_key.

    Raises:
        RuntimeError  — if MODEL_BACKEND_ENCRYPTION_KEY is not configured.
        ValueError    — if the token is invalid or the key does not match
                        (message: "api_key decryption failed"). The original
                        InvalidToken exception is NOT chained to avoid leaking
                        any key material or internal trace into error payloads.
    """
    f = _get_fernet()
    try:
        plaintext: bytes = f.decrypt(token.encode() if isinstance(token, str) else token)
    except InvalidToken:
        raise ValueError("api_key decryption failed") from None
    return plaintext.decode()

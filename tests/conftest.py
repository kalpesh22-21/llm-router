"""
Test bootstrap.

Set safe env defaults BEFORE any `app.*` import so that importing
`app.core.config` (which runs load_settings_from_vault at module import) does not
touch Vault or trip the JWT fail-closed guard. pytest imports conftest.py before
collecting test modules, so this runs first.
"""

import os

os.environ.setdefault("VAULT_ENABLED", "false")
os.environ.setdefault("JWT_SECRET_KEY", "unit-test-secret-not-for-production-0123456789")
os.environ.setdefault("API_KEY", "test")
os.environ.setdefault("PHOENIX_TRACING_ENABLED", "false")

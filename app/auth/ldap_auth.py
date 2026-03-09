"""
LDAP authentication helpers.

Uses ldap3 to:
  1. Bind with a service account to search the directory.
  2. Locate the user's DN.
  3. Attempt a bind as that user to verify their password.

All network calls are performed synchronously (ldap3 is blocking) and should
be called from a thread-pool executor in async contexts if needed; for the
typical login flow the latency is acceptable on the main thread.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import ldap3
from ldap3 import ALL_ATTRIBUTES, Connection, Server, SUBTREE
from ldap3.core.exceptions import LDAPBindError, LDAPException

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def _get_server() -> Server:
    """Return an ldap3 Server object for the configured LDAP host."""
    return Server(settings.LDAP_SERVER, get_info=ldap3.NONE)


def _service_bind() -> Optional[Connection]:
    """
    Open a connection bound as the service account.

    Returns None if the service bind fails (misconfiguration / network error).
    """
    server = _get_server()
    try:
        conn = Connection(
            server,
            user=settings.LDAP_BIND_DN,
            password=settings.LDAP_BIND_PASSWORD,
            auto_bind=True,
            raise_exceptions=True,
        )
        return conn
    except LDAPException as exc:
        logger.error("LDAP service bind failed: %s", exc)
        return None


def _find_user_dn(conn: Connection, username: str) -> Optional[str]:
    """
    Search LDAP for *username* and return its distinguished name.

    Returns None if the user is not found or the search fails.
    """
    search_filter = f"(uid={ldap3.utils.conv.escape_filter_chars(username)})"
    try:
        conn.search(
            search_base=settings.LDAP_BASE_DN,
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=["dn"],
        )
    except LDAPException as exc:
        logger.error("LDAP search failed for user %r: %s", username, exc)
        return None

    if not conn.entries:
        logger.debug("LDAP: user %r not found in %s", username, settings.LDAP_BASE_DN)
        return None

    return conn.entries[0].entry_dn


def authenticate_ldap(username: str, password: str) -> bool:
    """
    Verify *username* / *password* against the LDAP directory.

    Flow:
      1. Service-account bind to locate the user DN.
      2. Re-bind as the user to verify the password.

    Returns True on success, False on any failure.
    """
    if not username or not password:
        return False

    service_conn = _service_bind()
    if service_conn is None:
        logger.error("Cannot authenticate: LDAP service bind failed")
        return False

    user_dn = _find_user_dn(service_conn, username)
    service_conn.unbind()

    if user_dn is None:
        return False

    # Attempt user bind to verify password
    server = _get_server()
    try:
        user_conn = Connection(
            server,
            user=user_dn,
            password=password,
            auto_bind=True,
            raise_exceptions=True,
        )
        user_conn.unbind()
        logger.debug("LDAP authentication succeeded for user %r", username)
        return True
    except LDAPBindError:
        logger.debug("LDAP authentication failed for user %r (bad password)", username)
        return False
    except LDAPException as exc:
        logger.error("LDAP authentication error for user %r: %s", username, exc)
        return False


def ldap_user_exists(username: str) -> Optional[bool]:
    """
    Check whether *username* exists in the LDAP directory.

    Returns:
        True  — user found in LDAP.
        False — user not found (may have been deprovisioned).
        None  — LDAP unreachable or service bind failed (caller should fail open).
    """
    service_conn = _service_bind()
    if service_conn is None:
        return None  # LDAP down — fail open

    user_dn = _find_user_dn(service_conn, username)
    service_conn.unbind()

    if user_dn is None:
        logger.debug("LDAP: user %r not found during existence check", username)
        return False

    return True


def get_ldap_user_info(username: str) -> Dict[str, str]:
    """
    Fetch basic user info (e.g. email) from LDAP.

    Returns an empty dict if the user is not found or LDAP is unreachable.
    """
    service_conn = _service_bind()
    if service_conn is None:
        return {}

    search_filter = f"(uid={ldap3.utils.conv.escape_filter_chars(username)})"
    try:
        service_conn.search(
            search_base=settings.LDAP_BASE_DN,
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=["mail", "cn", "uid"],
        )
    except LDAPException as exc:
        logger.error("LDAP info search failed for user %r: %s", username, exc)
        service_conn.unbind()
        return {}

    result: Dict[str, str] = {}
    if service_conn.entries:
        entry = service_conn.entries[0]
        if "mail" in entry and entry["mail"]:
            result["email"] = str(entry["mail"])
        if "cn" in entry and entry["cn"]:
            result["display_name"] = str(entry["cn"])

    service_conn.unbind()
    return result

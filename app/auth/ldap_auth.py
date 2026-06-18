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
from ldap3.core.exceptions import LDAPBindError, LDAPException, LDAPOperationResult
from ldap3.utils.dn import escape_rdn
from ldap3.utils.conv import escape_filter_chars

from app.core.config import get_settings

from paycompy import ldap

logger = logging.getLogger(__name__)
settings = get_settings()


def _escape_ldap_username(username: str) -> str:
    """
    Escape special characters in username to prevent LDAP injection.

    LDAP injection can occur when user input containing special characters
    is used in LDAP bind operations or filters without proper escaping.

    This function escapes characters that are special in LDAP DNs:
    , = + < > # ; " \ and leading/trailing spaces
    """
    if not username:
        return username
    return escape_rdn(username)


def _escape_ldap_password(password: str) -> str:
    """
    Escape special characters in password to prevent LDAP injection.

    While passwords are typically not used in LDAP filters, escaping
    provides defense in depth against potential injection vectors.
    """
    if not password:
        return password
    # Passwords in LDAP bind operations don't typically need escaping,
    # but we strip control characters as a safety measure
    return "".join(char for char in password if ord(char) >= 32)


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

    # Escape username and password to prevent LDAP injection
    escaped_username = _escape_ldap_username(username)
    escaped_password = _escape_ldap_password(password)

    # Reject if escaping reduced either credential to empty. An empty SIMPLE-bind
    # password is treated as an anonymous/unauthenticated bind by many servers,
    # which would otherwise let a control-character-only password authenticate.
    if not escaped_username or not escaped_password:
        return False

    # Attempt user bind to verify password
    try:
        user_conn = ldap.Ldap(
            username=escaped_username,
            password=escaped_password,
            auto_bind=True,
            raise_exceptions=True,
        )
        user_has_access = check_if_user_has_access(
            ldap_conn=user_conn,
            allowed_groups=settings.ALLOWED_GROUPS,
            allowed_individuals=settings.ALLOWED_INDIVIUALS,
        )
        user_conn.unbind()
        if not user_has_access:
            raise LDAPOperationResult(result="Not in Allowed Groups or Indiviuals")

        logger.debug("LDAP authentication succeeded for user %r", username)
        return True
    except LDAPBindError:
        logger.debug("LDAP authentication failed for user %r (bad password)", username)
        return False
    except LDAPException as exc:
        logger.error("LDAP authentication error for user %r: %s", username, exc)
        return False
    except ValueError as exc:
        logger.error("LDAP authentication failed for user %r (bad password)", username)
        return False


def check_if_user_has_access(
    ldap_conn: ldap.Ldap,
    allowed_groups: list[str] | str = [],
    allowed_individuals: list[str] | str = [],
) -> bool:
    """
    Given a list of groups and/or usernames of individuals, it will return a boolean denoting whether
    the user has access.

    Parameters
    ----------
    allowed_groups : list[str]
        This is a list of strings that contain the Paycom security groups that the user should be a part of.

    allowed_individuals : list[str]
        This is a list of strings that contain the usernames of the individuals that should have access.

    Returns
    -------
    Bool
        Returns a boolean signifying access given the allowed_groups or the allowed_individuals.


    """
    if isinstance(allowed_groups, str):
        allowed_groups = [allowed_groups]

    if isinstance(allowed_individuals, str):
        allowed_individuals = [allowed_individuals]

    ldap_username_user = ldap_conn.get_username_of_user()

    if ldap_username_user == settings.SUPERUSER_USERNAME:
        return True

    is_authorized = False
    for individual in allowed_individuals:
        if individual == ldap_username_user:
            is_authorized = True

    if not is_authorized:
        authourized_groups = ldap_conn.get_group_membership_of_user()
        for group in allowed_groups:
            if group in authourized_groups:
                is_authorized = True

    return is_authorized


def get_ldap_user_info(username: str, password: str) -> Dict[str, str]:
    """
    Fetch basic user info (e.g. email) from LDAP.

    Returns an empty dict if the user is not found or LDAP is unreachable.
    """
    # Escape username and password to prevent LDAP injection
    escaped_username = _escape_ldap_username(username)
    escaped_password = _escape_ldap_password(password)

    result: Dict[str, str] = {}
    # Empty escaped credentials would attempt an anonymous bind — bail out with
    # an empty dict (the documented "user not found / unreachable" return).
    if not escaped_username or not escaped_password:
        return result
    try:
        user_conn = ldap.Ldap(
            username=escaped_username,
            password=escaped_password,
            auto_bind=True,
            raise_exceptions=True,
        )
        logger.debug("LDAP authentication succeeded for user %r", username)
    except LDAPBindError:
        logger.debug("LDAP authentication failed for user %r (bad password)", username)
        return {}
    except LDAPException as exc:
        logger.error("LDAP authentication error for user %r: %s", username, exc)
        return {}
    name = user_conn._whoami(username=username)
    result["email"] = user_conn.get_email_of_user(name=name)
    return result


def verify_ldap_user_exists(username: str) -> bool:
    """
    Verify that a user exists in LDAP without requiring a password.

    Uses a service account bind to search for the user in the directory.
    Returns True if the user exists, False otherwise.
    """
    if not username:
        return False

    # Escape username for use in an LDAP *search filter* (escape_filter_chars,
    # not escape_rdn which is for DNs) to prevent filter injection.
    escaped_username = escape_filter_chars(username)

    try:
        # Use service account to search for the user
        # This assumes the ldap.Ldap class can bind with service account credentials
        # and search for users without needing the user's password
        service_conn = ldap.Ldap(
            username=settings.LDAP_SERVICE_USERNAME,
            password=settings.LDAP_SERVICE_PASSWORD,
            auto_bind=True,
            raise_exceptions=True,
        )
        search_filter = f"(&(objectClass=person)(uid={escaped_username}))"
        # Search for the user
        service_conn.search(
            search_base="DC=paycomhq,DC=com",
            search_filter=search_filter,
            attributes=["uid"],
        )

        # Check if any entries were returned
        user_exists = len(service_conn.entries) > 0
        service_conn.unbind()

        if user_exists:
            logger.debug("LDAP user %r found in directory", username)
            return True
        else:
            logger.debug("LDAP user %r not found in directory", username)
            return False

    except LDAPException as exc:
        logger.error(
            "LDAP error while checking user existence for %r: %s", username, exc
        )
        return False
    except Exception as exc:
        logger.error("Unexpected error while checking LDAP user %r: %s", username, exc)
        return False

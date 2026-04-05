# Auth Internals

## LDAP ([app/auth/ldap_auth.py](../../app/auth/ldap_auth.py))

Uses the synchronous `ldap3` library. Three operations:

**`authenticate_ldap(username, password)`**
1. Bind with the service account (`LDAP_BIND_DN` / `LDAP_BIND_PASSWORD`).
2. Search for `(uid=<username>)` under `LDAP_BASE_DN` to get the user's DN.
3. Re-bind using that DN and the provided password to verify credentials.
4. Returns `True` on success, raises HTTP 401 on failure.

**`ldap_user_exists(username)`**
- Checks if the username exists in LDAP.
- Returns `None` (fail-open) if LDAP is unreachable — the caller treats `None` as "cannot verify, allow through".

**`get_ldap_user_info(username)`**
- Fetches `mail` and `cn` attributes.
- Returns `{"email": ..., "display_name": ...}` or `{}` on failure.

### Fail-Open Behavior

LDAP downtime does not block requests. `ldap_user_exists` returning `None` causes `PermissionService.is_user_active()` to skip deactivation. This is intentional — prefer availability over strict enforcement during LDAP outages.

## JWT ([app/auth/jwt_handler.py](../../app/auth/jwt_handler.py))

**Token payload:**
```json
{
  "sub": "1",
  "username": "alice",
  "is_admin": false,
  "iat": 1712345678,
  "exp": 1712349278
}
```

**`create_access_token(user_id, username, is_admin)`** — signs with HS256 using `SECRET_KEY`. Expiry from `JWT_EXPIRE_MINUTES` (default: 60).

**`decode_token(token)`** — raises HTTP 401 with a descriptive message for expired or invalid tokens.

## Probabilistic LDAP Re-check

`PermissionService.is_user_active()` runs a LDAP existence check on ~10% of requests (random). If the user no longer exists in LDAP, they are automatically deactivated in the DB. This catches deprovisioned users without hitting LDAP on every request.

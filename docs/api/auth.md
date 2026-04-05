# API: Auth (`/auth`)

## `POST /auth/login`

Authenticates via LDAP and issues a JWT.

**Request body:**
```json
{ "username": "alice", "password": "alice123" }
```

**Response:**
```json
{
  "access_token": "<jwt>",
  "token_type": "bearer",
  "expires_in": 3600
}
```

**Flow:**
1. LDAP bind with the provided credentials.
2. Upsert the user into the `users` table (creates if first login).
3. New user is auto-granted the default bucket.
4. If this is the first-ever login and the username matches `SUPERUSER_USERNAME` env var, the user is promoted to admin.
5. JWT issued with `sub` (user_id), `username`, `is_admin`, `iat`, `exp`.

## `GET /auth/me`

Returns the currently authenticated user.

**Requires:** `Authorization: Bearer <jwt>`

**Response:**
```json
{
  "id": 1,
  "username": "alice",
  "email": "alice@example.com",
  "is_active": true,
  "is_admin": false,
  "max_concurrent_requests": 1
}
```

# Permissions Service ([app/services/permission.py](../../app/services/permission.py))

`PermissionService` is the single source of truth for all access control operations. It owns all DB reads and writes related to users, buckets, and access grants.

## User Operations

| Method | Description |
|--------|-------------|
| `get_user(user_id)` | Fetch user by ID |
| `get_user_by_username(username)` | Fetch user by username |
| `list_users()` | All users |
| `create_user(username, email, is_admin)` | Create and auto-grant default bucket |
| `set_user_active(user_id, is_active)` | Activate / deactivate |
| `set_admin(user_id, is_admin)` | Grant / revoke admin |
| `set_user_max_concurrent(user_id, n)` | Update concurrent request cap |
| `count_users()` | Total user count (used for first-user bootstrap) |

## Active Check

`is_user_active(user_id)` is called on every proxy request:
1. Queries `users.is_active` from DB.
2. With ~10% probability, calls `ldap_user_exists()`.
3. If LDAP says the user no longer exists, sets `is_active = False` in DB and returns `False`.
4. If LDAP is unreachable (returns `None`), skips deactivation and returns the DB value.

## Model Access

`get_user_accessible_models(user_id)` returns the set of `model_id` strings the user can access:

```sql
SELECT bm.model_id
FROM user_bucket_access uba
JOIN bucket_models bm ON uba.bucket_id = bm.bucket_id
WHERE uba.user_id = :user_id
```

## Bucket Operations

| Method | Description |
|--------|-------------|
| `create_bucket(name, description, model_ids)` | Create bucket and associate models |
| `list_buckets()` | All buckets with their model IDs |
| `get_default_bucket()` | Fetch the bucket with `is_default = 1` |
| `set_default_bucket(bucket_id)` | Clear existing default, set new one |
| `grant_bucket_access(user_id, bucket_id, granted_by)` | Idempotent insert into `user_bucket_access` |
| `revoke_bucket_access(user_id, bucket_id)` | Delete from `user_bucket_access` |

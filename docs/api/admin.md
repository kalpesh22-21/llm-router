# API: Admin (`/admin`)

All endpoints require an admin JWT (`is_admin: true` in token).

## Users

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/users` | List all users |
| `POST` | `/admin/users` | Create a user (email fetched from LDAP if omitted) |
| `PATCH` | `/admin/users/{id}/active` | Activate or deactivate a user |
| `PATCH` | `/admin/users/{id}/admin` | Grant or revoke admin role |
| `PATCH` | `/admin/users/{id}/max-concurrent` | Set max concurrent request cap |
| `GET` | `/admin/users/{id}/buckets` | List bucket IDs the user has access to |
| `POST` | `/admin/users/{id}/buckets/{bucket_id}` | Grant bucket access |
| `DELETE` | `/admin/users/{id}/buckets/{bucket_id}` | Revoke bucket access |

## Buckets

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/buckets` | List all buckets with their model IDs |
| `POST` | `/admin/buckets` | Create a bucket with a list of `model_ids` |
| `PATCH` | `/admin/buckets/{id}/default` | Set as the default bucket (clears previous default) |

## Models

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/models` | List all model IDs registered in the backend registry |

## Notes

- Bucket grants are idempotent — granting an already-granted bucket does not error.
- The default bucket is automatically granted to every new user at creation time.
- Models are registered at startup from `config/models.yaml`; changing models requires a restart.

# Database Schema

## MySQL Tables ([migrations/schema.sql](../../migrations/schema.sql))

### `users`
| Column | Type | Notes |
|--------|------|-------|
| `id` | INT PK AUTO_INCREMENT | |
| `username` | VARCHAR(255) UNIQUE | |
| `email` | VARCHAR(255) | |
| `is_active` | TINYINT(1) | Default 1 |
| `is_admin` | TINYINT(1) | Default 0 |
| `max_concurrent_requests` | INT | Default 1 |
| `created_at` / `updated_at` | DATETIME | Auto-managed |

### `buckets`
| Column | Type | Notes |
|--------|------|-------|
| `id` | INT PK AUTO_INCREMENT | |
| `name` | VARCHAR(255) UNIQUE | |
| `description` | TEXT | |
| `is_default` | TINYINT(1) | Default 0; only one row should be 1 |
| `created_at` | DATETIME | |

### `bucket_models`
| Column | Type | Notes |
|--------|------|-------|
| `id` | INT PK AUTO_INCREMENT | |
| `bucket_id` | INT FK → `buckets` (CASCADE DELETE) | |
| `model_id` | VARCHAR(255) | |
| | UNIQUE | `(bucket_id, model_id)` |

### `user_bucket_access`
| Column | Type | Notes |
|--------|------|-------|
| `id` | INT PK AUTO_INCREMENT | |
| `user_id` | INT FK → `users` (CASCADE DELETE) | |
| `bucket_id` | INT FK → `buckets` (CASCADE DELETE) | |
| `granted_by` | INT FK → `users` (SET NULL on delete) | |
| `granted_at` | DATETIME | |
| | UNIQUE | `(user_id, bucket_id)` |

## Seed Data ([migrations/seed.sql](../../migrations/seed.sql))

| Bucket | Models |
|--------|--------|
| `default` | `gpt-4o` |
| `premium` | `gpt-4o`, `llama-3-70b` |
| `basic` | `mistral-7b` |

| User | Role | Buckets |
|------|------|---------|
| `admin` | admin | `premium` |
| `alice` | user | `default`, `basic` |
| `bob` | user | `default` |
| `charlie` | user | _(none beyond auto-default)_ |

## Redis Keys

| Key | Type | TTL | Description |
|-----|------|-----|-------------|
| `concurrent:{user_id}` | String (integer) | 300s | Active concurrent request count per user |

The 300s TTL is a safety net against crashed workers leaving the counter stuck.

-- =============================================================================
-- LLM Router — Development seed data
-- Runs after schema.sql (alphabetical order in docker-entrypoint-initdb.d/).
-- Safe to re-run: all inserts use INSERT IGNORE.
-- =============================================================================

USE llm_router;

-- -----------------------------------------------------------------------------
-- Additional buckets (schema.sql already seeds the 'default' bucket)
-- -----------------------------------------------------------------------------
INSERT IGNORE INTO buckets (name, description, is_default) VALUES
  ('premium', 'Full access: GPT-4o + LLaMA-3-70B', 0),
  ('basic',   'Lightweight access: Mistral-7B only', 0);

-- -----------------------------------------------------------------------------
-- Bucket → model assignments
-- Model IDs must match entries in config/models.yaml
-- -----------------------------------------------------------------------------

-- default bucket: gpt-4o
INSERT IGNORE INTO bucket_models (bucket_id, model_id)
  SELECT id, 'gpt-4o' FROM buckets WHERE name = 'default';

-- premium bucket: all models
INSERT IGNORE INTO bucket_models (bucket_id, model_id)
  SELECT id, 'gpt-4o'       FROM buckets WHERE name = 'premium';
INSERT IGNORE INTO bucket_models (bucket_id, model_id)
  SELECT id, 'llama-3-70b'  FROM buckets WHERE name = 'premium';

-- basic bucket: mistral only
INSERT IGNORE INTO bucket_models (bucket_id, model_id)
  SELECT id, 'mistral-7b'   FROM buckets WHERE name = 'basic';

-- -----------------------------------------------------------------------------
-- Pre-seeded users (normally created on first LDAP login; seeded here for demo)
-- Credentials are in ldap/users.ldif — password = <username>123
-- -----------------------------------------------------------------------------
INSERT IGNORE INTO users (username, email, is_active, is_admin) VALUES
  ('admin',   'admin@example.com',   1, 1),
  ('alice',   'alice@example.com',   1, 0),
  ('bob',     'bob@example.com',     1, 0),
  ('charlie', 'charlie@example.com', 1, 0);

-- -----------------------------------------------------------------------------
-- Bucket access grants
-- admin   → premium (gets everything)
-- alice   → default + basic
-- bob     → default only
-- charlie → no access yet (demo: admin can grant via UI)
-- -----------------------------------------------------------------------------

-- admin → premium
INSERT IGNORE INTO user_bucket_access (user_id, bucket_id, granted_by)
  SELECT u.id, b.id, NULL FROM users u, buckets b
  WHERE u.username = 'admin' AND b.name = 'premium';

-- alice → default
INSERT IGNORE INTO user_bucket_access (user_id, bucket_id, granted_by)
  SELECT u.id, b.id, NULL FROM users u, buckets b
  WHERE u.username = 'alice' AND b.name = 'default';

-- alice → basic
INSERT IGNORE INTO user_bucket_access (user_id, bucket_id, granted_by)
  SELECT u.id, b.id, NULL FROM users u, buckets b
  WHERE u.username = 'alice' AND b.name = 'basic';

-- bob → default
INSERT IGNORE INTO user_bucket_access (user_id, bucket_id, granted_by)
  SELECT u.id, b.id, NULL FROM users u, buckets b
  WHERE u.username = 'bob' AND b.name = 'default';

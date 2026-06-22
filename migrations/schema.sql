-- =============================================================================
-- LLM Router — MySQL schema
-- =============================================================================
-- Run this once against an empty database to create all required tables.
-- SQLAlchemy's create_all() will also create these tables on first startup,
-- but this file serves as the authoritative human-readable reference and can
-- be used for manual provisioning or CI database setup.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS llm_router
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE llm_router;

-- -----------------------------------------------------------------------------
-- users
-- Represents an authenticated operator / end-user of the router.
-- Created automatically on first successful LDAP login.
-- is_admin      : may manage users, buckets, and access control.
-- max_concurrent_requests : Redis-enforced concurrent-request cap per user.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id                      INT          NOT NULL AUTO_INCREMENT,
    username                VARCHAR(255) NOT NULL,
    email                   VARCHAR(255)          DEFAULT NULL,
    is_active               TINYINT(1)   NOT NULL DEFAULT 1,
    is_admin                TINYINT(1)   NOT NULL DEFAULT 0,
    max_concurrent_requests INT          NOT NULL DEFAULT 1,
    created_at              DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_users_username (username),
    INDEX ix_users_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- buckets
-- Named collections of model_ids.  Users are granted access to buckets rather
-- than individual models.  is_default=1 means new users are auto-enrolled.
-- Only one row should have is_default=1 at any time (enforced by the app).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS buckets (
    id           INT          NOT NULL AUTO_INCREMENT,
    name         VARCHAR(255) NOT NULL,
    description  TEXT                  DEFAULT NULL,
    is_default   TINYINT(1)   NOT NULL DEFAULT 0,
    default_model VARCHAR(255)         DEFAULT NULL,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_buckets_name (name),
    INDEX ix_buckets_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed a default bucket so new users automatically get some model access.
-- Adjust the name and insert matching bucket_models rows as needed.
INSERT IGNORE INTO buckets (name, description, is_default)
VALUES ('default', 'Default bucket for new users', 1);

-- -----------------------------------------------------------------------------
-- bucket_models
-- Maps model_id strings (as defined in config/models.yaml) to buckets.
-- A bucket can contain many models; a model can appear in many buckets.
-- The combination (bucket_id, model_id) must be unique.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bucket_models (
    id        INT          NOT NULL AUTO_INCREMENT,
    bucket_id INT          NOT NULL,
    model_id  VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_bucket_model (bucket_id, model_id),
    INDEX ix_bucket_models_bucket_id (bucket_id),
    CONSTRAINT fk_bucket_models_bucket
        FOREIGN KEY (bucket_id)
        REFERENCES buckets (id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- user_bucket_access
-- Records which users have been granted access to which buckets.
-- granted_by references the admin who created the grant (nullable
-- for system-generated grants such as default-bucket auto-enrolment).
-- The combination (user_id, bucket_id) is unique — no duplicate grants.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_bucket_access (
    id         INT      NOT NULL AUTO_INCREMENT,
    user_id    INT      NOT NULL,
    bucket_id  INT      NOT NULL,
    granted_by INT               DEFAULT NULL,
    granted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_user_bucket (user_id, bucket_id),
    INDEX ix_user_bucket_access_user_id (user_id),
    INDEX ix_user_bucket_access_bucket_id (bucket_id),
    CONSTRAINT fk_uba_user
        FOREIGN KEY (user_id)
        REFERENCES users (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_uba_bucket
        FOREIGN KEY (bucket_id)
        REFERENCES buckets (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_uba_granted_by
        FOREIGN KEY (granted_by)
        REFERENCES users (id)
        ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- service_accounts
-- Named service identities that authenticate via opaque API keys.
-- Not tied to LDAP — created and managed entirely through the admin API.
-- max_concurrent_requests: account-level default for keys that don't override.
-- created_by: the admin who created the account (nullable on admin deletion).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS service_accounts (
    id                      INT          NOT NULL AUTO_INCREMENT,
    name                    VARCHAR(255) NOT NULL,
    description             TEXT                  DEFAULT NULL,
    owner_team              VARCHAR(255)          DEFAULT NULL,
    is_active               TINYINT(1)   NOT NULL DEFAULT 1,
    max_concurrent_requests INT          NOT NULL DEFAULT 10,
    created_at              DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
    created_by              INT                   DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_service_accounts_name (name),
    INDEX ix_service_accounts_name (name),
    CONSTRAINT fk_sa_created_by
        FOREIGN KEY (created_by)
        REFERENCES users (id)
        ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- service_bucket_access
-- Records which service accounts have been granted access to which buckets.
-- Parallel structure to user_bucket_access but keyed on service_accounts.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS service_bucket_access (
    id         INT      NOT NULL AUTO_INCREMENT,
    service_id INT      NOT NULL,
    bucket_id  INT      NOT NULL,
    granted_by INT               DEFAULT NULL,
    granted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_service_bucket (service_id, bucket_id),
    INDEX ix_service_bucket_access_service_id (service_id),
    INDEX ix_service_bucket_access_bucket_id (bucket_id),
    CONSTRAINT fk_sba_service
        FOREIGN KEY (service_id)
        REFERENCES service_accounts (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_sba_bucket
        FOREIGN KEY (bucket_id)
        REFERENCES buckets (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_sba_granted_by
        FOREIGN KEY (granted_by)
        REFERENCES users (id)
        ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- api_keys
-- Long-lived opaque API keys belonging to service_accounts (NOT users).
-- The plaintext key is NEVER stored; only the SHA-256 hex digest and a
-- non-secret prefix are persisted. A NULL revoked_at means the key is active.
-- expires_at NULL means the key never expires (checked at resolve time).
-- last_used_at is updated on each cache-miss DB lookup (throttled to once
-- per cache TTL window) so it is not written on every request.
--
-- Per-key overrides (all nullable — NULL means "use account default"):
--   scopes      JSON  — list of operation scopes {"chat","embeddings","messages","models"};
--                        NULL/empty = all operations allowed.
--   model_ids   JSON  — list of model_id strings restricting this key to a subset;
--                        NULL/empty = all of the account's models.
--   max_concurrent_requests INT — per-key concurrency cap; NULL = account default.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_keys (
    id                      INT          NOT NULL AUTO_INCREMENT,
    service_id              INT          NOT NULL,
    name                    VARCHAR(255) NOT NULL,
    prefix                  VARCHAR(32)  NOT NULL,
    key_hash                VARCHAR(64)  NOT NULL,
    created_by              INT                   DEFAULT NULL,
    created_at              DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at              DATETIME              DEFAULT NULL,
    last_used_at            DATETIME              DEFAULT NULL,
    revoked_at              DATETIME              DEFAULT NULL,
    scopes                  JSON                  DEFAULT NULL,
    model_ids               JSON                  DEFAULT NULL,
    max_concurrent_requests INT                   DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_api_keys_key_hash (key_hash),
    INDEX ix_api_keys_service_id (service_id),
    INDEX ix_api_keys_prefix (prefix),
    CONSTRAINT fk_api_keys_service
        FOREIGN KEY (service_id)
        REFERENCES service_accounts (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_api_keys_created_by
        FOREIGN KEY (created_by)
        REFERENCES users (id)
        ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- model_backends
-- DB-driven backend registry (ADR-001: Dynamic Model Backend Management).
-- Stores one row per logical LLM backend that is managed via the admin UI.
--
-- Ownership / precedence note:
--   Backends are owned by the source that registers them first.
--   1. config/models.yaml  — highest priority; registered at pod startup.
--   2. Kubernetes discovery — registered before DB source starts.
--   3. THIS TABLE (model_backends) — lowest priority; DatabaseBackendSource
--      skips any model_id already claimed by a higher-priority source.
--   To hand a model over to DB management, remove it from models.yaml and
--   write it here via the admin UI.  No pod restart required.
--
-- api_key_enc: Fernet-encrypted (AES-128-CBC + HMAC-SHA256) api_key.
--   NULL means "use the global API_KEY env var fallback."
--   The plaintext key is NEVER stored or returned by any API endpoint.
-- version: incremented on every UPDATE; used for cheap change detection
--   by the background reconcile loop (compare DB row version vs in-memory).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_backends (
    id              INT           NOT NULL AUTO_INCREMENT,
    model_id        VARCHAR(255)  NOT NULL,
    backend_type    VARCHAR(64)   NOT NULL DEFAULT 'static',
    base_url        VARCHAR(2048) NOT NULL,
    api_key_enc     TEXT                   DEFAULT NULL,
    is_enabled      TINYINT(1)    NOT NULL DEFAULT 1,
    source          VARCHAR(64)   NOT NULL DEFAULT 'admin_ui',
    version         INT           NOT NULL DEFAULT 1,
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP,
    created_by      INT                    DEFAULT NULL,
    updated_by      INT                    DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_model_backends_model_id (model_id),
    INDEX ix_model_backends_is_enabled (is_enabled),
    CONSTRAINT fk_model_backends_created_by
        FOREIGN KEY (created_by)
        REFERENCES users (id)
        ON DELETE SET NULL,
    CONSTRAINT fk_model_backends_updated_by
        FOREIGN KEY (updated_by)
        REFERENCES users (id)
        ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

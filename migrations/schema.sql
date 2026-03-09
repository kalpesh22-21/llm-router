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
    id          INT          NOT NULL AUTO_INCREMENT,
    name        VARCHAR(255) NOT NULL,
    description TEXT                  DEFAULT NULL,
    is_default  TINYINT(1)   NOT NULL DEFAULT 0,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
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

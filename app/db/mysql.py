"""
MySQL / SQLAlchemy async database layer.

Defines the SQLAlchemy ORM models and provides an async session factory
plus the FastAPI dependency `get_db()`.
"""

from __future__ import annotations

import datetime
import logging
from typing import AsyncGenerator

from sqlalchemy import (
    URL,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    JSON,
    text,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import sqlparse

from app.core.config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

# ---------------------------------------------------------------------------
# Engine & session factory
# ---------------------------------------------------------------------------


def _build_url(
    host: str,
    port: int,
    user: str,
    password: str,
    db_name: str,
    drivername: str,
):
    # return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db_name}"
    connection_url = URL.create(
        drivername=drivername,
        username=user,
        password=password,
        host=host,
        database=db_name,
        port=port,  # Port is an integer
    )
    return connection_url


engine = create_async_engine(
    _build_url(
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        db_name=settings.DATABASE_NAME,
        drivername=settings.DATABASE_DRIVER,
    ),
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    echo=False,
)

# Log database connection info (excluding password)
logger.info(
    f"Connecting to database: driver={settings.DATABASE_DRIVER}, "
    f"database={settings.DATABASE_NAME}, user={settings.DATABASE_USER}"
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


# ---------------------------------------------------------------------------
# ORM base
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------


class User(Base):
    """
    Represents an authenticated user in the system.

    Users are created on first LDAP login. The is_admin flag controls
    what management operations they may perform.
    """

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(
        String(255), nullable=False, unique=True, index=True
    )
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Maximum number of simultaneous in-flight requests allowed for this user
    max_concurrent_requests: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationships
    bucket_accesses: Mapped[list[UserBucketAccess]] = relationship(
        "UserBucketAccess",
        back_populates="user",
        foreign_keys="UserBucketAccess.user_id",
        lazy="select",
    )

    def __repr__(self) -> str:  # pragma: no cover
        return f"<User id={self.id} username={self.username!r}>"


class Bucket(Base):
    """
    A named collection of models.

    Users are granted access to buckets, not individual models. The bucket
    that has is_default=True is automatically assigned to new users.
    """

    __tablename__ = "buckets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String(255), nullable=False, unique=True, index=True
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    default_model: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )

    # Relationships
    models: Mapped[list[BucketModel]] = relationship(
        "BucketModel",
        back_populates="bucket",
        lazy="select",
        cascade="all, delete-orphan",
    )
    user_accesses: Mapped[list[UserBucketAccess]] = relationship(
        "UserBucketAccess", back_populates="bucket", lazy="select"
    )

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Bucket id={self.id} name={self.name!r}>"


class BucketModel(Base):
    """
    Maps a model_id (string reference to models.yaml) to a Bucket.

    One bucket can contain many models; a model can appear in many buckets.
    """

    __tablename__ = "bucket_models"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bucket_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("buckets.id", ondelete="CASCADE"), nullable=False
    )
    model_id: Mapped[str] = mapped_column(String(255), nullable=False)

    __table_args__ = (
        UniqueConstraint("bucket_id", "model_id", name="uq_bucket_model"),
        Index("ix_bucket_models_bucket_id", "bucket_id"),
    )

    # Relationships
    bucket: Mapped[Bucket] = relationship("Bucket", back_populates="models")

    def __repr__(self) -> str:  # pragma: no cover
        return f"<BucketModel bucket_id={self.bucket_id} model_id={self.model_id!r}>"


class UserBucketAccess(Base):
    """
    Records that a user has been granted access to a bucket.

    `granted_by` references the admin/superuser who created the grant.
    """

    __tablename__ = "user_bucket_access"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    bucket_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("buckets.id", ondelete="CASCADE"), nullable=False
    )
    granted_by: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    granted_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("user_id", "bucket_id", name="uq_user_bucket"),
        Index("ix_user_bucket_access_user_id", "user_id"),
        Index("ix_user_bucket_access_bucket_id", "bucket_id"),
    )

    # Relationships
    user: Mapped[User] = relationship(
        "User", back_populates="bucket_accesses", foreign_keys=[user_id]
    )
    bucket: Mapped[Bucket] = relationship("Bucket", back_populates="user_accesses")

    def __repr__(self) -> str:  # pragma: no cover
        return f"<UserBucketAccess user_id={self.user_id} bucket_id={self.bucket_id}>"


# ---------------------------------------------------------------------------
# Service account models
# ---------------------------------------------------------------------------


class ServiceAccount(Base):
    """
    A named service identity that authenticates via opaque API keys.

    Service accounts are NOT LDAP users — they live in a dedicated table
    and have no password. All data-plane access goes through ApiKey resolution.
    """

    __tablename__ = "service_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Unique machine-readable name (used as username in payloads)
    name: Mapped[str] = mapped_column(
        String(255), nullable=False, unique=True, index=True
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Owning team or person label (informational)
    owner_team: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Account-level concurrency default — individual keys may override this
    max_concurrent_requests: Mapped[int] = mapped_column(
        Integer, nullable=False, default=10
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    # Admin who created this account (nullable: SET NULL on user deletion)
    created_by: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )

    # Relationships
    bucket_accesses: Mapped[list[ServiceBucketAccess]] = relationship(
        "ServiceBucketAccess",
        back_populates="service_account",
        lazy="select",
        cascade="all, delete-orphan",
    )
    api_keys: Mapped[list[ApiKey]] = relationship(
        "ApiKey",
        back_populates="service_account",
        lazy="select",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:  # pragma: no cover
        return f"<ServiceAccount id={self.id} name={self.name!r}>"


class ServiceBucketAccess(Base):
    """
    Records that a service account has been granted access to a bucket.

    Parallel structure to UserBucketAccess but keyed on service_accounts.id.
    """

    __tablename__ = "service_bucket_access"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    service_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("service_accounts.id", ondelete="CASCADE"), nullable=False
    )
    bucket_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("buckets.id", ondelete="CASCADE"), nullable=False
    )
    granted_by: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    granted_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("service_id", "bucket_id", name="uq_service_bucket"),
        Index("ix_service_bucket_access_service_id", "service_id"),
        Index("ix_service_bucket_access_bucket_id", "bucket_id"),
    )

    # Relationships
    service_account: Mapped[ServiceAccount] = relationship(
        "ServiceAccount", back_populates="bucket_accesses"
    )
    bucket: Mapped[Bucket] = relationship("Bucket")

    def __repr__(self) -> str:  # pragma: no cover
        return f"<ServiceBucketAccess service_id={self.service_id} bucket_id={self.bucket_id}>"


class ApiKey(Base):
    """
    Long-lived opaque API key belonging to a ServiceAccount.

    The plaintext key is NEVER stored. Only the SHA-256 hex digest (key_hash)
    and a non-secret prefix are persisted. The prefix is used for UI display
    so admins can identify which key they are looking at without exposing the secret.

    Lifecycle: a key is active when revoked_at IS NULL and (expires_at IS NULL or
    expires_at > now). Revocation is soft — set revoked_at to reject on next request.

    Per-key overrides (all nullable — NULL means "use account default"):
      scopes      — list of operation scopes restricting this key; NULL/empty = all ops.
      model_ids   — list of model_id strings further restricting model access; NULL/empty = all account models.
      max_concurrent_requests — per-key concurrency cap; NULL = use account.max_concurrent_requests.
    """

    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # The service account this key belongs to
    service_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("service_accounts.id", ondelete="CASCADE"), nullable=False
    )
    # Human-readable label, e.g. "ci-prod"
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # Non-secret leading slice of the key (first 16 chars) for UI identification
    prefix: Mapped[str] = mapped_column(String(32), nullable=False)
    # SHA-256 hex digest of the full key — the lookup key; UNIQUE enforces one row per key
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    # Admin who issued this key (nullable: SET NULL on user deletion)
    created_by: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    # Optional expiry in naive UTC; NULL means the key never expires
    expires_at: Mapped[datetime.datetime | None] = mapped_column(DateTime, nullable=True)
    # Updated on each cache-miss resolution (throttled to once per cache TTL window)
    last_used_at: Mapped[datetime.datetime | None] = mapped_column(DateTime, nullable=True)
    # Soft revocation timestamp; NULL means the key is active
    revoked_at: Mapped[datetime.datetime | None] = mapped_column(DateTime, nullable=True)

    # Per-key operation scope restriction — list of strings from {"chat","embeddings","messages","models"}
    # NULL or empty list means all operations are allowed
    scopes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Per-key model subset — list of model_id strings; NULL or empty = all account models
    model_ids: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Per-key concurrency override; NULL = use account.max_concurrent_requests
    max_concurrent_requests: Mapped[int | None] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        Index("ix_api_keys_service_id", "service_id"),
        Index("ix_api_keys_prefix", "prefix"),
    )

    # Relationships
    service_account: Mapped[ServiceAccount] = relationship(
        "ServiceAccount", back_populates="api_keys"
    )

    def __repr__(self) -> str:  # pragma: no cover
        return f"<ApiKey id={self.id} name={self.name!r} service_id={self.service_id}>"


# ---------------------------------------------------------------------------
# model_backends DDL — kept in sync with migrations/schema.sql (Task 1.1).
# Used by run_migrations() to create the table on existing DBs idempotently.
# ---------------------------------------------------------------------------

_MODEL_BACKENDS_CREATE_DDL = """\
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci\
"""


class ModelBackend(Base):
    """
    DB-driven backend registry entry (ADR-001: Dynamic Model Backend Management).

    One row per logical LLM backend managed via the admin UI. Encryption and
    decryption of api_key_enc is handled by app.services.model_backend_crypto —
    this ORM class stores and retrieves the column as a plain string.

    Ownership precedence (highest → lowest):
      1. config/models.yaml
      2. Kubernetes discovery
      3. This table (DatabaseBackendSource)

    If a model_id already exists in the registry from a higher-priority source,
    DatabaseBackendSource skips it and logs a warning. To hand a model over to
    DB management, remove it from models.yaml and insert it here via the admin UI.
    """

    __tablename__ = "model_backends"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Unique identifier for this model (e.g. "Qwen/Qwen3-8B")
    model_id: Mapped[str] = mapped_column(String(255), nullable=False)
    # Backend implementation type; v1 only uses 'static'; reserved for 'multi_cluster' later
    backend_type: Mapped[str] = mapped_column(
        String(64), nullable=False, default="static"
    )
    # Target inference server URL (stripped of trailing slash by the service layer)
    base_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    # Fernet-encrypted api_key; NULL means use global API_KEY env var fallback
    api_key_enc: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Soft-disable: False causes DatabaseBackendSource to unregister this model_id
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Origin label for audit/debug ('admin_ui', 'migration', etc.)
    source: Mapped[str] = mapped_column(String(64), nullable=False, default="admin_ui")
    # Monotonically increasing; incremented on every UPDATE for change detection.
    # NOTE: MySQL does NOT auto-increment this on UPDATE. Callers MUST explicitly
    # set version = version + 1 on every write, or change detection breaks.
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    # Admin who created this entry (nullable: SET NULL on user deletion)
    created_by: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    # Admin who last updated this entry (nullable: SET NULL on user deletion)
    updated_by: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("model_id", name="uq_model_backends_model_id"),
        Index("ix_model_backends_is_enabled", "is_enabled"),
    )

    # Relationships
    creator: Mapped[User | None] = relationship("User", foreign_keys=[created_by])
    updater: Mapped[User | None] = relationship("User", foreign_keys=[updated_by])

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<ModelBackend id={self.id} model_id={self.model_id!r} "
            f"backend_type={self.backend_type!r} is_enabled={self.is_enabled}>"
        )


class ContentFilterRule(Base):
    """
    Content filter rule for filtering user messages before they reach LLM backends.

    Supports three types of filtering:
    - REGEX: Pattern matching using regular expressions
    - KEYWORD: Simple keyword matching
    - EXTERNAL_URL: Check content via external API service
    """

    __tablename__ = "content_filter_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String(255), nullable=False, unique=True, index=True
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_type: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    # For REGEX and KEYWORD types
    pattern: Mapped[str | None] = mapped_column(Text, nullable=True)
    # For EXTERNAL_URL type
    external_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    # For EXTERNAL_URL type: extra parameters to include in the payload
    extra_params: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # For all types: comma-separated list of field names to check
    fields_to_check: Mapped[str | None] = mapped_column(
        String(1024), nullable=True, default="content"
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_blocking: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Violation message to show when content is blocked
    violation_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    created_by: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        Index("ix_content_filter_rules_active", "is_active"),
        Index("ix_content_filter_rules_type", "rule_type"),
    )

    # Relationships
    creator: Mapped[User | None] = relationship("User", foreign_keys=[created_by])

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<ContentFilterRule id={self.id} name={self.name!r} type={self.rule_type}>"
        )


class ModelCluster(Base):
    """
    Represents a model deployment cluster configuration.

    Tracks cluster assignments for models, including capacity settings,
    replica counts, and auto-scaling configuration. Each model can be
    deployed across multiple clusters with different priorities.
    """

    __tablename__ = "model_clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    cluster_id: Mapped[str] = mapped_column(String(255), nullable=False)
    base_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    api_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    number_of_replicas: Mapped[int | None] = mapped_column(Integer, nullable=True)
    seed_capacity_tokens_per_pod: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    auto_adapt_capacity: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Performance tuning knobs
    ttft_budget_ms: Mapped[float] = mapped_column(Float, nullable=False, default=500.0)
    itl_budget_ms: Mapped[float] = mapped_column(Float, nullable=False, default=50.0)
    ttft_alpha: Mapped[float] = mapped_column(Float, nullable=False, default=0.3)
    itl_alpha: Mapped[float] = mapped_column(Float, nullable=False, default=0.3)
    capacity_alpha: Mapped[float] = mapped_column(Float, nullable=False, default=0.1)

    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    # Timestamp of last YAML config load/synchronization
    config_updated_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    updated_by: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("model_id", "cluster_id", name="uq_model_cluster"),
        Index("ix_model_clusters_cluster_id", "cluster_id"),
        Index("ix_model_clusters_enabled", "is_enabled"),
        Index("ix_model_clusters_model_enabled", "model_id", "is_enabled"),
    )

    # Relationships
    updater: Mapped[User | None] = relationship("User", foreign_keys=[updated_by])

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<ModelCluster id={self.id} model_id={self.model_id!r} "
            f"cluster_id={self.cluster_id!r}>"
        )


class ModelClusterAuditLog(Base):
    """
    Audit log for model cluster configuration changes.

    Tracks all modifications to model cluster assignments, including
    capacity changes, replica updates, and enable/disable operations.
    Stores before/after values as JSON for complete change history.
    """

    __tablename__ = "model_cluster_audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_id: Mapped[str] = mapped_column(String(255), nullable=False)
    cluster_id: Mapped[str] = mapped_column(String(255), nullable=False)
    action: Mapped[str] = mapped_column(String(50), nullable=False)
    old_values: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    new_values: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    performed_by: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    performed_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_model_cluster_audit_model_id", "model_id"),
        Index("ix_model_cluster_audit_cluster_id", "cluster_id"),
        Index("ix_model_cluster_audit_performed_at", "performed_at"),
        Index(
            "ix_model_cluster_audit_model_cluster",
            "model_id",
            "cluster_id",
        ),
    )

    # Relationships
    performer: Mapped[User | None] = relationship("User", foreign_keys=[performed_by])

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<ModelClusterAuditLog id={self.id} model_id={self.model_id!r} "
            f"cluster_id={self.cluster_id!r} action={self.action!r}>"
        )


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields an async database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def create_all_tables() -> None:
    """Create all tables (used during application startup)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def run_migrations() -> None:
    """Run any runtime migrations that cannot be handled by create_all (e.g., adding columns to existing tables)."""
    from sqlalchemy import inspect

    async with engine.begin() as conn:

        def _columns_missing(sync_conn):
            inspector = inspect(sync_conn)
            buckets_cols = [c["name"] for c in inspector.get_columns("buckets")]
            missing = []
            if "default_model" not in buckets_cols:
                missing.append("default_model")
            return missing

        missing = await conn.run_sync(_columns_missing)
        dialect = conn.dialect.name

        if "default_model" in missing:
            logger.warning("Column 'default_model' missing from 'buckets', adding it now...")
            if dialect == "mysql":
                alter_sql = (
                    "ALTER TABLE buckets "
                    "ADD COLUMN default_model VARCHAR(255) DEFAULT NULL "
                    "AFTER is_default"
                )
            else:
                alter_sql = (
                    "ALTER TABLE buckets "
                    "ADD COLUMN default_model VARCHAR(255) DEFAULT NULL"
                )
            await conn.execute(text(alter_sql))
            logger.info("Column 'default_model' added to 'buckets' table")
        else:
            logger.info("Column 'default_model' already exists in 'buckets', skipping migration")

        # --- model_backends table (ADR-001) -----------------------------------
        def _model_backends_missing(sync_conn):
            inspector = inspect(sync_conn)
            return "model_backends" not in inspector.get_table_names()

        needs_model_backends = await conn.run_sync(_model_backends_missing)
        if needs_model_backends:
            logger.info("Table 'model_backends' not found — creating it now...")
            if dialect == "mysql":
                # MySQL-specific DDL (TINYINT, ENGINE=InnoDB, utf8mb4, ON UPDATE)
                await conn.execute(text(_MODEL_BACKENDS_CREATE_DDL))
            else:
                # Non-MySQL (e.g. SQLite test path): let SQLAlchemy create the
                # table from the ORM metadata with dialect-appropriate types.
                await conn.run_sync(
                    lambda sync_conn: Base.metadata.tables["model_backends"].create(
                        sync_conn, checkfirst=True
                    )
                )
            logger.info("Table 'model_backends' created successfully")
        else:
            logger.info("Table 'model_backends' already exists, skipping migration")

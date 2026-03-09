"""
MySQL / SQLAlchemy async database layer.

Defines the SQLAlchemy ORM models and provides an async session factory
plus the FastAPI dependency `get_db()`.
"""

from __future__ import annotations

import datetime
from typing import AsyncGenerator

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from app.core.config import get_settings

settings = get_settings()

# ---------------------------------------------------------------------------
# Engine & session factory
# ---------------------------------------------------------------------------

engine = create_async_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    echo=False,
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
    username: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Maximum number of simultaneous in-flight requests allowed for this user
    max_concurrent_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
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
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )

    # Relationships
    models: Mapped[list[BucketModel]] = relationship(
        "BucketModel", back_populates="bucket", lazy="select", cascade="all, delete-orphan"
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

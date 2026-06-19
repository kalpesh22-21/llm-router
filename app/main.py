"""
FastAPI application factory and lifespan management.

Startup sequence:
  1. Initialize Redis connection pool (required by other services)
  2. Create database tables (idempotent via create_all)
  3. Build backend registry from YAML configuration (without syncing to Redis)
  4. Synchronize YAML config to database (initialize_from_yaml)
  5. Start background sync services (start_sync)

Important Notes:
  - Redis is initialized FIRST to support subsequent operations
  - Database tables must exist before syncing YAML config to DB
  - The config_updated_at field tracks when YAML config was last loaded
  - Redis sync happens AFTER DB reconciliation, not before
  - Timestamp filtering prevents stale Redis data from overriding fresh DB state

Shutdown sequence (Graceful):
  1. On SIGTERM/SIGINT: Mark application as shutting down, reject new requests (503)
  2. Wait for active requests to complete (with timeout)
  3. Stop background services (config sync, drain monitor, content filter)
  4. Close Redis pool
  5. Close shared httpx client
  6. Dispose SQLAlchemy engine

Graceful Shutdown Behavior:
  - When SIGTERM is received, the application stops accepting new requests
  - Existing requests are allowed to complete (up to 30 seconds)
  - New requests receive HTTP 503 Service Unavailable during shutdown
  - Health check returns 503 during shutdown so load balancers remove the instance
"""

from __future__ import annotations

import logging
import pathlib
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import make_asgi_app

from app.api import admin, auth_routes, proxy
from app.backends import build_registry_from_config
from app.core.config import get_settings, load_settings_from_vault
from app.core.lifecycle import (
    setup_signal_handlers,
    wait_for_active_requests,
)
from app.core.middleware import GracefulShutdownMiddleware
from app.db.mysql import create_all_tables, run_migrations, engine, AsyncSessionLocal, User
from app.db.redis_client import close_redis, init_redis
from app.core.httpx import close_http_client
from app.services.cross_pod_sync_service import cross_pod_sync_service
from app.services.discovery_service import discovery_service
from app.services.drain_monitor import drain_monitor
from sqlalchemy import select


class HealthCheckFilter(logging.Filter):
    """Filter to exclude health check and metrics endpoints from access logs."""

    def filter(self, record: logging.LogRecord) -> bool:
        # Get the message from the record
        message = record.getMessage()
        # Filter out health and metrics endpoints
        excluded_paths = ["/health", "/metrics"]
        for path in excluded_paths:
            if f"GET {path} " in message or f" {path} " in message:
                return False
        return True


# Configure root logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
)

# Add filter to uvicorn access logger to exclude health checks
uvicorn_access_logger = logging.getLogger("uvicorn.access")
uvicorn_access_logger.addFilter(HealthCheckFilter())

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


async def _create_default_admin_user() -> None:
    """
    Create a default admin user on first startup if no users exist.

    This is useful for initial system setup when LDAP might not be configured.
    The admin password is set from ADMIN_INITIAL_PASSWORD environment variable.
    """
    settings = load_settings_from_vault()

    if not settings.ADMIN_INITIAL_PASSWORD:
        return

    async with AsyncSessionLocal() as session:
        try:
            # Check if any admin user exist
            result = await session.execute(
                select(User).where(User.username == settings.SUPERUSER_USERNAME)
            )
            admin = result.scalar_one_or_none()

            if admin is not None:
                # Users already exist, don't create default admin
                return

            # Create default admin user (password is not stored in DB)
            admin_user = User(
                username=settings.SUPERUSER_USERNAME,
                email="admin@localhost",
                is_active=True,
                is_admin=True,
                max_concurrent_requests=10,
            )
            session.add(admin_user)
            await session.commit()

            logger.info(
                "Default admin user created (username='admin', id=%d). "
                "Please change the password or create new users via the admin UI.",
                admin_user.id,
            )
        except Exception as exc:
            await session.rollback()
            logger.error("Failed to create default admin user: %s", exc)
            # Don't raise - this is not critical for startup


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application startup and shutdown."""
    logger.info("=== LLM Router starting up ===")

    # 0. Register graceful-shutdown signal handlers now that the event loop is
    # running and uvicorn's handlers are installed (so they can be chained).
    setup_signal_handlers()

    # 1. Init Redis first (required by other services)
    try:
        await init_redis()
        logger.info("Redis connection pool ready")
    except Exception as exc:
        logger.error("Redis initialisation failed: %s", exc)
        raise

    # 2. Create DB tables (idempotent, required before accessing DB)
    try:
        await create_all_tables()
        logger.info("Database tables ready")
    except Exception as exc:
        logger.error("Database initialisation failed: %s", exc)
        raise

    # 2.2. Run runtime migrations (e.g., add missing columns to existing tables)
    try:
        await run_migrations()
        logger.info("Runtime migrations complete")
    except Exception as exc:
        logger.warning("Runtime migrations failed (non-critical): %s", exc)

    # 2.5. Create default admin user if no users exist and ADMIN_INITIAL_PASSWORD is set
    try:
        await _create_default_admin_user()
    except Exception as exc:
        logger.warning("Default admin user creation failed (non-critical): %s", exc)

    # 3. Load model configs and build backend registry
    try:
        await build_registry_from_config()
        logger.info("Backend registry initialised")
    except Exception as exc:
        logger.error("Failed to build backend registry: %s", exc)
        raise

    # 3.5. Start the discovery service: run an initial probe of static backends
    # (so availability is correct before serving) and keep it in sync in the
    # background, so a down model returns 503 and auto-recovers without restart.
    try:
        await discovery_service.start()
        logger.info("Discovery service started")
    except Exception as exc:
        logger.warning("Discovery service failed to start (non-critical): %s", exc)

    # 4. Sync YAML config to database (must happen after DB tables exist)
    try:
        await cross_pod_sync_service.initialize_from_yaml()
        logger.info("Configuration synchronized from YAML to database")
    except Exception as exc:
        logger.warning("Config sync from YAML failed (non-critical): %s", exc)

    # 5. Start background services (cross-pod sync polling and drain monitor)
    try:
        await cross_pod_sync_service.start_sync()
        logger.info("Cross-pod sync service started")
    except Exception as exc:
        logger.warning("Cross-pod sync service failed to start (non-critical): %s", exc)

    try:
        await drain_monitor.start_monitoring()
        logger.info("Drain monitor service started")
    except Exception as exc:
        logger.warning("Drain monitor service failed to start (non-critical): %s", exc)

    # 6. Start content filter background updater
    try:
        from app.services.content_filter import content_filter_service
        content_filter_service.start_background_updater()
        logger.info("Content filter background updater started")
    except Exception as exc:
        logger.error("Content filter background updater failed to start: %s", exc)
        raise

    logger.info("=== LLM Router ready to serve requests ===")

    # Yield control to FastAPI - this keeps the app running
    yield

    # Shutdown
    logger.info("=== LLM Router shutting down ===")

    # 1. Wait for active requests to complete (graceful shutdown)
    try:
        await wait_for_active_requests(timeout=30.0)
    except Exception as exc:
        logger.warning("Error waiting for active requests: %s", exc)

    # 2. Stop content filter background updater
    try:
        content_filter_service.stop_background_updater()
        logger.info("Content filter background updater stopped")
    except Exception as exc:
        logger.warning("Content filter background updater cleanup error: %s", exc)

    # 3. Stop background services
    try:
        await cross_pod_sync_service.stop_sync()
        logger.info("Cross-pod sync service stopped")
    except Exception as exc:
        logger.warning("Error stopping cross-pod sync service: %s", exc)
    
    try:
        await drain_monitor.stop_monitoring()
        logger.info("Drain monitor service stopped")
    except Exception as exc:
        logger.warning("Error stopping drain monitor service: %s", exc)

    try:
        await discovery_service.stop()
        logger.info("Discovery service stopped")
    except Exception as exc:
        logger.warning("Error stopping discovery service: %s", exc)

    await close_redis()
    await close_http_client()
    await engine.dispose()
    logger.info("=== LLM Router shut down cleanly ===")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()
    app = FastAPI(
        title="LLM Router",
        description=(
            "Phase 1 static-config LLM proxy with LDAP auth, "
            "per-user rate limiting, bucket-based access control, "
            "and Prometheus metrics."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs" if settings.DOCS_ENABLED else None,
        redoc_url="/redoc" if settings.DOCS_ENABLED else None,
    )

    # ------------------------------------------------------------------ #
    # Graceful shutdown middleware (must be first to catch all requests)
    # ------------------------------------------------------------------ #
    app.add_middleware(GracefulShutdownMiddleware)

    # ------------------------------------------------------------------ #
    # CORS
    # ------------------------------------------------------------------ #
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Tighten in production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # NOTE: signal handlers are registered inside the lifespan startup (not here)
    # so that a running event loop exists and uvicorn's own handlers are already
    # installed to chain to. Registering at create_app() time captured _loop=None,
    # which silently disabled the graceful drain.

    # ------------------------------------------------------------------ #
    # Routers
    # ------------------------------------------------------------------ #
    app.include_router(auth_routes.router)
    app.include_router(admin.router)
    app.include_router(proxy.router)

    # ------------------------------------------------------------------ #
    # Prometheus metrics endpoint
    # ------------------------------------------------------------------ #
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    # ------------------------------------------------------------------ #
    # UI (static SPA)
    # ------------------------------------------------------------------ #
    _static_dir = pathlib.Path(__file__).parent / "static"
    app.mount("/ui", StaticFiles(directory=_static_dir, html=True), name="ui")

    # ------------------------------------------------------------------ #
    # Global exception handlers
    # ------------------------------------------------------------------ #

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        logger.exception(
            "Unhandled exception on %s %s", request.method, request.url.path
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "An internal server error occurred"},
        )

    # ------------------------------------------------------------------ #
    # Health check
    # ------------------------------------------------------------------ #

    @app.get("/health", tags=["health"], include_in_schema=False)
    async def health_check() -> dict:
        return {"status": "ok"}

    return app


app = create_app()

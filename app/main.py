"""
FastAPI application factory and lifespan management.

Startup sequence:
  1. Parse models.yaml and build BackendRegistry
  2. Create database tables (idempotent via create_all)
  3. Initialise Redis connection pool

Shutdown sequence:
  1. Close Redis pool
  2. Close shared httpx client
  3. Dispose SQLAlchemy engine
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
from app.backends.static import build_registry_from_config, close_http_client
from app.db.mysql import create_all_tables, engine
from app.db.redis_client import close_redis, init_redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application startup and shutdown."""
    logger.info("=== LLM Router starting up ===")

    # 1. Load model configs and build backend registry
    try:
        build_registry_from_config()
        logger.info("Backend registry initialised")
    except Exception as exc:
        logger.error("Failed to build backend registry: %s", exc)
        raise

    # 2. Create DB tables (idempotent)
    try:
        await create_all_tables()
        logger.info("Database tables ready")
    except Exception as exc:
        logger.error("Database initialisation failed: %s", exc)
        raise

    # 3. Init Redis
    try:
        await init_redis()
        logger.info("Redis connection pool ready")
    except Exception as exc:
        logger.error("Redis initialisation failed: %s", exc)
        raise

    logger.info("=== LLM Router ready to serve requests ===")

    yield

    # Shutdown
    logger.info("=== LLM Router shutting down ===")
    await close_redis()
    await close_http_client()
    await engine.dispose()
    logger.info("=== LLM Router shut down cleanly ===")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="LLM Router",
        description=(
            "Phase 1 static-config LLM proxy with LDAP auth, "
            "per-user rate limiting, bucket-based access control, "
            "and Prometheus metrics."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

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

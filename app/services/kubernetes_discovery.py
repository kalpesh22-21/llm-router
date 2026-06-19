"""
KubernetesServiceDiscovery — register backends from labelled K8s Services.

A second discovery *source* (DISCOVERY_MODE in {kubernetes, both}): instead of
(or alongside) static models.yaml, periodically list Services in the configured
namespaces matching a label selector and register one StaticBackend per service.
The existing DiscoveryService then health-probes them like any other backend.

Design (per the agreed decisions):
  - model_id comes from a configurable label (DISCOVERY_K8S_MODEL_ID_LABEL).
  - Periodic LIST + reconcile every DISCOVERY_K8S_RESYNC_SECONDS (no watch).
  - URL = <scheme>://<svc>.<ns>.<cluster_domain>:<port><path>, from config
    defaults with optional per-service label overrides.
  - "both" mode is safe: this only ever touches model_ids it owns (tracked in
    self._managed); it never removes or overwrites a config-defined model.
  - The K8s client is behind a ServiceLister so tests/E2E inject a fake without
    a real cluster; the kubernetes_asyncio import is guarded so the app boots in
    config mode without it.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol

from app.backends.base import registry
from app.backends.static import StaticBackend
from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


# --------------------------------------------------------------------------- #
# Service listing abstraction
# --------------------------------------------------------------------------- #


@dataclass
class DiscoveredService:
    name: str
    namespace: str
    labels: Dict[str, str] = field(default_factory=dict)


class ServiceLister(Protocol):
    async def list_services(self) -> List[DiscoveredService]: ...
    async def close(self) -> None: ...


class FakeServiceLister:
    """Test/E2E lister: re-reads a JSON file each call so services can be
    added/removed live by editing the file. Format:
        [{"name": "...", "namespace": "...", "labels": {"...": "..."}}]
    """

    def __init__(self, path: str) -> None:
        self._path = path

    async def list_services(self) -> List[DiscoveredService]:
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except FileNotFoundError:
            return []
        except Exception as exc:
            logger.warning("FakeServiceLister: failed to read %s: %s", self._path, exc)
            return []
        return [
            DiscoveredService(
                name=s["name"],
                namespace=s.get("namespace", "default"),
                labels=dict(s.get("labels", {})),
            )
            for s in data
        ]

    async def close(self) -> None:
        return None


# kubernetes_asyncio is optional — only needed for the real K8s lister.
try:
    from kubernetes_asyncio import client as _k8s_client
    from kubernetes_asyncio import config as _k8s_config

    _K8S_AVAILABLE = True
except ImportError:
    _k8s_client = None
    _k8s_config = None
    _K8S_AVAILABLE = False


class K8sServiceLister:
    """Lists Services from the Kubernetes API (in-cluster or via kubeconfig)."""

    def __init__(self) -> None:
        self._api = None

    async def _ensure_api(self) -> None:
        if self._api is not None:
            return
        if not _K8S_AVAILABLE:
            raise RuntimeError(
                "kubernetes_asyncio is not installed but DISCOVERY_MODE requires "
                "Kubernetes discovery. Install it or set DISCOVERY_MODE=config."
            )
        if settings.DISCOVERY_K8S_KUBECONFIG:
            await _k8s_config.load_kube_config(
                config_file=settings.DISCOVERY_K8S_KUBECONFIG
            )
        else:
            _k8s_config.load_incluster_config()
        self._api = _k8s_client.CoreV1Api()

    async def list_services(self) -> List[DiscoveredService]:
        await self._ensure_api()
        namespaces = [
            n.strip()
            for n in settings.DISCOVERY_K8S_NAMESPACES.split(",")
            if n.strip()
        ]
        selector = settings.DISCOVERY_K8S_LABEL_SELECTOR
        out: List[DiscoveredService] = []
        for ns in namespaces:
            resp = await self._api.list_namespaced_service(ns, label_selector=selector)
            for svc in resp.items:
                out.append(
                    DiscoveredService(
                        name=svc.metadata.name,
                        namespace=svc.metadata.namespace,
                        labels=dict(svc.metadata.labels or {}),
                    )
                )
        return out

    async def close(self) -> None:
        if self._api is not None:
            await self._api.api_client.close()
            self._api = None


def make_default_lister() -> ServiceLister:
    """Pick the lister from settings: fake-file (tests/E2E) else real K8s."""
    if settings.DISCOVERY_K8S_FAKE_SERVICES_FILE:
        logger.warning(
            "KubernetesServiceDiscovery using FAKE lister from %s (test/E2E only)",
            settings.DISCOVERY_K8S_FAKE_SERVICES_FILE,
        )
        return FakeServiceLister(settings.DISCOVERY_K8S_FAKE_SERVICES_FILE)
    return K8sServiceLister()


# --------------------------------------------------------------------------- #
# Discovery service
# --------------------------------------------------------------------------- #


class KubernetesServiceDiscovery:
    """Reconciles the backend registry from labelled K8s Services."""

    def __init__(self, lister: Optional[ServiceLister] = None) -> None:
        self._lister = lister
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        # model_id -> base_url, for the backends THIS service owns (so we never
        # touch config-defined models, and can detect endpoint changes).
        self._managed: Dict[str, str] = {}

    # ------------------------------------------------------------------ #
    # Service -> backend mapping
    # ------------------------------------------------------------------ #

    def _service_to_backend(self, svc: DiscoveredService):
        """Return (model_id, base_url, StaticBackend) or None if not a model svc."""
        model_id = svc.labels.get(settings.DISCOVERY_K8S_MODEL_ID_LABEL, "").strip()
        if not model_id:
            logger.debug(
                "Service %s/%s has no %s label; skipping",
                svc.namespace,
                svc.name,
                settings.DISCOVERY_K8S_MODEL_ID_LABEL,
            )
            return None

        scheme = svc.labels.get(settings.DISCOVERY_K8S_SCHEME_LABEL) or settings.DISCOVERY_SCHEME
        port = svc.labels.get(settings.DISCOVERY_K8S_PORT_LABEL) or settings.DISCOVERY_DEFAULT_PORT
        path = svc.labels.get(settings.DISCOVERY_K8S_PATH_LABEL) or settings.DISCOVERY_PATH
        if not str(path).startswith("/"):
            path = "/" + str(path)

        host = f"{svc.name}.{svc.namespace}.{settings.DISCOVERY_K8S_CLUSTER_DOMAIN}"
        base_url = f"{scheme}://{host}:{port}{path}"

        api_key = settings.API_KEY if settings.API_KEY not in ("", "EMPTY") else ""
        backend = StaticBackend(model_id=model_id, base_url=base_url, api_key=api_key)
        return model_id, backend.base_url, backend

    # ------------------------------------------------------------------ #
    # Reconciliation
    # ------------------------------------------------------------------ #

    async def reconcile(self) -> None:
        services = await self._lister.list_services()

        desired: Dict[str, tuple] = {}  # model_id -> (base_url, backend)
        for svc in services:
            mapped = self._service_to_backend(svc)
            if mapped is None:
                continue
            model_id, base_url, backend = mapped
            if model_id in desired:
                logger.warning(
                    "Duplicate model_id %r from multiple services; keeping the first",
                    model_id,
                )
                continue
            desired[model_id] = (base_url, backend)

        # Additions / endpoint changes.
        for model_id, (base_url, backend) in desired.items():
            if model_id in self._managed:
                if self._managed[model_id] != base_url:
                    registry.upsert(backend)
                    self._managed[model_id] = base_url
                    logger.info("Discovery: updated %r -> %s", model_id, base_url)
                continue
            if registry.contains(model_id):
                # Owned by static config (or another source) — never clobber it.
                logger.warning(
                    "Discovery: model_id %r already registered (config-owned); "
                    "skipping discovered service",
                    model_id,
                )
                continue
            registry.upsert(backend)
            self._managed[model_id] = base_url
            logger.info("Discovery: registered %r -> %s", model_id, base_url)

        # Removals: backends we own whose service has disappeared.
        for model_id in list(self._managed.keys()):
            if model_id not in desired:
                registry.unregister(model_id)
                del self._managed[model_id]
                logger.info("Discovery: unregistered %r (service gone)", model_id)

        logger.debug(
            "Discovery reconcile complete: %d managed model(s)", len(self._managed)
        )

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def _loop(self) -> None:
        interval = settings.DISCOVERY_K8S_RESYNC_SECONDS
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            try:
                await self.reconcile()
            except Exception:
                logger.exception("K8s discovery reconcile failed; retrying next resync")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def start(self) -> None:
        """Run an initial reconcile, then start the background resync loop."""
        if self._task is not None:
            logger.warning("KubernetesServiceDiscovery already started")
            return
        if self._lister is None:
            self._lister = make_default_lister()
        self._stop_event = asyncio.Event()
        try:
            await self.reconcile()  # initial pass before serving
        except Exception:
            # Don't let a transient API error block startup — the loop retries.
            logger.exception("K8s discovery initial reconcile failed; loop will retry")
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "KubernetesServiceDiscovery started (resync=%ss, namespaces=%r, selector=%r)",
            settings.DISCOVERY_K8S_RESYNC_SECONDS,
            settings.DISCOVERY_K8S_NAMESPACES,
            settings.DISCOVERY_K8S_LABEL_SELECTOR,
        )

    async def stop(self) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Error awaiting K8s discovery task during stop")
            self._task = None
        if self._lister is not None:
            try:
                await self._lister.close()
            except Exception:
                logger.exception("Error closing service lister")
        logger.info("KubernetesServiceDiscovery stopped")


# Module-level singleton, started/stopped from the app lifespan.
kubernetes_discovery = KubernetesServiceDiscovery()

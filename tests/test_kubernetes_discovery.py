"""
Unit tests for KubernetesServiceDiscovery: Service->backend mapping and the
reconcile loop (add / remove / skip-config-owned / endpoint-change).

Pure/mocked — an in-memory fake lister stands in for the K8s API, and the
module-level registry is swapped for a fresh one per test. asyncio.run() keeps
this to core pytest (no pytest-asyncio).
"""

import asyncio

import pytest

from app.backends.base import BackendRegistry
from app.backends.static import StaticBackend
import app.services.kubernetes_discovery as kd
from app.services.kubernetes_discovery import (
    DiscoveredService,
    KubernetesServiceDiscovery,
)


class _Lister:
    def __init__(self, services=None):
        self.services = list(services or [])

    async def list_services(self):
        return list(self.services)

    async def close(self):
        return None


def _svc(name, model_id=None, ns="default", **labels):
    lbls = dict(labels)
    if model_id is not None:
        lbls["llm-router/model-id"] = model_id
    return DiscoveredService(name=name, namespace=ns, labels=lbls)


@pytest.fixture
def reg(monkeypatch):
    r = BackendRegistry()
    monkeypatch.setattr(kd, "registry", r)
    return r


# --------------------------------------------------------------------------- #
# Service -> backend mapping
# --------------------------------------------------------------------------- #


def test_maps_service_with_defaults(reg):
    d = KubernetesServiceDiscovery(_Lister())
    mapped = d._service_to_backend(_svc("vllm-qwen", model_id="Qwen/Qwen3-8B"))
    assert mapped is not None
    model_id, base_url, backend = mapped
    assert model_id == "Qwen/Qwen3-8B"  # model id can contain '/'
    assert base_url == "http://vllm-qwen.default.svc.cluster.local:8000/v1"
    assert isinstance(backend, StaticBackend)


def test_per_service_label_overrides(reg):
    d = KubernetesServiceDiscovery(_Lister())
    svc = _svc(
        "m",
        model_id="x",
        **{"llm-router/port": "9000", "llm-router/scheme": "https", "llm-router/path": "v2"},
    )
    _, base_url, _ = d._service_to_backend(svc)
    assert base_url == "https://m.default.svc.cluster.local:9000/v2"


def test_service_without_model_id_label_is_skipped(reg):
    d = KubernetesServiceDiscovery(_Lister())
    assert d._service_to_backend(_svc("m")) is None


# --------------------------------------------------------------------------- #
# Reconcile
# --------------------------------------------------------------------------- #


def test_reconcile_registers_new_service(reg):
    d = KubernetesServiceDiscovery(_Lister([_svc("svc-a", model_id="model-a")]))
    asyncio.run(d.reconcile())
    b = reg.get("model-a")
    assert b is not None
    assert b.base_url == "http://svc-a.default.svc.cluster.local:8000/v1"


def test_reconcile_removes_vanished_service(reg):
    lister = _Lister([_svc("svc-a", model_id="model-a")])
    d = KubernetesServiceDiscovery(lister)
    asyncio.run(d.reconcile())
    assert reg.contains("model-a")

    lister.services = []  # service disappears
    asyncio.run(d.reconcile())
    assert not reg.contains("model-a")


def test_reconcile_never_touches_config_owned_model(reg):
    # A static/config-owned backend exists for the same model_id.
    reg.register(StaticBackend("model-a", "http://static/v1", "k"))
    lister = _Lister([_svc("svc-a", model_id="model-a")])
    d = KubernetesServiceDiscovery(lister)

    asyncio.run(d.reconcile())
    # Discovery must NOT overwrite the config backend...
    assert reg.get("model-a").base_url == "http://static/v1"

    # ...and must NOT remove it when the service later vanishes.
    lister.services = []
    asyncio.run(d.reconcile())
    assert reg.contains("model-a")
    assert reg.get("model-a").base_url == "http://static/v1"


def test_reconcile_updates_on_endpoint_change(reg):
    lister = _Lister([_svc("svc-a", model_id="model-a")])
    d = KubernetesServiceDiscovery(lister)
    asyncio.run(d.reconcile())

    # Same model_id, new port → endpoint changes → backend replaced.
    lister.services = [_svc("svc-a", model_id="model-a", **{"llm-router/port": "9999"})]
    asyncio.run(d.reconcile())
    assert reg.get("model-a").base_url == "http://svc-a.default.svc.cluster.local:9999/v1"

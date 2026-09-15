"""Backend registry — how "configuration only" is actually implemented.

Every adapter in :mod:`ano.gcp` is obtained through :func:`build`, which looks
up a factory by ``(service, backend)``. Application code therefore never names
a backend, never imports a client library, and never branches on "are we in the
cloud?" — it asks the registry for the service the configuration selected.

::

    store = build("store", cfg)          # AnalyticalStore, local or BigQuery
    store.ensure_schema()
    store.insert_rows("metric_samples", rows)

The offline substitutes register themselves when their module is imported (which
:mod:`ano.gcp` does for you).

The ``gcp`` backend
-------------------
The real Google Cloud client libraries are **not** vendored here. This machine
has no package manager, and requirement R6 forbids a runtime network
dependency — see ``.agents/teamwork_preview_orchestrator/DECISIONS.md`` D1 and
the recon evidence in ``.agents/explorer_survey_env/handoff.md`` (21 of 23
third-party packages absent; ``pip`` does not exist).

So a deployment supplies them: ``Config.backend_plugin`` names a module present
in the deployment image, which is imported on first use and whose ``register()``
function installs the real factories. Nothing in this repository changes.

If a ``gcp`` backend is selected and no plugin provides it, :func:`build` raises
:class:`BackendUnavailableError` naming the exact client library that is
missing. **It never falls back to the local substitute.** A run that claims to
be writing to BigQuery while actually appending to a local file would be
worthless as a demonstration and dishonest as a deliverable.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any

from ano.config import GCP, LOCAL, SERVICES, Config

__all__ = [
    "BackendUnavailableError",
    "register_backend",
    "unregister_backend",
    "registered_backends",
    "build",
    "GCP_SERVICE_NAMES",
    "GCP_CLIENT_LIBRARIES",
]

#: The real Google Cloud service each adapter stands in for. Also used in error
#: messages so an operator can see immediately what would be talked to.
GCP_SERVICE_NAMES: dict[str, str] = {
    "logging": "Cloud Logging",
    "metrics": "Google Cloud Managed Service for Prometheus",
    "messaging": "Pub/Sub",
    "pipeline": "Dataflow",
    "store": "BigQuery",
    "alerting": "Cloud Monitoring",
}

#: The client library a deployment plugin would use for each service.
GCP_CLIENT_LIBRARIES: dict[str, str] = {
    "logging": "google-cloud-logging",
    "metrics": "google-cloud-monitoring",
    "messaging": "google-cloud-pubsub",
    "pipeline": "apache-beam[gcp] (DataflowRunner)",
    "store": "google-cloud-bigquery",
    "alerting": "google-cloud-monitoring",
}

_FACTORIES: dict[tuple[str, str], Callable[[Config], Any]] = {}
_LOADED_PLUGINS: set[str] = set()


class BackendUnavailableError(RuntimeError):
    """Raised when the configured backend has no registered implementation."""


def register_backend(
    service: str, backend: str, factory: Callable[[Config], Any]
) -> None:
    """Register a factory for ``(service, backend)``.

    Args:
        service: One of :data:`ano.config.SERVICES`.
        backend: ``"local"`` or ``"gcp"``.
        factory: Callable taking a :class:`ano.config.Config` and returning the
            adapter instance.

    Raises:
        ValueError: for an unknown service or backend.
    """
    if service not in SERVICES:
        raise ValueError(
            f"unknown service {service!r}; expected one of {list(SERVICES)}"
        )
    if backend not in (LOCAL, GCP):
        raise ValueError(f"unknown backend {backend!r}; expected 'local' or 'gcp'")
    if not callable(factory):
        raise TypeError("factory must be callable")
    _FACTORIES[(service, backend)] = factory


def unregister_backend(service: str, backend: str) -> None:
    """Remove a registration. Used by tests; harmless if absent."""
    _FACTORIES.pop((service, backend), None)


def registered_backends() -> tuple[tuple[str, str], ...]:
    """Every registered ``(service, backend)`` pair, sorted."""
    return tuple(sorted(_FACTORIES))


def _load_plugin(config: Config) -> None:
    """Import and run ``Config.backend_plugin`` once, if configured."""
    plugin = config.backend_plugin
    if not plugin or plugin in _LOADED_PLUGINS:
        return
    try:
        module = importlib.import_module(plugin)
    except ImportError as exc:
        raise BackendUnavailableError(
            f"backend_plugin {plugin!r} could not be imported: {exc}. It must be "
            "present in the deployment image and expose register() to install "
            "the real Google Cloud client factories."
        ) from exc
    register = getattr(module, "register", None)
    if not callable(register):
        raise BackendUnavailableError(
            f"backend_plugin {plugin!r} does not expose a callable register()"
        )
    register(register_backend)
    _LOADED_PLUGINS.add(plugin)


def build(service: str, config: Config) -> Any:
    """Construct the adapter for ``service`` under ``config``.

    Args:
        service: One of :data:`ano.config.SERVICES`.
        config: The run configuration. ``config.backend_for(service)`` decides
            which implementation is built.

    Returns:
        The adapter instance.

    Raises:
        BackendUnavailableError: if the selected backend has no implementation.
            The message names the real service and the client library a
            deployment plugin would need to provide.
    """
    backend = config.backend_for(service)
    if backend == GCP:
        _load_plugin(config)
    factory = _FACTORIES.get((service, backend))
    if factory is None:
        if backend == GCP:
            raise BackendUnavailableError(
                f"configuration selects the real {GCP_SERVICE_NAMES[service]} "
                f"backend for service {service!r}, but no implementation is "
                "registered. The Google Cloud client libraries are deliberately "
                "not vendored into this repository (no package manager is "
                "available and requirement R6 forbids a runtime network "
                "dependency). Deploy with Config.backend_plugin naming a module "
                f"that registers a {service!r}/'gcp' factory built on "
                f"{GCP_CLIENT_LIBRARIES[service]}, or set "
                f"{service}_backend='local' to use the offline substitute. "
                "This build will not silently fall back to the local "
                "substitute."
            )
        raise BackendUnavailableError(
            f"no {backend!r} implementation registered for service {service!r}; "
            "importing ano.gcp registers the offline substitutes"
        )
    return factory(config)

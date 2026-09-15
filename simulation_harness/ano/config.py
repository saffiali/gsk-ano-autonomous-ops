"""Single configuration object for the whole system.

Requirement R6 is the reason this module exists:

    "The same application and pipeline code must be deployable to a real Google
    Cloud project without modification - configuration only."

So every Google Cloud adapter in :mod:`ano.gcp` is constructed from a
:class:`Config`, and the choice between the offline local substitute and the
real managed service is a *field value*, never an ``if`` in application code and
never a different import.

Two profiles ship with the code — :func:`local_profile` and
:func:`gcp_profile`. Diff them with::

    python3 -m ano.config --profile local > /tmp/local.json
    python3 -m ano.config --profile gcp   > /tmp/gcp.json
    diff /tmp/local.json /tmp/gcp.json

The diff contains data only. That is the evidence for R6's "configuration only"
clause.

Where a Config comes from, in precedence order
----------------------------------------------
1. Explicit keyword arguments / :meth:`Config.with_overrides`.
2. A JSON file named by ``ANO_CONFIG_FILE`` or passed to
   :meth:`Config.from_json`.
3. Environment variables prefixed ``ANO_`` (see :meth:`Config.from_env`).
4. The ``local`` profile defaults, which are what a clean checkout runs with:
   offline, no credentials, artifacts under ``artifacts/``.

Extension without editing this file
-----------------------------------
``Config.options`` is an open string-keyed map for milestone-specific settings
(``options["embedding_dim"] = "128"``), reachable from the environment as
``ANO_OPTION_EMBEDDING_DIM``. Use :meth:`Config.option_int`,
:meth:`Config.option_float` and :meth:`Config.option_bool` to read them with a
typed default. This keeps ownership of this file with milestone M1 while
letting every other milestone configure itself.

What "backend: gcp" does here
-----------------------------
Selecting ``gcp`` selects the *real* managed service. The Google Cloud client
libraries are deliberately **not** vendored into this repository — the
environment has no package manager and requirement R6 forbids a network
dependency at runtime (see ``.agents/teamwork_preview_orchestrator/DECISIONS.md``
D1). Instead, ``backend_plugin`` names a module that is present in the
deployment image and that registers the real clients. If ``gcp`` is selected
and no plugin is configured, adapter construction **fails loudly** with an
explanation. It never silently falls back to the local substitute: a demo that
claims to be talking to BigQuery while writing to a local file would be a lie.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, replace
from typing import Any

__all__ = [
    "LOCAL",
    "GCP",
    "BACKENDS",
    "SERVICES",
    "ENV_PREFIX",
    "ConfigError",
    "Config",
    "local_profile",
    "gcp_profile",
    "load_config",
    "default_config",
]

#: Offline local substitute (the default; what a clean checkout runs).
LOCAL = "local"
#: Real Google Cloud managed service.
GCP = "gcp"
#: Every legal backend value.
BACKENDS: frozenset[str] = frozenset({LOCAL, GCP})

#: The services whose backend can be selected independently. Each maps to one
#: adapter in :mod:`ano.gcp`.
SERVICES: tuple[str, ...] = (
    "logging",
    "metrics",
    "messaging",
    "pipeline",
    "store",
    "alerting",
)

#: Environment variable prefix.
ENV_PREFIX = "ANO_"

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})


class ConfigError(ValueError):
    """Raised for an invalid or unusable configuration."""


def _parse_bool(text: str, name: str) -> bool:
    lowered = text.strip().lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise ConfigError(
        f"{name}: expected a boolean (one of "
        f"{sorted(_TRUE | _FALSE)}), got {text!r}"
    )


@dataclass(frozen=True, slots=True)
class Config:
    """Immutable configuration for one run of the system.

    Backend selection
    -----------------
    ``backend`` is the default for every service. Any service may override it
    (``store_backend="gcp"`` while everything else stays local), which is what
    makes a staged migration to Google Cloud expressible as configuration.

    Attributes:
        backend: Default backend, ``"local"`` or ``"gcp"``.
        logging_backend: Override for Cloud Logging, or ``None`` to inherit.
        metrics_backend: Override for Google Managed Prometheus.
        messaging_backend: Override for Pub/Sub.
        pipeline_backend: Override for Dataflow.
        store_backend: Override for BigQuery.
        alerting_backend: Override for Cloud Monitoring.
        backend_plugin: Dotted module path providing the real Google Cloud
            client factories, imported only when a ``gcp`` backend is selected.
            See :mod:`ano.gcp.registry`.
        project_id: Google Cloud project id. Required when any backend is
            ``gcp``; ignored offline.
        location: Google Cloud location/region, e.g. ``"europe-west2"``.
        artifacts_dir: Root for everything the run writes. Git-ignored.
        seed: Master seed. Every stochastic component derives its own stream
            from this via :func:`ano.contracts.determinism.rng`.
        log_name: Cloud Logging log id (the ``logName`` suffix).
        log_path: Offline JSONL destination, or ``None`` to derive it from
            ``artifacts_dir``.
        metric_prefix: Metric name prefix, e.g. ``"ano"``.
        metric_path: Offline metric destination, or ``None`` to derive it.
        topic_prefix: Pub/Sub topic name prefix.
        subscription_prefix: Pub/Sub subscription name prefix.
        dataset: BigQuery dataset id.
        sqlite_path: Offline analytical store path. ``":memory:"`` for an
            ephemeral store; ``None`` derives a file under ``artifacts_dir``.
        alert_path: Offline alert destination, or ``None`` to derive it.
        notification_channel: Cloud Monitoring notification channel resource
            name, used only when alerting runs against the real service.
        options: Open map of milestone-specific settings.
    """

    backend: str = LOCAL
    logging_backend: str | None = None
    metrics_backend: str | None = None
    messaging_backend: str | None = None
    pipeline_backend: str | None = None
    store_backend: str | None = None
    alerting_backend: str | None = None
    backend_plugin: str | None = None

    project_id: str | None = None
    location: str = "europe-west2"

    artifacts_dir: str = "artifacts"
    seed: int = 1234

    log_name: str = "ano-telemetry"
    log_path: str | None = None

    metric_prefix: str = "ano"
    metric_path: str | None = None

    topic_prefix: str = "ano"
    subscription_prefix: str = "ano-sub"

    dataset: str = "ano"
    sqlite_path: str | None = None

    alert_path: str | None = None
    notification_channel: str | None = None

    options: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.validate()

    # -- validation --------------------------------------------------------
    def validate(self) -> None:
        """Raise :class:`ConfigError` if this configuration is unusable."""
        if self.backend not in BACKENDS:
            raise ConfigError(
                f"backend must be one of {sorted(BACKENDS)}, got {self.backend!r}"
            )
        for service in SERVICES:
            value = getattr(self, f"{service}_backend")
            if value is not None and value not in BACKENDS:
                raise ConfigError(
                    f"{service}_backend must be one of {sorted(BACKENDS)} or null, "
                    f"got {value!r}"
                )
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise ConfigError(f"seed must be an integer, got {self.seed!r}")
        if not isinstance(self.options, Mapping):
            raise ConfigError("options must be a mapping of string to string")
        for key, value in self.options.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ConfigError(
                    "options keys and values must both be strings; got "
                    f"{key!r}: {value!r}"
                )
        if self.uses_gcp() and not self.project_id:
            services = ", ".join(
                service for service in SERVICES if self.backend_for(service) == GCP
            )
            raise ConfigError(
                f"project_id is required because these services are configured "
                f"for the real Google Cloud backend: {services}"
            )

    # -- backend resolution ------------------------------------------------
    def backend_for(self, service: str) -> str:
        """Return the resolved backend for ``service``.

        Args:
            service: One of :data:`SERVICES`.

        Returns:
            ``"local"`` or ``"gcp"``.
        """
        if service not in SERVICES:
            raise ConfigError(
                f"unknown service {service!r}; expected one of {list(SERVICES)}"
            )
        override = getattr(self, f"{service}_backend")
        return override if override is not None else self.backend

    def is_local(self, service: str) -> bool:
        """True if ``service`` resolves to the offline local substitute."""
        return self.backend_for(service) == LOCAL

    def uses_gcp(self) -> bool:
        """True if any service resolves to the real Google Cloud backend."""
        return any(self.backend_for(service) == GCP for service in SERVICES)

    # -- derived paths -----------------------------------------------------
    def artifact_path(self, *parts: str) -> str:
        """Path under ``artifacts_dir``; does not create anything."""
        return os.path.join(self.artifacts_dir, *parts)

    def ensure_artifacts_dir(self, *parts: str) -> str:
        """Create and return a directory under ``artifacts_dir``."""
        path = self.artifact_path(*parts)
        os.makedirs(path, exist_ok=True)
        return path

    def resolved_log_path(self) -> str:
        """Offline destination for Cloud Logging entries."""
        if self.log_path:
            return self.log_path
        return self.artifact_path("logs", f"{self.log_name}.jsonl")

    def resolved_metric_path(self) -> str:
        """Offline destination for Managed Prometheus samples."""
        if self.metric_path:
            return self.metric_path
        return self.artifact_path("metrics", f"{self.metric_prefix}-samples.jsonl")

    def resolved_sqlite_path(self) -> str:
        """Offline analytical-store path, or ``":memory:"``."""
        if self.sqlite_path:
            return self.sqlite_path
        return self.artifact_path("store", f"{self.dataset}.sqlite3")

    def resolved_alert_path(self) -> str:
        """Offline destination for Cloud Monitoring alerts."""
        if self.alert_path:
            return self.alert_path
        return self.artifact_path("alerts", "alerts.jsonl")

    def qualified_topic(self, topic: str) -> str:
        """Prefix a Pub/Sub topic name, mirroring real project naming."""
        return f"{self.topic_prefix}-{topic}" if self.topic_prefix else topic

    def qualified_subscription(self, subscription: str) -> str:
        """Prefix a Pub/Sub subscription name."""
        return (
            f"{self.subscription_prefix}-{subscription}"
            if self.subscription_prefix
            else subscription
        )

    def qualified_metric(self, name: str) -> str:
        """Prefix a metric name, mirroring GMP metric naming."""
        return f"{self.metric_prefix}_{name}" if self.metric_prefix else name

    # -- typed option access ----------------------------------------------
    def option(self, name: str, default: str | None = None) -> str | None:
        """Return an open-map option as text."""
        return self.options.get(name, default)

    def option_int(self, name: str, default: int) -> int:
        """Return an option parsed as ``int``."""
        raw = self.options.get(name)
        if raw is None:
            return default
        try:
            return int(raw)
        except ValueError as exc:
            raise ConfigError(f"option {name!r}: expected an integer, got {raw!r}") from exc

    def option_float(self, name: str, default: float) -> float:
        """Return an option parsed as ``float``."""
        raw = self.options.get(name)
        if raw is None:
            return default
        try:
            return float(raw)
        except ValueError as exc:
            raise ConfigError(f"option {name!r}: expected a number, got {raw!r}") from exc

    def option_bool(self, name: str, default: bool) -> bool:
        """Return an option parsed as ``bool``."""
        raw = self.options.get(name)
        if raw is None:
            return default
        return _parse_bool(raw, f"option {name!r}")

    # -- construction ------------------------------------------------------
    def with_overrides(self, **overrides: Any) -> "Config":
        """Return a copy with ``overrides`` applied (validated)."""
        unknown = sorted(set(overrides) - {f.name for f in fields(self)})
        if unknown:
            raise ConfigError(
                "unknown configuration field(s): " + ", ".join(unknown)
            )
        return replace(self, **overrides)

    def with_options(self, **options: str) -> "Config":
        """Return a copy with additional open-map options merged in."""
        merged = dict(self.options)
        merged.update({key: str(value) for key, value in options.items()})
        return replace(self, options=merged)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain JSON-compatible dict."""
        data: dict[str, Any] = {}
        for f in fields(self):
            value = getattr(self, f.name)
            data[f.name] = dict(value) if f.name == "options" else value
        return data

    def to_json(self, indent: int = 2) -> str:
        """Serialise to deterministic JSON text."""
        return json.dumps(self.to_dict(), sort_keys=True, indent=indent) + "\n"

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "Config":
        """Build a Config from a mapping, rejecting unknown keys."""
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(data) - known)
        if unknown:
            raise ConfigError(
                "unknown configuration key(s): "
                + ", ".join(unknown)
                + "; known keys are "
                + ", ".join(sorted(known))
            )
        kwargs = dict(data)
        if "options" in kwargs and kwargs["options"] is not None:
            options = kwargs["options"]
            if not isinstance(options, Mapping):
                raise ConfigError("options must be a JSON object")
            kwargs["options"] = {str(k): str(v) for k, v in options.items()}
        return cls(**kwargs)

    @classmethod
    def from_json(cls, path: str | os.PathLike[str]) -> "Config":
        """Read a Config from a JSON file."""
        with open(path, "r", encoding="utf-8") as handle:
            try:
                data = json.load(handle)
            except json.JSONDecodeError as exc:
                raise ConfigError(f"{path}: invalid JSON: {exc}") from exc
        if not isinstance(data, Mapping):
            raise ConfigError(f"{path}: expected a JSON object at the top level")
        return cls.from_mapping(data)

    @classmethod
    def from_env(
        cls, env: Mapping[str, str] | None = None, base: "Config | None" = None
    ) -> "Config":
        """Overlay ``ANO_``-prefixed environment variables onto ``base``.

        Scalar fields map to ``ANO_<FIELD_NAME_UPPER>``; for example
        ``ANO_BACKEND``, ``ANO_PROJECT_ID``, ``ANO_STORE_BACKEND``,
        ``ANO_SQLITE_PATH``, ``ANO_SEED``. Anything of the form
        ``ANO_OPTION_<NAME>`` becomes ``options["<name lowercased>"]``.

        ``ANO_CONFIG_FILE`` is honoured first: the file is loaded and the
        remaining environment variables are applied on top of it.
        """
        environment = os.environ if env is None else env
        config = base if base is not None else cls()
        config_file = environment.get(f"{ENV_PREFIX}CONFIG_FILE")
        if config_file:
            config = cls.from_json(config_file)

        overrides: dict[str, Any] = {}
        options = dict(config.options)
        field_names = {f.name for f in fields(cls)}
        for key, raw in sorted(environment.items()):
            if not key.startswith(ENV_PREFIX):
                continue
            suffix = key[len(ENV_PREFIX) :]
            if suffix == "CONFIG_FILE":
                continue
            if suffix.startswith("OPTION_"):
                options[suffix[len("OPTION_") :].lower()] = raw
                continue
            name = suffix.lower()
            if name not in field_names:
                raise ConfigError(
                    f"{key} does not name a configuration field; use "
                    f"{ENV_PREFIX}OPTION_{suffix} for milestone-specific settings"
                )
            if name == "seed":
                try:
                    overrides[name] = int(raw)
                except ValueError as exc:
                    raise ConfigError(f"{key}: expected an integer, got {raw!r}") from exc
            elif name == "options":
                raise ConfigError(
                    f"{key} is not settable directly; use "
                    f"{ENV_PREFIX}OPTION_<NAME> instead"
                )
            elif raw == "":
                overrides[name] = None
            else:
                overrides[name] = raw
        overrides["options"] = options
        return replace(config, **overrides)


def local_profile(**overrides: Any) -> Config:
    """The offline profile: local substitutes for every managed service.

    This is what a clean checkout runs. No credentials, no network, everything
    written under ``artifacts/``.
    """
    return Config(
        backend=LOCAL,
        project_id=None,
        artifacts_dir="artifacts",
    ).with_overrides(**overrides)


def gcp_profile(
    project_id: str = "gsk-ano-prod",
    location: str = "europe-west2",
    **overrides: Any,
) -> Config:
    """The deployed profile: real Google Cloud managed services.

    Identical application code; only these values differ from
    :func:`local_profile`. ``backend_plugin`` names the module in the deployment
    image that registers the real client factories — see :mod:`ano.gcp.registry`.
    """
    return Config(
        backend=GCP,
        backend_plugin="ano_gcp_clients",
        project_id=project_id,
        location=location,
        artifacts_dir="artifacts",
    ).with_overrides(**overrides)


#: Named profiles, for ``python3 -m ano.config --profile <name>``.
_PROFILES = {"local": local_profile, "gcp": gcp_profile}


def default_config() -> Config:
    """The default configuration: the offline local profile."""
    return local_profile()


def load_config(
    path: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> Config:
    """Load the effective configuration.

    Order: local-profile defaults, then ``path`` (or ``ANO_CONFIG_FILE``), then
    ``ANO_``-prefixed environment variables.

    Args:
        path: Optional JSON config file.
        env: Environment mapping; defaults to ``os.environ``.
    """
    base = Config.from_json(path) if path is not None else default_config()
    return Config.from_env(env=env, base=base)


def _main(argv: list[str] | None = None) -> int:
    """Print a named profile as JSON.

    ``diff`` of the two profiles is the R6 "configuration only" evidence.
    """
    parser = argparse.ArgumentParser(
        prog="python3 -m ano.config",
        description=(
            "Print an ANO configuration profile as JSON. The diff between the "
            "'local' and 'gcp' profiles contains configuration data only - no "
            "code - which is the evidence for requirement R6."
        ),
    )
    parser.add_argument(
        "--profile",
        choices=sorted(_PROFILES),
        default="local",
        help="which profile to print (default: local)",
    )
    parser.add_argument(
        "--effective",
        action="store_true",
        help="apply ANO_* environment variables on top of the profile",
    )
    args = parser.parse_args(argv)
    config = _PROFILES[args.profile]()
    if args.effective:
        config = Config.from_env(base=config)
    sys.stdout.write(config.to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())

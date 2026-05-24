"""Runtime settings loader for PROTEA (T-OPS.6).

Backs the historical ``Settings`` frozen dataclass with a
``pydantic-settings`` ``BaseSettings`` pipeline. The public surface
(``Settings`` + ``load_settings(project_root, *, env_prefix=...)``)
stays byte-identical so the dozens of call sites and the
``tests/test_settings.py`` suite keep working without edits.

Source priority (highest first)
-------------------------------
1. process environment (``PROTEA_*`` by default)
2. ``.env`` file at the project root (read by python-dotenv)
3. ``protea/config/system.yaml`` (flattened from nested YAML keys)
4. built-in defaults (declared on the model)

The YAML loader is a custom :class:`PydanticBaseSettingsSource` so we
can keep the historical nested YAML shape (``database.url``,
``storage.minio.endpoint``) while exposing flat field names to the
rest of the codebase.

Deprecation warnings
--------------------
Two backward-compat affordances emit :class:`DeprecationWarning`:

* legacy unprefixed env aliases (``DATABASE_URL``, ``AMQP_URL``) are
  honoured but flagged so deployment manifests migrate to the
  ``PROTEA_*`` prefix at their own pace
* callers passing ``env_prefix`` other than ``PROTEA_`` get a warning;
  pydantic-settings handles the prefix natively, but the override path
  is undocumented and only kept for one or two legacy scripts
"""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

_DEFAULT_ALLOWED_ORIGINS: tuple[str, ...] = (
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://protea.ngrok.app",
)

_DEFAULT_DB_URL = "postgresql+psycopg://usuario:clave@localhost:5432/BioData"
_DEFAULT_AMQP_URL = "amqp://guest:guest@localhost:5672/"
_DEFAULT_ARTIFACTS_DIR = "storage/evaluation_artifacts"
_DEFAULT_STORAGE_BACKEND = "local"
_DEFAULT_MINIO_BUCKET = "protea"

# Legacy env aliases (no PROTEA_ prefix) honoured for one deprecation
# cycle. Keys are the modern PROTEA_* names, values are the legacy ones
# that callers might still set. Populated in :func:`_apply_legacy_env_aliases`
# at load time.
_LEGACY_ENV_ALIASES: dict[str, str] = {
    "PROTEA_DB_URL": "DATABASE_URL",
    "PROTEA_AMQP_URL": "AMQP_URL",
}


@dataclass(frozen=True)
class Settings:
    """Immutable view of resolved runtime settings.

    Kept as a frozen dataclass (not a pydantic ``BaseModel``) so the
    historical attribute access patterns and ``MagicMock`` test doubles
    keep working unchanged. Constructed by :func:`load_settings` from a
    private :class:`_ProteaBaseSettings` after path-resolution + tuple
    coercion.
    """

    db_url: str
    amqp_url: str
    artifacts_dir: Path
    storage_backend: str = "local"
    storage_root: Path | None = None
    minio_endpoint: str | None = None
    minio_bucket: str = "protea"
    minio_access_key: str | None = None
    minio_secret_key: str | None = None
    minio_secure: bool = False
    allowed_origins: tuple[str, ...] = _DEFAULT_ALLOWED_ORIGINS
    # Optional absolute path to the Anc2Vec npz artefact. When set, the
    # ``protea.core.anc2vec_embeddings`` shim uses this path instead of
    # its repo-relative fallback. See the deployment runbook for the
    # full resolution chain (env > artifact store > repo fallback).
    anc2vec_path: str | None = None
    # Maximum anonymous quick-annotate calls per IP hash per UTC day.
    # Override with env var PROTEA_ANON_QUOTA_PER_DAY.
    anon_quota_per_day: int = 5


class _YamlConfigSource(PydanticBaseSettingsSource):
    """Pydantic-settings source that reads ``protea/config/system.yaml``.

    Flattens the nested YAML shape (kept for human readability) into
    the flat field names on :class:`_ProteaBaseSettings`. Missing files
    yield no values so the env / dotenv / defaults chain still drives
    the result.
    """

    def __init__(self, settings_cls: type[BaseSettings], project_root: Path) -> None:
        super().__init__(settings_cls)
        self._project_root = project_root
        self._data: dict[str, Any] | None = None

    def _load(self) -> dict[str, Any]:
        if self._data is not None:
            return self._data
        path = self._project_root / "protea" / "config" / "system.yaml"
        if not path.exists():
            self._data = {}
            return self._data
        with path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        flat: dict[str, Any] = {}
        db = raw.get("database") or {}
        if "url" in db:
            flat["db_url"] = db["url"]
        queue = raw.get("queue") or {}
        if "amqp_url" in queue:
            flat["amqp_url"] = queue["amqp_url"]
        storage = raw.get("storage") or {}
        if "artifacts_dir" in storage:
            flat["artifacts_dir"] = storage["artifacts_dir"]
        if "backend" in storage:
            flat["storage_backend"] = storage["backend"]
        if "root" in storage:
            flat["storage_root"] = storage["root"]
        minio = storage.get("minio") or {}
        if "endpoint" in minio:
            flat["minio_endpoint"] = minio["endpoint"]
        if "bucket" in minio:
            flat["minio_bucket"] = minio["bucket"]
        if "access_key" in minio:
            flat["minio_access_key"] = minio["access_key"]
        if "secret_key" in minio:
            flat["minio_secret_key"] = minio["secret_key"]
        if "secure" in minio:
            flat["minio_secure"] = minio["secure"]
        cors = raw.get("cors") or {}
        if "allowed_origins" in cors:
            flat["allowed_origins"] = cors["allowed_origins"]
        anc2vec = raw.get("anc2vec") or {}
        if "path" in anc2vec:
            flat["anc2vec_path"] = anc2vec["path"]

        self._data = flat
        return self._data

    def get_field_value(
        self, field: Any, field_name: str
    ) -> tuple[Any, str, bool]:
        data = self._load()
        if field_name in data:
            return data[field_name], field_name, False
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        return dict(self._load())


def _make_settings_cls(env_prefix: str, env_file: Path | None) -> type[BaseSettings]:
    """Build a ``BaseSettings`` subclass parameterised by ``env_prefix``.

    The class is constructed at call time because :class:`SettingsConfigDict`
    bakes ``env_prefix`` and ``env_file`` into the class, not the instance.
    Generated classes are cheap (no schema cache pressure in practice) and
    stay private to this module. ``env_file`` is the absolute path to the
    project's ``.env`` so the loader is independent of the process CWD.
    """

    class _ProteaBaseSettings(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix=env_prefix,
            env_file=str(env_file) if env_file is not None else None,
            env_file_encoding="utf-8",
            extra="ignore",
            case_sensitive=False,
        )

        db_url: str = _DEFAULT_DB_URL
        amqp_url: str = _DEFAULT_AMQP_URL
        artifacts_dir: str = _DEFAULT_ARTIFACTS_DIR
        storage_backend: str = _DEFAULT_STORAGE_BACKEND
        storage_root: str | None = None
        minio_endpoint: str | None = None
        minio_bucket: str = _DEFAULT_MINIO_BUCKET
        minio_access_key: str | None = None
        minio_secret_key: str | None = None
        minio_secure: bool = False
        anc2vec_path: str | None = None
        anon_quota_per_day: int = 5
        # ``allowed_origins`` is stored as a list internally so pydantic's
        # JSON-mode env parsing does not try to JSON-decode the raw
        # comma-separated env value. The field validator below normalises
        # both shapes (list-from-YAML and str-from-env) into a list.
        allowed_origins: list[str] | None = Field(default=None)

        @field_validator("storage_backend", mode="before")
        @classmethod
        def _lower_backend(cls, value: Any) -> Any:
            if isinstance(value, str):
                return value.lower()
            return value

        @field_validator("allowed_origins", mode="before")
        @classmethod
        def _parse_allowed_origins(cls, value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, str):
                # Empty string disables CORS (matches T5.5 contract).
                return [p.strip() for p in value.split(",") if p.strip()]
            if isinstance(value, (list, tuple)):
                return [str(o).strip() for o in value if str(o).strip()]
            return value

    return _ProteaBaseSettings


def _resolve_path(raw: str | None, project_root: Path) -> Path | None:
    if raw is None or raw == "":
        return None
    p = Path(raw)
    if not p.is_absolute():
        p = project_root / p
    return p


def _apply_legacy_env_aliases(env_prefix: str) -> None:
    """Promote unprefixed legacy env vars into the ``PROTEA_*`` namespace.

    Emits a :class:`DeprecationWarning` per match so deploy manifests
    migrate to the canonical names. No-op when the modern name is
    already set so callers can override safely.
    """
    if env_prefix != "PROTEA_":
        # Custom prefixes opt out of the legacy alias path; they are
        # internal-only and the alias map is keyed on PROTEA_*.
        return
    for modern, legacy in _LEGACY_ENV_ALIASES.items():
        if modern in os.environ:
            continue
        if legacy in os.environ:
            warnings.warn(
                f"env var {legacy!r} is deprecated; set {modern!r} instead",
                DeprecationWarning,
                stacklevel=3,
            )
            os.environ[modern] = os.environ[legacy]


def _build_raw_settings(project_root: Path, env_prefix: str) -> Any:
    """Construct the ``BaseSettings`` instance with the source hierarchy
    bound to ``project_root``.

    Split out of :func:`load_settings` so the orchestrator stays under
    the method-LOC budget; the inner subclass is needed to close over
    ``yaml_source`` since ``settings_customise_sources`` is a classmethod.
    """
    env_file = project_root / ".env"
    settings_cls = _make_settings_cls(env_prefix, env_file if env_file.exists() else None)
    yaml_source = _YamlConfigSource(settings_cls, project_root)

    class _ProteaSettingsWithYaml(settings_cls):  # type: ignore[valid-type, misc]
        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls_inner: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            # Highest priority first: env > .env > yaml > defaults.
            return (
                env_settings,
                dotenv_settings,
                yaml_source,
                file_secret_settings,
                init_settings,
            )

    return _ProteaSettingsWithYaml()


def _materialise(raw: Any, project_root: Path) -> Settings:
    """Convert the raw ``BaseSettings`` into the frozen ``Settings`` dataclass.

    Handles the two post-validation steps that pydantic does not do for
    us: path resolution against ``project_root`` and the CORS allowlist
    default fallback (``None`` from the model means "no source supplied
    a value", which we translate into the built-in default tuple).
    """
    artifacts_dir = _resolve_path(raw.artifacts_dir, project_root)
    assert artifacts_dir is not None  # default is non-empty, so never None
    storage_root = _resolve_path(raw.storage_root, project_root)
    allowed = (
        _DEFAULT_ALLOWED_ORIGINS
        if raw.allowed_origins is None
        else tuple(raw.allowed_origins)
    )
    return Settings(
        db_url=raw.db_url,
        amqp_url=raw.amqp_url,
        artifacts_dir=artifacts_dir,
        storage_backend=raw.storage_backend,
        storage_root=storage_root,
        minio_endpoint=raw.minio_endpoint,
        minio_bucket=raw.minio_bucket,
        minio_access_key=raw.minio_access_key,
        minio_secret_key=raw.minio_secret_key,
        minio_secure=raw.minio_secure,
        allowed_origins=allowed,
        anc2vec_path=raw.anc2vec_path,
        anon_quota_per_day=raw.anon_quota_per_day,
    )


def load_settings(project_root: Path, *, env_prefix: str = "PROTEA_") -> Settings:
    """Resolve runtime settings via env > ``.env`` > ``system.yaml`` > defaults.

    See the module docstring for the full env-var contract and the
    deprecation matrix. ``env_prefix`` defaults to ``"PROTEA_"``; passing
    anything else emits a :class:`DeprecationWarning`.
    """
    if env_prefix != "PROTEA_":
        warnings.warn(
            f"non-default env_prefix={env_prefix!r} is deprecated; "
            "the PROTEA_ prefix is now enforced by pydantic-settings",
            DeprecationWarning,
            stacklevel=2,
        )
    _apply_legacy_env_aliases(env_prefix)
    return _materialise(_build_raw_settings(project_root, env_prefix), project_root)

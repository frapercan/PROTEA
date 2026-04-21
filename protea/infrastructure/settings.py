from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Settings:
    db_url: str
    amqp_url: str
    artifacts_dir: Path
    admin_token: str
    storage_backend: str = "local"
    storage_root: Path | None = None
    minio_endpoint: str | None = None
    minio_bucket: str = "protea"
    minio_access_key: str | None = None
    minio_secret_key: str | None = None
    minio_secure: bool = False


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def load_settings(project_root: Path, *, env_prefix: str = "PROTEA_") -> Settings:
    """
    Load settings from:
      1) protea/config/system.yaml (relative to project root)
      2) environment variables (override YAML values)

    Expected env vars:
      - PROTEA_DB_URL
      - PROTEA_AMQP_URL
      - PROTEA_STORAGE_BACKEND            (local | minio)
      - PROTEA_STORAGE_ROOT               (path for local backend)
      - PROTEA_MINIO_ENDPOINT             (e.g. localhost:9000)
      - PROTEA_MINIO_BUCKET
      - PROTEA_MINIO_ACCESS_KEY
      - PROTEA_MINIO_SECRET_KEY
      - PROTEA_MINIO_SECURE               (truthy for HTTPS)
    """
    system_path = project_root / "protea" / "config" / "system.yaml"
    system = _load_yaml(system_path)

    file_db_url: str | None = system.get("database", {}).get("url")
    file_amqp_url: str | None = system.get("queue", {}).get("amqp_url")

    db_url = (
        os.getenv(f"{env_prefix}DB_URL")
        or file_db_url
        or "postgresql+psycopg://usuario:clave@localhost:5432/BioData"
    )
    amqp_url = (
        os.getenv(f"{env_prefix}AMQP_URL") or file_amqp_url or "amqp://guest:guest@localhost:5672/"
    )

    storage_cfg = system.get("storage", {}) or {}

    raw_artifacts = (
        os.getenv(f"{env_prefix}ARTIFACTS_DIR")
        or storage_cfg.get("artifacts_dir")
        or "storage/evaluation_artifacts"
    )
    artifacts_dir = Path(raw_artifacts)
    if not artifacts_dir.is_absolute():
        artifacts_dir = project_root / artifacts_dir

    admin_token = (
        os.getenv(f"{env_prefix}ADMIN_TOKEN") or system.get("admin", {}).get("token") or ""
    )

    storage_backend = (
        os.getenv(f"{env_prefix}STORAGE_BACKEND")
        or storage_cfg.get("backend")
        or "local"
    ).lower()

    raw_storage_root = (
        os.getenv(f"{env_prefix}STORAGE_ROOT")
        or storage_cfg.get("root")
    )
    storage_root: Path | None
    if raw_storage_root:
        storage_root = Path(raw_storage_root)
        if not storage_root.is_absolute():
            storage_root = project_root / storage_root
    else:
        storage_root = None

    minio_cfg = storage_cfg.get("minio", {}) or {}
    minio_endpoint = os.getenv(f"{env_prefix}MINIO_ENDPOINT") or minio_cfg.get("endpoint")
    minio_bucket = os.getenv(f"{env_prefix}MINIO_BUCKET") or minio_cfg.get("bucket") or "protea"
    minio_access_key = os.getenv(f"{env_prefix}MINIO_ACCESS_KEY") or minio_cfg.get("access_key")
    minio_secret_key = os.getenv(f"{env_prefix}MINIO_SECRET_KEY") or minio_cfg.get("secret_key")
    minio_secure = _as_bool(
        os.getenv(f"{env_prefix}MINIO_SECURE", minio_cfg.get("secure", False))
    )

    return Settings(
        db_url=db_url,
        amqp_url=amqp_url,
        artifacts_dir=artifacts_dir,
        admin_token=admin_token,
        storage_backend=storage_backend,
        storage_root=storage_root,
        minio_endpoint=minio_endpoint,
        minio_bucket=minio_bucket,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
        minio_secure=minio_secure,
    )

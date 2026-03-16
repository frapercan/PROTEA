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


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_settings(project_root: Path, *, env_prefix: str = "PROTEA_") -> Settings:
    """
    Load settings from:
      1) protea/config/system.yaml (relative to project root)
      2) environment variables (override YAML values)

    Expected env vars:
      - PROTEA_DB_URL
      - PROTEA_AMQP_URL
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

    raw_artifacts = (
        os.getenv(f"{env_prefix}ARTIFACTS_DIR")
        or system.get("storage", {}).get("artifacts_dir")
        or "storage/evaluation_artifacts"
    )
    artifacts_dir = Path(raw_artifacts)
    if not artifacts_dir.is_absolute():
        artifacts_dir = project_root / artifacts_dir

    return Settings(db_url=db_url, amqp_url=amqp_url, artifacts_dir=artifacts_dir)

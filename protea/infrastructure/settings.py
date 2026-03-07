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


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_settings(project_root: Path, *, env_prefix: str = "PROTEA_") -> Settings:
    """
    Load settings from:
      1) src/protea/config/system.yaml
      2) environment variables override

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
    amqp_url = os.getenv(f"{env_prefix}AMQP_URL") or file_amqp_url or "amqp://guest:guest@localhost:5672/"

    return Settings(db_url=db_url, amqp_url=amqp_url)

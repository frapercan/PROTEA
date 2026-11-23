"""What the cluster's file stages share: hashes, records and the environment a pass ran in.

A file stage (ADR-D60, decision 4) reads files placed beforehand and writes files
with records; the platform's ingest verifies them. The embedding bank (ADR-D62)
and the lexicon bank (ADR-D63) write their records and name their software and
card the same way, through these helpers. Standard library only, but for the
optional torch the environment asks about the card.
"""

from __future__ import annotations

import hashlib
import json
import socket
from collections.abc import Sequence
from pathlib import Path
from typing import Any


def file_sha256(path: Path) -> str:
    """The sha256 of a file, read in blocks."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 22), b""):
            digest.update(block)
    return digest.hexdigest()


def write_record(path: Path, record: dict[str, Any]) -> None:
    """A record as sorted, indented JSON with a final newline."""
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_record(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def environment(device: str, packages: Sequence[str]) -> dict[str, Any]:
    """The packages' versions (and the commit of a package installed from git), the card and the host."""
    from importlib.metadata import PackageNotFoundError, distribution

    software: dict[str, Any] = {}
    for name in packages:
        try:
            dist = distribution(name)
        except PackageNotFoundError:
            continue
        software[name] = dist.version
        direct = dist.read_text("direct_url.json")
        if direct and "vcs_info" in direct:
            software[f"{name}_commit"] = json.loads(direct)["vcs_info"].get("commit_id")
    card = "cpu"
    try:
        import torch

        software["cuda"] = torch.version.cuda
        if device.startswith("cuda") and torch.cuda.is_available():
            card = torch.cuda.get_device_name(torch.device(device))
    except ImportError:
        pass
    return {"software": software, "card": card, "host": socket.gethostname()}

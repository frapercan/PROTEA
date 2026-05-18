"""Register a pre-staged dataset as a PROTEA ``Dataset`` row.

The lab dumps train/eval parquets + ``manifest.json`` to disk (or to
MinIO under its own key) without running PROTEA's
``export_research_dataset`` pipeline. This script reads the lab's
``manifest.json``, computes the manifest sha256, builds canonical
artifact URIs from the dataset directory, and posts them to
``POST /datasets/import-by-reference`` so downstream slices (booster
training, FARM-EXP digest backfill, lab pull-by-name) can resolve the
dataset via the registry.

Usage::

    poetry run python scripts/import_dataset_by_reference.py \\
        --dataset-dir /path/to/bench-v1-K5-v226-lineage \\
        [--name <override>] [--storage-backend local|minio] \\
        [--api http://127.0.0.1:8000] [--api-key <key>] [--force]

The dataset directory must contain ``manifest.json`` plus optional
``train.parquet`` / ``eval.parquet``. URIs are built as ``file://``
paths against the resolved directory; pass ``--key-prefix`` and a
custom ``--storage-backend`` for non local layouts.

On success, prints the new ``Dataset.id`` to stdout.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import requests

DEFAULT_API = "http://127.0.0.1:8000"


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--dataset-dir",
        required=True,
        type=Path,
        help="Path to the dataset directory containing manifest.json + parquets",
    )
    p.add_argument(
        "--name",
        default=None,
        help="Override Dataset.name (default: manifest['name'] or directory name)",
    )
    p.add_argument(
        "--storage-backend",
        default="local",
        help="Backend identifier; default 'local'",
    )
    p.add_argument(
        "--key-prefix",
        default=None,
        help="Artifact store prefix (default: datasets/<name>/)",
    )
    p.add_argument(
        "--external-source",
        default=None,
        help="Provenance tag stored in meta (e.g. 'protea-reranker-lab@<sha>')",
    )
    p.add_argument(
        "--api",
        default=DEFAULT_API,
        help=f"PROTEA API root (default {DEFAULT_API})",
    )
    p.add_argument(
        "--api-key",
        default=None,
        help="API key for X-API-Key header (or set PROTEA_API_KEY env)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Replace any existing Dataset row with the same name",
    )
    return p.parse_args()


def _uri_for(path: Path) -> str:
    return f"file://{path.resolve().as_posix()}"


def _build_body(dataset_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    manifest_path = dataset_dir / "manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(f"missing manifest: {manifest_path}")
    manifest_bytes = manifest_path.read_bytes()
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    manifest: dict[str, Any] = json.loads(manifest_bytes)

    name = args.name or manifest.get("name") or dataset_dir.name
    key_prefix = args.key_prefix or f"datasets/{name}/"

    train_path = dataset_dir / "train.parquet"
    eval_path = dataset_dir / "eval.parquet"

    body: dict[str, Any] = {
        "name": name,
        "storage_backend": args.storage_backend,
        "key_prefix": key_prefix,
        "manifest_uri": _uri_for(manifest_path),
        "schema_sha": manifest.get("schema_sha", ""),
        "manifest_sha": manifest_sha,
        "k": int(manifest.get("k", 5)),
        "annotation_source": manifest.get("annotation_source", "goa"),
        "n_train_rows": int(manifest.get("n_train_rows", 0)),
        "n_eval_rows": int(manifest.get("n_eval_rows", 0)),
        "embedding_config_id": manifest.get("embedding_config_id"),
        "ontology_snapshot_id": manifest.get("ontology_snapshot_id"),
        "train_snapshot_pairs": list(manifest.get("train_snapshot_pairs", [])),
        "eval_snapshot_pair": manifest.get("eval_snapshot_pair"),
        "producer_version": manifest.get("producer_version"),
        "producer_git_sha": manifest.get("producer_git_sha"),
        "force": bool(args.force),
    }

    if train_path.is_file():
        body["train_uri"] = _uri_for(train_path)
    if eval_path.is_file():
        body["eval_uri"] = _uri_for(eval_path)
    if args.external_source:
        body["external_source"] = args.external_source

    if not body["schema_sha"]:
        raise SystemExit(
            f"manifest at {manifest_path} is missing 'schema_sha'; cannot register"
        )

    return body


def _post(api: str, api_key: str | None, body: dict[str, Any]) -> dict[str, Any]:
    headers = {}
    if api_key:
        headers["X-API-Key"] = api_key
    resp = requests.post(
        f"{api.rstrip('/')}/datasets/import-by-reference",
        json=body,
        headers=headers,
        timeout=30,
    )
    if resp.status_code == 409:
        raise SystemExit(
            f"Dataset {body['name']!r} already exists; pass --force to replace"
        )
    if resp.status_code >= 300:
        raise SystemExit(
            f"POST /datasets/import-by-reference failed: "
            f"{resp.status_code} {resp.text}"
        )
    return resp.json()


def main() -> None:
    a = _args()
    dataset_dir = a.dataset_dir.resolve()
    if not dataset_dir.is_dir():
        raise SystemExit(f"not a directory: {dataset_dir}")

    import os

    api_key = a.api_key or os.environ.get("PROTEA_API_KEY")

    body = _build_body(dataset_dir, a)
    print(
        f"[import] registering dataset {body['name']!r} "
        f"(schema_sha={body['schema_sha']}, "
        f"manifest_sha={body['manifest_sha'][:12]}...)",
        file=sys.stderr,
    )
    result = _post(a.api, api_key, body)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

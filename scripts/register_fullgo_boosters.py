"""Register the three fullgo per-category boosters (NK / LK / PK).

The fullgo champion (mean f_micro_w 0.3911 on the official 7401 LAFA frame)
scores each candidate with the per-category booster matching the protein's
CAFA category, exactly as the INT-5 per-category dispatch resolves them at
inference (``reranker_{nk,lk,pk}_artifact_uri`` on the batch payload). This
operational helper takes the three booster blobs already staged in MinIO (or
on local disk), pairs each with the SAME live feature-schema sha, and POSTs
them to the running PROTEA API's existing
``POST /reranker-models/import-by-reference`` endpoint. No new insert path is
added: registration reuses the one router INT-5 already relies on.

The script is idempotent (``--force`` overwrites an existing row of the same
name) and pins every body to the SAME live ``feature_schema_sha`` (the value
``protea_contracts.compute_feature_schema_sha`` returns for the families the
scorer materialises at inference). Recording the live sha on every booster is
what lets the FARM-EXP.5 guard accept them at score time and reject a
schema-drifted layout.

Usage::

    poetry run python scripts/register_fullgo_boosters.py \\
        --nk-uri  s3://protea-rerankers/fullgo/ensemble_gbm_NK.txt \\
        --lk-uri  s3://protea-rerankers/fullgo/ensemble_gbm_LK.txt \\
        --pk-uri  s3://protea-rerankers/fullgo/ensemble_gbm_PK.txt \\
        --feature-schema-sha <live sha> \\
        --dataset-id <uuid> \\
        [--name-prefix fullgo] [--external-source protea-reranker-lab@<sha>] \\
        [--api http://127.0.0.1:8000] [--api-key <key>] [--force]

When ``--feature-schema-sha`` is omitted the script computes it from the live
families (``infer_active_feature_families`` with all predict flags on, the
exact set the scorer uses) via ``protea_contracts`` so a single source of truth
is used. Each booster is registered with ``training.cell`` set to its category
(``nk`` / ``lk`` / ``pk``) so the router stamps ``RerankerModel.category``.

Categorical features
--------------------
A booster trained WITH the four categorical columns (``aspect`` /
``qualifier`` / ``evidence_code`` / ``taxonomic_relation``) must carry its
per-column ``pd.factorize`` vocabulary so the predict-time scorer encodes the
strings against the same codes seen at training. Pass
``--categorical-codes-json`` to record that vocabulary on the run body; the
router persists it to ``RerankerModel.metrics['__categorical_codes__']`` for
audit / UI. The LOAD-BEARING predict-time copy, however, is the booster's
SIBLING ``run.json`` in the artifact store (``runs/<id>/run.json`` next to
``model.txt``): the live scorer recovers ``categorical_codes`` from there
(``load_per_category_categorical_codes``), exactly as the universal path does,
because the batch payload only carries the artifact URI. So the lab must upload
that sibling run.json with the ``categorical_codes`` block alongside the
booster blob. Omit ``--categorical-codes-json`` (and the sibling block) for
numeric-only boosters; the scorer then falls back to the historical bare
``apply_reranker`` path with no behaviour change.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any

import requests

DEFAULT_API = "http://127.0.0.1:8000"
CATEGORIES = ("nk", "lk", "pk")


@dataclass(frozen=True)
class _BoosterArgs:
    """The three booster URIs keyed by lowercase category."""

    uris: dict[str, str]


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--nk-uri", required=True, help="Storage URI of the NK booster blob")
    p.add_argument("--lk-uri", required=True, help="Storage URI of the LK booster blob")
    p.add_argument("--pk-uri", required=True, help="Storage URI of the PK booster blob")
    p.add_argument(
        "--feature-schema-sha",
        default=None,
        help="Live feature-schema sha (default: compute from live families via protea_contracts)",
    )
    p.add_argument(
        "--dataset-id",
        default=None,
        help="UUID of the source Dataset row the boosters were trained on",
    )
    p.add_argument(
        "--name-prefix",
        default="fullgo",
        help="RerankerModel.name prefix; rows become '<prefix>-<cat>' (default 'fullgo')",
    )
    p.add_argument(
        "--external-source",
        default=None,
        help="Provenance tag (e.g. 'protea-reranker-lab@<git-sha>')",
    )
    p.add_argument("--api", default=DEFAULT_API, help=f"PROTEA API root (default {DEFAULT_API})")
    p.add_argument(
        "--api-key",
        default=None,
        help="API key for X-API-Key header (or set PROTEA_API_KEY env)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Replace any existing RerankerModel rows with the same name",
    )
    p.add_argument(
        "--categorical-codes-json",
        default=None,
        help=(
            "Path to a JSON file with the per-column categorical vocabulary "
            "({column: [val0, val1, ...]}) the boosters were trained on. When "
            "given, it is recorded on every run body under "
            "``run.categorical_codes`` so the router stamps "
            "``RerankerModel.metrics['__categorical_codes__']`` and the "
            "predict-time scorer can encode the categorical features "
            "consistently. Omit when the boosters are numeric-only."
        ),
    )
    return p.parse_args()


def _load_categorical_codes(path: str | None) -> dict[str, list[str]] | None:
    """Read the per-column categorical vocabulary JSON, or ``None``.

    The file must decode to a ``{column: [val0, val1, ...]}`` mapping (the
    ``pd.factorize`` order the boosters trained on). Raises ``SystemExit`` on a
    malformed file so a misconfigured registration fails loudly rather than
    silently dropping the vocabulary.
    """
    if path is None:
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            codes = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"--categorical-codes-json could not be read: {exc}") from exc
    if not isinstance(codes, dict) or not all(
        isinstance(k, str) and isinstance(v, list) for k, v in codes.items()
    ):
        raise SystemExit(
            "--categorical-codes-json must decode to {column: [value, ...]} "
            f"(got {type(codes).__name__})"
        )
    return codes


def live_feature_families() -> list[str]:
    """The feature families the live fullgo predict pipeline materialises.

    Computed exactly as the inference-time scorer does
    (:meth:`RerankerScorer.resolve_live_schema_sha`), which derives the
    schema-sha families purely from the predict-time flags via
    ``infer_active_feature_families``. The fullgo champion runs with
    alignments + taxonomy + v6 all enabled, so the booster's recorded sha
    matches the value the scorer computes at score time and the FARM-EXP.5
    guard accepts it. The self_prior / association / classifier signals ride
    the prediction dicts but are not part of the schema-sha family set (they
    are not modelled by ``infer_active_feature_families``).
    """
    from protea.core.reranker import infer_active_feature_families

    return infer_active_feature_families(
        compute_alignments=True,
        compute_taxonomy=True,
        compute_v6_features=True,
    )


def _resolve_schema_sha(supplied: str | None) -> str:
    """Return the live feature-schema sha, computing it when not supplied.

    Prefers the explicit ``--feature-schema-sha``; otherwise derives it from
    the live families via ``protea_contracts`` so the registered fingerprint
    equals what the scorer computes at inference. Raising here (instead of
    registering a NULL sha) keeps the FARM-EXP.5 guard intact.
    """
    if supplied:
        return supplied
    try:
        from protea_contracts import compute_feature_schema_sha
    except Exception as exc:  # pragma: no cover - exercised only without contracts
        raise SystemExit(
            "no --feature-schema-sha and protea_contracts is not importable; "
            f"cannot derive the live sha ({exc})"
        ) from exc
    return compute_feature_schema_sha(live_feature_families())


def build_request_body(
    *,
    category: str,
    artifact_uri: str,
    feature_schema_sha: str,
    name_prefix: str,
    dataset_id: str | None,
    external_source: str | None,
    force: bool,
    families: list[str] | None = None,
    categorical_codes: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build one ``import-by-reference`` body for a per-category booster.

    The ``run.features.feature_schema_sha`` carries the live sha so the router
    stamps it onto the ``RerankerModel`` row even when ``protea_contracts`` is
    not importable in the API image. ``spec_yaml`` sets ``training.cell`` to the
    category so ``RerankerModel.category`` is stamped for INT-5 dispatch.
    ``families`` (when given) is recorded as ``run.features.families_enabled``.
    ``categorical_codes`` (when given) is recorded as ``run.categorical_codes``,
    which the router persists to ``RerankerModel.metrics['__categorical_codes__']``
    so the predict-time scorer can encode the categorical features against the
    booster's training vocabulary.
    """
    cat = category.lower()
    if cat not in CATEGORIES:
        raise ValueError(f"category must be one of {CATEGORIES}, got {category!r}")
    run_id = f"{name_prefix}-{cat}"
    features: dict[str, Any] = {"feature_schema_sha": feature_schema_sha}
    if families is not None:
        features["families_enabled"] = list(families)
    run_block: dict[str, Any] = {"run_id": run_id, "features": features}
    if categorical_codes is not None:
        run_block["categorical_codes"] = categorical_codes
    body: dict[str, Any] = {
        "artifact_uri": artifact_uri,
        "spec_yaml": f"name: {run_id}\ntraining:\n  cell: {cat}\n",
        "run": run_block,
        "name": run_id,
        "force": bool(force),
    }
    if dataset_id:
        body["dataset_id"] = dataset_id
    if external_source:
        body["external_source"] = external_source
    return body


def build_all_bodies(
    boosters: _BoosterArgs,
    *,
    feature_schema_sha: str,
    name_prefix: str,
    dataset_id: str | None,
    external_source: str | None,
    force: bool,
    families: list[str] | None = None,
    categorical_codes: dict[str, list[str]] | None = None,
) -> list[dict[str, Any]]:
    """Build the three NK / LK / PK request bodies in canonical order."""
    return [
        build_request_body(
            category=cat,
            artifact_uri=boosters.uris[cat],
            feature_schema_sha=feature_schema_sha,
            name_prefix=name_prefix,
            dataset_id=dataset_id,
            external_source=external_source,
            force=force,
            families=families,
            categorical_codes=categorical_codes,
        )
        for cat in CATEGORIES
    ]


def _post(api: str, api_key: str | None, body: dict[str, Any]) -> dict[str, Any]:
    headers = {}
    if api_key:
        headers["X-API-Key"] = api_key
    resp = requests.post(
        f"{api.rstrip('/')}/reranker-models/import-by-reference",
        json=body,
        headers=headers,
        timeout=30,
    )
    if resp.status_code == 409:
        raise SystemExit(f"RerankerModel {body['name']!r} already exists; pass --force to replace")
    if resp.status_code >= 300:
        raise SystemExit(
            f"POST /reranker-models/import-by-reference failed for {body['name']!r}: "
            f"{resp.status_code} {resp.text}"
        )
    return resp.json()


def _families_or_none() -> list[str] | None:
    """The live families to record, or ``None`` when protea is not importable."""
    try:
        return live_feature_families()
    except Exception:  # pragma: no cover - script run outside the protea env
        return None


def main() -> None:
    a = _args()
    api_key = a.api_key or os.environ.get("PROTEA_API_KEY")
    schema_sha = _resolve_schema_sha(a.feature_schema_sha)
    families = _families_or_none()
    categorical_codes = _load_categorical_codes(a.categorical_codes_json)
    boosters = _BoosterArgs(uris={"nk": a.nk_uri, "lk": a.lk_uri, "pk": a.pk_uri})
    bodies = build_all_bodies(
        boosters,
        feature_schema_sha=schema_sha,
        name_prefix=a.name_prefix,
        dataset_id=a.dataset_id,
        external_source=a.external_source,
        force=a.force,
        families=families,
        categorical_codes=categorical_codes,
    )
    results = []
    for body in bodies:
        print(
            f"[register] {body['name']!r} uri={body['artifact_uri']} "
            f"feature_schema_sha={schema_sha}",
            file=sys.stderr,
        )
        results.append(_post(a.api, api_key, body))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()

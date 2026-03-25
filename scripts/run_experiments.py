#!/usr/bin/env python
"""Experiment battery: train on GOA-N, evaluate against GOA-M.

K (limit_per_entry) is fixed via --limit.  The battery sweeps all other
axes: scoring formula, feature engineering, and distance threshold.

Submits prediction jobs through the PROTEA API, polls until completion,
computes CAFA metrics for every (PredictionSet × ScoringConfig × category)
triple and writes the results to a TSV file.

Usage
-----
    python scripts/run_experiments.py \\
        --goa-train  <annotation_set_id>   \\
        --goa-test   <annotation_set_id>   \\
        --emb-config <embedding_config_id> \\
        --ontology   <ontology_snapshot_id> \\
        [--limit     10]                   \\
        [--api-url   http://localhost:8000] \\
        [--output    results/goa200_vs_goa229.tsv] \\
        [--groups    A,C,D,E]

Groups
------
  A  Scoring-config sweep         (no features, threshold=None)
  C  Feature-engineering sweep    (all base scorings)
  D  Full-composite scoring       (alignment_weighted / composite)
  E  Distance-threshold sweep     (no features, all base scorings)
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Typing helpers
# ---------------------------------------------------------------------------

JsonDict = dict[str, Any]

# ---------------------------------------------------------------------------
# Experiment matrix definitions
# ---------------------------------------------------------------------------

# Each PredictionSetSpec describes a single prediction-job to submit.
# Multiple scoring configs will be applied to every PredictionSet.


@dataclass
class PredictionSpec:
    """Configuration for one prediction job."""

    label: str
    limit_per_entry: int = 10
    distance_threshold: float | None = None
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    aspect_separated_knn: bool = True
    search_backend: str = "numpy"
    # which scoring-config names to evaluate on this spec
    scoring_configs: list[str] = field(default_factory=list)


# Scoring configs that require alignment or taxonomy signals
_NEEDS_ALIGN = {"alignment_weighted", "composite"}
_NEEDS_TAXONOMY = {"composite"}

# Preset names that the runner will seed into the DB via POST /scoring/configs/presets
ALL_PRESET_NAMES = [
    "embedding_only",
    "embedding_plus_evidence",
    "evidence_primary",
    "alignment_weighted",
    "composite",
    "iea_dominant",
    "iea_equalised",
    "embedding_dominant",
]

# Base scoring configs (no feature engineering required)
BASE_SCORINGS = [
    "embedding_only",
    "embedding_plus_evidence",
    "evidence_primary",
    "iea_dominant",
    "iea_equalised",
    "embedding_dominant",
]


def build_experiment_matrix(groups: set[str], limit: int) -> list[PredictionSpec]:
    """Build the experiment matrix with a fixed K (limit_per_entry).

    Groups
    ------
    A  Scoring sweep — one PredictionSet (no features, no threshold).
       Evaluates all base scoring configs on the same raw distances.
    C  Feature-engineering sweep — two additional PredictionSets:
         C1: +alignments (NW+SW)
         C2: +alignments +taxonomy
       Each evaluated with all applicable scoring configs.
    D  Full-composite standalone entry (only if C is absent, to avoid
       running the same KNN twice).
    E  Distance-threshold sweep — three PredictionSets (no features):
         threshold ∈ {0.2, 0.3, 0.5}
       Evaluated with all base scoring configs.
    """
    specs: list[PredictionSpec] = []

    if "A" in groups:
        # Single PredictionSet, no features, no threshold.
        # All base scoring configs are applied post-hoc → one job only.
        specs.append(
            PredictionSpec(
                label=f"A_base_k{limit}",
                limit_per_entry=limit,
                scoring_configs=BASE_SCORINGS,
            )
        )

    if "C" in groups:
        specs.append(
            PredictionSpec(
                label=f"C1_align_k{limit}",
                limit_per_entry=limit,
                compute_alignments=True,
                compute_taxonomy=False,
                scoring_configs=BASE_SCORINGS + ["alignment_weighted"],
            )
        )
        specs.append(
            PredictionSpec(
                label=f"C2_full_k{limit}",
                limit_per_entry=limit,
                compute_alignments=True,
                compute_taxonomy=True,
                scoring_configs=BASE_SCORINGS + ["alignment_weighted", "composite"],
            )
        )

    if "D" in groups and "C" not in groups:
        # C2 already covers this; only add when C is skipped.
        specs.append(
            PredictionSpec(
                label=f"D_composite_k{limit}",
                limit_per_entry=limit,
                compute_alignments=True,
                compute_taxonomy=True,
                scoring_configs=["alignment_weighted", "composite"] + BASE_SCORINGS,
            )
        )

    if "E" in groups:
        for thresh in [0.2, 0.3, 0.5]:
            label = f"E_thresh{int(thresh * 100):02d}_k{limit}"
            specs.append(
                PredictionSpec(
                    label=label,
                    limit_per_entry=limit,
                    distance_threshold=thresh,
                    scoring_configs=BASE_SCORINGS,
                )
            )

    return specs


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


class ProteaClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, **params) -> JsonDict:
        r = requests.get(f"{self.base}{path}", params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: JsonDict) -> JsonDict:
        r = requests.post(
            f"{self.base}{path}", json=body, timeout=self.timeout
        )
        r.raise_for_status()
        return r.json()

    # ── Scoring configs ────────────────────────────────────────────────────

    def seed_preset_scoring_configs(self) -> list[str]:
        result = self._post("/scoring/configs/presets", {})
        return result.get("created", [])

    def list_scoring_configs(self) -> dict[str, str]:
        """Returns {name: id}."""
        configs = self._get("/scoring/configs")
        return {c["name"]: c["id"] for c in configs}

    # ── Jobs ───────────────────────────────────────────────────────────────

    def submit_predict_job(
        self,
        embedding_config_id: str,
        annotation_set_id: str,
        ontology_snapshot_id: str,
        spec: PredictionSpec,
        meta: JsonDict | None = None,
    ) -> str:
        payload: JsonDict = {
            "embedding_config_id": embedding_config_id,
            "annotation_set_id": annotation_set_id,
            "ontology_snapshot_id": ontology_snapshot_id,
            "limit_per_entry": spec.limit_per_entry,
            "compute_alignments": spec.compute_alignments,
            "compute_taxonomy": spec.compute_taxonomy,
            "aspect_separated_knn": spec.aspect_separated_knn,
            "search_backend": spec.search_backend,
        }
        if spec.distance_threshold is not None:
            payload["distance_threshold"] = spec.distance_threshold

        body: JsonDict = {
            "operation": "predict_go_terms",
            "queue_name": "protea.jobs",
            "payload": payload,
            "meta": meta or {"experiment_label": spec.label},
        }
        resp = self._post("/jobs", body)
        return resp["id"]

    def wait_for_job(
        self,
        job_id: str,
        poll_interval: float = 5.0,
        timeout: float = 3600.0,
    ) -> JsonDict:
        """Block until the job reaches a terminal state, then return the job dict."""
        deadline = time.monotonic() + timeout
        while True:
            job = self._get(f"/jobs/{job_id}")
            status = job["status"]
            if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
                return job
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"Job {job_id} did not finish within {timeout}s (last status: {status})"
                )
            time.sleep(poll_interval)

    def find_prediction_set_for_job(self, job_id: str) -> str | None:
        """Look up the PredictionSet created by a completed predict job.

        The coordinator stores the prediction_set_id in the job meta or in
        its events.  We check the events first.
        """
        events = self._get(f"/jobs/{job_id}/events")
        for ev in reversed(events):
            payload = ev.get("payload") or {}
            if "prediction_set_id" in payload:
                return payload["prediction_set_id"]
        # Fallback: scan prediction sets ordered by creation (newest first)
        return None

    # ── Metrics ───────────────────────────────────────────────────────────

    def compute_metrics(
        self,
        prediction_set_id: str,
        scoring_config_id: str,
        old_annotation_set_id: str,
        new_annotation_set_id: str,
        ontology_snapshot_id: str,
        category: str = "nk",
    ) -> JsonDict:
        return self._get(
            f"/scoring/prediction-sets/{prediction_set_id}/metrics",
            scoring_config_id=scoring_config_id,
            old_annotation_set_id=old_annotation_set_id,
            new_annotation_set_id=new_annotation_set_id,
            ontology_snapshot_id=ontology_snapshot_id,
            category=category,
        )


# ---------------------------------------------------------------------------
# PredictionSet discovery
# ---------------------------------------------------------------------------


def resolve_prediction_set(
    client: ProteaClient, job_id: str, job_result: JsonDict
) -> str | None:
    """Try several strategies to find the PredictionSet ID for a completed job."""
    # Strategy 1: look in job events
    ps_id = client.find_prediction_set_for_job(job_id)
    if ps_id:
        return ps_id

    # Strategy 2: check job meta
    meta = job_result.get("meta") or {}
    if "prediction_set_id" in meta:
        return meta["prediction_set_id"]

    return None


# ---------------------------------------------------------------------------
# Result row
# ---------------------------------------------------------------------------

RESULT_COLUMNS = [
    "experiment_label",
    "prediction_set_id",
    "scoring_config",
    "category",
    "fmax",
    "auc_pr",
    "best_threshold",
    "limit_per_entry",
    "distance_threshold",
    "compute_alignments",
    "compute_taxonomy",
    "job_id",
    "job_status",
]


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------


def run(args: argparse.Namespace) -> int:
    client = ProteaClient(args.api_url)

    # ── 1. Seed preset scoring configs ────────────────────────────────────
    print("[setup] Seeding preset scoring configs …", flush=True)
    created = client.seed_preset_scoring_configs()
    if created:
        print(f"        Created: {created}", flush=True)
    else:
        print("        All presets already present.", flush=True)

    scoring_map = client.list_scoring_configs()
    print(f"        Available configs: {list(scoring_map.keys())}", flush=True)

    # ── 2. Build experiment matrix ─────────────────────────────────────────
    groups = set(args.groups.upper().split(","))
    specs = build_experiment_matrix(groups, limit=args.limit)
    print(
        f"\n[matrix] {len(specs)} PredictionSet(s) to run across groups {groups} "
        f"with K={args.limit}."
    )

    # ── 3. Submit all prediction jobs ─────────────────────────────────────
    submitted: list[tuple[PredictionSpec, str]] = []  # (spec, job_id)

    for spec in specs:
        print(f"\n[submit] {spec.label} …", end=" ", flush=True)
        try:
            job_id = client.submit_predict_job(
                embedding_config_id=args.emb_config,
                annotation_set_id=args.goa_train,
                ontology_snapshot_id=args.ontology,
                spec=spec,
            )
            submitted.append((spec, job_id))
            print(f"job_id={job_id}", flush=True)
        except requests.HTTPError as exc:
            print(f"ERROR: {exc.response.status_code} {exc.response.text}", flush=True)
            if not args.skip_errors:
                return 1

    if not submitted:
        print("\n[error] No jobs submitted.", flush=True)
        return 1

    # ── 4. Poll until all jobs complete ───────────────────────────────────
    print(f"\n[poll] Waiting for {len(submitted)} job(s) to finish …", flush=True)
    completed: list[tuple[PredictionSpec, str, JsonDict]] = []

    for spec, job_id in submitted:
        print(f"       Polling {spec.label} (job {job_id}) …", end=" ", flush=True)
        try:
            job_result = client.wait_for_job(
                job_id,
                poll_interval=args.poll_interval,
                timeout=args.job_timeout,
            )
            status = job_result["status"]
            print(status, flush=True)
            completed.append((spec, job_id, job_result))
        except TimeoutError as exc:
            print(f"TIMEOUT: {exc}", flush=True)
            if not args.skip_errors:
                return 1

    # ── 5. Resolve PredictionSet IDs ──────────────────────────────────────
    print("\n[resolve] Looking up PredictionSet IDs …", flush=True)
    resolved: list[tuple[PredictionSpec, str, str]] = []  # (spec, job_id, ps_id)

    for spec, job_id, job_result in completed:
        if job_result["status"] != "SUCCEEDED":
            print(f"  SKIP {spec.label}: job {job_id} ended with {job_result['status']}")
            continue
        ps_id = resolve_prediction_set(client, job_id, job_result)
        if ps_id:
            print(f"  {spec.label}: prediction_set_id={ps_id}")
            resolved.append((spec, job_id, ps_id))
        else:
            print(f"  WARNING: could not find PredictionSet for job {job_id} ({spec.label})")
            if not args.skip_errors:
                return 1

    if not resolved:
        print("[error] No PredictionSets resolved.", flush=True)
        return 1

    # ── 6. Compute metrics for every (PredictionSet × scoring × category) ─
    print(f"\n[metrics] Computing metrics for {len(resolved)} prediction set(s) …", flush=True)
    rows: list[dict[str, Any]] = []

    for spec, job_id, ps_id in resolved:
        for scoring_name in spec.scoring_configs:
            scoring_id = scoring_map.get(scoring_name)
            if scoring_id is None:
                print(f"  WARNING: scoring config '{scoring_name}' not in DB, skipping.")
                continue

            for category in ["nk", "lk"]:
                tag = f"{spec.label}/{scoring_name}/{category}"
                print(f"  {tag} … ", end="", flush=True)
                try:
                    result = client.compute_metrics(
                        prediction_set_id=ps_id,
                        scoring_config_id=scoring_id,
                        old_annotation_set_id=args.goa_train,
                        new_annotation_set_id=args.goa_test,
                        ontology_snapshot_id=args.ontology,
                        category=category,
                    )
                    fmax = result.get("fmax", "")
                    auc = result.get("auc_pr", "")
                    best_t = result.get("best_threshold", "")
                    print(f"Fmax={fmax:.4f}  AUC-PR={auc:.4f}  @t={best_t:.3f}", flush=True)

                    rows.append(
                        {
                            "experiment_label": spec.label,
                            "prediction_set_id": ps_id,
                            "scoring_config": scoring_name,
                            "category": category,
                            "fmax": fmax,
                            "auc_pr": auc,
                            "best_threshold": best_t,
                            "limit_per_entry": spec.limit_per_entry,
                            "distance_threshold": spec.distance_threshold
                            if spec.distance_threshold is not None
                            else "",
                            "compute_alignments": spec.compute_alignments,
                            "compute_taxonomy": spec.compute_taxonomy,
                            "job_id": job_id,
                            "job_status": "SUCCEEDED",
                        }
                    )
                except requests.HTTPError as exc:
                    print(f"ERROR {exc.response.status_code}: {exc.response.text}", flush=True)
                    rows.append(
                        {
                            "experiment_label": spec.label,
                            "prediction_set_id": ps_id,
                            "scoring_config": scoring_name,
                            "category": category,
                            "fmax": "ERROR",
                            "auc_pr": "ERROR",
                            "best_threshold": "ERROR",
                            "limit_per_entry": spec.limit_per_entry,
                            "distance_threshold": spec.distance_threshold
                            if spec.distance_threshold is not None
                            else "",
                            "compute_alignments": spec.compute_alignments,
                            "compute_taxonomy": spec.compute_taxonomy,
                            "job_id": job_id,
                            "job_status": "SUCCEEDED",
                        }
                    )

    # ── 7. Write results ──────────────────────────────────────────────────
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=RESULT_COLUMNS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n[done] {len(rows)} result rows written to {output}", flush=True)

    # Also dump a JSON summary for easier programmatic consumption
    json_output = output.with_suffix(".json")
    with json_output.open("w") as fh:
        json.dump(
            {
                "goa_train": args.goa_train,
                "goa_test": args.goa_test,
                "embedding_config": args.emb_config,
                "ontology_snapshot": args.ontology,
                "limit_per_entry": args.limit,
                "groups": args.groups,
                "results": rows,
            },
            fh,
            indent=2,
        )
    print(f"       JSON summary written to {json_output}", flush=True)

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--goa-train",
        required=True,
        metavar="UUID",
        help="AnnotationSet ID to use as KNN reference (e.g. GOA200).",
    )
    p.add_argument(
        "--goa-test",
        required=True,
        metavar="UUID",
        help="AnnotationSet ID to use as ground truth (e.g. GOA229).",
    )
    p.add_argument(
        "--emb-config",
        required=True,
        metavar="UUID",
        help="EmbeddingConfig UUID (must match the stored SequenceEmbeddings).",
    )
    p.add_argument(
        "--ontology",
        required=True,
        metavar="UUID",
        help="OntologySnapshot UUID to use for GO DAG evaluation.",
    )
    p.add_argument(
        "--api-url",
        default="http://localhost:8000",
        metavar="URL",
        help="PROTEA API base URL (default: http://localhost:8000).",
    )
    p.add_argument(
        "--output",
        default="results/experiments.tsv",
        metavar="PATH",
        help="Output TSV path (default: results/experiments.tsv).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=10,
        metavar="K",
        help="Fixed limit_per_entry (K) for all prediction jobs (default: 10).",
    )
    p.add_argument(
        "--groups",
        default="A,C,D,E",
        metavar="GROUPS",
        help=(
            "Comma-separated list of experiment groups to run "
            "(A=scoring, C=features, D=composite, E=threshold). "
            "Default: A,C,D,E."
        ),
    )
    p.add_argument(
        "--poll-interval",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="Seconds between job-status polls (default: 10).",
    )
    p.add_argument(
        "--job-timeout",
        type=float,
        default=7200.0,
        metavar="SECONDS",
        help="Maximum seconds to wait for a single job (default: 7200).",
    )
    p.add_argument(
        "--skip-errors",
        action="store_true",
        help="Continue past HTTP errors and failed jobs instead of aborting.",
    )
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))

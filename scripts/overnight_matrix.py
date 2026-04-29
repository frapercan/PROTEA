"""Overnight 8-PLM canonical benchmark: bootstrap + predict × 8 + eval × 8.

This script is a *single-shot* launcher designed to be started before going to
sleep. It owns the full pipeline from an empty DB (modulo embeddings) to a
summary Fmax table.

Phases
------
Phase 0 — Bootstrap (idempotent):
    1. load_ontology_snapshot for GO release 2024-03-28  (matches GOA 220)
    2. load_ontology_snapshot for GO release 2026-01-23  (matches GOA 230 — pivot)
    3. load_goa_annotations  release 220  → AnnotationSet A220
    4. load_goa_annotations  release 230  → AnnotationSet A230
       (auto-triggers generate_evaluation_set(220, 230) with pivot=2026-01-23)
    5. wait for the auto-triggered EvaluationSet to land.

Phase A — KNN predictions × 8 embedding configs:
    For every EmbeddingConfig currently in the DB, submit one
    ``predict_go_terms`` job over the delta accessions (NK ∪ LK ∪ PK) of the
    EvaluationSet. All feature families are on:

        search_backend            = faiss
        limit_per_entry           = 5
        aspect_separated_knn      = True
        compute_alignments        = True
        compute_taxonomy          = True
        compute_reranker_features = True
        compute_v6_features       = True

Phase B — CAFA evaluation × 8 prediction sets:
    For every successful PredictionSet from Phase A, submit
    ``run_cafa_evaluation`` against the bootstrap EvaluationSet. No
    ``scoring_config_id`` (baseline / raw KNN score).

Outputs
-------
    results/overnight_matrix_<timestamp>/
        bootstrap.json                 phase 0 ids and timings
        predictions.tsv                phase A manifest
        evaluations.tsv                phase B manifest
        summary_fmax.tsv               model × aspect × category Fmax table
        run.log                        human-readable trace

Usage
-----
    poetry run python scripts/overnight_matrix.py
    poetry run python scripts/overnight_matrix.py --dry-run
    poetry run python scripts/overnight_matrix.py --only esmc_300m,prostt5_xl

Every step checks the DB for existing artefacts and skips when the work is
already done. Safe to re-run after a partial failure.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import select

from protea.core.evaluation import load_evaluation_data_for_set
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings


# ── Canonical benchmark constants ────────────────────────────────────────────

ONTOLOGIES = [
    # (obo_url, obo_version, human label)
    (
        "https://release.geneontology.org/2024-03-28/ontology/go.obo",
        "releases/2024-03-28",
        "GO 2024-03-28 (for GOA 220)",
    ),
    (
        "https://release.geneontology.org/2026-01-23/ontology/go.obo",
        "releases/2026-01-23",
        "GO 2026-01-23 (pivot, for GOA 230)",
    ),
]

# The pivot OntologySnapshot used for KNN prediction and evaluation.
PIVOT_OBO_VERSION = "releases/2026-01-23"

# GOA releases to load.
GOA_LOADS = [
    # (source_version, gaf_url, obo_version)
    (
        "220",
        "https://ftp.ebi.ac.uk/pub/databases/GO/goa/old/UNIPROT/goa_uniprot_all.gaf.220.gz",
        "releases/2024-03-28",
    ),
    (
        "230",
        "https://ftp.ebi.ac.uk/pub/databases/GO/goa/old/UNIPROT/goa_uniprot_all.gaf.230.gz",
        "releases/2026-01-23",
    ),
]

# The KNN reference AnnotationSet (used by predict_go_terms) is GOA 220;
# the EvaluationSet is always the (220, 230) pair with pivot 2026-01-23.
REFERENCE_SOURCE_VERSION = "220"
EVAL_OLD_SOURCE_VERSION = "220"
EVAL_NEW_SOURCE_VERSION = "230"


# ── Utilities ────────────────────────────────────────────────────────────────


def _short_label(model_name: str) -> str:
    low = (model_name or "").lower()
    if "esm2_t36_3b" in low:
        return "esm2_3b"
    if "esm2_t33_650m" in low:
        return "esm2_650m"
    if "esmc_300m" in low:
        return "esmc_300m"
    if "esmc_600m" in low:
        return "esmc_600m"
    if "ankh-base" in low:
        return "ankh_base"
    if "ankh-large" in low:
        return "ankh_large"
    if "prostt5" in low:
        return "prostt5_xl"
    if "prot_t5_xl" in low:
        return "prott5_xl"
    return low.split("/")[-1]


class Log:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)

    def __call__(self, msg: str) -> None:
        line = f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}"
        print(line, flush=True)
        with self.path.open("a") as f:
            f.write(line + "\n")


def _post(api: str, path: str, **kw) -> dict[str, Any]:
    r = requests.post(f"{api}{path}", **kw)
    if r.status_code >= 400:
        raise SystemExit(f"POST {path} → {r.status_code} {r.text[:500]}")
    return r.json()


def _get(api: str, path: str, **kw) -> Any:
    r = requests.get(f"{api}{path}", **kw)
    r.raise_for_status()
    return r.json()


def _poll_job(
    api: str, job_id: str, *, poll: float, timeout: float, log: Log
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_status = None
    while True:
        job = _get(api, f"/jobs/{job_id}", timeout=30)
        status = (job.get("status") or "").upper()
        job["status"] = status
        if status != last_status:
            cur = job.get("progress_current") or "?"
            tot = job.get("progress_total") or "?"
            log(f"    job {job_id[:8]} {status}  ({cur}/{tot})")
            last_status = status
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if status != "SUCCEEDED":
                log(f"    job {job_id} → {status}: {job.get('error_message', '')[:400]}")
            return job
        if time.monotonic() > deadline:
            raise TimeoutError(f"job {job_id} timed out after {timeout}s")
        time.sleep(poll)


def _resolve_prediction_set(api: str, job_id: str) -> str | None:
    """Extract prediction_set_id from job events / result."""
    events = _get(api, f"/jobs/{job_id}/events", timeout=30)
    for ev in reversed(events):
        fields = ev.get("fields") or ev.get("payload") or {}
        if fields.get("prediction_set_id"):
            return fields["prediction_set_id"]
    job = _get(api, f"/jobs/{job_id}", timeout=30)
    result = job.get("result") or {}
    return result.get("prediction_set_id")


# ── Phase 0 — bootstrap ──────────────────────────────────────────────────────


def _ensure_ontology(
    api: str, *, obo_url: str, obo_version: str, factory, log: Log,
    poll: float, timeout: float,
) -> str:
    with factory() as s:
        row = s.execute(
            select(OntologySnapshot).where(OntologySnapshot.obo_version == obo_version)
        ).scalar_one_or_none()
        if row is not None:
            log(f"  ontology {obo_version} exists: {row.id}")
            return str(row.id)

    log(f"  submitting load_ontology_snapshot for {obo_version}")
    res = _post(api, "/annotations/snapshots/load", json={"obo_url": obo_url}, timeout=60)
    job_id = res["id"]
    job = _poll_job(api, job_id, poll=poll, timeout=timeout, log=log)
    if job["status"] != "SUCCEEDED":
        raise SystemExit(f"ontology load failed for {obo_version}")

    with factory() as s:
        row = s.execute(
            select(OntologySnapshot).where(OntologySnapshot.obo_version == obo_version)
        ).scalar_one()
        log(f"  ontology {obo_version} loaded: {row.id}")
        return str(row.id)


def _ensure_goa(
    api: str, *, source_version: str, gaf_url: str, ontology_snapshot_id: str,
    factory, log: Log, poll: float, timeout: float,
) -> tuple[str, str | None]:
    """Return (annotation_set_id, auto_eval_job_id | None)."""
    with factory() as s:
        row = s.execute(
            select(AnnotationSet).where(
                AnnotationSet.source == "goa",
                AnnotationSet.source_version == source_version,
            )
        ).scalar_one_or_none()
        if row is not None:
            log(f"  goa {source_version} exists: {row.id}")
            return str(row.id), None

    log(f"  submitting load_goa_annotations for goa {source_version}")
    body = {
        "ontology_snapshot_id": ontology_snapshot_id,
        "gaf_url": gaf_url,
        "source_version": source_version,
    }
    res = _post(api, "/annotations/sets/load-goa", json=body, timeout=60)
    job_id = res["id"]
    job = _poll_job(api, job_id, poll=poll, timeout=timeout, log=log)
    if job["status"] != "SUCCEEDED":
        raise SystemExit(f"goa load failed for {source_version}")

    result = job.get("result") or {}
    ann_id = result.get("annotation_set_id")
    auto_eval = result.get("auto_eval_job_id")
    if not ann_id:
        # Fallback: look up via ORM.
        with factory() as s:
            row = s.execute(
                select(AnnotationSet).where(
                    AnnotationSet.source == "goa",
                    AnnotationSet.source_version == source_version,
                )
            ).scalar_one()
            ann_id = str(row.id)
    log(f"  goa {source_version} loaded: {ann_id[:8]} (auto_eval={auto_eval})")
    return str(ann_id), auto_eval


def _ensure_eval_set(
    api: str, *, old_annotation_set_id: str, new_annotation_set_id: str,
    pivot_ontology_snapshot_id: str, auto_eval_job_id: str | None,
    factory, log: Log, poll: float, timeout: float,
) -> str:
    with factory() as s:
        row = s.execute(
            select(EvaluationSet).where(
                EvaluationSet.old_annotation_set_id == uuid.UUID(old_annotation_set_id),
                EvaluationSet.new_annotation_set_id == uuid.UUID(new_annotation_set_id),
            )
        ).scalar_one_or_none()
        if row is not None:
            log(f"  eval_set exists: {row.id}")
            return str(row.id)

    if auto_eval_job_id:
        log(f"  waiting for auto-triggered generate_evaluation_set job {auto_eval_job_id[:8]}")
        job = _poll_job(api, auto_eval_job_id, poll=poll, timeout=timeout, log=log)
        if job["status"] != "SUCCEEDED":
            log(f"  auto-trigger job {auto_eval_job_id} failed; submitting explicit")
            auto_eval_job_id = None

    if not auto_eval_job_id:
        log(f"  submitting explicit generate_evaluation_set")
        body = {
            "old_annotation_set_id": old_annotation_set_id,
            "new_annotation_set_id": new_annotation_set_id,
            "pivot_ontology_snapshot_id": pivot_ontology_snapshot_id,
        }
        res = _post(api, "/annotations/evaluation-sets/generate", json=body, timeout=60)
        job = _poll_job(api, res["id"], poll=poll, timeout=timeout, log=log)
        if job["status"] != "SUCCEEDED":
            raise SystemExit("generate_evaluation_set failed")

    with factory() as s:
        row = s.execute(
            select(EvaluationSet).where(
                EvaluationSet.old_annotation_set_id == uuid.UUID(old_annotation_set_id),
                EvaluationSet.new_annotation_set_id == uuid.UUID(new_annotation_set_id),
            )
        ).scalar_one()
        log(f"  eval_set ready: {row.id}")
        return str(row.id)


def run_phase0(args, *, factory, log: Log) -> dict[str, Any]:
    log("=" * 60)
    log("Phase 0 — Bootstrap")
    t0 = time.monotonic()

    ontology_ids: dict[str, str] = {}
    for url, ver, label in ONTOLOGIES:
        log(f"- {label}")
        oid = _ensure_ontology(
            args.api, obo_url=url, obo_version=ver, factory=factory, log=log,
            poll=args.poll, timeout=args.ontology_timeout,
        )
        ontology_ids[ver] = oid

    goa_ids: dict[str, str] = {}
    last_auto_eval: str | None = None
    for version, url, ver in GOA_LOADS:
        log(f"- GOA release {version}")
        aid, auto_eval = _ensure_goa(
            args.api, source_version=version, gaf_url=url,
            ontology_snapshot_id=ontology_ids[ver],
            factory=factory, log=log,
            poll=args.poll, timeout=args.goa_timeout,
        )
        goa_ids[version] = aid
        if auto_eval:
            last_auto_eval = auto_eval

    log("- EvaluationSet (220 → 230, reconciled, pivot 2026-01-23)")
    eval_id = _ensure_eval_set(
        args.api,
        old_annotation_set_id=goa_ids[EVAL_OLD_SOURCE_VERSION],
        new_annotation_set_id=goa_ids[EVAL_NEW_SOURCE_VERSION],
        pivot_ontology_snapshot_id=ontology_ids[PIVOT_OBO_VERSION],
        auto_eval_job_id=last_auto_eval,
        factory=factory, log=log,
        poll=args.poll, timeout=args.eval_timeout,
    )

    out = {
        "ontology_ids": ontology_ids,
        "annotation_set_ids": goa_ids,
        "evaluation_set_id": eval_id,
        "pivot_ontology_snapshot_id": ontology_ids[PIVOT_OBO_VERSION],
        "reference_annotation_set_id": goa_ids[REFERENCE_SOURCE_VERSION],
        "elapsed_seconds": time.monotonic() - t0,
    }
    log(f"Phase 0 done in {out['elapsed_seconds']:.0f}s")
    return out


# ── Phase A — predictions ────────────────────────────────────────────────────


def _delta_accessions(factory, eval_set_id: str) -> list[str]:
    with factory() as s:
        e = s.get(EvaluationSet, uuid.UUID(eval_set_id))
        if e is None:
            raise SystemExit(f"EvaluationSet {eval_set_id} not found")
        data, _ = load_evaluation_data_for_set(s, e)
        return sorted(set(data.nk) | set(data.lk) | set(data.pk))


def _submit_predict(
    api: str, *, embedding_config_id: str, annotation_set_id: str,
    ontology_snapshot_id: str, query_accessions: list[str], k: int, label: str,
) -> str:
    body = {
        "operation": "predict_go_terms",
        "queue_name": "protea.jobs",
        "payload": {
            "embedding_config_id": embedding_config_id,
            "annotation_set_id": annotation_set_id,
            "ontology_snapshot_id": ontology_snapshot_id,
            "query_accessions": query_accessions,
            "limit_per_entry": k,
            "search_backend": "faiss",
            "faiss_index_type": "IVFFlat",
            "aspect_separated_knn": True,
            "compute_alignments": True,
            "compute_taxonomy": True,
            "compute_reranker_features": True,
            "compute_v6_features": True,
        },
        "meta": {"experiment_label": label},
    }
    return _post(api, "/jobs", json=body, timeout=60)["id"]


def run_phase_a(args, *, bootstrap: dict, factory, out_dir: Path, log: Log) -> list[dict]:
    log("=" * 60)
    log("Phase A — predict_go_terms × 8 PLMs")
    accessions = _delta_accessions(factory, bootstrap["evaluation_set_id"])
    log(f"  delta accessions (NK∪LK∪PK): {len(accessions)}")

    configs = _get(args.api, "/embeddings/configs", timeout=30)
    only = {a.strip() for a in (args.only or "").split(",") if a.strip()}
    plans: list[dict] = []
    for c in configs:
        lab = _short_label(c["model_name"])
        if only and lab not in only:
            continue
        if (c.get("embedding_count") or 0) < args.min_embeddings:
            log(f"  [skip] {lab} embeddings={c.get('embedding_count')}")
            continue
        plans.append({"label": lab, "config": c})
    log(f"  planned: {len(plans)} prediction jobs (K={args.k})")

    if args.dry_run:
        for p in plans:
            log(f"    [dry] {p['label']:12s}  config={p['config']['id']}")
        return []

    submitted: list[dict] = []
    for p in plans:
        lab = p["label"]
        try:
            job_id = _submit_predict(
                args.api,
                embedding_config_id=p["config"]["id"],
                annotation_set_id=bootstrap["reference_annotation_set_id"],
                ontology_snapshot_id=bootstrap["pivot_ontology_snapshot_id"],
                query_accessions=accessions,
                k=args.k,
                label=f"overnight_matrix/{lab}/k{args.k}",
            )
            log(f"  [submit] {lab:12s}  job={job_id}")
            submitted.append({
                "model": lab,
                "model_name": p["config"]["model_name"],
                "embedding_config_id": p["config"]["id"],
                "k": args.k,
                "job_id": job_id,
            })
        except SystemExit as exc:
            log(f"  [err] {lab}: {exc}")

    # Poll.
    rows: list[dict] = []
    for s in submitted:
        log(f"  polling {s['model']} ({s['job_id']})")
        try:
            job = _poll_job(
                args.api, s["job_id"], poll=args.poll, timeout=args.predict_timeout, log=log,
            )
            status = job["status"]
            ps_id = _resolve_prediction_set(args.api, s["job_id"]) if status == "SUCCEEDED" else ""
            rows.append({**s, "status": status, "prediction_set_id": ps_id or ""})
            log(f"  [done]   {s['model']:12s}  {status}  ps={ps_id}")
        except TimeoutError as exc:
            log(f"  [timeout] {s['model']}: {exc}")
            rows.append({**s, "status": "TIMEOUT", "prediction_set_id": ""})

    # Manifest.
    manifest = out_dir / "predictions.tsv"
    cols = ["model", "model_name", "embedding_config_id", "k", "job_id",
            "status", "prediction_set_id"]
    with manifest.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    log(f"  → {manifest}")
    return rows


# ── Phase B — evaluations ────────────────────────────────────────────────────


def _submit_eval(api: str, *, eval_set_id: str, prediction_set_id: str) -> str:
    body = {"prediction_set_id": prediction_set_id}
    return _post(
        api, f"/annotations/evaluation-sets/{eval_set_id}/run", json=body, timeout=60,
    )["id"]


def run_phase_b(args, *, bootstrap: dict, pred_rows: list[dict],
                out_dir: Path, log: Log) -> list[dict]:
    log("=" * 60)
    log("Phase B — run_cafa_evaluation × prediction sets")
    ready = [r for r in pred_rows if r["status"] == "SUCCEEDED" and r["prediction_set_id"]]
    log(f"  {len(ready)}/{len(pred_rows)} prediction_sets ready for eval")
    if not ready:
        log("  nothing to evaluate; skipping")
        return []

    if args.dry_run:
        for r in ready:
            log(f"    [dry] eval {r['model']:12s}  ps={r['prediction_set_id']}")
        return []

    submitted: list[dict] = []
    for r in ready:
        try:
            job_id = _submit_eval(
                args.api,
                eval_set_id=bootstrap["evaluation_set_id"],
                prediction_set_id=r["prediction_set_id"],
            )
            log(f"  [submit] eval {r['model']:12s}  job={job_id}")
            submitted.append({**r, "eval_job_id": job_id})
        except SystemExit as exc:
            log(f"  [err] {r['model']}: {exc}")

    rows: list[dict] = []
    for s in submitted:
        log(f"  polling {s['model']} eval ({s['eval_job_id']})")
        try:
            job = _poll_job(
                args.api, s["eval_job_id"], poll=args.poll,
                timeout=args.eval_run_timeout, log=log,
            )
            status = job["status"]
            result = job.get("result") or {}
            eval_result_id = result.get("evaluation_result_id") or ""
            results = result.get("results") or {}
            rows.append({
                **s,
                "eval_status": status,
                "evaluation_result_id": eval_result_id,
                "results": results,
            })
            log(f"  [done] eval {s['model']:12s}  {status}  er={eval_result_id}")
        except TimeoutError as exc:
            log(f"  [timeout] eval {s['model']}: {exc}")
            rows.append({**s, "eval_status": "TIMEOUT",
                         "evaluation_result_id": "", "results": {}})

    # Eval manifest.
    manifest = out_dir / "evaluations.tsv"
    cols = ["model", "model_name", "embedding_config_id", "k",
            "prediction_set_id", "eval_job_id", "eval_status",
            "evaluation_result_id"]
    with manifest.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, delimiter="\t")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})
    log(f"  → {manifest}")

    # Summary Fmax table: model × category × aspect.
    summary_path = out_dir / "summary_fmax.tsv"
    with summary_path.open("w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["model", "category", "aspect", "fmax", "coverage", "n_proteins"])
        for r in rows:
            if r["eval_status"] != "SUCCEEDED":
                continue
            results = r["results"]
            for cat in ("NK", "LK", "PK"):
                cell = results.get(cat) or {}
                for asp in ("BPO", "MFO", "CCO"):
                    m = cell.get(asp)
                    if m is None:
                        continue
                    w.writerow([
                        r["model"], cat, asp,
                        m.get("fmax", ""), m.get("coverage", ""), m.get("n_proteins", ""),
                    ])
    log(f"  → {summary_path}")

    # Quick textual summary.
    for r in rows:
        if r["eval_status"] != "SUCCEEDED":
            continue
        fmaxs: list[float] = []
        for cat in ("NK", "LK", "PK"):
            cell = r["results"].get(cat) or {}
            for asp in ("BPO", "MFO", "CCO"):
                m = cell.get(asp)
                if m and m.get("fmax") is not None:
                    fmaxs.append(float(m["fmax"]))
        if fmaxs:
            log(f"  {r['model']:12s}  avg Fmax = {sum(fmaxs)/len(fmaxs):.4f}  ({len(fmaxs)}/9 cells)")

    return rows


# ── Entry point ──────────────────────────────────────────────────────────────


def _args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--only", default="",
                    help="Comma-separated model short labels to include (default: all).")
    ap.add_argument("--min-embeddings", type=int, default=1)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-bootstrap", action="store_true",
                    help="Resolve bootstrap IDs from DB; do not submit any Phase 0 job.")
    ap.add_argument("--poll", type=float, default=15.0)
    ap.add_argument("--ontology-timeout", type=float, default=2 * 3600)
    ap.add_argument("--goa-timeout", type=float, default=10 * 3600)
    ap.add_argument("--eval-timeout", type=float, default=2 * 3600)
    ap.add_argument("--predict-timeout", type=float, default=12 * 3600)
    ap.add_argument("--eval-run-timeout", type=float, default=2 * 3600)
    return ap.parse_args()


def _resolve_existing_bootstrap(factory, log: Log) -> dict[str, Any]:
    with factory() as s:
        onto = {o.obo_version: str(o.id) for o in s.execute(select(OntologySnapshot)).scalars()}
        ann = {a.source_version: str(a.id)
               for a in s.execute(
                   select(AnnotationSet).where(AnnotationSet.source == "goa")
               ).scalars()}
        pivot_id = onto.get(PIVOT_OBO_VERSION)
        ref_id = ann.get(REFERENCE_SOURCE_VERSION)
        new_id = ann.get(EVAL_NEW_SOURCE_VERSION)
        if not (pivot_id and ref_id and new_id):
            raise SystemExit(
                "--skip-bootstrap: could not resolve ontology/annotation sets from DB"
            )
        row = s.execute(
            select(EvaluationSet).where(
                EvaluationSet.old_annotation_set_id == uuid.UUID(ref_id),
                EvaluationSet.new_annotation_set_id == uuid.UUID(new_id),
            )
        ).scalar_one_or_none()
        if row is None:
            raise SystemExit("--skip-bootstrap: EvaluationSet (220,230) not found")
        log(f"  resolved eval_set={row.id}  pivot={pivot_id}  ref={ref_id}")
        return {
            "ontology_ids": onto,
            "annotation_set_ids": ann,
            "evaluation_set_id": str(row.id),
            "pivot_ontology_snapshot_id": pivot_id,
            "reference_annotation_set_id": ref_id,
            "elapsed_seconds": 0.0,
        }


def main() -> int:
    args = _args()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = PROJECT_ROOT / "results" / f"overnight_matrix_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    log = Log(out_dir / "run.log")
    log(f"overnight_matrix START — output={out_dir}")
    log(f"  api={args.api}  k={args.k}  dry_run={args.dry_run}")

    settings = load_settings(PROJECT_ROOT)
    factory = build_session_factory(settings.db_url)

    if args.skip_bootstrap:
        log("Phase 0 — skipped (resolving from DB)")
        bootstrap = _resolve_existing_bootstrap(factory, log)
    else:
        bootstrap = run_phase0(args, factory=factory, log=log)

    (out_dir / "bootstrap.json").write_text(json.dumps(bootstrap, indent=2, default=str))

    pred_rows = run_phase_a(args, bootstrap=bootstrap, factory=factory,
                            out_dir=out_dir, log=log)
    run_phase_b(args, bootstrap=bootstrap, pred_rows=pred_rows,
                out_dir=out_dir, log=log)

    log("overnight_matrix DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())

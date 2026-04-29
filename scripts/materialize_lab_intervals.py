"""Materialize EvaluationSet + QuerySet rows for every snapshot pair the lab
benchmarks consume.

The lab dump (``train_reranker_auto`` with ``dump_only=True``) historically
recomputed the per-pair delta on the fly via ``compute_evaluation_data``.
That violated the project rule "never recompute on-the-fly what can be
persisted and reused".  This script materializes the missing artefacts so
both the lab dump and ``predict_go_terms`` can read them by id.

For each consecutive pair ``(v_old, v_new)`` in ``--versions``:

  1. Resolve AnnotationSet ids by ``source_version``.
  2. Skip if an EvaluationSet already exists for that ``(old, new)`` pair.
  3. Otherwise queue a ``generate_evaluation_set`` job (reconciled mode,
     pivot snapshot) and wait for it to land.
  4. Skip QuerySet creation if a row named ``delta-{v_old}-{v_new}`` exists.
  5. Stream ``/annotations/evaluation-sets/{id}/delta-proteins.fasta?category=all``
     and POST it to ``/query-sets``.

Usage:

    python scripts/materialize_lab_intervals.py \\
        --versions 160 165 170 175 180 185 190 195 200 205 211 215 220 230

The default version list matches ``run_overnight_esmc300.sh``.
"""

from __future__ import annotations

import argparse
import io
import sys
import time

import requests

API = "http://127.0.0.1:8000"
PIVOT_SNAPSHOT = "35c3ad67-3002-47db-8f71-eeed69d22ad6"  # GO 2026-01-23 (post-wipe pivot)
DEFAULT_VERSIONS = [160, 165, 170, 175, 180, 185, 190, 195, 200, 205, 211, 215, 220, 230]
ANNOTATION_SOURCE = "goa"


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--versions", nargs="+", type=int, default=DEFAULT_VERSIONS)
    p.add_argument("--pivot", default=PIVOT_SNAPSHOT)
    p.add_argument("--api", default=API)
    p.add_argument("--source", default=ANNOTATION_SOURCE)
    p.add_argument("--poll-interval", type=float, default=5.0)
    p.add_argument("--poll-timeout", type=float, default=900.0,
                   help="seconds to wait for each generate_evaluation_set job")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def _resolve_annotation_sets(api: str, versions: list[int], source: str) -> dict[int, str]:
    r = requests.get(f"{api}/annotations/sets", params={"limit": 500}, timeout=30)
    r.raise_for_status()
    rows = r.json() if isinstance(r.json(), list) else r.json().get("items", [])
    needed = {str(v) for v in versions}
    out: dict[int, str] = {}
    for row in rows:
        v = str(row.get("source_version", ""))
        if v in needed and row.get("source") == source:
            out[int(v)] = row["id"]
    missing = [v for v in versions if v not in out]
    if missing:
        raise SystemExit(f"AnnotationSet missing for versions: {missing}")
    return out


def _existing_eval_sets(api: str) -> dict[tuple[str, str], dict]:
    r = requests.get(f"{api}/annotations/evaluation-sets", timeout=30)
    r.raise_for_status()
    out: dict[tuple[str, str], dict] = {}
    for row in r.json():
        out[(row["old_annotation_set_id"], row["new_annotation_set_id"])] = row
    return out


def _existing_query_sets(api: str) -> dict[str, str]:
    r = requests.get(f"{api}/query-sets", timeout=30)
    r.raise_for_status()
    rows = r.json() if isinstance(r.json(), list) else r.json().get("items", [])
    return {row["name"]: row["id"] for row in rows}


def _wait_job(api: str, job_id: str, *, poll: float, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    last_status = None
    while time.monotonic() < deadline:
        r = requests.get(f"{api}/jobs/{job_id}", timeout=30)
        r.raise_for_status()
        st = r.json()
        status = st.get("status")
        if status != last_status:
            print(f"    job {job_id[:8]} status={status}")
            last_status = status
        if status in {"succeeded", "failed", "cancelled"}:
            if status != "succeeded":
                raise SystemExit(f"job {job_id} ended with status={status}: {st.get('result')}")
            return st
        time.sleep(poll)
    raise SystemExit(f"job {job_id} timed out after {timeout}s")


def _generate_eval_set(api: str, *, old_set_id: str, new_set_id: str, pivot: str,
                       poll: float, timeout: float) -> str:
    body = {
        "old_annotation_set_id": old_set_id,
        "new_annotation_set_id": new_set_id,
        "pivot_ontology_snapshot_id": pivot,
    }
    r = requests.post(f"{api}/annotations/evaluation-sets/generate", json=body, timeout=30)
    r.raise_for_status()
    job_id = r.json()["id"]
    print(f"    queued generate_evaluation_set job {job_id[:8]}")
    _wait_job(api, job_id, poll=poll, timeout=timeout)

    # Resolve the new EvaluationSet by (old, new) pair
    existing = _existing_eval_sets(api)
    eval_row = existing.get((old_set_id, new_set_id))
    if eval_row is None:
        raise SystemExit(
            f"EvaluationSet not found after job succeeded for ({old_set_id[:8]},{new_set_id[:8]})"
        )
    return eval_row["id"]


def _upload_query_set(api: str, *, name: str, description: str, fasta_bytes: bytes) -> str:
    files = {"file": (f"{name}.fasta", io.BytesIO(fasta_bytes), "text/x-fasta")}
    data = {"name": name, "description": description}
    r = requests.post(f"{api}/query-sets", files=files, data=data, timeout=120)
    if r.status_code >= 400:
        raise SystemExit(f"query-set upload failed: {r.status_code} {r.text[:300]}")
    return r.json()["id"]


def main() -> None:
    a = _args()
    versions = sorted(set(a.versions))
    pairs = list(zip(versions[:-1], versions[1:], strict=False))
    if not pairs:
        raise SystemExit("need at least 2 versions to form one pair")

    print(f"[mat] resolving AnnotationSets for {len(versions)} versions ({a.source})")
    v_to_id = _resolve_annotation_sets(a.api, versions, a.source)
    for v in versions:
        print(f"  v{v:>3} → {v_to_id[v][:8]}")

    print(f"[mat] {len(pairs)} pairs to process")
    eval_existing = _existing_eval_sets(a.api)
    qs_existing = _existing_query_sets(a.api)

    summary: list[tuple[int, int, str, str, str]] = []  # (v_old, v_new, action_eval, action_qs, qs_id)
    for v_old, v_new in pairs:
        old_id, new_id = v_to_id[v_old], v_to_id[v_new]
        qs_name = f"delta-{v_old}-{v_new}"
        print(f"\n[pair] {v_old}→{v_new}  (old={old_id[:8]} new={new_id[:8]})")

        # 1) EvaluationSet
        eval_row = eval_existing.get((old_id, new_id))
        if eval_row is not None:
            eval_id = eval_row["id"]
            eval_action = "exists"
            print(f"    EvaluationSet exists: {eval_id[:8]}")
        else:
            if a.dry_run:
                print("    [dry-run] would generate EvaluationSet")
                summary.append((v_old, v_new, "would-create", "would-create", "-"))
                continue
            eval_id = _generate_eval_set(
                a.api, old_set_id=old_id, new_set_id=new_id, pivot=a.pivot,
                poll=a.poll_interval, timeout=a.poll_timeout,
            )
            eval_action = "created"
            print(f"    EvaluationSet created: {eval_id[:8]}")

        # 2) QuerySet
        if qs_name in qs_existing:
            qs_id = qs_existing[qs_name]
            qs_action = "exists"
            print(f"    QuerySet '{qs_name}' exists: {qs_id[:8]}")
        else:
            if a.dry_run:
                print(f"    [dry-run] would download FASTA and upload QuerySet '{qs_name}'")
                summary.append((v_old, v_new, eval_action, "would-create", "-"))
                continue
            print(f"    downloading delta-proteins.fasta for eval_set {eval_id[:8]}")
            r = requests.get(
                f"{a.api}/annotations/evaluation-sets/{eval_id}/delta-proteins.fasta",
                params={"category": "all"},
                timeout=120,
            )
            r.raise_for_status()
            fasta = r.content
            n_records = fasta.count(b">")
            print(f"    FASTA size={len(fasta):,} bytes, records≈{n_records}")
            description = (
                f"Delta proteins for evaluation_set {eval_id[:8]} ({v_old}->{v_new} reconciled). "
                "Used as query_set for predict_go_terms scoping to the evaluation universe only."
            )
            qs_id = _upload_query_set(a.api, name=qs_name, description=description, fasta_bytes=fasta)
            qs_action = "created"
            print(f"    QuerySet created: {qs_id[:8]}  ({n_records} entries)")

        summary.append((v_old, v_new, eval_action, qs_action, qs_id))

    print("\n[mat] === summary ===")
    print(f"  {'pair':<10} {'eval_set':<10} {'query_set':<10} {'qs_id'}")
    for v_old, v_new, ea, qa, qid in summary:
        print(f"  {v_old}-{v_new:<5} {ea:<10} {qa:<10} {qid}")
    print(f"\n[mat] {len(summary)} pairs processed")


if __name__ == "__main__":
    sys.exit(main())

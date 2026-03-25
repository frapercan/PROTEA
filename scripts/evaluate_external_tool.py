#!/usr/bin/env python3
"""Evaluate an external tool's GO predictions using the same CAFA protocol as PROTEA.

This script:
  1. Connects to PROTEA's DB to compute the NK/LK/PK ground truth for a given
     EvaluationSet (same logic as run_cafa_evaluation).
  2. Parses an external tool's output (eggNOG-mapper, InterProScan, BLAST) and
     converts it to CAFA-format predictions (protein  go_id  score).
  3. Runs cafaeval for NK, LK, PK and prints the Fmax table.

Usage:
  poetry run python scripts/evaluate_external_tool.py \
      --evaluation-set-id 42b34e79-6fe9-4fa0-b718-02f43a1e3192 \
      --tool emapper \
      --input /path/to/test_proteins.emapper.annotations \
      [--ia-file /path/to/IA_cafa6.tsv]
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import tempfile
import uuid
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy.orm import Session

from protea.core.evaluation import compute_evaluation_data
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.session import build_session_factory, session_scope
from protea.infrastructure.settings import load_settings

# ---------------------------------------------------------------------------
# Parsers for external tools
# ---------------------------------------------------------------------------

def parse_emapper(path: str) -> dict[str, set[str]]:
    """Parse eggNOG-mapper .annotations file → {protein: {GO:xxxx, ...}}."""
    predictions: dict[str, set[str]] = {}
    with open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 10:
                continue
            protein = cols[0]
            gos = cols[9]
            if gos == "-" or not gos.strip():
                continue
            go_set = {g.strip() for g in gos.split(",") if g.strip().startswith("GO:")}
            if go_set:
                predictions[protein] = go_set
    return predictions


def parse_interproscan(path: str) -> dict[str, set[str]]:
    """Parse InterProScan TSV output → {protein: {GO:xxxx, ...}}.

    InterProScan TSV columns (0-indexed):
      0: protein accession
      13: GO annotations (pipe-separated)
    """
    predictions: dict[str, set[str]] = {}
    with open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 14:
                continue
            protein = cols[0]
            gos = cols[13]
            if not gos or gos == "-":
                continue
            go_set = set()
            for g in gos.split("|"):
                g = g.strip()
                if g.startswith("GO:"):
                    # Strip source suffix e.g. "GO:0016020(InterPro)" → "GO:0016020"
                    paren = g.find("(")
                    go_set.add(g[:paren] if paren != -1 else g)
            if go_set:
                predictions.setdefault(protein, set()).update(go_set)
    return predictions


def parse_blast_go(path: str) -> dict[str, set[str]]:
    """Parse a simple TSV with columns: protein  go_id  [score].

    This is a generic CAFA-like format that can be produced from BLAST results
    via custom post-processing.
    """
    predictions: dict[str, set[str]] = {}
    with open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 2:
                continue
            protein = cols[0]
            go_id = cols[1]
            if go_id.startswith("GO:"):
                predictions.setdefault(protein, set()).add(go_id)
    return predictions


def parse_pannzer2(path: str) -> dict[str, dict[str, float]]:
    """Parse PANNZER2 anno.out file → {protein: {GO:xxxx: ppv_score, ...}}.

    PANNZER2 anno.out columns (tab-separated):
      0: qpid (query protein ID)
      1: type (e.g. MF_ARGOT, BP_ARGOT, CC_ARGOT)
      2: score (raw ARGOT score)
      3: PPV (positive predictive value, 0-1 calibrated confidence)
      4: id (GO ID, e.g. GO:0005524)
      5: desc (GO term description)

    We use PPV as the confidence score and filter for ARGOT predictions only.
    """
    predictions: dict[str, dict[str, float]] = {}
    with open(path) as f:
        for line in f:
            if line.startswith("#") or line.startswith("qpid"):
                continue
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 5:
                continue
            protein = cols[0]
            pred_type = cols[1]
            go_id = cols[4]
            if not go_id.startswith("GO:"):
                continue
            # Filter for ARGOT predictions (best PANNZER2 method)
            if "ARGOT" not in pred_type:
                continue
            try:
                ppv = float(cols[3])
            except (ValueError, IndexError):
                ppv = 1.0
            if protein not in predictions:
                predictions[protein] = {}
            # Keep highest PPV per (protein, GO) pair
            if go_id not in predictions[protein] or ppv > predictions[protein][go_id]:
                predictions[protein][go_id] = ppv
    return predictions


# Type alias: parsers return either binary sets or scored dicts
Predictions = dict[str, set[str]] | dict[str, dict[str, float]]

PARSERS: dict[str, callable] = {
    "emapper": parse_emapper,
    "interproscan": parse_interproscan,
    "blast": parse_blast_go,
    "pannzer2": parse_pannzer2,
}


# ---------------------------------------------------------------------------
# Ground truth + evaluation
# ---------------------------------------------------------------------------

def write_gt(annotations: dict[str, set[str]], path: str) -> None:
    with open(path, "w") as f:
        for protein in sorted(annotations):
            for go_id in sorted(annotations[protein]):
                f.write(f"{protein}\t{go_id}\n")


def write_cafa_predictions(
    predictions: Predictions,
    delta_proteins: set[str],
    path: str,
) -> int:
    """Write CAFA-format predictions for delta proteins.

    Accepts either binary predictions ({protein: {go_ids}}) or scored
    predictions ({protein: {go_id: score}}). Binary predictions are
    written with score 1.0.

    Returns: number of (protein, GO) pairs written.
    """
    n = 0
    with open(path, "w") as f:
        for protein in sorted(predictions):
            if protein not in delta_proteins:
                continue
            terms = predictions[protein]
            if isinstance(terms, dict):
                for go_id in sorted(terms):
                    f.write(f"{protein}\t{go_id}\t{terms[go_id]:.4f}\n")
                    n += 1
            else:
                for go_id in sorted(terms):
                    f.write(f"{protein}\t{go_id}\t1.0000\n")
                    n += 1
    return n


def download_file(url: str, dest: str) -> None:
    import gzip
    import shutil

    import requests

    if url.startswith("/") or url.startswith("file://"):
        local = url[len("file://"):] if url.startswith("file://") else url
        if url.endswith(".gz"):
            with gzip.open(local, "rb") as src, open(dest, "wb") as f:
                shutil.copyfileobj(src, f)
        else:
            shutil.copy2(local, dest)
        return

    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    if url.endswith(".gz"):
        with open(dest, "wb") as f:
            f.write(gzip.decompress(resp.content))
    else:
        with open(dest, "w") as f:
            f.write(resp.text)


NS_LABELS = {
    "biological_process": "BPO",
    "molecular_function": "MFO",
    "cellular_component": "CCO",
}


def run_evaluation(
    session: Session,
    eval_set_id: uuid.UUID,
    predictions: Predictions,
    ia_file: str | None = None,
    artifacts_dir: str | None = None,
) -> dict[str, dict[str, float]]:
    from cafaeval.evaluation import cafa_eval

    eval_set = session.get(EvaluationSet, eval_set_id)
    if eval_set is None:
        raise ValueError(f"EvaluationSet {eval_set_id} not found")

    ann_old = session.get(AnnotationSet, eval_set.old_annotation_set_id)
    snapshot = session.get(OntologySnapshot, ann_old.ontology_snapshot_id)

    print("Computing ground truth delta...")
    data = compute_evaluation_data(
        session,
        eval_set.old_annotation_set_id,
        eval_set.new_annotation_set_id,
        ann_old.ontology_snapshot_id,
    )
    print(f"  NK: {data.nk_proteins} proteins, {data.nk_annotations} annotations")
    print(f"  LK: {data.lk_proteins} proteins, {data.lk_annotations} annotations")
    print(f"  PK: {data.pk_proteins} proteins, {data.pk_annotations} annotations")

    delta_proteins = set(data.nk) | set(data.lk) | set(data.pk)
    covered = delta_proteins & set(predictions)
    print(f"\nExternal tool covers {len(covered)}/{len(delta_proteins)} delta proteins "
          f"({100*len(covered)/len(delta_proteins):.1f}%)")

    # Release DB connection before cafaeval forks
    session.commit()

    results: dict[str, dict[str, float]] = {}

    with tempfile.TemporaryDirectory(prefix="protea_ext_eval_") as tmpdir:
        # Download OBO
        print(f"Downloading OBO from {snapshot.obo_url}...")
        obo_path = os.path.join(tmpdir, "go.obo")
        download_file(snapshot.obo_url, obo_path)

        # Resolve IA file
        ia_path = ia_file
        if ia_path is None and snapshot.ia_url:
            ia_path = os.path.join(tmpdir, "ia.tsv")
            print(f"Downloading IA from {snapshot.ia_url}...")
            download_file(snapshot.ia_url, ia_path)
        if ia_path:
            print(f"Using IA file: {ia_path}")
        else:
            print("WARNING: No IA file — using uniform IC=1")

        # Write ground truth
        gt_dir = artifacts_dir or tmpdir
        os.makedirs(gt_dir, exist_ok=True)

        nk_path = os.path.join(gt_dir, "gt_NK.tsv")
        lk_path = os.path.join(gt_dir, "gt_LK.tsv")
        pk_path = os.path.join(gt_dir, "gt_PK.tsv")
        pk_known_path = os.path.join(gt_dir, "pk_known_terms.tsv")

        write_gt(data.nk, nk_path)
        write_gt(data.lk, lk_path)
        write_gt(data.pk, pk_path)
        write_gt(data.pk_known, pk_known_path)

        # Write predictions
        pred_dir = os.path.join(gt_dir, "predictions")
        os.makedirs(pred_dir, exist_ok=True)
        pred_path = os.path.join(pred_dir, "predictions.tsv")
        n_written = write_cafa_predictions(predictions, delta_proteins, pred_path)
        print(f"Wrote {n_written} prediction pairs for {len(covered)} proteins")

        # Run cafaeval per setting
        for setting, gt_file, known_file in [
            ("NK", nk_path, None),
            ("LK", lk_path, None),
            ("PK", pk_path, pk_known_path),
        ]:
            print(f"\nEvaluating {setting}...")
            try:
                old_sigterm = signal.signal(signal.SIGTERM, signal.SIG_DFL)
                old_sigint = signal.signal(signal.SIGINT, signal.SIG_DFL)
                try:
                    df, dfs_best = cafa_eval(
                        obo_path,
                        pred_dir,
                        gt_file,
                        ia=ia_path,
                        exclude=known_file,
                        prop="max",
                        norm="cafa",
                        n_cpu=1,
                    )
                finally:
                    signal.signal(signal.SIGTERM, old_sigterm)
                    signal.signal(signal.SIGINT, old_sigint)

                df_f = dfs_best.get("f")
                if df_f is not None and not df_f.empty:
                    df_f = df_f.reset_index()
                    for _, row in df_f.iterrows():
                        ns = NS_LABELS.get(str(row.get("ns", "")))
                        if ns:
                            key = f"{setting}-{ns}"
                            results[key] = {
                                "fmax": round(float(row.get("f", 0)), 4),
                                "precision": round(float(row.get("pr", 0)), 4),
                                "recall": round(float(row.get("rc", 0)), 4),
                                "coverage": round(float(row.get("cov_max", row.get("cov", 0))), 4),
                            }
                            print(f"  {key}: Fmax={results[key]['fmax']:.3f}  "
                                  f"P={results[key]['precision']:.3f}  "
                                  f"R={results[key]['recall']:.3f}  "
                                  f"Cov={results[key]['coverage']:.3f}")

            except Exception as exc:
                print(f"  {setting} FAILED: {exc}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Evaluate external tool predictions with CAFA protocol")
    parser.add_argument("--evaluation-set-id", required=True, help="EvaluationSet UUID")
    parser.add_argument("--tool", required=True, choices=list(PARSERS.keys()), help="External tool format")
    parser.add_argument("--input", required=True, help="Path to tool output file")
    parser.add_argument("--ia-file", default=None, help="Path to IA TSV file (optional)")
    parser.add_argument("--artifacts-dir", default=None, help="Directory to save evaluation artifacts")
    args = parser.parse_args()

    settings = load_settings(Path(__file__).resolve().parent.parent)
    factory = build_session_factory(settings.db_url)

    print(f"Parsing {args.tool} output from {args.input}...")
    parse_fn = PARSERS[args.tool]
    predictions = parse_fn(args.input)
    print(f"Parsed {len(predictions)} proteins with GO predictions")

    eval_set_id = uuid.UUID(args.evaluation_set_id)

    with session_scope(factory) as session:
        results = run_evaluation(
            session,
            eval_set_id,
            predictions,
            ia_file=args.ia_file,
            artifacts_dir=args.artifacts_dir,
        )

    # Print summary table
    print("\n" + "=" * 80)
    print("SUMMARY — Fmax (IA-weighted)")
    print("=" * 80)
    header = f"{'Method':<20} {'NK-BPO':>8} {'NK-MFO':>8} {'NK-CCO':>8} {'LK-BPO':>8} {'LK-MFO':>8} {'LK-CCO':>8} {'PK-BPO':>8} {'PK-MFO':>8} {'PK-CCO':>8}"
    print(header)
    print("-" * len(header))

    row = f"{args.tool:<20}"
    for setting in ["NK", "LK", "PK"]:
        for ns in ["BPO", "MFO", "CCO"]:
            key = f"{setting}-{ns}"
            val = results.get(key, {}).get("fmax", 0.0)
            row += f" {val:>8.3f}"
    print(row)
    print()


if __name__ == "__main__":
    main()

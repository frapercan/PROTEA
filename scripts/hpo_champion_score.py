"""Per-cell champion-score HPO over the platform (A-SCORE.2).

Derives the champion ``ScoringConfig`` per (category, aspect) cell that maximises
the IA-weighted ``f_micro_w`` on the SELECT 220->227 validation window, driving
everything through PROTEA's HTTP surface:

  trial -> POST /scoring/configs (the trial params) ->
  POST /jobs batch_rescore_evaluation (one prediction_set + the whole
  generation's configs, evaluated in a single load) ->
  read f_micro_w per cell from GET .../results.

Search strategy (no Optuna dependency): a coarse grid over the cheap re-scorable
axes (ia_prior gamma, a small set of signal-weight vectors, evidence-tier
presets) crossed with the OUTER K axis (which Ankh-base prediction_set), then a
local random refine around the per-cell leaders. Soft-vote temperature is
predict-time only (#628) and is deliberately excluded.

Guard (LEAN, beat-pooled-or-revert): every config is evaluated on TWO disjoint
protein folds of the SAME validation window (the batch op's ``protein_folds``
hash split). The per-cell champion is selected on fold 0 (train) and kept only
if it also beats the pooled (all-cells) best config on fold 1 (valid); otherwise
the cell reverts to the pooled config. No new EvaluationSet rows, no homology
clustering (reserved for A-SCORE.4).

Read-only on prediction / annotation data: the harness only creates
ScoringConfigs and EvaluationResults. Concurrency is bounded by
``--max-inflight`` (default 2) so the freshly-redeployed DB is never overloaded.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import requests

# --- Fixed inputs (A-SCORE.2 spawn context) -------------------------------

PRED_SETS_BY_K: dict[int, str] = {
    3: "8a7cc6c9-3456-4e33-8f35-fc74fbc57004",
    5: "b7dcd6e7-a1ae-4ae8-a75f-31c44cc5bfd4",
    10: "d988c1df-5aad-4c34-b435-e5f181e045b4",
    30: "746e68e4-1f1c-4e64-ad79-efbe833b1206",
}
EVAL_SET_ID = "a3be0a6d-7f21-4e7e-8ac8-eabccf77e8e0"
IA_FILE = (
    "/home/frapercan/Thesis2/repositories/PROTEA/data/benchmarks/"
    "ia_select_220/IA_35c3ad67_goa220.tsv"
)
EVAL_QUEUE = "protea.evaluations"

CATS = ("NK", "LK", "PK")
ASPECTS = ("MFO", "BPO", "CCO")

# Composite baseline f_micro_w per cell (from the existing Ankh-base composite
# eval), used to report lift. NK-MFO / PK-BPO are the smoke cells.
COMPOSITE_BASELINE: dict[str, float] = {
    "NK-MFO": 0.4935,
    "PK-BPO": 0.0711,
}

# Signal keys the linear scorer understands (DEFAULT_WEIGHTS superset).
SIGNAL_KEYS = (
    "embedding_similarity",
    "identity_nw",
    "identity_sw",
    "taxonomic_proximity",
    "neighbor_vote_fraction",
    "coverage",
    "ref_annotation_density",
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
)


# --------------------------------------------------------------------------
# HTTP client
# --------------------------------------------------------------------------


@dataclass
class Client:
    base: str
    token: str
    session: requests.Session = field(default_factory=requests.Session)

    @property
    def _h(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def create_scoring_config(self, name: str, weights: dict, params: dict | None,
                              formula: str, evidence_weights: dict | None) -> str:
        body: dict[str, Any] = {"name": name, "formula": formula, "weights": weights}
        if params is not None:
            body["params"] = params
        if evidence_weights is not None:
            body["evidence_weights"] = evidence_weights
        r = self.session.post(f"{self.base}/scoring/configs", json=body, headers=self._h, timeout=60)
        r.raise_for_status()
        return str(r.json()["id"])

    def dispatch_batch(self, pred_set_id: str, config_ids: list[str], *,
                       folds: int, fold: int, seed: int, th_step: float = 0.01) -> str:
        payload = {
            "evaluation_set_id": EVAL_SET_ID,
            "prediction_set_id": pred_set_id,
            "scoring_config_ids": config_ids,
            "ia_file": IA_FILE,
            "protein_folds": folds,
            "protein_fold": fold,
            "protein_seed": seed,
            "th_step": th_step,
        }
        body = {"operation": "batch_rescore_evaluation", "queue_name": EVAL_QUEUE,
                "payload": payload}
        r = self.session.post(f"{self.base}/jobs", json=body, headers=self._h, timeout=60)
        if r.status_code == 409:  # dedup: an identical batch is already running
            return str(r.json().get("id") or r.json().get("job_id"))
        r.raise_for_status()
        return str(r.json()["id"])

    def job_status(self, job_id: str) -> str:
        r = self.session.get(f"{self.base}/jobs/{job_id}", headers=self._h, timeout=60)
        r.raise_for_status()
        return str(r.json()["status"])

    def list_results(self) -> list[dict]:
        r = self.session.get(
            f"{self.base}/annotations/evaluation-sets/{EVAL_SET_ID}/results",
            headers=self._h, timeout=120,
        )
        r.raise_for_status()
        return list(r.json())


# --------------------------------------------------------------------------
# f_micro_w lookup
# --------------------------------------------------------------------------


def fmw(result: dict, cat: str, aspect: str) -> float | None:
    """Pull f_micro_w for one cell out of an EvaluationResult blob."""
    blob = (result.get("results") or {}).get(cat) or {}
    asp = blob.get(aspect) or blob.get(aspect.lower()) or {}
    v = asp.get("f_micro_w") if isinstance(asp, dict) else None
    return float(v) if v is not None else None


def results_by_config(client: Client) -> dict[str, dict]:
    """Newest EvaluationResult per scoring_config_id (the list is newest-first)."""
    out: dict[str, dict] = {}
    for row in client.list_results():
        cid = row.get("scoring_config_id")
        if cid and cid not in out:
            out[cid] = row
    return out


# --------------------------------------------------------------------------
# Trial search space
# --------------------------------------------------------------------------


def base_weight_vectors() -> list[dict[str, float]]:
    """A small coarse set of signal-weight presets to seed the search.

    A-SCORE.1 found vote + taxonomy strongest and alignment redundant; this seeds
    those plus the embedding-only and a few rich-axis mixes, to be RE-CONFIRMED on
    the de-saturated (IA-prior) score rather than assumed.
    """
    return [
        {"embedding_similarity": 1.0},
        {"embedding_similarity": 1.0, "neighbor_vote_fraction": 1.0},
        {"embedding_similarity": 1.0, "neighbor_vote_fraction": 1.0, "taxonomic_proximity": 0.5},
        {"embedding_similarity": 1.0, "neighbor_vote_fraction": 0.5, "taxonomic_proximity": 0.5,
         "identity_sw": 0.3},
        {"embedding_similarity": 1.0, "neighbor_vote_fraction": 1.0,
         "anc2vec_neighbor_maxcos": 0.5},
        {"embedding_similarity": 1.0, "neighbor_vote_fraction": 0.5, "taxonomic_proximity": 0.5,
         "ref_annotation_density": 0.3},
    ]


GAMMAS = (0.0, 0.25, 0.5, 1.0)  # ia_prior gamma; 0.0 = prior off

# The IA-exact prior (source="ia", reads the real IA file as term_ia) needs the
# Step-0 scorer wiring deployed. Until that PR merges + redeploys, the live stack
# falls back to source="frequency" (a GO-IDF proxy); the search is identical, only
# the prior's exactness differs. Selected at runtime via --ia-source.


@dataclass(frozen=True)
class Trial:
    k: int
    weights: dict[str, float]
    gamma: float
    ia_source: str
    formula: str = "linear"
    evidence_weights: dict | None = None

    def params(self) -> dict | None:
        if self.gamma <= 0.0:
            return None
        return {"ia_prior": {"enabled": True, "source": self.ia_source, "gamma": self.gamma}}

    def signature(self) -> str:
        w = ",".join(f"{k}:{v}" for k, v in sorted(self.weights.items()))
        return f"k{self.k}|{self.formula}|g{self.gamma}|{self.ia_source}|{w}"


def coarse_trials(ks: list[int], ia_source: str = "ia") -> list[Trial]:
    trials: list[Trial] = []
    for k, wv, gamma in itertools.product(ks, base_weight_vectors(), GAMMAS):
        trials.append(Trial(k=k, weights=dict(wv), gamma=gamma, ia_source=ia_source))
    # de-dup by signature
    seen: dict[str, Trial] = {}
    for t in trials:
        seen.setdefault(t.signature(), t)
    return list(seen.values())


def refine_trials(leader: Trial, n: int, rng: random.Random) -> list[Trial]:
    """Local random perturbation of a leader trial's weights + gamma."""
    out: list[Trial] = []
    for _ in range(n):
        w = dict(leader.weights)
        # jitter existing weights and occasionally toggle one extra signal
        for key in list(w):
            if key == "embedding_similarity":
                continue
            w[key] = round(max(0.0, w[key] + rng.uniform(-0.3, 0.3)), 2)
            if w[key] == 0.0:
                del w[key]
        if rng.random() < 0.4:
            extra = rng.choice(SIGNAL_KEYS[1:])
            w[extra] = round(rng.uniform(0.2, 0.8), 2)
        gamma = round(max(0.0, leader.gamma + rng.uniform(-0.3, 0.3)), 3)
        out.append(Trial(k=leader.k, weights=w, gamma=gamma, ia_source=leader.ia_source))
    return out


# --------------------------------------------------------------------------
# Dispatch + collect (bounded concurrency, grouped by (K, fold))
# --------------------------------------------------------------------------


def _register_configs(client: Client, trials: list[Trial], tag: str) -> dict[str, Trial]:
    """Create one ScoringConfig per trial; return {config_id: trial}."""
    cfg_to_trial: dict[str, Trial] = {}
    for i, t in enumerate(trials):
        name = f"hpo_{tag}_{i:04d}_{uuid.uuid4().hex[:6]}"
        cid = client.create_scoring_config(
            name=name, weights=t.weights, params=t.params(),
            formula=t.formula, evidence_weights=t.evidence_weights,
        )
        cfg_to_trial[cid] = t
    return cfg_to_trial


def _wait_jobs(client: Client, jobs: list[str], poll: float = 15.0) -> None:
    pending = set(jobs)
    while pending:
        time.sleep(poll)
        for jid in list(pending):
            st = client.job_status(jid)
            if st in ("SUCCEEDED", "succeeded"):
                pending.discard(jid)
            elif st in ("FAILED", "failed", "CANCELLED", "cancelled"):
                raise RuntimeError(f"batch job {jid} ended {st}")


def evaluate_trials(client: Client, trials: list[Trial], *, tag: str, folds: int,
                    fold: int, seed: int, max_inflight: int, th_step: float = 0.01
                    ) -> dict[str, dict]:
    """Register configs, dispatch batched eval jobs (one per K), collect results.

    Returns {config_id: {trial, cells: {cell: f_micro_w}}}.
    """
    cfg_to_trial = _register_configs(client, trials, tag)
    by_k: dict[int, list[str]] = {}
    for cid, t in cfg_to_trial.items():
        by_k.setdefault(t.k, []).append(cid)

    job_ids: list[str] = []
    inflight: list[str] = []
    for k, cids in by_k.items():
        jid = client.dispatch_batch(PRED_SETS_BY_K[k], cids, folds=folds, fold=fold, seed=seed,
                                    th_step=th_step)
        job_ids.append(jid)
        inflight.append(jid)
        if len(inflight) >= max_inflight:
            _wait_jobs(client, inflight)
            inflight = []
    if inflight:
        _wait_jobs(client, inflight)

    rb = results_by_config(client)
    out: dict[str, dict] = {}
    for cid, t in cfg_to_trial.items():
        res = rb.get(cid)
        cells: dict[str, float | None] = {}
        if res is not None:
            for cat in CATS:
                for asp in ASPECTS:
                    cells[f"{cat}-{asp}"] = fmw(res, cat, asp)
        out[cid] = {"trial": t, "cells": cells, "result_id": res.get("id") if res else None}
    return out


def best_per_cell(evald: dict[str, dict], cells: list[str]) -> dict[str, tuple[str, float]]:
    """For each cell, the (config_id, f_micro_w) with the highest score."""
    best: dict[str, tuple[str, float]] = {}
    for cid, info in evald.items():
        for cell in cells:
            v = info["cells"].get(cell)
            if v is None:
                continue
            if cell not in best or v > best[cell][1]:
                best[cell] = (cid, v)
    return best


# --------------------------------------------------------------------------
# Main per-cell HPO with beat-pooled-or-revert guard
# --------------------------------------------------------------------------


def run_hpo(client: Client, cells: list[str], *, ks: list[int], seed: int,
            refine_per_cell: int, max_inflight: int, out_dir: str,
            ia_source: str = "ia", folds: int = 2, th_step: float = 0.01) -> dict:
    rng = random.Random(seed)
    os.makedirs(out_dir, exist_ok=True)
    train_fold = 0
    valid_fold = 1 if folds > 1 else 0  # folds==1 -> degraded guard on full cohort

    # --- Generation 1: coarse grid on the train fold ---
    coarse = coarse_trials(ks, ia_source=ia_source)
    print(f"[gen1] {len(coarse)} coarse trials over K={ks} ia_source={ia_source} folds={folds}")
    train = evaluate_trials(client, coarse, tag="g1train", folds=folds, fold=train_fold, seed=seed,
                            max_inflight=max_inflight, th_step=th_step)

    # --- Generation 2: refine around each cell's train leader ---
    leaders = best_per_cell(train, cells)
    refine: list[Trial] = []
    for _cell, (cid, _score) in leaders.items():
        refine.extend(refine_trials(train[cid]["trial"], refine_per_cell, rng))
    refine_train = evaluate_trials(client, refine, tag="g2train", folds=folds, fold=train_fold,
                                   seed=seed, max_inflight=max_inflight, th_step=th_step) if refine else {}

    all_train = {**train, **refine_train}
    train_best = best_per_cell(all_train, cells)

    # Pooled best = the single config with the highest MEAN f_micro_w across the
    # target cells on train (the all-cells generalist to beat).
    pooled_cid, pooled_mean = _pooled_best(all_train, cells)

    # --- Validate the per-cell champion vs pooled on the held-out fold ---
    candidate_cids = {cid for cid, _ in train_best.values()} | {pooled_cid}
    candidate_trials = [all_train[c]["trial"] for c in candidate_cids]
    valid = evaluate_trials(client, candidate_trials, tag="valid", folds=folds, fold=valid_fold,
                            seed=seed, max_inflight=max_inflight, th_step=th_step)
    valid_by_trial_sig = {info["trial"].signature(): info for info in valid.values()}

    champions: dict[str, dict] = {}
    for cell in cells:
        tr_cid, tr_score = train_best[cell]
        cell_sig = all_train[tr_cid]["trial"].signature()
        pooled_sig = all_train[pooled_cid]["trial"].signature()
        cell_valid = valid_by_trial_sig.get(cell_sig, {}).get("cells", {}).get(cell)
        pooled_valid = valid_by_trial_sig.get(pooled_sig, {}).get("cells", {}).get(cell)
        kept = (
            cell_valid is not None and pooled_valid is not None and cell_valid > pooled_valid
        )
        champions[cell] = {
            "train_best_config": tr_cid,
            "train_f_micro_w": tr_score,
            "cell_valid_f_micro_w": cell_valid,
            "pooled_valid_f_micro_w": pooled_valid,
            "verdict": "specialize" if kept else "revert_to_pooled",
            "champion_config": tr_cid if kept else pooled_cid,
            "champion_trial": (all_train[tr_cid] if kept else all_train[pooled_cid])["trial"].__dict__,
            "composite_baseline": COMPOSITE_BASELINE.get(cell),
        }

    report = {
        "cells": cells,
        "ks": ks,
        "ia_source": ia_source,
        "guard": (
            "held_out_protein_fold" if folds > 1
            else "DEGRADED_same_cohort (folds=1: deployed batch op lacks the fold "
                 "split; PR pending redeploy)"
        ),
        "pooled_best": {"config_id": pooled_cid, "train_mean_f_micro_w": pooled_mean},
        "champions": champions,
    }
    with open(os.path.join(out_dir, "hpo_report.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)
    return report


def _pooled_best(evald: dict[str, dict], cells: list[str]) -> tuple[str, float]:
    best_cid, best_mean = "", -1.0
    for cid, info in evald.items():
        vals = [info["cells"].get(c) for c in cells]
        vals = [v for v in vals if v is not None]
        if not vals:
            continue
        mean = sum(vals) / len(vals)
        if mean > best_mean:
            best_cid, best_mean = cid, mean
    return best_cid, best_mean


def register_champions(client: Client, report: dict) -> dict[str, str]:
    """Re-create each champion as a clearly-named, persisted ScoringConfig."""
    named: dict[str, str] = {}
    for cell, info in report["champions"].items():
        t = info["champion_trial"]
        weights = t["weights"]
        gamma = t.get("gamma", 0.0)
        params = (
            {"ia_prior": {"enabled": True, "source": t.get("ia_source", "ia"), "gamma": gamma}}
            if gamma and gamma > 0 else None
        )
        cat, asp = cell.split("-")
        name = f"champion_{cat.lower()}_{asp.lower()}"
        cid = client.create_scoring_config(
            name=name, weights=weights, params=params,
            formula=t.get("formula", "linear"), evidence_weights=t.get("evidence_weights"),
        )
        named[cell] = cid
    return named


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description="A-SCORE.2 per-cell champion-score HPO")
    ap.add_argument("--base", default=os.environ.get("PROTEA_API", "http://127.0.0.1:8000"))
    ap.add_argument("--token-file", default="/tmp/ascore2_token.txt")
    ap.add_argument("--cells", nargs="+", default=["NK-MFO", "PK-BPO"],
                    help="Cells to optimise (smoke = NK-MFO PK-BPO).")
    ap.add_argument("--ks", nargs="+", type=int, default=[3, 5, 10, 30])
    ap.add_argument("--seed", type=int, default=20260611)
    ap.add_argument("--refine-per-cell", type=int, default=6)
    ap.add_argument("--max-inflight", type=int, default=2)
    ap.add_argument("--ia-source", choices=["ia", "frequency"], default="ia",
                    help="ia = exact IA-file prior (needs Step-0 scorer deployed); "
                         "frequency = GO-IDF proxy fallback for the un-redeployed stack.")
    ap.add_argument("--folds", type=int, default=2,
                    help="Internal train/valid protein folds (2 = held-out guard; "
                         "1 = degraded same-cohort guard when the fold split is not deployed).")
    ap.add_argument("--th-step", type=float, default=0.01,
                    help="cafaeval tau-sweep step. 0.01 = full resolution (slow); a coarser "
                         "value (e.g. 0.05) speeds search trials at a small metric-resolution cost.")
    ap.add_argument("--out-dir", default=os.environ.get("ASCORE2_OUT", "/tmp/ascore2_hpo"))
    ap.add_argument("--register", action="store_true",
                    help="Register the champions as named ScoringConfigs after the search.")
    args = ap.parse_args()

    with open(args.token_file) as f:
        token = f.read().strip()
    client = Client(base=args.base, token=token)

    t0 = time.time()
    report = run_hpo(
        client, args.cells, ks=args.ks, seed=args.seed,
        refine_per_cell=args.refine_per_cell, max_inflight=args.max_inflight,
        out_dir=args.out_dir, ia_source=args.ia_source, folds=args.folds,
        th_step=args.th_step,
    )
    report["runtime_s"] = round(time.time() - t0, 1)

    if args.register:
        report["registered_champions"] = register_champions(client, report)
    with open(os.path.join(args.out_dir, "hpo_report.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)

    print("=" * 70)
    for cell, info in report["champions"].items():
        base = info.get("composite_baseline")
        print(f"{cell:8s} verdict={info['verdict']:18s} "
              f"train_fmw={info['train_f_micro_w']:.4f} "
              f"cell_valid={info['cell_valid_f_micro_w']} "
              f"pooled_valid={info['pooled_valid_f_micro_w']} "
              f"baseline={base}")
    print(f"runtime {report['runtime_s']}s  report -> {args.out_dir}/hpo_report.json")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

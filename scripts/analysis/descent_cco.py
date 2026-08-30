"""Generate CCO annotations by descending the ontology, against two baselines.

Run:  poetry run python scripts/analysis/descent_cco.py

The perimeter is the curated channel only. The IEA channel is excluded because
the 227->230 transition removes 1,152,561 IEA pairs against roughly 25,000
curated ones, so a model trained on both would be modelling an electronic
pipeline's maintenance schedule. Isolated, the curated CCO channel grows
monotonically across all four releases.

Temporal split, never random: train on 220->226, fit the threshold on
226->227, and test on 227->230. Each is a later window than the one before.
"""

from __future__ import annotations

import csv
import os
import random
import sys
import time
from collections import defaultdict

import numpy as np
import torch
from sqlalchemy import create_engine, text

from protea.core.ontology.dag import Dag
from protea.core.ontology.descent import DescentConfig, DescentModel, frontier, training_boundary
from protea.core.ontology.descent_walk import _extra, fit_descent, walk
from protea.core.ontology.order_encoder import OrderEncoder, TrainConfig
from protea.core.ontology.training import fit

SNAP = "36038118-37ba-4858-8677-f5b5d730bf56"
SETS = {220: "cbb35a32-44e4-4e39-b524-05b4b7433727",
        226: "86e5de3e-d36b-4800-b8ce-3cbace2d6dd8",
        227: "ec9f5c2c-cc1c-4e22-8cda-d1fe53ca86b3",
        230: "9a14f9cc-a18f-47ed-8511-b0cd2ff29953"}
MODEL = "facebook/esm2_t33_650M_UR50D"
IC_PATH = "/tmp/ic_t0.csv"
N_SAMPLE = 60000


def load_dag(con) -> Dag:
    return Dag.from_pairs([(p, c) for p, c in con.execute(text(
        "SELECT pt.go_id, ch.go_id FROM go_term_relationship r "
        "JOIN go_term ch ON ch.id = r.child_go_term_id "
        "JOIN go_term pt ON pt.id = r.parent_go_term_id "
        "WHERE r.ontology_snapshot_id = :s AND r.relation_type IN ('is_a','part_of') "
        "AND ch.aspect = 'C'"), {"s": SNAP})])


def load_state(con, dag: Dag, version: int) -> dict[str, set[str]]:
    """A protein's curated CCO terms, ancestor-closed, which is what a walk holds."""
    direct: dict[str, set[str]] = defaultdict(set)
    for p, g in con.execute(text(
            "SELECT a.protein_accession, t.go_id FROM protein_go_annotation a "
            "JOIN go_term t ON t.id = a.go_term_id WHERE a.annotation_set_id = :s "
            "AND a.qualifier NOT ILIKE '%NOT%' AND a.evidence_code <> 'IEA' "
            "AND t.aspect = 'C'"), {"s": SETS[version]}):
        if g in dag.index:
            direct[p].add(g)
    return {p: gs | {a for g in gs for a in dag.ancestors(g)} for p, gs in direct.items()}


def load_embeddings(con, accs: list[str]) -> dict[str, np.ndarray]:
    out: dict[str, np.ndarray] = {}
    for acc, emb in con.execute(text(
            "SELECT p.accession, se.embedding FROM sequence_embedding se "
            "JOIN embedding_config ec ON ec.id = se.embedding_config_id "
            "JOIN protein p ON p.sequence_id = se.sequence_id "
            "WHERE ec.model_name = :m AND p.accession = ANY(:a)"),
            {"m": MODEL, "a": accs}):
        v = (np.fromstring(emb.strip("[]"), sep=",", dtype=np.float32)
             if isinstance(emb, str) else np.asarray(emb, dtype=np.float32))
        if v.size:
            out[acc] = v / (np.linalg.norm(v) + 1e-9)
    return out


def examples(dag: Dag, old: dict, new: dict, emb: dict, prots: list[str]):
    """(protein, term, label) for the only decisions a walk ever makes."""
    rows = []
    for p in prots:
        if p not in emb or p not in old or p not in new:
            continue
        s, t = old[p], new[p]
        add = t - s
        if not add:
            continue
        _, neg = training_boundary(dag, t)
        rows.extend((p, g, 1) for g in add)
        rows.extend((p, g, 0) for g in neg)
    return rows


def micro_f_ia(pred: dict[str, set[str]], true: dict[str, set[str]],
               ia: dict[str, float]) -> tuple[float, float, float]:
    """IA-weighted pooled micro F, which is the statistic the surfaces read.

    Pooled rather than per-protein averaged, and weighted by information
    accretion, because predicting `cellular_anatomical_entity` correctly is not
    the same achievement as predicting a specific complex.
    """
    tp = fp = fn = 0.0
    for p, t in true.items():
        g = pred.get(p, set())
        tp += sum(ia.get(x, 0.0) for x in g & t)
        fp += sum(ia.get(x, 0.0) for x in g - t)
        fn += sum(ia.get(x, 0.0) for x in t - g)
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    return (2 * prec * rec / (prec + rec) if prec + rec else 0.0), prec, rec


def frequency_walk(dag: Dag, seeds: set[str], freq: dict[str, float], keep: int) -> set[str]:
    """THE BASELINE THAT MAKES THE NUMBER READABLE.

    The same descent, deciding by how often a term is used rather than by
    anything about this protein. It was measured reaching median rank 106 of
    40,214 on a held-out term, so it is not a strawman, and any claim the model
    makes is a claim over this.
    """
    out: set[str] = set()
    accepted = set(seeds)
    for _ in range(12):
        cand = sorted(frontier(dag, accepted), key=lambda t: -freq.get(t, 0.0))[:keep]
        if not cand:
            break
        out.update(cand)
        accepted.update(cand)
    return out


def main() -> int:
    url = os.environ.get("PROTEA_DB_URL", "")
    if not url:
        print("PROTEA_DB_URL no esta definido", file=sys.stderr)
        return 2
    t0 = time.time()
    rng = random.Random(0)
    eng = create_engine(url, future=True)
    with eng.connect() as con:
        dag = load_dag(con)
        st = {v: load_state(con, dag, v) for v in SETS}
        print(f"  CCO: {len(dag.terms):,} terminos, raiz {dag.roots()}")
        pool = sorted(set(st[220]) & set(st[226]) & set(st[227]) & set(st[230]))
        sample = rng.sample(pool, min(N_SAMPLE, len(pool)))
        emb = load_embeddings(con, sample)
    print(f"  proteinas con historial en las cuatro releases: {len(pool):,}, "
          f"muestreadas {len(sample):,}, con embedding {len(emb):,}  ({time.time()-t0:.0f}s)")

    ic = {g: float(v) for g, v in csv.reader(open(IC_PATH))}
    ia = {t: ic.get(t, max(ic.values())) for t in dag.terms}
    freq: dict[str, float] = defaultdict(float)
    for gs in st[220].values():
        for g in gs:
            freq[g] += 1.0

    ocfg = TrainConfig(dim=64, epochs=25, batch=8192, lr=0.05, negatives=4)
    closure = set(dag.closure())
    onto = fit(OrderEncoder(dag, ocfg), sorted(closure), closure, ocfg)
    # The term's own statistics belong IN its representation. The first run
    # asked the model to rediscover the frequency prior from the order
    # embedding alone, and lost to that prior 0.1061 to 0.1569. A model made to
    # reinvent its own baseline before beating it starts the race behind.
    stat = torch.tensor(
        [[np.log1p(freq.get(t, 0.0)) / 12.0, ia[t] / max(ia.values()),
          len(dag.parents_of(t)) / 4.0, len(dag.children_of(t)) / 20.0]
         for t in dag.terms], dtype=torch.float32)
    V = torch.cat([torch.abs(onto.emb.weight).detach(), stat], dim=1)
    print(f"  ontologia CCO congelada en {V.shape[1]}d "
          f"(orden {V.shape[1]-4} + 4 estadisticos)  ({time.time()-t0:.0f}s)")

    accs = [a for a in sample if a in emb]
    tr, va = accs[: int(0.8 * len(accs))], accs[int(0.8 * len(accs)) :]
    train_rows = examples(dag, st[220], st[226], emb, tr)
    print(f"  ejemplos de entrenamiento (220->226): {len(train_rows):,}  "
          f"positivos {sum(r[2] for r in train_rows):,}")

    dim = len(next(iter(emb.values())))
    cfg = DescentConfig(context_dim=dim, term_dim=V.shape[1], epochs=15, batch=4096)
    model = DescentModel(dag, V, cfg)

    def batches():
        rows = list(train_rows)
        rng.shuffle(rows)
        for s in range(0, len(rows), cfg.batch):
            blk = rows[s : s + cfg.batch]
            ctx = torch.tensor(np.stack([emb[p] for p, _, _ in blk]))
            ids = torch.tensor([dag.index[g] for _, g, _ in blk], dtype=torch.long)
            ex = _extra(dag, [g for _, g, _ in blk], 1, set())
            y = torch.tensor([float(v) for _, _, v in blk])
            yield ctx, ids, ex, y

    fit_descent(model, batches, cfg, log=print)

    # The threshold is fitted on the MIDDLE window, never on the test one.
    print(f"\n  ajustando tau en 226->227 ({len(va):,} proteinas)")
    true_va = {p: st[227][p] - st[226][p] for p in va if p in st[227] and p in st[226]}
    true_va = {p: t for p, t in true_va.items() if t}
    best = (0.0, 0.5)
    for tau in (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9):
        pred = {p: set(walk(model, torch.tensor(emb[p]), tau=tau, seeds=st[226][p]))
                for p in true_va}
        f, _, _ = micro_f_ia(pred, true_va, ia)
        if f > best[0]:
            best = (f, tau)
    print(f"    mejor tau {best[1]}  (f_micro_w de validacion {best[0]:.4f})")

    print(f"\n  --- PRUEBA 227->230, ventana nunca vista ({time.time()-t0:.0f}s) ---")
    true_te = {p: st[230][p] - st[227][p] for p in va if p in st[230] and p in st[227]}
    true_te = {p: t for p, t in true_te.items() if t}
    pred = {p: set(walk(model, torch.tensor(emb[p]), tau=best[1], seeds=st[227][p]))
            for p in true_te}
    # The baseline's knob is fitted on the SAME validation window as tau. The
    # first run swept it on the test set and took the best, which handed the
    # baseline an advantage the model did not get, and the model still lost.
    bk = max((1, 2, 3), key=lambda k: micro_f_ia(
        {p: frequency_walk(dag, st[226][p], freq, k) for p in true_va}, true_va, ia)[0])
    print(f"  mejor top-k del prior, ajustado en 226->227: {bk}")
    rows = [("descenso generativo", pred)]
    for keep in sorted({bk, 1, 2, 3}):
        tag = f"prior de frecuencia (top {keep})" + ("  <- ajustado" if keep == bk else "")
        rows.append((tag, {p: frequency_walk(dag, st[227][p], freq, keep) for p in true_te}))
    rows.append(("no anadir nada", {p: set() for p in true_te}))
    n_add = sum(len(t) for t in true_te.values())
    print(f"  proteinas evaluadas {len(true_te):,}, adiciones verdaderas {n_add:,}")
    if len(true_te) < 500:
        print("  AVISO: menos de 500 proteinas de prueba. Un intervalo de confianza\n"
              "  se comeria cualquier diferencia; esto no decide nada.\n")
    else:
        print()
    for tag, pr in rows:
        f, prec, rec = micro_f_ia(pr, true_te, ia)
        n = sum(len(x) for x in pr.values())
        print(f"  {tag:28s} f_micro_w {f:.4f}  P {prec:.4f}  R {rec:.4f}  emitidos {n:,}")
    print(f"\n  total {time.time()-t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)
    raise SystemExit(main())

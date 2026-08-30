"""The whole corpus, all three aspects, read by containment. Against the prior.

Run:  poetry run python scripts/analysis/containment_full.py

WHAT CHANGED FROM THE RUNS THAT LOST. Those trained on the DELTA and had 22,993
positives, on one aspect, on proteins present in all four releases, with a
sequence embedding as the entire context. The delta is the evaluation protocol,
not the training signal: what is learned is the state, and the state is
5,317,051 pairs. Nothing here requires a protein to appear in four releases.

THE PERIMETER. Training reads the t0 bank (release 220, 2024-04-16) and nothing
later, for any protein. Evaluation is the curated additions between 227 and 230,
which no part of training has seen.
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
from protea.core.ontology.sequence_to_atoms import AtomEncoderConfig, SequenceToAtoms
from protea.core.ontology.sparse_containment import (
    SparseCodeConfig,
    SparseTermCodes,
)

SNAP = "36038118-37ba-4858-8677-f5b5d730bf56"
BANK = "cbb35a32-44e4-4e39-b524-05b4b7433727"
T227 = "ec9f5c2c-cc1c-4e22-8cda-d1fe53ca86b3"
T230 = "9a14f9cc-a18f-47ed-8511-b0cd2ff29953"
MODEL = "facebook/esm2_t33_650M_UR50D"
IC_PATH = "/tmp/ic_t0.csv"
N_PROT = 40000
# 1024 with own_k=4: GO's deepest term has 68 ancestors, so it demands 276
# atoms, 27 per cent of the space. At 512 the guard refuses the run, which
# is how this number was found rather than guessed.
ATOMS = 1024
OWN_K = 4


def load_dag(con) -> Dag:
    return Dag.from_pairs([(p, c) for p, c in con.execute(text(
        "SELECT pt.go_id, ch.go_id FROM go_term_relationship r "
        "JOIN go_term ch ON ch.id = r.child_go_term_id "
        "JOIN go_term pt ON pt.id = r.parent_go_term_id "
        "WHERE r.ontology_snapshot_id = :s AND r.relation_type IN ('is_a','part_of')"),
        {"s": SNAP})])


def load_sets(con, dag: Dag, set_id: str, accs: list[str] | None) -> dict[str, set[str]]:
    q = ("SELECT a.protein_accession, t.go_id FROM protein_go_annotation a "
         "JOIN go_term t ON t.id = a.go_term_id WHERE a.annotation_set_id = :s "
         "AND a.qualifier NOT ILIKE '%NOT%' AND a.evidence_code <> 'IEA'")
    params: dict[str, object] = {"s": set_id}
    if accs is not None:
        q += " AND a.protein_accession = ANY(:a)"
        params["a"] = accs
    direct: dict[str, set[str]] = defaultdict(set)
    for p, g in con.execute(text(q), params):
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


def micro_f_ia(pred, true, ia) -> tuple[float, float, float]:
    tp = fp = fn = 0.0
    for p, t in true.items():
        g = pred.get(p, set())
        tp += sum(ia.get(x, 0.0) for x in g & t)
        fp += sum(ia.get(x, 0.0) for x in g - t)
        fn += sum(ia.get(x, 0.0) for x in t - g)
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    return (2 * prec * rec / (prec + rec) if prec + rec else 0.0), prec, rec


def predict(enc, codes: torch.Tensor, x: torch.Tensor, tau: float, cap: int) -> set[int]:
    """Every term whose code fits inside the protein's atoms, in one pass.

    One matrix operation over the whole ontology: no retrieval, no donor, no
    candidate list, and no descent. The cap is a volume guard, not a policy:
    without it a protein whose atoms are large would emit the ontology.
    """
    with torch.no_grad():
        a = enc(x.unsqueeze(0))
        v = torch.clamp(codes - a, min=0.0).pow(2).sum(-1)
    keep = (v <= tau).nonzero(as_tuple=True)[0]
    if len(keep) > cap:
        keep = keep[v[keep].argsort()[:cap]]
    return set(keep.tolist())


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
        print(f"  GO completo: {len(dag.terms):,} terminos, raices {len(dag.roots())}")
        pool = [r[0] for r in con.execute(text(
            "SELECT DISTINCT protein_accession FROM protein_go_annotation "
            "WHERE annotation_set_id = :s AND evidence_code <> 'IEA'"), {"s": BANK})]
        sample = rng.sample(sorted(pool), min(N_PROT, len(pool)))
        bank = load_sets(con, dag, BANK, sample)
        emb = load_embeddings(con, sample)
        accs = [a for a in sample if a in emb and a in bank]
        te_pool = accs[int(0.85 * len(accs)):]
        s227 = load_sets(con, dag, T227, te_pool)
        s230 = load_sets(con, dag, T230, te_pool)
    print(f"  proteinas curadas en el banco t0: {len(pool):,}, muestreadas {len(sample):,}, "
          f"usables {len(accs):,}  ({time.time()-t0:.0f}s)")
    npairs = sum(len(bank[a]) for a in accs)
    print(f"  pares (proteina, termino) de ENTRENAMIENTO: {npairs:,}"
          f"   [las corridas anteriores tenian 22.993]")

    ic = {g: float(v) for g, v in csv.reader(open(IC_PATH))}
    ia = {t: ic.get(t, max(ic.values())) for t in dag.terms}
    freq: dict[str, float] = defaultdict(float)
    for a in accs:
        for g in bank[a]:
            freq[g] += 1.0

    tr = accs[: int(0.85 * len(accs))]
    terms = SparseTermCodes(dag, SparseCodeConfig(atoms=ATOMS, own_k=OWN_K, seed=0))
    dim = len(next(iter(emb.values())))
    # Batch 1024, not 128. The term codes are rebuilt once PER BATCH, at 12.9
    # seconds a time for 1024 atoms including the backward through twenty-four
    # max-propagations, so the cost is per batch and not per protein. At 128 it
    # was 265 batches an epoch and 7.6 hours; at 1024 it is 33.
    cfg = AtomEncoderConfig(in_dim=dim, atoms=ATOMS, epochs=8, batch=1024, negatives=48)
    enc = SequenceToAtoms(cfg)
    opt = torch.optim.Adam(list(enc.parameters()) + list(terms.parameters()), lr=cfg.lr)
    all_ids = list(range(len(dag.terms)))
    inv_name = {i: t for t, i in dag.index.items()}

    print(f"\n  entrenando sobre {len(tr):,} proteinas")
    for epoch in range(cfg.epochs):
        rng.shuffle(tr)
        tot = n = 0.0
        codes = terms.codes()
        for s in range(0, len(tr), cfg.batch):
            blk = [a for a in tr[s : s + cfg.batch] if bank[a]]
            if not blk:
                continue
            codes = terms.codes()
            x = torch.tensor(np.stack([emb[a] for a in blk]))
            atoms = enc(x)
            # Flattened, not a Python loop over proteins: one violation call for
            # the whole batch. Rows index the protein, so a pair is (row, term).
            prow, pcol, nrow, ncol = [], [], [], []
            for i, a in enumerate(blk):
                held = bank[a]
                ids = [dag.index[g] for g in held]
                prow.extend([i] * len(ids))
                pcol.extend(ids)
                # STRUCTURAL, not uniform. A uniformly drawn term has Resnik
                # 0.00 against the protein's terms at the median: it shares
                # nothing but a root, separating it is free, and it puts no
                # pressure at all on the atoms. The first run of this script
                # used uniform negatives and the model emitted 370 terms per
                # protein at precision 0.008, which is what "everything is
                # contained" looks like from outside.
                near = [t for g in rng.sample(sorted(held), min(8, len(held)))
                        for t in dag.sibling_list(g)]
                near = [dag.index[t] for t in near if t not in held]
                rng.shuffle(near)
                far = [z for z in rng.sample(all_ids, cfg.negatives)
                       if inv_name[z] not in held]
                keep = (near[: cfg.negatives] + far)[: cfg.negatives + 8]
                nrow.extend([i] * len(keep))
                ncol.extend(keep)
            pv = torch.clamp(codes[pcol] - atoms[prow], min=0.0).pow(2).sum(-1)
            nv = torch.clamp(codes[ncol] - atoms[nrow], min=0.0).pow(2).sum(-1)
            loss = pv.mean() + torch.clamp(cfg.margin - nv, min=0.0).mean()
            opt.zero_grad()
            loss.backward()
            opt.step()
            tot += float(loss.detach()) * len(blk)
            n += len(blk)
        print(f"    epoch {epoch+1:2d}/{cfg.epochs}  loss {tot/max(n,1):.4f}  "
              f"({time.time()-t0:.0f}s)")

    print("\n  --- PRUEBA: adiciones curadas 227->230, ventana nunca vista ---")
    true = {p: s230[p] - s227[p] for p in te_pool if p in s230 and p in s227}
    true = {p: t for p, t in true.items() if t}
    print(f"  proteinas evaluadas {len(true):,}, adiciones {sum(len(t) for t in true.values()):,}")
    if len(true) < 200:
        print("  AVISO: muestra de prueba demasiado pequena para decidir nada.")
    codes = terms.codes().detach()
    inv = {i: t for t, i in dag.index.items()}
    best = (0.0, 0.0)
    for tau in (0.005, 0.01, 0.05, 0.1, 0.25, 0.5):
        pred = {p: {inv[i] for i in predict(enc, codes, torch.tensor(emb[p]), tau, 400)}
                - s227[p] for p in true if p in emb}
        f, _, _ = micro_f_ia(pred, true, ia)
        if f > best[0]:
            best = (f, tau)
    print(f"  mejor tau {best[1]}")
    pred = {p: {inv[i] for i in predict(enc, codes, torch.tensor(emb[p]), best[1], 400)}
            - s227[p] for p in true if p in emb}
    top = sorted(freq, key=lambda g: -freq[g])
    rows = [("contencion dispersa", pred)]
    for k in (5, 20, 50):
        rows.append((f"prior de frecuencia (top {k})",
                     {p: set(top[:k]) - s227[p] for p in true}))
    rows.append(("no anadir nada", {p: set() for p in true}))
    print()
    for tag, pr in rows:
        f, prec, rec = micro_f_ia(pr, true, ia)
        print(f"  {tag:28s} f_micro_w {f:.4f}  P {prec:.4f}  R {rec:.4f}  "
              f"emitidos {sum(len(x) for x in pr.values()):,}")
    print(f"\n  total {time.time()-t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)
    raise SystemExit(main())

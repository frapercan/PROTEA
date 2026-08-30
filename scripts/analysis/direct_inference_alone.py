"""Part two: predict GO terms from a sequence, with no retrieval and no donor.

Run:  poetry run python scripts/analysis/direct_inference_alone.py

The perimeter is what was known at prediction time: the t0 bank, the ontology,
and the sequence embeddings. No annotation dated after 2024-04-10 is read, for
any protein, because a homologue acquiring a term inside the window is exactly
the signal the evaluation measures.

Judged against the frequency prior, which is the baseline that makes the number
readable and which reached median rank 106 of 40,214 in the join probe.
"""

from __future__ import annotations

import os
import random
import sys
import time

import numpy as np
import torch
from sqlalchemy import create_engine, text

from protea.core.ontology.dag import Dag
from protea.core.ontology.order_encoder import OrderEncoder, TrainConfig
from protea.core.ontology.projector_training import (
    StructuralNegatives,
    _pad,
    fit_projector,
)
from protea.core.ontology.protein_projector import ProjectorConfig, ProteinProjector
from protea.core.ontology.training import fit
from scripts.analysis.ontology_encoder_alone import load_go

BANK = "cbb35a32-44e4-4e39-b524-05b4b7433727"
ARM = "9995651a-d748-444e-b142-83218fc5dea5"
MODEL = "facebook/esm2_t33_650M_UR50D"
N_TRAIN, N_TEST, POS, NEG = 24000, 1500, 24, 48


def _bank_terms(con, dag: Dag, accs: list[str]) -> dict[str, set[str]]:
    """The protein's t0 terms, propagated up, which is what it is known to be."""
    rows = con.execute(
        text(
            "SELECT a.protein_accession, t.go_id FROM protein_go_annotation a "
            "JOIN go_term t ON t.id = a.go_term_id "
            "WHERE a.annotation_set_id = :b AND a.qualifier NOT ILIKE '%NOT%' "
            "AND a.protein_accession = ANY(:a)"
        ),
        {"b": BANK, "a": accs},
    ).all()
    out: dict[str, set[str]] = {}
    for p, g in rows:
        if g in dag.index:
            out.setdefault(p, set()).add(g)
    for p, gs in out.items():
        up = set(gs)
        for g in gs:
            up |= dag.ancestors(g)
        out[p] = {g for g in up if g in dag.index}
    return out


def _denials(con, dag: Dag, accs: list[str]) -> dict[str, set[str]]:
    rows = con.execute(
        text(
            "SELECT a.protein_accession, t.go_id FROM protein_go_annotation a "
            "JOIN go_term t ON t.id = a.go_term_id "
            "WHERE a.annotation_set_id = :b AND a.qualifier ILIKE '%NOT%' "
            "AND a.protein_accession = ANY(:a)"
        ),
        {"b": BANK, "a": accs},
    ).all()
    out: dict[str, set[str]] = {}
    for p, g in rows:
        if g in dag.index:
            out.setdefault(p, set()).update({g} | dag.descendants(g))
    return out


def _embeddings(con, accs: list[str]) -> dict[str, np.ndarray]:
    rows = con.execute(
        text(
            "SELECT p.accession, se.embedding FROM sequence_embedding se "
            "JOIN embedding_config ec ON ec.id = se.embedding_config_id "
            "JOIN protein p ON p.sequence_id = se.sequence_id "
            "WHERE ec.model_name = :m AND p.accession = ANY(:a)"
        ),
        {"m": MODEL, "a": accs},
    ).all()
    out: dict[str, np.ndarray] = {}
    for acc, emb in rows:
        # pgvector comes back as its text form over this driver.
        v = (
            np.fromstring(emb.strip("[]"), sep=",", dtype=np.float32)
            if isinstance(emb, str)
            else np.asarray(emb, dtype=np.float32)
        )
        if v.size:
            out[acc] = v / (np.linalg.norm(v) + 1e-9)
    return out


def main() -> int:
    url = os.environ.get("PROTEA_DB_URL", "")
    if not url:
        print("PROTEA_DB_URL no esta definido", file=sys.stderr)
        return 2
    t0 = time.time()
    rng = random.Random(0)

    dag = load_go(url)
    closure = set(dag.closure())
    ocfg = TrainConfig(dim=64, epochs=12, batch=8192)
    onto = fit(OrderEncoder(dag, ocfg), sorted(closure), closure, ocfg)
    V = torch.abs(onto.emb.weight).detach()
    print(f"  ontologia congelada: {len(dag.terms):,} terminos en {V.shape[1]}d "
          f"({time.time() - t0:.0f}s)")

    eng = create_engine(url, future=True)
    with eng.connect() as con:
        query = [r[0] for r in con.execute(
            text("SELECT DISTINCT protein_accession FROM go_prediction WHERE prediction_set_id=:p"),
            {"p": ARM})]
        pool = [r[0] for r in con.execute(
            text("SELECT DISTINCT protein_accession FROM protein_go_annotation WHERE annotation_set_id=:b"),
            {"b": BANK})]
        train_accs = rng.sample(sorted(set(pool) - set(query)), N_TRAIN + N_TEST)
        terms = _bank_terms(con, dag, train_accs)
        deny = _denials(con, dag, train_accs)
        emb = _embeddings(con, train_accs)

    usable = [a for a in train_accs if a in emb and len(terms.get(a, ())) >= 4]
    rng.shuffle(usable)
    tr, te = usable[:-N_TEST], usable[-N_TEST:]
    dim = len(emb[usable[0]])
    print(f"  proteinas usables: {len(usable):,}  (entrenamiento {len(tr):,}, prueba {len(te):,})")
    print(f"  embedding {MODEL.split('/')[-1]}: {dim}d\n")

    sampler = StructuralNegatives(dag, deny, seed=0)
    cfg = ProjectorConfig(in_dim=dim, out_dim=V.shape[1], epochs=12, batch=256)

    def batches():
        order = list(tr)
        rng.shuffle(order)
        for s in range(0, len(order), cfg.batch):
            blk = order[s : s + cfg.batch]
            x = torch.tensor(np.stack([emb[a] for a in blk]))
            pos = _pad([[dag.index[t] for t in rng.sample(sorted(terms[a]),
                        min(POS, len(terms[a])))] for a in blk], POS)
            neg = _pad([[dag.index[t] for t in sampler.for_protein(a, terms[a], NEG)]
                        for a in blk], NEG)
            yield x, pos, neg

    model = fit_projector(ProteinProjector(V, cfg), batches, cfg, log=print)

    # Held-out: rank one true term of each test protein against the whole
    # ontology, minus what the protein is already known to have.
    freq = np.zeros(len(dag.terms))
    for a in tr:
        for t in terms[a]:
            freq[dag.index[t]] += 1
    ranks, franks = [], []
    with torch.no_grad():
        for s in range(0, len(te), 256):
            blk = te[s : s + 256]
            scores = model.score_all(torch.tensor(np.stack([emb[a] for a in blk]))).numpy()
            for i, a in enumerate(blk):
                have = terms[a]
                held = rng.choice(sorted(have))
                mask = np.array([t not in have or t == held for t in dag.terms])
                j = dag.index[held]
                ranks.append(1 + int(((scores[i] > scores[i][j]) & mask).sum()))
                franks.append(1 + int(((freq > freq[j]) & mask).sum()))

    r, f = np.array(ranks, dtype=float), np.array(franks, dtype=float)
    print(f"\n  --- termino retenido, rango entre {len(dag.terms):,} "
          f"(azar p50 ~{len(dag.terms)//2:,}) ---")
    for tag, a in (("proyector de secuencia", r), ("prior de frecuencia", f)):
        print(f"  {tag:24s} p50 {np.median(a):7,.0f}  h@10 {(a <= 10).mean():6.1%}  "
              f"h@100 {(a <= 100).mean():6.1%}  h@1000 {(a <= 1000).mean():6.1%}")
    print(f"\n  n = {len(r):,}   total {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    # Line-buffered on purpose: these runs take tens of minutes and a
    # buffered stdout means no progress is readable until they end.
    sys.stdout.reconfigure(line_buffering=True)
    raise SystemExit(main())

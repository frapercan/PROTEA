"""Is the order space usable as a target for a protein at all.

The idea Part 2 rests on: a protein that has terms T should sit at the
coordinate-wise MAX of their order vectors, because that is the smallest point
the whole set subsumes. If that holds, scoring any term for any protein is one
penalty and needs no per-term parameter, which is what makes it one-shot.

If it does not hold, the rest is not worth writing. So: build the join from a
protein's KNOWN terms, then ask whether a HELD-OUT true term of the same
protein scores better than a random term. Nothing is learned here; this only
asks whether the geometry the ontology encoder produced can carry a protein.
"""
import os
import random

import numpy as np
import torch
from sqlalchemy import create_engine, text

from protea.core.ontology.order_encoder import TrainConfig
from protea.core.ontology.training import fit
from scripts.analysis.ontology_encoder_alone import load_go

url = os.environ["PROTEA_DB_URL"]
dag = load_go(url)
closure = set(dag.closure())
print(f"  ontologia {len(dag.terms):,} terminos, cierre {len(closure):,}")
cfg = TrainConfig(dim=64, epochs=12, batch=8192, lr=0.05, negatives=4)
model = fit(dag, sorted(closure), closure, cfg)
V = torch.abs(model.emb.weight).detach().numpy()
idx = model.dag.index
print("  encoder entrenado\n")

# The bank's annotations for query proteins, propagated up, as the term set.
eng = create_engine(url, future=True)
with eng.connect() as con:
    rows = con.execute(text("""
        select a.protein_accession, t.go_id from protein_go_annotation a
        join go_term t on t.id=a.go_term_id
        where a.annotation_set_id='cbb35a32-44e4-4e39-b524-05b4b7433727'
          and a.qualifier not ilike '%NOT%'
          and a.protein_accession in (
            select distinct protein_accession from go_prediction
            where prediction_set_id='9995651a-d748-444e-b142-83218fc5dea5' limit 3000)
    """)).all()
by_p = {}
for p, g in rows:
    if g in idx:
        by_p.setdefault(p, set()).add(g)
# propagate up: the protein has every ancestor of every term it has
for p, gs in by_p.items():
    up = set(gs)
    for g in gs:
        up |= dag.ancestors(g)
    by_p[p] = {g for g in up if g in idx}
by_p = {p: g for p, g in by_p.items() if len(g) >= 8}
print(f"  proteinas con >=8 terminos: {len(by_p):,}")

# How often each term is used, as the baseline any placement has to beat.
freq = {}
for gs in by_p.values():
    for g in gs:
        freq[g] = freq.get(g, 0) + 1
rng = random.Random(0)
all_terms = list(dag.terms)
FV = np.array([freq.get(t, 0) for t in all_terms], dtype=float)
held_pen, rand_pen, ranks, franks = [], [], [], []
for gs in list(by_p.values())[:800]:
    terms = sorted(gs)
    held = rng.choice(terms)
    known = [t for t in terms if t != held and held not in dag.ancestors(t)]
    if len(known) < 4:
        continue
    join = V[[idx[t] for t in known]].max(axis=0)          # the protein's point
    P = np.square(np.clip(V - join, 0, None)).sum(axis=1)
    held_pen.append(float(P[idx[held]]))
    # Against the WHOLE ontology, minus what the protein is already known to
    # have. 500 random candidates would have made this look several hundred
    # times better than it is.
    mask = np.array([t not in gs or t == held for t in all_terms])
    hp = P[idx[held]]
    ranks.append(1 + int(((P < hp) & mask).sum()))
    rand_pen.extend(rng.sample(list(P[mask]), 5))
    fh = FV[idx[held]]
    franks.append(1 + int(((FV > fh) & mask).sum()))

h, r, k = np.array(held_pen), np.array(rand_pen), np.array(ranks, dtype=float)
fk = np.array(franks, dtype=float)
print(f"\n  termino verdadero retenido : penalizacion p50 {np.median(h):.4f}")
print(f"  termino al azar            : penalizacion p50 {np.median(r):.4f}")
print(f"  separacion                 : {np.median(r)/max(np.median(h),1e-9):.1f}x")
print(f"\n  rango del retenido entre los {len(all_terms):,} terminos (azar: p50 ~{len(all_terms)//2:,})")
print(f"    join de la ontologia : p50 {np.median(k):6,.0f}   h@10 {(k<=10).mean():6.1%}   h@100 {(k<=100).mean():6.1%}")
print(f"    frecuencia del termino: p50 {np.median(fk):6,.0f}   h@10 {(fk<=10).mean():6.1%}   h@100 {(fk<=100).mean():6.1%}")
print(f"\n  n = {len(k):,} proteinas")

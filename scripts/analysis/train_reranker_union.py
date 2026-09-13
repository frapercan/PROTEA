"""Two rerankers over the union of four complementary arms, cross-fitted.

WITH and WITHOUT n_arms, which is the count of arms proposing a candidate. On
this table that single column separates 0.14 per cent positives from 2.96 per
cent, a factor of 21, so it is the strongest feature available and also the one
that costs the most to serve: computing it requires running all four
retrievals.

A CAVEAT ON WHAT "WITHOUT" MEANS. Dropping n_arms does NOT produce a model
servable from one arm. Every other column here is still a cross-arm aggregate:
k_min is the best rank across four searches, identity_nw_max the best alignment
of four. A genuinely single-arm model needs single-arm features and is a third
experiment, not this one. Reported so the second model is not read as something
it is not.

Cross-fitted by PROTEIN over five folds. A protein never appears in both the
training and the scoring side, which is the precondition that decides whether
any of this generalises.

The baseline it must beat is the ordering the system already serves: candidates
sorted by distance. A reranker that does not beat that is not worth serving.
"""

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import average_precision_score, roc_auc_score

CSV = "/home/bioxaxi2/Thesis-laptop/campana/reranker_train.csv"
DROP = {"protein_accession", "go_id", "aspect", "label", "fold"}

df = pd.read_csv(CSV)
print(f"  filas {len(df):,}  positivos {int(df.label.sum()):,}  ({df.label.mean()*100:.2f}%)")

feat_all = [c for c in df.columns if c not in DROP]
feat_no = [c for c in feat_all if c != "n_arms"]
print(f"  features con n_arms: {len(feat_all)}   sin: {len(feat_no)}")

PARAMS = dict(
    objective="binary", learning_rate=0.05, num_leaves=63,
    min_data_in_leaf=200, feature_fraction=0.8, bagging_fraction=0.8,
    bagging_freq=1, verbose=-1, num_threads=6,
)

def cross_fit(feats: list[str], tag: str) -> np.ndarray:
    """Out-of-fold scores. Each protein is scored by a model that never saw it."""
    out = np.zeros(len(df))
    for k in sorted(df.fold.unique()):
        tr, te = df.fold != k, df.fold == k
        booster = lgb.train(
            PARAMS,
            lgb.Dataset(df.loc[tr, feats], label=df.loc[tr, "label"]),
            num_boost_round=300,
        )
        out[te.to_numpy()] = booster.predict(df.loc[te, feats])
        print(f"    {tag} fold {k}: entrenado sobre {int(tr.sum()):,}, puntuado {int(te.sum()):,}")
    return out

def recall_at_k(score: np.ndarray, k: int) -> float:
    """Fraction of all true pairs kept when each protein keeps its top k.

    This is the number that decides whether a reranker earns its place: the
    system already discards deep candidates, so what matters is how much truth
    survives a cut, not how well the whole list is ordered.
    """
    t = df.assign(s=score)
    top = t.sort_values("s", ascending=False).groupby("protein_accession").head(k)
    return top.label.sum() / df.label.sum()

results = {}
for tag, feats in (("CON n_arms", feat_all), ("SIN n_arms", feat_no)):
    print(f"\n  == {tag} ==")
    s = cross_fit(feats, tag)
    results[tag] = s
    print(f"    AUC {roc_auc_score(df.label, s):.4f}   AP {average_precision_score(df.label, s):.4f}")

# The baseline the system already serves: nearest first.
base = -df.dist_min.to_numpy()
print(f"\n  == linea base (orden por distancia) ==")
print(f"    AUC {roc_auc_score(df.label, base):.4f}   AP {average_precision_score(df.label, base):.4f}")

print(f"\n  recall al recortar a los mejores k candidatos por proteina")
print(f"  {'k':>4}  {'distancia':>10}  {'SIN n_arms':>11}  {'CON n_arms':>11}")
for k in (1, 3, 5, 10, 30):
    r = [recall_at_k(base, k), recall_at_k(results["SIN n_arms"], k), recall_at_k(results["CON n_arms"], k)]
    print(f"  {k:>4}  {r[0]*100:9.1f}%  {r[1]*100:10.1f}%  {r[2]*100:10.1f}%")

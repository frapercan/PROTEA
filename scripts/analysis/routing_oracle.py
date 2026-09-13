"""The routing ceiling over all twelve arms, and how fast it saturates.

Computed on the per-protein f_w, which is the quantity behind cafaeval's
fmax_w, the statistic CAFA scores. PROTEA's own reading surfaces report
f_micro_w instead, built from the pooled confusion matrix, and the two answer
different questions: on one arm pair the same contrast is +0.0708 in fmax_w and
+0.0255 in f_micro_w.

The oracle over two arms bounds a pairwise choice. The oracle over twelve
bounds the whole Routing node: it is the most any per-protein model selection,
combination or channelling policy could ever be worth on this corpus.

It is a CEILING with a perfect chooser, not a forecast. What makes it useful is
the saturation curve beside it: if most of the gain arrives at three arms,
routing means serving three models and is buildable; if it needs ten, it is a
bound and not a plan.

The null for this table was measured at exactly 0.0000, from the same recipe run
twice, so none of what follows is retrieval noise.
"""

import io
import subprocess
import sys
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, "/home/bioxaxi2/Thesis-laptop/PROTEA")
from protea.api.routers._graph_panels import detectable_effect  # noqa: E402
from protea.core.operations._run_cafa_helpers import eval_artifact_key  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402
from protea.infrastructure.storage import get_artifact_store  # noqa: E402

STORE = get_artifact_store(load_settings(Path("/home/bioxaxi2/Thesis-laptop/PROTEA")))


def q(sql: str) -> str:
    return subprocess.run(
        ["psql", "-h", "localhost", "-U", "protea", "-d", "protea", "-tAc", sql],
        capture_output=True, text=True,
        env={"PGPASSWORD": "protea", "PATH": "/usr/bin:/bin"},
    ).stdout.strip()


def grid(result_id: str, setting: str) -> pd.DataFrame:
    key = eval_artifact_key(uuid.UUID(result_id), f"{setting}/per_protein.parquet")
    return pd.read_parquet(io.BytesIO(STORE.get(key)))


rows = q(
    "select er.id::text||' '||coalesce(ec.display_name, ec.model_name) "
    "from evaluation_result er "
    "join prediction_set ps on ps.id = er.prediction_set_id "
    "join embedding_config ec on ec.id = ps.embedding_config_id "
    "where er.max_sequence_rank = 30 "
    "  and ps.meta->>'code_revision' like 'bc7c423%' "
    "  and er.id in (select distinct on (prediction_set_id) id from evaluation_result "
    "                where max_sequence_rank = 30 order by prediction_set_id, created_at desc) "
    "order by ec.model_name"
).splitlines()
arms = [r.split(" ", 1) for r in rows if r.strip()]
print(f"  brazos evaluados: {len(arms)}")
if len(arms) < 3:
    raise SystemExit("  no hay suficientes brazos todavia")

for setting in ("NK", "LK", "PK"):
    for ns in ("biological_process", "molecular_function", "cellular_component"):
        series = {}
        for rid, name in arms:
            g = grid(rid, setting)
            series[name] = g[g.namespace == ns].groupby("protein_accession")["f_w"].max()
        frame = pd.DataFrame(series).dropna()
        if frame.empty:
            continue
        S = frame.to_numpy().T                      # arms x proteins
        means = S.mean(axis=1)
        best_i = int(means.argmax())
        best_single = float(means[best_i])
        full_oracle = float(S.max(axis=0).mean()) - best_single
        floor = detectable_effect(frame.shape[0]) or float("nan")

        # greedy forward selection: how fast does the ceiling arrive
        chosen = [best_i]
        curve = []
        running = S[best_i].copy()
        for _ in range(min(4, len(arms) - 1)):
            gains = [
                (float(np.maximum(running, S[j]).mean()) - best_single, j)
                for j in range(len(arms)) if j not in chosen
            ]
            g, j = max(gains)
            chosen.append(j)
            running = np.maximum(running, S[j])
            curve.append(g)
        pct = [f"{c / full_oracle * 100:3.0f}%" for c in curve] if full_oracle > 0 else []
        print(
            f"  {setting}.{ns[:3].upper()} n={frame.shape[0]:5d} "
            f"mejor={best_single:.4f} ({arms[best_i][1][:14]}) "
            f"oraculo12=+{full_oracle:.4f} ({full_oracle / floor:4.1f}x suelo) "
            f"| 2,3,4,5 brazos: {' '.join(pct)}"
        )

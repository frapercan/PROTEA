#!/usr/bin/env python
"""Materialise the floor census with its inputs, so it survives losing either half.

WHY THIS EXISTS. The census that fixes the graph's floors is a join of
``evaluation_result`` against ``job.payload``: the metric lives in the first and the
conditioning set that makes the metric attributable lives in the second. Deleting results
while keeping jobs orphans one half and destroys the other, and the census then has the
exact shape this project spends its time hunting, a number whose producer is gone.

Run it before any operation that removes either table. It is read-only.

A row is usable only when its job survives, because with no job there is no operation and
therefore not even the defaults are determined. ``max_terms``, ``max_distance`` and
``protein_subset`` appear only in the sweeps that varied them (36, 18 and 17 jobs), so
their absence means the default was taken and is recorded as such rather than as a gap.

The census is a join of evaluation_result against job.payload. The wipe removes the
first and keeps the second, so after it the numbers cannot be recomputed from either
half. This writes one row per (result, category, aspect) carrying the FULL conditioning
set alongside the metric, plus an explicit flag for the rows whose job is gone and whose
conditioning set is therefore unrecoverable for ever.
"""
import hashlib, json, os, re, sys
import pandas as pd
from sqlalchemy import create_engine, text

ROOT = "/home/bioxaxi2/Thesis-laptop/PROTEA"
url = re.search(r'^PROTEA_DB_URL=(.*)$', open(f"{ROOT}/.env").read(), re.M).group(1).strip()
os.environ["PGOPTIONS"] = "-c default_transaction_read_only=on -c statement_timeout=600s"
eng = create_engine(url, connect_args={"application_name": "protea-floor-census"})

SQL = text("""
SELECT
  er.id::text                       AS result_id,
  er.created_at,
  er.job_id::text                   AS job_id,
  (j.id IS NOT NULL)                AS job_recoverable,
  er.evaluation_set_id::text        AS evaluation_set_id,
  er.prediction_set_id::text        AS prediction_set_id,
  er.scoring_config_id::text        AS scoring_config_id,
  er.leakage_role, er.temporal_window,
  er.frame::text                    AS frame_column,
  er.arms_enabled::text             AS arms_enabled,
  -- the half the wipe would orphan: the conditioning set lives in the job payload
  j.payload->>'max_terms'                    AS max_terms,
  j.payload->>'max_distance'                 AS max_distance,
  j.payload->>'protein_subset_label'         AS protein_subset_label,
  jsonb_array_length(CASE WHEN jsonb_typeof(j.payload->'protein_subset')='array'
       THEN j.payload->'protein_subset' END)   AS protein_subset_n,
  md5((j.payload->'protein_subset')::text)     AS protein_subset_md5,
  COALESCE(j.payload->>'scoring_config_id', j.payload->>'scoring_config_ids') AS payload_scoring,
  j.operation                                AS producing_operation,
  j.payload->>'protein_fold'                 AS protein_fold,
  j.payload->>'information_accretion_set_id' AS ia_set_id,
  j.payload->>'prop'                         AS prop,
  j.payload->>'norm'                         AS norm,
  -- and the half the wipe deletes outright
  ps.embedding_config_id::text      AS embedding_config_id,
  ps.annotation_set_id::text        AS annotation_set_id,
  ps.ontology_snapshot_id::text     AS ontology_snapshot_id,
  ps.limit_per_entry                AS k,
  ps.distance_threshold,
  ps.meta->>'donor_policy'          AS donor_policy,
  ps.meta->>'search_backend'        AS search_backend,
  ps.meta->>'aspect_separated_knn'  AS aspect_separated_knn,
  ps.meta->>'features'              AS features,
  ec.model_name, ec.layer_indices::text AS layer_indices, ec.pooling,
  cat.k                             AS category,
  asp.k                             AS aspect,
  (asp.v->>'f_micro_w')::float8     AS f_micro_w,
  (asp.v->>'fmax_w')::float8        AS fmax_w,
  (asp.v->>'fmax')::float8          AS fmax,
  (asp.v->>'f_micro')::float8       AS f_micro,
  (asp.v->>'precision_w')::float8   AS precision_w,
  (asp.v->>'recall_w')::float8      AS recall_w,
  (asp.v->>'tau')::float8           AS tau,
  (asp.v->>'coverage_w')::float8    AS coverage_w,
  (asp.v->>'coverage_at_tau')::float8 AS coverage_at_tau,
  (asp.v->>'n_proteins')::int       AS n_proteins_at_tau
FROM evaluation_result er
LEFT JOIN job j              ON j.id  = er.job_id
LEFT JOIN prediction_set ps  ON ps.id = er.prediction_set_id
LEFT JOIN embedding_config ec ON ec.id = ps.embedding_config_id
CROSS JOIN LATERAL jsonb_each(COALESCE(er.results,'{}'::jsonb)) AS cat(k, v)
CROSS JOIN LATERAL jsonb_each(cat.v)                            AS asp(k, v)
WHERE cat.k IN ('NK','LK','PK') AND asp.k IN ('BPO','MFO','CCO')
ORDER BY er.created_at, er.id, cat.k, asp.k
""")

with eng.connect() as c:
    df = pd.read_sql(SQL, c)

# The conditioning set the census actually groups on. A row missing any of it cannot
# enter a contrast, and saying which is the point of the artefact.
# max_terms / max_distance / protein_subset appear ONLY in the sweeps that varied them
# (36, 18 and 17 jobs). Their absence means the operation's default was used, not that the
# value is unknown. What genuinely cannot be recovered is a result whose job is gone: with
# no job there is no operation, so not even the defaults are determined.
COND = ["evaluation_set_id","prediction_set_id","scoring_config_id","leakage_role",
        "temporal_window","max_terms","max_distance","protein_subset_label",
        "ia_set_id","protein_fold","k","embedding_config_id","producing_operation"]
df["conditioning_complete"] = df["job_recoverable"] & df["ia_set_id"].notna()
for c in ("max_terms","max_distance","protein_subset_label"):
    df[c + "_defaulted"] = df["job_recoverable"] & df[c].isna()

out = "/home/bioxaxi2/Thesis-laptop/PROTEA/results/floor_census"
os.makedirs(out, exist_ok=True)
pq = f"{out}/floor_census.parquet"
df.to_parquet(pq, index=False)
sha = hashlib.sha256(open(pq,"rb").read()).hexdigest()

man = {
  "produced_at_utc": str(df["created_at"].max()),
  "rows": len(df), "results": df.result_id.nunique(),
  "sha256": sha, "bytes": os.path.getsize(pq),
  "conditioning_fields": COND,
  "rows_with_complete_conditioning": int(df.conditioning_complete.sum()),
  "rows_without": int((~df.conditioning_complete).sum()),
  "pct_usable": round(100*df.conditioning_complete.mean(),2),
  "results_with_recoverable_job": int(df.groupby("result_id").job_recoverable.first().sum()),
  "producing_operations": df.groupby("result_id").producing_operation.first()
                             .fillna("(job absent)").value_counts().to_dict(),
  "frame_known": int(df.groupby("result_id").ia_set_id.first().notna().sum()),
  "distinct_frames": sorted(x for x in df.ia_set_id.dropna().unique()),
  "note": ("The census is a join of evaluation_result against job.payload. A wipe that "
           "removes results and keeps jobs orphans one half and deletes the other, so the "
           "numbers become unrecomputable from either. This file carries both halves."),
}
json.dump(man, open(f"{out}/manifest.json","w"), indent=2)

print(f"  filas               {len(df):,}  ({df.result_id.nunique():,} resultados x 9 paneles)")
print(f"  sha256              {sha[:32]}...")
print(f"  tamano              {os.path.getsize(pq):,} bytes")
print(f"  condicionante completo  {df.conditioning_complete.sum():,} / {len(df):,}  ({100*df.conditioning_complete.mean():.1f}%)")
print(f"  resultados con job      {df.groupby('result_id').job_recoverable.first().sum():,} / {df.result_id.nunique():,}")
print()
print("  por panel, filas utilizables:")
g = df.groupby(["category","aspect"]).conditioning_complete.agg(["sum","count"])
for (c,a),r in g.iterrows():
    print(f"    {c} {a}   {int(r['sum']):>5,} / {int(r['count']):>5,}   {100*r['sum']/r['count']:>5.1f}%")

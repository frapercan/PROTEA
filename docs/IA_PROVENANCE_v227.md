# Information Accretion (IA) provenance for the v227 LAFA-comparable benchmark

This note records which Information Accretion table PROTEA uses when it
reports IA-weighted metrics (`f_micro_w`, `fmax_w`, `s_min`) for the
v227 LAFA-comparable benchmark, why that file was chosen, and how it
differs from the other candidates.

## Why IA source matters

cafaeval weights every GO term by its information content (IA). Rare,
specific terms count more than common, easy-to-predict ones. Without an
IA file cafaeval falls back to uniform IC=1, which inflates Fmax (high
frequency terms dominate) and makes the number incomparable to CAFA and
LAFA. The headline metric for the v227 benchmark is `f_micro_w` (the
IA-weighted micro F), so the IA file is load-bearing for comparability.

## Candidates compared (2026-06-06)

Three IA tables were on disk:

- LAFA-knn t0 (Sep 2025): `protea-lafa-knn/lafa_t0_Sep_2025/IA.tsv`, 39906 terms.
- Lab recompute (v227): `protea-reranker-lab:datasets/ia/IA-swissprot-exp-v227.txt`
  (branch `ia/lafa-aligned-ia`, PR #55), 38739 terms.
- Generic CAFA6: `PROTEA/data/benchmarks/IA_cafa6.tsv` (snapshot `35c3ad67`
  `ia_url`), 40122 terms.

Pairwise comparison:

- LAFA-knn vs generic CAFA6: 39840 common terms, max abs diff 14.59,
  mean abs diff 0.128, 20107 common terms differ by >1e-3. The generic
  CAFA6 IA is a DIFFERENT corpus and must not be used for v227.
- LAFA-knn vs lab recompute: 38650 common terms, max abs diff 14.59,
  mean abs diff 0.077, 9473 common terms differ by >1e-3, Pearson
  r=0.982. Strongly correlated; the large per-term diffs are
  corpus-edge effects on rare terms (terms present in one corpus and
  absent/zero in the other). Both are legitimate v227 SwissProt IA
  tables computed on the same Sep 2025 t0, but they are NOT identical.

## Decision

Authoritative v227 IA = `protea-lafa-knn/lafa_t0_Sep_2025/IA.tsv`.

Rationale: this is the exact table the deployed LAFA endpoint
(protea-lafa.ngrok.app) scored its predictions against, so it is the
reference for the comparability gate (re-evaluating the PROTEA-KNN
predictions must reproduce the LAFA numbers on record). The lab
recompute is a faithful independent reconstruction (r=0.98) kept for
provenance, but the on-record LAFA numbers were produced with the
LAFA-knn file, so that file wins for the v227 benchmark.

The generic `IA_cafa6.tsv` is explicitly rejected for v227: it is a
different corpus and is what snapshot `35c3ad67`'s `ia_url` points at.
That snapshot is SHARED with v226 runs, so it is NOT globally repointed.
Instead each v227 eval dispatch passes an explicit `ia_file` in the
`run_cafa_evaluation` payload; payload `ia_file` takes precedence over
the snapshot `ia_url` (see `_resolve_ia_file`), and the resolved path is
logged as `run_cafa_evaluation.ia_resolved` so a v227 run visibly loads
the v227 IA.

## How to dispatch a v227 eval with the right IA

In the `run_cafa_evaluation` payload set:

```json
{
  "evaluation_set_id": "<v227 eval set>",
  "prediction_set_id": "<pred set>",
  "ia_file": "/path/to/lafa_t0_Sep_2025/IA.tsv"
}
```

The eval will emit `run_cafa_evaluation.ia_resolved` with that path,
and `EvaluationResult.results[<setting>][<aspect>]` will carry
`f_micro_w`, `fmax_w`, `f_micro`, and `s_min` alongside the plain
`fmax`.

# Feature leakage audit (F-EVAL-PROTOCOL.b)

Companion to ADR-D40 (`docs/source/adr/D40-leakage-free-temporal-eval-protocol.rst`),
which formalises the rolling-origin TRAIN/VALID/TEST protocol and the golden
no-leakage rule this document applies family by family.

## The golden rule

Every reranker feature family must satisfy a single invariant:

> A feature must be computable **identically** for a never-seen protein with
> **zero known labels**, using only data dated at or before the training
> cutoff (t0).

A feature that violates this leaks: its value encodes membership in a split,
a category, or the label set, and the booster picks it up as a proxy for the
target instead of learning a transferable signal. The reported Fmax then
overstates generalisation.

The cautionary template is the **anc2vec replication artifact** (PROTEA fix
`223299c`): the feature definition itself was clean, but the way the training
parquet was sharded per category (NK/LK/PK) turned the harmless
`anc2vec_query_known_count` column into a near-perfect category-id, because
the same row was emitted into all three shards with only the label
re-assigned. The fix was structural (filter rows by per-(protein, aspect)
category membership before labelling), not a feature removal. See
"anc2vec_query" below for the post-fix verification.

## Scope

The audited column set is the canonical
`protea_contracts.feature_schema.ALL_FEATURES` (and the `FEATURE_FAMILIES`
grouping). Producers read: `protea/core/feature_engineering.py`,
`protea/core/feature_enricher.py`, `protea/core/_feature_enricher_helpers.py`,
`protea/core/_pair_feature_compute.py`, `protea/core/_leaf_record_builder.py`,
the per-category shard builders in `protea/core/training_dump/_train_split.py`
and `_test_split.py`, and `protea_method.feature_enricher` /
`protea_method.lineage`.

Two distinct sources of leakage are checked per family:

1. **Definitional** (does the feature's *value* depend on the label of the
   row, on data after t0, or on the test/train split it lands in?)
2. **Constructional** (does the *pipeline* that materialises the column
   inject split/category/label information that the definition does not, the
   anc2vec class of bug?)

## Verdict by family

| Family | Columns | Verdict | Reason |
|-|-|-|-|
| `knn_distance` | `distance`, `neighbor_min_distance`, `neighbor_mean_distance`, `neighbor_distance_std` | PASS | Embedding-space KNN distances from the query to t0 corpus neighbours. No label, no future data, no split dependence. Empty-known protein: identical (still has neighbours). |
| `knn_vote` | `k_position`, `vote_count`, `neighbor_vote_fraction` | PASS | Counts/positions over the same t0 neighbour votes. Independent of the query's own labels. |
| `alignment_nw` | `identity_nw`, `similarity_nw`, `alignment_score_nw`, `gaps_pct_nw`, `alignment_length_nw` | PASS | Parasail NW alignment of query sequence vs neighbour sequence (`feature_engineering.compute_nw`). Pure sequence-vs-sequence; no labels. |
| `alignment_sw` | `identity_sw`, `similarity_sw`, `alignment_score_sw`, `gaps_pct_sw`, `alignment_length_sw` | PASS | As `alignment_nw`, Smith-Waterman. Sequence-only. |
| `length` | `length_query`, `length_ref` | PASS | Raw sequence lengths. No labels. |
| `taxonomy_pair` | `taxonomic_distance`, `taxonomic_common_ancestors`, `taxonomic_relation` | PASS | NCBI lineage distance between query taxon and neighbour taxon (`feature_engineering.compute_taxonomy`). Taxonomy is static metadata, not label-derived. |
| `taxonomy_voters` | `tax_voters_same_frac`, `tax_voters_close_frac`, `tax_voters_mean_common_ancestors` | PASS | Consensus over the *voting neighbours'* taxa for a candidate term. Aggregates neighbour taxonomy, never the query's labels. |
| `go_context` | `go_term_frequency`, `ref_annotation_density` | PASS | Corpus-level statistics of the t0 reference set (term frequency, per-neighbour annotation density). Same for every query; not query-label-derived. |
| `anc2vec_neighbor` | `anc2vec_neighbor_cos`, `anc2vec_neighbor_maxcos`, `anc2vec_has_emb` | PASS | Cosine of the candidate term's anc2vec embedding (pretrained GO 2020-10) against the centroid/matrix of the **neighbours'** annotation terms. Uses neighbour labels (legitimate t0 transfer signal), never the query's own labels. |
| `anc2vec_query` | `anc2vec_query_known_cos`, `anc2vec_query_known_maxcos`, `anc2vec_query_known_count` | PASS (was the offender; fixed + verified) | Cosine of the candidate vs the query's **pre-cutoff known** annotations (`eval_data.known` = experimental terms at t0, see `evaluation.compute_evaluation_data`). For a never-seen protein `known` is empty -> `_count=0`, `_cos/_maxcos=NaN`, identical to the genuine-NK case. The historical leak was *constructional*: per-category shard replication made `_count` a category-id. Verified fixed (see below). |
| `emb_pca` | `emb_pca_query_0..15` | PASS | PCA-16 projection of the query embedding. The PCA basis is fit transductively on the full (train+test) pool per PLM, unsupervised, cached by config_id (ADR, `project_pca_transductive_decision_2026_05_20`); it sees embeddings only, no labels, and is symmetric across train/test. No label leakage. |
| `lineage` | `lineage_is_ancestor_of_known`, `lineage_is_descendant_of_known`, `lineage_ancestor_of_count`, `lineage_descendant_of_count` | PASS | Whether a candidate term is an ancestor/descendant of the query's **pre-cutoff known** terms (`protea_method.lineage`, fed `query_known_gos=eval_data.known`). Empty-known protein -> all zero (the producer's documented default, mirrored by `_LeafRecordBuilder._lineage_default_fields`). Same pre-cutoff input as `anc2vec_query`; same replication caveat, same structural fix applies. |
| `annotation_meta` | `qualifier`, `evidence_code`, `aspect` | PASS | Provenance of the **neighbour's** annotation that cast the vote (`qualifier`/`evidence_code` from `ann`) and the candidate term's `aspect`. None is the query's label. `aspect` is the term's namespace, not the target. |

No family fails the golden rule after the `223299c` structural fix.

## anc2vec_query / lineage: post-fix construction verification

These two families read the query's pre-cutoff `known` set, so they are the
families most exposed to the replication-artifact class of leak. The fix is
verified to be in force on the current pipeline:

`protea/core/training_dump/_train_split.py::_label_and_write_train_split_shards`
computes per-category `(protein, aspect)` membership and applies a `cat_mask`
that drops every row whose `(protein, aspect)` does **not** belong to the
category *before* assigning the label and writing the shard:

```python
membership = _compute_test_cat_membership(eval_data, ctx.go_id_map, ctx.aspect_map)
for cat in _CATEGORIES:
    members = membership[cat]
    cat_mask = np.fromiter(((acc, asp) in members for acc, asp in ...), ...)
    cat_df = base_df.loc[cat_mask].copy()   # <- row only in its true category shard
    ...
    cat_df[LABEL_COLUMN] = labels
```

The test split applies the same membership mask
(`_test_split.py::_compute_test_cat_membership` + the writer that filters on
`membership[cat]`). Because each `(protein, aspect)` now lands in exactly one
category shard (CAFA's definition: NK is global-zero, LK/PK are per
namespace), `anc2vec_query_known_count` (and the `lineage_*` known-relation
flags) can no longer act as a category boundary: within a shard the column
varies for the genuine reason (how many pre-cutoff terms the protein had),
not as a deterministic NK-vs-synthetic-negative marker.

Storyline framing for the thesis (per `project_anc2vec_leakage_mechanism`):
this was a **replication artifact**, not temporal label leakage. The feature
is temporally honest (pre-cutoff input); only the shard construction had to
be made category-disjoint.

## Structural enforcement (this slice)

The temporal half of the no-future-data rule is now enforced structurally by
the **cutoff guard** (`protea/core/band_registry.py` +
`scripts/check_cutoff_guard.py`, wired into `.github/workflows/lint.yml`):
every band pins a `t0_cutoff`, and any artifact reference that carries a
parseable release date later than that cutoff (e.g. scoring the v227 band,
t0 `2025-09-04`, against ontology `releases/2026-01-23`) fails CI and the
runtime guard in `run_cafa_evaluation`. That complements this audit: the
guard catches *future data*, this audit catches *label / split leakage* in
the feature definitions and their construction.

## Deferred

The lab-side select-on-VALID refactor (`phase3a_*_sweep.py` selecting
champions on the same v226->v230 window it reports) is **out of scope** for
this slice and is tracked as `F-EVAL-PROTOCOL.b-lab`.

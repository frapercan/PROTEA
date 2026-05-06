ADR-007: Contract-first integration with ``protea-reranker-lab``
================================================================

:Date: 2026-04-21
:Author: frapercan

The problem
-----------

Re-ranker development is iterative and research-shaped: we want to try
out new feature families, different boosting objectives
(binary vs LambdaRank), IA weighting schemes, and training protocols
(per-cell vs full, multi-snapshot vs single-snapshot) without reshaping
the production predict path every time. At the same time, a trained
re-ranker must eventually be usable from inside PROTEA's
``predict_go_terms`` batch worker.

Two natural but incorrect structures were considered and rejected:

- **Single repository, shared runtime.** Lab training code would live
  inside ``protea/core/`` and be imported by predict-time workers.
  This forces every experimental LightGBM knob, callback, or eval
  harness into PROTEA's dependency tree (``optuna``, ``seaborn``,
  notebook code) and couples the schedule of research iteration to the
  schedule of production releases.
- **Single repository, disjoint packages.** Separate ``protea`` and
  ``protea_lab`` top-level packages under one repo. Still leaks lab
  imports into the production image (Python's import system does not
  care about package boundaries at install time), and creates a single
  review/merge pipeline for two workflows with very different cadences.

What we do
----------

PROTEA and ``protea-reranker-lab`` live in **separate repositories**
coupled only through a narrow contract:

1. **A file-format contract** — the frozen dataset layout. PROTEA's
   ``export_research_dataset`` operation writes exactly three files
   under an ``ArtifactStore`` key prefix:

   - ``train.parquet`` — all training shards concatenated with
     ``category`` and ``snapshot_pair`` columns;
   - ``eval.parquet`` — held-out evaluation shards;
   - ``manifest.json`` — schema version ``v2``, producer version and
     git sha, snapshot pair list, ``schema_sha`` fingerprint.

2. **A code contract** — the lab's ``protea_reranker_lab.contracts``
   module exposes two symbols PROTEA imports:

   - ``ManifestV1`` — Pydantic model for the manifest, used by
     ``protea.core.parquet_export`` at export time for best-effort
     validation (dev-only; silently skipped in production images that
     don't install the lab);
   - ``compute_feature_schema_sha(feature_families: list[str]) -> str``
     — deterministic 12-hex-char fingerprint used at **predict time**
     to verify the live feature set matches the booster's expectations.

3. **An artefact contract** — lab training writes
   ``runs/<name>/{run.json, spec.yaml, model.txt}``. PROTEA's
   ``scripts/register_reranker.py`` parses those files, uploads the
   booster through the ``ArtifactStore``, and inserts a
   ``RerankerModel`` row with provenance
   (``producer_version`` / ``producer_git_sha`` / ``spec_yaml``).

Strict feature-schema equality
------------------------------

``feature_schema_sha`` is computed over the sorted list of feature
families active at training time. At predict time the batch worker
recomputes it from its own active flags and compares for **strict
equality** — not subset, not prefix.

Rejected alternative: subset compatibility. If the booster was trained
on ``{knn, annotation_meta, alignment_nw, length}`` and the live
pipeline has ``{knn, annotation_meta, alignment_nw, length,
taxonomy_pair}``, a subset-match would feed the booster only its
trained columns. But LightGBM boosters are sensitive to the ordering
and distribution of the training feature matrix; a superset at
inference is still a drift in the implicit joint distribution the
model learned. Strict equality fails safe: the batch worker emits
``reranker.schema_mismatch`` and falls back to KNN ordering, which is
always a legitimate baseline. A missed re-ranking hit is preferable to
silently miscalibrated scores.

Trade-offs
----------

- **Two-repo friction.** Changing a feature family requires coordinated
  commits in both repos plus a dataset re-export. Mitigated by keeping
  the contract surface tiny (three files + two symbols) and by making
  ``register_reranker.py`` reject runs whose ``schema_sha`` does not
  match any PROTEA-side snapshot of the same feature-family list.
- **Production images ship without the lab.** ``protea_reranker_lab``
  is a dev-only dependency of PROTEA. The predict-time import is
  guarded — a missing lab install causes the batch worker to emit
  ``reranker.skipped`` with ``reason=contracts_unavailable`` and fall
  back to KNN ordering, without crashing. In production we ship the
  lab alongside PROTEA (single editable path dep) precisely so
  ``compute_feature_schema_sha`` is available.

Rejected
--------

- **Dynamic feature-family negotiation.** Letting the worker infer
  which columns the booster expects from ``booster.feature_name()``
  and building them lazily. Too fragile — names in the booster do
  not carry family grouping, so every feature engineering change
  would need manual backwards-compat shims.
- **Pickling ``ExperimentSpec`` objects across repos.** Requires both
  sides to share Python class identity; defeats the point of
  decoupling and breaks on any lab refactor.

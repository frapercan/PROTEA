ADR-D45: Governing the JSONB-blob feature seam
==============================================

:Status: Proposed
:Date: 2026-06-22
:Author: Francisco Miguel Pérez Canales
:Phase: T-GOBERNANZA

Context
~~~~~~~
PROTEA fingerprints its reranker feature set and refuses to score with a
booster whose features have drifted. The canonical feature set is declared
in ``protea-contracts`` (``src/protea_contracts/feature_schema.py``:
``ALL_FEATURES``, ``FEATURE_FAMILIES``) and fingerprinted by
``compute_feature_schema_sha(families, drop)``. At inference the scorer
recomputes the live sha from the active families and raises
``SchemaShaMismatchError`` when it disagrees with the booster's recorded
``RerankerModel.feature_schema_sha`` (see
``protea/core/operations/predict_go_terms/_reranker_scorer.py``, the
``_check_schema_sha`` path around lines 290 to 370, and the families chosen
by ``protea_method.reranker.infer_active_feature_families``). This is the
**governed surface**: a rename, reorder, or addition of a contract feature
forces a SemVer major bump on ``protea-contracts`` and is caught at serve.

There is a second feature surface that is NOT governed by that sha. Four
families ride the ``GOPrediction.features`` JSONB blob
(``protea/infrastructure/orm/models/embedding/go_prediction.py``,
``features`` column) and are computed post-KNN, each behind its own
``compute_*`` payload flag, in
``protea/core/operations/predict_go_terms/_post_knn_pipeline.py``:

- **classifier** (``compute_classifier`` -> ``apply_classifier``),
- **self_prior** (``compute_self_prior`` -> ``apply_self_prior``),
- **association** (``compute_association`` -> ``apply_association``),
- **IA / information accretion** (``compute_ia`` -> ``apply_ia``).

These producers and their flags live in PROTEA core and protea-method
(``infer_active_feature_families`` only knows the contract families: knn,
alignment, taxonomy, anc2vec, emb_pca, go_context, annotation_meta). The
four blob families are never named in ``feature_schema.py``, so they do not
enter ``compute_feature_schema_sha`` and the guard is structurally blind to
them. A change to their VALUES (a producer that was a no-op at train time
becoming populated at serve time, a config change in the producer, a
different IA file) does not change any sha and trips no check.

The risk is train/serve **value** skew, not schema skew. The schema-sha
guard verifies that the same column NAMES are present; it cannot verify that
the same column VALUES were produced by the same producers under the same
configuration. The blob seam is exactly where that invariant is unenforced.

Incident (the concrete evidence)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The night of 2026-06-21 an INT-8 native re-run regressed the reranked LAFA
``f_micro_w`` from 0.3745 to 0.3462. The booster trio had been trained when
the ``association`` producer was effectively zero (the cooccurrence the
producer reads was not built for the training t0 sets, so the feature was a
near-constant column at train time). The serve-time run populated
``association`` with real values. The booster therefore consumed an
out-of-distribution column it had learned to ignore, and the score
collapsed by roughly 0.028. The column NAME was unchanged, so
``feature_schema_sha`` matched and the guard passed silently. This is a
textbook train/serve value skew through the ungoverned JSONB-blob seam, of
the same shape as the earlier 0.315 collapse, but invisible to the one
mechanism PROTEA has for catching reranker drift.

Decision
~~~~~~~~
Do not force the four blob families into ``ALL_FEATURES`` /
``compute_feature_schema_sha``. That would over-couple: the contract sha is
a SemVer-load-bearing fingerprint of the lab parquet column set, and the
blob families are produced inside PROTEA core (not by the lab), are gated
per run, and legitimately vary in presence across runs. Folding them in
would force a contracts major bump and a full booster re-train on every
producer config change, and it still would not capture a value change with a
stable config. The right unit of governance for these is **value
provenance**, not schema identity.

Adopt instead a documented boundary plus a lightweight provenance marker and
a serve-time check:

1. **Name the boundary (this ADR).** The governed surface is the contract
   ``ALL_FEATURES`` via ``feature_schema_sha``. The ungoverned surface is the
   ``GOPrediction.features`` blob families (classifier, self_prior,
   association, IA). State this in the data-model docs next to the
   ``GOPrediction.features`` column and next to ``RerankerModel`` so the seam
   is visible to anyone training or registering a booster.

2. **Record blob-feature provenance on the training artifact.** When a
   ``Dataset`` is exported and a ``RerankerModel`` is registered, record
   which blob-feature producers were active and the config they ran under
   (the set of ``compute_*`` flags that were on, plus a content marker for
   each producer's inputs: for ``association`` the cooccurrence build keyed
   to the training t0 sets, for ``IA`` the IA file identity, for self_prior
   and classifier the producer revision). This is a small JSON marker on the
   training side (a ``blob_feature_provenance`` field on ``Dataset`` /
   ``RerankerModel``, paralleling the existing ``feature_schema_sha`` /
   ``external_source`` provenance), not a new sha in the contract.

3. **Warn or refuse on provenance mismatch at serve.** At inference, the
   scorer compares the serve-time active blob producers and their config
   marker against the booster's recorded ``blob_feature_provenance``. On a
   mismatch (for example: ``association`` was a no-op at train time but is
   populated now), emit a structured warning event and, behind a strict
   flag, refuse with a dedicated error code, mirroring the
   ``SchemaShaMismatchError`` path but for VALUE provenance rather than
   schema identity. This catches the INT-8 class of regression at registration
   or serve, where the schema-sha guard cannot.

Keep step 1 mandatory and immediate (documentation, this ADR). Steps 2 and 3
are the proposed mechanism; they are deliberately scoped to a provenance
marker and a comparison, with no change to the contract sha and no forced
booster re-train.

Consequences
~~~~~~~~~~~~~
- The two feature surfaces are named and the asymmetry is no longer
  implicit: schema identity is governed by ``feature_schema_sha``; blob
  value provenance is governed by the marker.
- A booster trained against a zero (or differently configured) blob
  producer can no longer be served silently against a populated one; the
  INT-8 0.3745 to 0.3462 regression would have surfaced as a provenance
  warning (or a refusal under strict mode) rather than a silent score
  collapse.
- The contract sha stays a clean fingerprint of the lab parquet column set;
  no over-coupling, no forced contracts major bump for a PROTEA-side producer
  change, no full re-train on every IA-file or cooccurrence rebuild.
- The provenance marker is a small additive field on existing rows
  (``Dataset`` / ``RerankerModel``); no migration of the prediction hot path,
  and legacy rows simply carry a null marker (treated as unknown, warn-only).

Resolution
~~~~~~~~~~
Open. This ADR records the boundary and proposes the provenance marker plus
the serve-time check. The governed surface (``feature_schema_sha`` in
``feature_schema.py`` and ``_reranker_scorer.py``) and the ungoverned blob
producers (``_post_knn_pipeline.py``) both exist today; D45 names the seam
between them and the incident that proves it is load-bearing. Implementation
of the marker and the check is deferred to a follow-up slice and is gated
on the beat-lafa-1 booster-registration flow that would carry the field.

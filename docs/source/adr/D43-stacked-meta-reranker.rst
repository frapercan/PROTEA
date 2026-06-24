ADR-D43: Stacked meta-reranker (evidence scorers plus a shallow per-category combiner)
======================================================================================

:Status: Accepted
:Date: 2026-06-17
:Phase: MR (meta-reranker)

Context
-------
The reranker grew into a monolithic single-level booster: one per-category
LightGBM model consuming a wide raw feature matrix (KNN pair features, taxonomy,
anc2vec, classifier, priors) directly. Two problems followed.

First, expansion was expensive. Adding a signal (a new PLM, a new prior, an
N-hop propagation) meant touching the feature schema, the dump, the booster
training, and the schema fingerprint together. That couples feature engineering
to model cadence and conflicts with NFR-ARCH (hexagonal, scale green) and FR-1
(every lever UI-actionable).

Second, the monolith collapsed Partial-Knowledge (PK). A single high-capacity
model trained to rank on a wide matrix optimises an internal AUC-like objective,
but the benchmark metric is thresholded ``f_micro_w``. On PK the gap between the
two bit: the booster's scores did not calibrate, so the thresholded score fell
below even the linear baseline (single-level 0.317 vs linear 0.315 on the frame
that the offline champion reaches 0.391). The lever that fixes this is not more
model capacity; it is a low-dimensional, calibratable combination.

Decision
--------
Replace the monolith with a stacked meta-reranker built from two ports:

* an **EvidenceScorer** port: a wide set of independent signals, each emitting
  ONE calibrated score per candidate, keyed on the snapshot-agnostic ``go_id``
  string. A scorer declares the CAFA categories it applies to (priors do not
  apply to No-Knowledge). Each scorer is pure, independently sealable, and
  measurable.

* a **Combiner** port: a shallow, per-category meta-learner that consumes the
  VECTOR of scorer outputs (a handful of components), never the raw feature
  matrix. Low input dimensionality is what keeps it low-variance and
  calibratable, which is the direct remedy for the PK collapse.

Depth lives INSIDE a scorer (N-hop propagation over the GO-DAG or co-occurrence)
or in a compute cascade (telescoping stages), NEVER in combiner stacking.
Rationale: stacking depth has diminishing returns plus added variance plus
leakage surface; the productive lever is evidence diversity (Krogh-Vedelsby),
which the wide scorer set provides.

Both ports resolve through a dict-backed ``ScorerRegistry`` (the hexagonal seam,
mirroring ``OperationRegistry``), so the active scorers and the per-category
combiner are configurable per run and surfaceable in the UI (FR-1, no
dead-ends).

This ADR records the architecture and the foundation (MR-0 plus MR-1): the two
ports, the registry, a trivial reference combiner, and adapters that expose the
four already-computed producer signals (KNN similarity, the M2 classifier,
``self_prior``, ``association``) as scorers. The trained per-category combiner is
MR-2; calibration is MR-3; UI integration is MR-4.

Consequences
------------
- Adding a lever costs ONE new ``EvidenceScorer`` plus one column in the
  combiner input plus a retrain of the cheap combiner. Candidate retrieval and
  the other scorers do not change.
- Nothing is thrown away. The existing linear ``scoring_config`` is the
  degenerate fixed-weight combiner; the monolithic booster is the degenerate
  scorer-less case. The reference ``LinearCombiner`` / ``PassThroughCombiner``
  make those special cases concrete.
- The MR-0 / MR-1 foundation is ADDITIVE: the ports are NOT wired into the live
  predict / eval path in this slice, so the existing single-level path behaves
  identically (golden-parquet, reranker, predict, and run_cafa tests stay
  green). The adapters read the same per-candidate keys the producers already
  stamp (and #643 already persists on ``GOPrediction.features``), so the eval /
  combiner path can read them without recomputation.
- Calibration becomes first-class (per-scorer or in the combiner), so the
  thresholded ``f_micro_w`` behaves rather than fighting an internal ranking
  objective.

MR-1 base-evidence scorers
--------------------------
The first combiner pass over only the four producer signals (knn_similarity,
classifier, self_prior, association) fixed PK calibration but LOST NK and LK,
because that score vector omitted the **base sequence / taxonomy / domain /
label-embedding evidence** the old monolith consumed. Five additional thin
adapter ports recover it, each reading a first-class ``GOPrediction`` column the
producers already stamp:

* **alignment** (``alignment_score_sw``): Smith-Waterman local alignment, the
  strongest sequence-homology evidence (higher is stronger).
* **taxonomy** (``taxonomic_distance``): taxonomic proximity, emitted as-is
  (lower is closer; the combiner learns the sign).
* **label_embedding** (``anc2vec_neighbor_maxcos``): GO label-embedding KNN
  cosine.
* **interpro** (``interpro_score``): InterPro domain-signature evidence.
* **term_frequency** (``go_term_frequency``): base-rate / calibration signal.

All five **apply to NK / LK / PK** (defined from sequence / embeddings / domains,
no known terms needed). The canonical score-vector order becomes base evidence
first, then the original four: alignment, taxonomy, label_embedding, interpro,
term_frequency, knn_similarity, classifier, self_prior, association. This is
ADDITIVE (the ports are still not wired into the live predict / eval path); it
gives the MR-2 combiner the NK / LK evidence the priors-plus-classifier-plus-KNN
vector lacked.

Resolution
----------
Open. Foundation (MR-0 ports plus registry, MR-1 producer adapters) landed on
``feat/mr-foundation``. The trained per-category combiner (MR-2), the
calibration layer (MR-3), and the UI integration (MR-4) follow as separate
shippable slices, each measured on ``/benchmark`` against the single-level
0.317 / linear 0.315 baseline toward the 0.391 floor.

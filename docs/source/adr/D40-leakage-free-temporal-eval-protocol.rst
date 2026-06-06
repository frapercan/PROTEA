ADR-D40: Leakage-free temporal evaluation protocol (rolling-origin TRAIN/VALID/TEST)
===================================================================================

:Status: Accepted
:Date: 2026-06-06
:Author: Francisco Miguel Pérez Canales
:Phase: F-EVAL

Context
~~~~~~~

Protein function annotation accretes over time. A protein that carries no
experimental GO term today may gain one next release; the ground truth is
a moving target tied to a GOA snapshot date. Any evaluation that shuffles
proteins into random folds therefore leaks the future into the past: a
random split can place a protein's late annotation in the training corpus
and score the model on a term it was effectively shown. The reported number
is then optimistic and not comparable to a real CAFA round, where the
challenge is to predict annotations that do not exist yet at submission
time.

PROTEA's evaluation is already temporal in spirit: datasets and evaluation
sets are keyed by an OLD snapshot (the reference / training cutoff) and a
NEW snapshot (the ground-truth target). The export pipeline pins the KNN
corpus and reranker labels to snapshot pairs, and the benchmark reports a
GOA-band Fmax (for example the v226 to v230 window). Two gaps remain:

1. **No first-class selection window.** Today a cell is scored on one
   evaluation window and the same number is used both to pick the best
   recipe (PLM, K, reranker variant, LightGBM hyperparameters, the
   selective-deploy decision) and to report the result. Selecting and
   reporting on the same window is a winner's-curse: the reported Fmax is
   biased upward by the selection itself. The lab sweep scripts pick
   champions on the identical band they report.

2. **The no-leakage rule for features is informal.** The anc2vec
   replication artefact (see References) showed how a feature can silently
   encode a label-derived bucket id. That class of leak is not specific to
   anc2vec; every feature family needs the same scrutiny, and the rule that
   protects against it has never been written down as a contract.

This ADR formalises the protocol. The enforcement code that mechanises it
(a cutoff CI guard, a per-feature leakage audit, the lab selection-sweep
refactor, and a fresh-cutoff inference path) is deferred to follow-up
slices and is named explicitly in "Scope and follow-ups" below. This record
is the design and decision; it changes one structural detail (a window-role
marker on evaluation metadata) and otherwise binds future work.

Decision
~~~~~~~~

**1. Rolling-origin temporal split. Random or shuffled cross-validation is
forbidden.** Evaluation uses three time-ordered windows defined by two
origins ``t0`` and ``t1`` over the GOA release timeline, with ``t-1`` the
training cutoff that precedes ``t0``:

- **TRAIN** (releases up to ``t-1``): builds the KNN corpus, the reranker
  training labels, and the information-accretion (IA) weights. Nothing in
  TRAIN may reference a release later than ``t-1``.
- **VALID** (``t-1`` to ``t0``): model, recipe, and hyperparameter
  selection; early stopping; and the operating threshold ``tau``. This is
  the only window touched during tuning.
- **TEST** (``t0`` to ``t1``): touched exactly once, after selection is
  frozen. The Fmax reported on TEST is the LAFA-comparable number.

Shuffled or random K-fold cross-validation is not permitted for the
reported number, because a random fold can place a protein's later
annotation alongside its own earlier state and leak the target.

**2. Each pipeline stage is bound to one window.** The split is a binding
of stages to windows, not a row-level partition:

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Stage
     - Window
     - Binding rule
   * - KNN corpus
     - TRAIN
     - Pinned to the ``t-1`` cutoff; no later embedding or annotation.
   * - Reranker training labels
     - TRAIN
     - Derived from annotation growth strictly inside TRAIN.
   * - IA weights
     - TRAIN
     - Computed from the ``t-1`` corpus only (the metric weighting is
       frozen before selection).
   * - Recipe and hyperparameter selection
     - VALID
     - PLM, K, reranker variant, LightGBM hyperparameters, and the
       selective-deploy decision are chosen here.
   * - Operating threshold ``tau``
     - VALID
     - Fixed on VALID and reported alongside the Fmax-over-threshold curve.
   * - Final evaluation
     - TEST
     - Run once with the frozen recipe and ``tau``; this is the reported
       number.

**3. Selection happens on VALID, never on TEST.** The reported number is
either the VALID-selected model evaluated once on TEST, or the full grid
reported on TEST with no in-TEST selection. Picking the maximum over a grid
on TEST and reporting that maximum is forbidden: it reintroduces the
winner's-curse the VALID window exists to remove.

**4. Operating threshold is part of the protocol.** The decision threshold
``tau`` is fixed on VALID and reported together with the threshold-swept
Fmax. A number reported at a threshold tuned on TEST is not protocol-
compliant.

**5. The golden no-leakage rule for features.** Every feature must be
computable identically for a never-seen protein that has zero known labels
at the cutoff. If a feature's value depends, directly or indirectly, on the
ground-truth labels of the protein being scored (or on a quantity that is a
deterministic function of those labels, such as a replication-bucket id),
it leaks and must be fixed or dropped. The anc2vec replication artefact is
the cautionary example: a per-category replication of training rows turned
a "known count" feature into a bucket identifier that separated genuine
no-knowledge proteins from synthetic negatives. The fix filtered
``(protein, aspect)`` pairs by genuine category membership. The rule is a
contract every feature family must satisfy, not an anc2vec-specific patch.

**6. Fresh-cutoff principle for a real CAFA round.** For a new submission,
the whole pipeline must read only data at or before a single declared
cutoff ``t0``, enforced structurally rather than by convention. A submission
produced this way has no annotation, embedding, corpus row, or feature that
postdates ``t0``. The single-knob inference path that realises this is a
follow-up (see below); this ADR fixes the principle it must satisfy.

**7. Structural change shipped with this ADR.** Evaluation metadata gains a
first-class window-role marker so a cell can be scored on a VALID window
distinct from TEST. A nullable ``window_role`` field on the evaluation set
takes ``"valid"`` or ``"test"`` (or remains unbound as ``NULL`` for
ad-hoc episodes), guarded by a closed-vocabulary CHECK constraint. The
marker is metadata only: it does not change how the delta between snapshots
is computed, and it leaves existing rows untouched (``NULL``). It is the
minimum plumbing that lets selection read the VALID set and reporting read
the TEST set without the two collapsing onto one window.

Scope and follow-ups
~~~~~~~~~~~~~~~~~~~~~~

This ADR and its companion change are PART .a of F-EVAL-PROTOCOL: the
protocol record plus the VALID-window marker. The enforcement and tooling
are explicitly deferred and tracked as follow-ups:

- **.b cutoff CI guard.** A structural check that fails if any produced
  artefact (corpus, features, labels, IA) references a release later than
  the declared cutoff, mirroring the ALL_FEATURES producer-consumer guard.
- **.b per-feature leakage audit.** Every feature family is checked against
  the golden rule of decision point 5; offenders are documented and fixed
  or dropped. The anc2vec fix is the template.
- **.b lab selection-sweep refactor.** The protea-reranker-lab sweep
  scripts are reworked to select on the VALID window and freeze the recipe,
  ``tau``, and selective-deploy decision before TEST is read.
- **.c fresh-cutoff inference path.** A single cutoff knob drives the whole
  pipeline end to end and produces a CAFA submission with no data later
  than ``t0``.

Consequences
~~~~~~~~~~~~~

**Positive**

- The reported benchmark number stops being inflated by selection on its
  own window; the VALID and TEST windows are structurally distinct.
- The protocol is now a written contract that thesis chapter 6 can cite and
  that any external reviewer can check, rather than an implicit convention.
- The golden feature rule generalises the anc2vec lesson into a standing
  acceptance criterion for every future feature family.
- The window-role marker is forward-compatible: it adds a column without
  touching existing rows or the delta computation.

**Negative**

- Producing a genuine VALID window costs an extra evaluation-set
  generation per cell (one more snapshot pair to materialise and score).
- Existing published numbers that were selected and reported on the same
  window are, strictly, optimistic; chapter 6 must either re-report under
  the protocol or annotate the caveat. This ADR does not re-run any grid.

**Neutral**

- No scoring recipe, dataset row, reranker model, or cafaeval result is
  changed by this ADR. The only schema change is the nullable
  ``window_role`` marker, which defaults to ``NULL`` on every existing row.
- The protocol composes with, and is orthogonal to, the IA-weighted metric
  work and the GOA band rebuild; neither is a precondition for the other.

Supersedes
~~~~~~~~~~

Nothing. No prior ADR formalised the temporal split or the feature
no-leakage rule. This record is the first formal statement of the
evaluation protocol.

References
~~~~~~~~~~

- Memory entry ``project_anc2vec_leakage_mechanism``: the replication
  artefact where a per-category replication turned a known-count feature
  into a bucket identifier; the template for the deferred feature audit.
- Memory entry ``project_lafa_band_pivot_2026_05_28``: GOA-band evaluation
  set lineage (the OLD / NEW snapshot pairing that the windows build on).
- Memory entry ``project_lb3_paired_ci_2026_05_18``: paired bootstrap
  protocol for the reported champion, which this split makes leakage-free.
- ADR D34 (selective rerank resurrection): recompute on the current band
  rather than reconstruct historical cells.
- ADR D36 (PLM axis explicit in dataset naming): the ``bench-v1-K5`` style
  dataset naming the windows are scored over.
- Slice catalog ``agent-farm/plans/farm-platform/PLAN.md``: F-EVAL-PROTOCOL
  (this slice and its .b / .c follow-ups).
- Migration ``f2a4c6e8b0d1_add_window_role_to_evaluation_set``: the
  ``window_role`` column that realises decision point 7.

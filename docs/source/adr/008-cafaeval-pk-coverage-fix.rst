ADR-008: PK coverage fix in cafaeval fork
==========================================

:Date: 2026-04-23
:Author: frapercan
:Status: Accepted

Context
-------

Upstream ``cafaeval`` (pinned at ``claradepaolis/CAFA-evaluator-PK``) reports
``coverage`` values greater than ``1.0`` for the Partial-Knowledge (PK)
evaluation branch. Coverage is defined as the fraction of eligible proteins
for which the predictor emits at least one scored term at a given threshold,
which by definition is bounded in ``[0, 1]``. Values of ``1.3–1.9`` are
observed on every PROTEA benchmark run.

The root cause is an asymmetry between the numerator and the denominator of
the coverage ratio in the PK kernel of ``cafaeval/evaluation.py``:

.. list-table::
   :header-rows: 1
   :widths: 25 55 20

   * - Quantity
     - Definition in upstream code
     - Location
   * - ``metrics['n']`` (numerator)
     - Number of proteins in ``proteins_with_gt`` (any GT annotation in
       TOI, **pre-exclusion**) that receive at least one valid prediction
       at threshold ``τ``.
     - ``compute_confusion_matrix_exclude_sparse``, line 304.
   * - ``ne`` (denominator)
     - Number of proteins with at least one GT annotation in TOI that
       survives the per-protein ``exclude_matrix`` (i.e. **novel**
       annotations the predictor should recover).
     - ``_count_proteins_in_toi`` with ``exclude_matrix != None``, line
       627; materialised in ``evaluate_prediction``, line 657.

Under the PK branch the exclusion mask is applied to prediction values
(``valid = (pred_sub != 0) & toi_mask & ~excluded_mask``, line 266) and to
the per-TP weighting, but **not** to the row count that becomes
``metrics['n']``. Proteins whose TOI annotations are fully contained in
``t0`` (no novel GT in ``t1``) are therefore:

- excluded from ``ne`` (correct),
- but still counted in ``metrics['n']`` whenever the predictor emits a
  non-excluded term for them (incorrect).

The observed ``coverage > 1`` is the visible symptom. The silent
secondary effect is that ``precision`` under
``normalization='cafa'`` uses ``metrics['n']`` as its denominator
(``normalize()``, line 569), so precision is under-divided, tightened
by the same factor. On the 220→230 PROTEA benchmark this drags PK Fmax
from its true value down by 30–40 %.

Decision
--------

``cafaeval-protea`` (fork commit ``cec8ccd``) applies a one-line semantic
fix inside ``compute_confusion_matrix_exclude_sparse``:

.. code-block:: python

   # Restrict the row count to proteins that still have ≥1 GT annotation
   # in TOI after the per-protein exclude mask. Without this, `n` counts
   # proteins whose TOI annotations were all already known in t0, while
   # the denominator `ne` drops them, producing coverage > 1.
   eligible_rows = (
       (gt_sub != 0) & toi_mask[None, :] & (~excluded_mask)
   ).any(axis=1)

   metrics[:, 0] = ((pred_at_tau > 0) & eligible_rows[:, None]).sum(axis=0)

The patch runs in numpy (bool mask broadcast + a sum), so it is bounded
in cost by ``O(n_prot × n_terms)``, the same complexity as the sibling
mask lines it mirrors (lines 266 and 273). No allocation changes, no new
dependencies.

The NK/LK kernel path (``compute_confusion_matrix_sparse``) is not
touched because it has no exclusion concept: ``proteins_with_gt`` already
matches ``ne`` by construction, and coverage is bounded by
``[0, 1]`` without any extra masking.

Why this was not caught by the fork's parity tests
--------------------------------------------------

The parity tests in ``tests/diff/test_oracle_parity.py`` compare the
fork's output against a frozen pickle of the upstream evaluator run on
the same corpora. They enforce bit-/ULP-exact equality across every
column (including ``n`` and ``cov``), so any semantic correction in
the PK branch will look like a regression.

To keep the parity gate honest, the fix is accompanied by two changes
in the test suite:

1. ``tests/test_pk_coverage_bug.py``: a positive regression gate with
   a synthetic three-protein PK scenario. One of the proteins has all
   its GT annotations pre-excluded. The test asserts that
   ``metrics['n'] ≤ ne`` and that TP/FP/FN/recall are unaffected. This
   test fails if the fix is ever reverted.

2. ``tests/diff/test_oracle_parity.py``: a ``_maybe_xfail_pk()`` helper
   xfails the PK variants of the oracle parity with a documented reason.
   The NK/LK variants continue to enforce bit-exact parity with
   upstream. The xfail is intentional and load-bearing: it records
   the fact that the fork has deliberately diverged from upstream on
   PK semantics, not because of a numerical drift.

Consequences
------------

The sections below document the measured impact and operational steps required
after applying the fix.

Effect on the PROTEA 220→230 benchmark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After re-running the 15 PK evaluations under the patched fork:

.. list-table::
   :header-rows: 1
   :widths: 20 25 25 25

   * - Cell
     - Fmax (before)
     - Fmax (after)
     - Δ
   * - PK BPO
     - 0.130
     - 0.198
     - +0.068
   * - PK CCO
     - 0.301
     - 0.366
     - +0.065
   * - PK MFO
     - 0.210
     - 0.291
     - +0.081
   * - PK BPO coverage
     - 1.94
     - 0.97
     - −0.97
   * - PK BPO precision
     - 0.088
     - 0.157
     - +0.069

NK and LK cells are unchanged within float noise. The thesis PK metrics
reported hereafter reflect the corrected computation.

Operational implication
~~~~~~~~~~~~~~~~~~~~~~~

- ``cafaeval-protea`` is installed in PROTEA via a ``file://`` path
  dependency. After pulling a new fork commit, the venv must be force-
  reinstalled::

      pip install --force-reinstall --no-deps /path/to/cafaeval-protea

  because ``poetry install`` treats the local path as satisfied once
  the lockfile hash matches. A plain ``poetry install`` will silently
  leave the old module in ``site-packages/``.

- Every live ``worker-evaluations`` process holds the ``cafaeval``
  module in memory. Reinstalling the package does **not** hot-patch
  running workers; they must be restarted to pick up the fix::

      systemctl --user restart protea-worker-evaluations

- ``EvaluationResult`` rows persisted before the fix carry the buggy
  ``cov`` / ``precision`` values and must be discarded (DELETE via
  ``/annotations/evaluation-sets/{id}/results/{rid}``; the endpoint
  cascades to MinIO artifacts). The launcher re-fires new runs
  automatically for any ``prediction_set`` that loses its eval.

We should push this fix upstream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The bug exists verbatim in ``claradepaolis/CAFA-evaluator-PK`` and, as
far as we can tell, has never been flagged in an issue. The fix is
small, semantically justified, and strictly improves correctness for
every downstream user of the PK branch. An upstream report with the
minimal reproducer from ``tests/test_pk_coverage_bug.py`` is pending.

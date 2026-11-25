ADR-D64: Methods are fitted inside the platform, tuned on 211->227 and decided once on 227->230
===============================================================================================

:Status: Proposed (2026-11-17). It writes down two of the author's decisions of
         that day. The first is the replication frame: "estamos ajustando todo
         en 211-227... y la evaluacion decisiva sera 227->230, nos olvidamos de
         los resultados que obtuvimos... como si estuviera limpio. La decision
         se toma sobre anterior a 227-230". The second is the plan, whose fourth
         step is this mechanism ("Sigamos tus recomendaciones").
:Date: 2026-11-17
:Author: Francisco Miguel Perez Canales
:Phase: platform refactor, phases 5 and 6 (methods and the unified evaluation)
:Depends on: ADR-D60 (the method contract, the compute-stage contract, the
             evaluation contract and the weights rule), ADR-D54 (the weights
             registry), ADR-D55 (evaluated explorations), ADR-D61 (the strata's
             homology table), ADR-D62 (a file stage on the cluster)

Context: the contract exists, the fitting does not
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ADR-D60 gives a method its own identity: a ``MethodSpec`` whose id derives from
its canonical JSON, its training windows by evaluation set id, its pins by
sha256, and a leakage guard that reads them. Its weights rule says that weights
fitted on GO labels are refit inside the platform.

Nothing in the platform fits anything yet. ``run_lab_method`` and
``ingest_method_predictions`` run or ingest what was trained elsewhere. The
method tables, the weights registry and the prediction sets are empty in
development.

The three families must now be replicated from zero and tuned. Each has
fitted parts:

- the embedding-reranker family: its ranker;
- the world-model family: the model;
- the explainable-units family: its maps and its combiner.

Decision
~~~~~~~~

**1. A fit is a platform operation of its family.** It takes a ``MethodSpec`` and
the fit windows by evaluation set id, builds its training set from the universe,
fits, and declares the result:

- the weights in ``model_weights``, with the windows they saw;
- the ``method`` row, whose id derives from the spec;
- one ``method_training_window`` row per window (role ``fit``);
- one ``method_pin`` per weight file, by sha256.

The leakage guard refuses a window that ends after the instance's cut before
anything runs.

**2. The training set comes from the universe only.** It is built from the
photos, the windows, the homology tables and the banks (ADR-D62 and ADR-D63),
by the family's own operations. It is stored in the artifact store with its
sha256 and is never read from a laboratory file.

**3. Where it runs is a matter of need.**

- A fit that fits a CPU runs on ``protea.training``. That covers the trees, the
  count maps and the combiners.
- A fit that needs a card runs on the cluster as a file stage. Its training set
  is placed by sha256, and its weights come back with a manifest that an ingest
  operation verifies (ADR-D60, decision 4).
- Data sent to the cluster is logged, as for the photos.

**4. Instance 211 is fitted and tuned before anything of 230 exists.**

- It fits on 160->194, 177->205 and 194->211.
- It is read on 211->227 as often as tuning needs. Reading selects; it never
  fits.
- A variant is a new ``MethodSpec``: its recipe differs, so its id differs. It
  is fitted, predicts the targets of 211->227 and is compared with the current
  reference by the evaluator:

  - by cell (knowledge type and aspect), by identity band (from the cut's
    homology table) and by length band, each with its n;
  - with a paired bootstrap over proteins.

- Each comparison is stored as an evaluated exploration (ADR-D55) with its
  verdict. The rule that decides a selection is written before its numbers are
  read.

**5. One frozen recipe, one decisive read.**

- When the author freezes the recipe, instance 227 is the same spec refit up to
  227, with 211->227 added as a fit window.
- It is read on 227->230 exactly once. Every decision is taken before that
  read.
- The earlier results on 227->230 (October and the talk) are set aside and
  never used to decide anything. The window is treated as clean by the
  author's decision, and the record says that it had been read before.
- 230 stays outside development until the read. The mark stays at 227. The
  read goes through a single-read path opened by the author's word, as the
  October test did. There, the evaluator refuses a second read of the same
  frame for the same frozen recipe.

**6. Fitting needs the author's word.** No fit is launched without it, as for
every training since October.

Consequences
~~~~~~~~~~~~

- Every number of the replication comes with the method id, its windows, its
  pins and the evaluator's commit. Laboratory numbers do not carry over.
- Tuning is cheap to repeat and safe from leakage: windows after the cut are
  refused, and 211->227 never enters a fit of instance 211.
- The spent-frame refusal of ADR-D60, decision 5, gains one declared exception:
  the single decisive read of 227->230 for a frozen recipe, on the author's
  word.

Alternatives considered
~~~~~~~~~~~~~~~~~~~~~~~

- **Fit in the laboratory and ingest the weights.** That is how the old
  methods arrived, and the weights rule forbids it for anything that saw GO
  labels. Rejected.
- **Cross-fit inside 211->227.** It carries the period's trends into the
  method: card 10 was voided for that. Rejected.
- **A post-235 window as the decisive read.** None exists yet, and the author
  chose 227->230. Rejected.

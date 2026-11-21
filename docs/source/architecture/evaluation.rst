CAFA Evaluation Protocol
========================

.. contents:: On this page
   :local:
   :depth: 2

PROTEA implements the evaluation protocol used in the CAFA5 (Critical
Assessment of protein Function Annotation) challenge. This page explains the
protocol, the NK/LK/PK classification, and how to run an evaluation end-to-end
within PROTEA.

Background: the CAFA temporal holdout
--------------------------------------

CAFA evaluates protein function prediction by exploiting the growth of
experimental GO annotations over time:

- **t0.** An older annotation snapshot (the *reference* set). Methods may
  use these annotations as training signal.
- **t1.** A newer annotation snapshot (the *ground truth*). Proteins that
  gained new experimental GO annotations between t0 and t1 form the test set
  (the *delta*).

Only annotations with experimental evidence codes are considered
(EXP, IDA, IMP, IGI, IEP, IPI, and their ECO equivalents). Annotations with a
NOT qualifier (meaning the protein is *not* associated with that term) are
excluded, and their exclusion is propagated to all GO descendants through the
``is_a`` and ``part_of`` relationships.

Formal definition
-----------------

This section gives a rigorous statement of the NK/LK/PK partitioning. The
notation follows :cite:`cafa2013` and matches the reference implementation in
:mod:`protea.core.evaluation`.

Preliminaries
~~~~~~~~~~~~~

Let :math:`\mathcal{P}` denote the universe of UniProt proteins present in
PROTEA and let :math:`\mathcal{N} = \{\mathrm{F}, \mathrm{P}, \mathrm{C}\}`
denote the three GO namespaces (molecular function, biological process and
cellular component). A fixed GO snapshot :math:`\sigma` determines a set of
terms :math:`\mathcal{G}_\sigma` together with an aspect function
:math:`\alpha_\sigma: \mathcal{G}_\sigma \to \mathcal{N}` that assigns each
term to exactly one namespace.

Define the GO children relation under the subset of edges used by the
evaluation protocol (the ``is_a`` and ``part_of`` relationships only):

.. math::

   D_\sigma \;=\; \{(u, v) \in \mathcal{G}_\sigma \times \mathcal{G}_\sigma
      \;:\; v \text{ is a direct } \mathtt{is\_a} \text{ or }
      \mathtt{part\_of} \text{ child of } u \}.

The reflexive transitive closure of :math:`D_\sigma` yields, for every term
:math:`t`, its set of descendants

.. math::

   \mathrm{desc}_\sigma(t) \;=\; \{ v \in \mathcal{G}_\sigma
      \;:\; (t, v) \in D_\sigma^{+} \}.

Let :math:`\mathsf{Exp}` denote the set of experimental evidence codes

.. math::

   \mathsf{Exp} \;=\; \{\mathrm{EXP},\, \mathrm{IDA},\, \mathrm{IPI},\,
      \mathrm{IMP},\, \mathrm{IGI},\, \mathrm{IEP},\, \mathrm{TAS},\,
      \mathrm{IC}\}

together with their ECO equivalents (the mapping is enumerated in
``protea.core.evidence_codes``).

Annotation sets and NOT-propagation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A raw annotation set :math:`s` loaded into PROTEA is a set of tuples
:math:`(p, t, q, e) \in \mathcal{P} \times \mathcal{G}_\sigma \times Q \times E`
where :math:`q` is a qualifier (possibly containing ``NOT``) and :math:`e` an
evidence code. The *negative closure* of :math:`s` over two annotation sets
:math:`s_0, s_1` sharing the same snapshot :math:`\sigma` is

.. math::

   \mathsf{neg}(s_0, s_1) \;=\; \bigcup_{\substack{(p, t, q, e)\, \in\, s_0\, \cup\, s_1 \\ \texttt{NOT} \,\in\, q}}
       \; \{p\} \times \bigl(\{t\} \cup \mathrm{desc}_\sigma(t)\bigr).

That is: every NOT-qualified annotation for a protein propagates to all
descendants of the negated term through :math:`D_\sigma`, and the resulting
``(protein, term)`` pairs are excluded from both sides of the delta. This
matches the function :func:`protea.core.evaluation._build_negative_keys`.

The set of experimental, non-negated annotations of protein :math:`p` in
namespace :math:`n` at time :math:`t_i` is then

.. math::

   A_i(p, n) \;=\;
      \bigl\{\, g \in \mathcal{G}_\sigma
      \;:\; (p, g, q, e) \in s_i,
      \; e \in \mathsf{Exp},
      \; \texttt{NOT} \notin q,
      \; \alpha_\sigma(g) = n,
      \; (p, g) \notin \mathsf{neg}(s_0, s_1) \bigr\}.

The per-namespace delta is the standard set difference

.. math::

   \Delta(p, n) \;=\; A_1(p, n) \setminus A_0(p, n),

and a protein belongs to the *delta set*
:math:`\mathcal{P}_\Delta \subseteq \mathcal{P}` iff it gained at least one
annotation in at least one namespace:

.. math::

   \mathcal{P}_\Delta \;=\;
      \Bigl\{ p \in \mathcal{P} \;:\; \bigcup_{n \in \mathcal{N}} \Delta(p, n) \neq \emptyset \Bigr\}.

Partitioning
~~~~~~~~~~~~

The NK, LK and PK subsets are defined over :math:`(p, n)` pairs with
:math:`p \in \mathcal{P}_\Delta` and :math:`\Delta(p, n) \neq \emptyset`.
Let the indicator :math:`\kappa(p) \in \{0, 1\}` record whether the protein
had **any** experimental annotation at :math:`t_0`:

.. math::

   \kappa(p) \;=\; \mathbb{1}\!\Bigl[ \bigcup_{n' \in \mathcal{N}} A_0(p, n') \neq \emptyset \Bigr].

**No-Knowledge (NK):** a single per-protein category:

.. math::

   \mathrm{NK} \;=\;
      \bigl\{ (p, g) \;:\; p \in \mathcal{P}_\Delta,\;
         \kappa(p) = 0,\;
         g \in \textstyle\bigcup_{n \in \mathcal{N}} \Delta(p, n) \bigr\}.

**Limited-Knowledge (LK):** a per-``(protein, namespace)`` category:

.. math::

   \mathrm{LK} \;=\;
      \bigl\{ (p, g) \;:\; \kappa(p) = 1,\;
         \exists\, n \in \mathcal{N},\;
         A_0(p, n) = \emptyset,\;
         g \in \Delta(p, n) \bigr\}.

**Partial-Knowledge (PK):** also per-``(protein, namespace)``:

.. math::

   \mathrm{PK} \;=\;
      \bigl\{ (p, g) \;:\; \kappa(p) = 1,\;
         \exists\, n \in \mathcal{N},\;
         A_0(p, n) \neq \emptyset,\;
         g \in \Delta(p, n) \bigr\}.

The associated *known-term exclusion file* used by ``cafaeval`` via the
``-known`` flag on the PK pass is

.. math::

   \mathrm{PK}_{\text{known}} \;=\;
      \bigl\{ (p, g) \;:\; (p, n) \in \mathrm{PK}_\text{keys},\;
         g \in A_0(p, n) \bigr\},

where :math:`\mathrm{PK}_\text{keys} = \{(p, n) : \exists\, g,\,
(p, g) \in \mathrm{PK} \wedge \alpha_\sigma(g) = n\}`.

Properties
~~~~~~~~~~

The partition has three properties that make the protocol well-defined and
directly testable against the implementation.

**Disjointness of NK from LK and PK.**
A protein classified as NK satisfies :math:`\kappa(p) = 0`, which means
:math:`A_0(p, n) = \emptyset` for every namespace :math:`n`. The LK and PK
conditions both require :math:`\kappa(p) = 1`, so no protein appears in both
NK and LK ∪ PK. Formally:

.. math::

   \{p : (p, g) \in \mathrm{NK}\} \;\cap\;
   \{p : (p, g) \in \mathrm{LK} \cup \mathrm{PK}\} \;=\; \emptyset.

**LK / PK mutual exclusion per namespace.**
For a fixed namespace :math:`n`, the conditions :math:`A_0(p, n) = \emptyset`
and :math:`A_0(p, n) \neq \emptyset` are mutually exclusive. Therefore no
``(protein, namespace)`` pair can be simultaneously LK and PK.

**LK and PK are *not* mutually exclusive across namespaces.**
A protein with :math:`A_0(p, \mathrm{F}) \neq \emptyset` and
:math:`A_0(p, \mathrm{P}) = \emptyset` can gain new annotations in *both*
namespaces at :math:`t_1`; it then contributes PK pairs in :math:`\mathrm{F}`
and LK pairs in :math:`\mathrm{P}` simultaneously. This is not a bug of the
protocol: it reflects the per-``(protein, namespace)`` granularity that
distinguishes CAFA5 from earlier rounds.

**Relation to the implementation.**
The definitions above correspond line-by-line to the classification loop of
:func:`protea.core.evaluation.compute_evaluation_data`:

- :math:`A_i(p, n)` ← ``_load_experimental_annotations_by_ns``;
- :math:`\mathsf{neg}(s_0, s_1)` ← ``_build_negative_keys``;
- :math:`\kappa(p)` ← ``had_anything_old = bool(old_ns_map)``;
- the per-namespace ``delta_ns = new_ns - old_ns`` computes :math:`\Delta(p, n)`;
- the branches ``if not old_ns`` and ``else`` realise the LK / PK separation;
- :math:`\mathrm{PK}_{\text{known}}` is accumulated in the ``pk_known`` dict.

NK / LK / PK classification
-----------------------------

A key feature of CAFA5 is that test proteins are not treated uniformly.
Classification is determined **per (protein, namespace)**, where namespace is
one of Molecular Function (MFO), Biological Process (BPO), or Cellular
Component (CCO).

**NK. No-Knowledge.**
   The protein had **no** experimental annotations in **any** namespace at t0.
   All its new annotations across all namespaces form the NK ground truth.
   Evaluating NK targets tests a method's ability to make predictions from
   sequence alone, without any prior functional signal.

**LK. Limited-Knowledge.**
   The protein had experimental annotations in **some** namespaces at t0, but
   **not** in namespace S. It gained new annotations in S at t1. Those new
   annotations in S are the LK ground truth for that (protein, S) pair.
   Evaluating LK tests transfer across namespaces.

**PK. Partial-Knowledge.**
   The protein already had experimental annotations in namespace S at t0, and
   gained **additional** annotations in S at t1. Only the novel terms are
   ground truth; the old terms are collected in a ``pk_known_terms.tsv`` file
   and passed to ``cafaeval`` with the ``-known`` flag, which excludes them
   from scoring. This prevents credit for simply repeating prior annotations.

.. important::
   A single protein can be **LK in one namespace and PK in another
   simultaneously**. For example, a protein with MFO and BPO annotations at t0
   that gains new CCO and BPO annotations at t1 will be LK for CCO and PK for
   BPO.

Toy example
~~~~~~~~~~~

.. code-block:: text

   Protein P1 at t0:  MFO={GO:0003674}   BPO={}       CCO={}
   Protein P1 at t1:  MFO={GO:0003674}   BPO={GO:0008150}  CCO={GO:0005575}

   had_anything_old = True (had MFO)

   Namespace BPO: old_BPO={}  → LK (empty at t0, gained GO:0008150)
   Namespace CCO: old_CCO={}  → LK (empty at t0, gained GO:0005575)
   Namespace MFO: no new terms → not in test set for this namespace

   Protein P2 at t0:  BPO={GO:0006355}   (all others empty)
   Protein P2 at t1:  BPO={GO:0006355, GO:0045893}

   Namespace BPO: old_BPO={GO:0006355}  delta={GO:0045893}
     → PK ground truth = {GO:0045893}
     → pk_known = {GO:0006355}  (passed as -known)

   Protein P3 at t0:  (no annotations in any namespace)
   Protein P3 at t1:  MFO={GO:0003674}   BPO={GO:0008150}

   had_anything_old = False → NK
   NK ground truth = {GO:0003674, GO:0008150} (all new terms)

Evaluation flow in PROTEA
--------------------------

.. code-block:: text

   1. Load two GOA annotation sets (old = t0, new = t1).
   2. POST /annotations/evaluation-sets/generate
      → queues generate_evaluation_set job
      → computes delta and creates EvaluationSet row with stats
   3. Download delta-proteins.fasta (all NK+LK+PK sequences).
   4. POST /jobs  (compute_embeddings, query_set_id=...)
      → compute ESM-2 embeddings for delta proteins
   5. POST /embeddings/predict  (predict_go_terms, query_set_id=...)
      → run kNN GO transfer; creates a PredictionSet of candidates
   6. POST /jobs  (score_prediction_set, prediction_set_id=..., scoring_config_id=...)
      → scores each candidate once and stores the scores; creates the scored set
   7. POST /annotations/evaluation-sets/{id}/run  (prediction_set_id = the scored set)
      → queues run_cafa_evaluation job
      → runs cafaeval for NK, LK, PK over the window's terms of interest;
        creates EvaluationResult
   8. View results in the Evaluation UI or download artifacts.zip.

The ``cafaeval`` command equivalent, on the files ``artifacts.zip`` holds, with
the pivot's OBO and the IA table the run was given:

.. code-block:: bash

   python -m cafaeval go-basic.obo predictions/ gt_NK.tsv -ia ia.tsv \
     -toi terms_of_interest.txt -prop fill -norm cafa -no_orphans -th_step 0.01 -out_dir results/NK
   python -m cafaeval go-basic.obo predictions/ gt_LK.tsv -ia ia.tsv \
     -toi terms_of_interest.txt -prop fill -norm cafa -no_orphans -th_step 0.01 -out_dir results/LK
   python -m cafaeval go-basic.obo predictions/ gt_PK.tsv -ia ia.tsv \
     -toi terms_of_interest.txt -prop fill -norm cafa -no_orphans -th_step 0.01 \
     -known pk_known_terms.tsv -out_dir results/PK

Where each target sits: the window's placement
----------------------------------------------

Results are reported per stratum (:mod:`protea.core.strata`), and two of the
axes describe a target's sequence rather than a method: its length, and its
identity to the closest protein whose function had been measured when the
window opened. ``place_window_targets`` reads both once per window and stores
them (``window_placement`` declares the placement, ``window_placement_target``
holds one row per target), so two methods evaluated on one window place every
protein in the same band.

- **Length** is the length of the target's sequence in the full photo of the
  opening cut (ADR-D59).
- **Identity** is to the closest OTHER protein of that photo with a truth row
  (the thirteen LAFA codes). An identical sequence under another accession
  places the target at 100, through the membership table (ADR-D61, decision
  3). Otherwise the best hit of the cut's homology table among the truth
  members' sequences decides: highest bit score, then identity, e-value and
  md5. With neither, the target is in the ``none`` band, a population and not
  a missing value.
- The bit score ranks first because it grows with the length of the alignment.
  The highest identity alone would let a short local alignment at 100 % place
  a protein among the near-identical ones.
- Identity is MMseqs2's ``fident`` as it was printed, three decimals, so the
  percentage is rounded to one decimal. Stored as float32, 0.300 reads
  0.30000001, which would otherwise cross the edge of the twilight band.
- No threshold is applied, neither by the homology table (ADR-D61, decision 4)
  nor here.

Every reader of the two axes reads the placement: ``stratify_evaluation``, a
paired comparison restricted to a stratum, and the two stratum pages. A window
with no placement is refused with the operation to run. Only the three
retrieval axes (donor evidence, taxonomy and propagation) read a method's own
neighbours, and only when they are asked for; they describe that method's
retrieval and are crossed with the homology band like any other pair of axes.

Data model
----------

``EvaluationSet``
   Stores the (old\_annotation\_set\_id, new\_annotation\_set\_id) pair and a
   JSONB ``stats`` dict with delta/NK/LK/PK protein and annotation counts.
   Created by ``generate_evaluation_set``.

   Two fields make it a window of a frame (ADR-D60, decision 1):

   - ``window_role``: ADR-D40's ``valid`` and ``test``, and the three roles of a
     frame, ``training`` (its gains may be fitted on), ``validation`` (a method
     instance is selected and read on it, never fitted on it) and
     ``test_spent`` (a test window whose single read has happened).
   - ``truth_rules``: the rules the build applied, as ``profile``,
     ``toi_rule``, ``not_codes`` and ``closure``. They are read from the path
     the build took and the graphs it resolved, never copied from the payload:
     when both corpora and every graph are one snapshot, the same-snapshot path
     reads none of the three payload rules, and the row says so. The closure
     says how the graphs relate to the window's own ends (``per_side``,
     ``t0_graph``, ``t1_graph``), which is the form that compares across cuts.
     Rows built before the column hold NULL and read as legacy.

   A payload can name a rule set instead of spelling the rules out.
   ``truth_rule_set: "october-headline-frame"`` is the author's choice of
   2026-11-15 for the four training windows, the rules of the October headline
   window ``ea0f4334``: ``projected``, ``live_in_both_ends``, ``experimental``,
   and both sides closed under the graph of t0, which is also the pivot. A build
   that would apply anything else is refused before the delta is computed.
   :func:`protea.core.window_declarations.common_truth_rules` refuses a list of
   windows whose stored rules differ, or cannot be compared at all.

``EvaluationResult``
   Stores per-setting (NK/LK/PK) and per-namespace (MFO/BPO/CCO) metrics:
   Fmax, precision, recall, τ (threshold), and coverage. Created by
   ``run_cafa_evaluation``. Multiple ``EvaluationResult`` rows can exist per
   ``EvaluationSet``, one per (prediction\_set, run).

See :doc:`../reference/infrastructure` for the full ORM schema.

Benchmark: PROTEA vs external tools
-------------------------------------

PROTEA's headline result lives in one place. The sealed board (``f_micro_w``
= 0.40765 on the leakage-free v227 to v230 frame, first in seven of the nine
NK/LK/PK by BPO/MFO/CCO cells) is stated once in :doc:`/results`. This page is
the home of the *protocol* that produces the board, not of the numbers
themselves.

The earlier Fmax board on the superseded GOA 220 to 229 window (ESM-C 300M
embeddings plus a three-generation LightGBM re-ranker), together with its
external-tool comparison and the quantified data-leakage analysis, is retained
verbatim under a superseded banner in :doc:`/historical/pre-v227`. Those
figures were never the sealed board and must not be quoted as a current
result.

**Why an external-tool comparison needs a temporal caveat.** Any tool scored
against its *current* reference database has access to functional knowledge
published after t0, and that knowledge is part of the ground truth. PROTEA
enforces temporal integrity by design: the reference set is frozen at t0, the
ground truth is the delta gained by t1, and every snapshot is versioned in the
database. Pannzer2, InterProScan, and eggNOG-mapper cannot be pinned to a
historical release (the Pannzer2 web server offers no version selection,
eggNOG publishes no historical orthology snapshots, and InterProScan uses the
latest InterPro release at run time), so their scores are an optimistic upper
bound under data leakage rather than a fair comparison. The exact
``(protein, GO term)`` match rates that quantify that leakage are preserved
with the superseded board in :doc:`/historical/pre-v227`.

Evaluating external tools
~~~~~~~~~~~~~~~~~~~~~~~~~~

External tools can be evaluated against the same ground truth using
``scripts/evaluate_external_tool.py``:

.. code-block:: bash

   poetry run python scripts/evaluate_external_tool.py \
       --evaluation-set-id <uuid> \
       --tool emapper \
       --input /path/to/annotations.emapper.annotations

   poetry run python scripts/evaluate_external_tool.py \
       --evaluation-set-id <uuid> \
       --tool pannzer2 \
       --input /path/to/anno.out

Supported formats: ``emapper``, ``pannzer2``, ``interproscan``, ``blast``.

.. _eval-band-registry:

.. rubric:: Per-band canonical (ontology snapshot, IA) registry

A *band* is a GOA evaluation window (for example ``v226`` or ``v227``). Each
band binds two derived artefacts that are pinned rather than free-floated:

1. An ``OntologySnapshot`` identified by its ``obo_version``. The snapshot
   governs True-Path propagation, the term universe, and orphan handling.
   Every cell in the band scores against the same snapshot.
2. An Information Accretion (IA) artefact. The IA weights each GO term by its
   information content on the t0 corpus of that band. A table this platform
   computed is identified by its content sha256, the
   ``InformationAccretionSet.content_sha256`` its row records
   (``Band.ia_sha256s``): the row id is a uuid4 minted by the database that
   computed it, so the same table rebuilt in another universe keeps its digest
   and gets another id. A table supplied from outside is identified by its
   file name (``Band.ia_tokens``).

The registry is ``protea.core.band_registry.BANDS``. Authoritative pairs:

.. list-table::
   :header-rows: 1
   :widths: 10 28 28 20

   * - Band
     - GOA t0
     - Canonical ``obo_version``
     - Canonical IA
   * - v226
     - goa 226 (2025-05-03)
     - ``releases/2025-03-16``
     - ``IA_cafa6.tsv``
   * - v211
     - goa 211 (2022-09-16)
     - ``releases/2022-07-01``
     - the production row ``80bbc9e4``, by set id (legacy)
   * - v227
     - goa 227 (2025-09-04)
     - ``releases/2025-07-22``
     - ``IA.tsv`` / ``IA-swissprot-exp-v227.txt``; the production row
       ``1aaff913``, by set id (legacy)

No computed table is pinned by content yet. The two production tables were
pinned by the set id of their rows before the digest was the identity; those
ids stay in ``Band.legacy_ia_set_ids`` so the evaluations that pinned them keep
resolving, and nothing new is added there. When a run pins its IA by
``information_accretion_set_id``, the guard reads that row's digest and checks
the band by it; the set id still resolves a legacy row.

Adding a new band requires one new ``Band`` row in ``BANDS``. No
``obo_version`` and no IA identity may be shared by two bands; the CI guard
(``scripts/check_band_registry.py``, wired into ``lint.yml``) enforces this.
No band may open after the mark (227): a ``Band`` named for a later release
cannot be built.

**Why a snapshot/IA mismatch inflates a phantom gap.** If a cell declared for
one band is scored with the snapshot or IA of another band, the comparison
measures artefact drift rather than prediction quality. A cross-band snapshot
changes the propagation closure and the term universe; a cross-band IA
reweights ``f_micro_w`` against a foreign corpus. The ``v226`` ``IA_cafa6.tsv``
and the ``v227`` ``IA.tsv`` disagree by up to 14.6 on shared terms. The
registry fix binds both artefacts to the band and rejects any mix.

**Dispatching a banded evaluation.** Pass the band name in the
``run_cafa_evaluation`` payload and supply the canonical IA for that band:

.. code-block:: json

   {
     "evaluation_set_id": "<eval set>",
     "prediction_set_id": "<pred set>",
     "band": "v227",
     "ia_file": "/path/to/lafa_t0_Sep_2025/IA.tsv"
   }

The operation resolves the pivot snapshot from the ``EvaluationSet``, verifies
its ``obo_version`` is canonical for the declared band, verifies the IA token,
emits ``run_cafa_evaluation.band_verified``, and only then calls ``cafaeval``.
A cross-band cell raises ``BandMismatchError`` and the job fails immediately.

.. _eval-lafa-parity:

.. rubric:: LAFA evaluation parity

PROTEA's ``run_cafa_evaluation`` operation uses the same ``cafaeval`` fork
binary as LAFA (CAFA_forever). When the same prediction is scored on both
sides, the headline metric must agree within LAFA's 3-decimal rounding
(epsilon about 5e-4). This section documents the alignment.

**Headline metric.** Both pipelines report ``f_micro_w``: the IA-weighted
micro-averaged F-measure, taken per namespace (BPO / CCO / MFO) at the
threshold that maximises it. LAFA reads it from column 31 of
``evaluation_best_f_micro_w.tsv``; PROTEA reads it from
``dfs_best["f_micro_w"]`` in ``parse_results``.

**cafaeval flag alignment.** The table below lists every flag where LAFA and
PROTEA were previously misaligned and the current state after the fix
(PRs #599, #601):

.. list-table::
   :header-rows: 1
   :widths: 20 15 20 30

   * - Flag
     - LAFA
     - PROTEA (after fix)
     - Impact
   * - ``th_step``
     - ``0.01`` (default)
     - ``0.01`` (was ``0.001``)
     - Dominant gap: finer grid inflated ``f_micro_w`` by up to +0.0144
   * - ``max_terms``
     - unlimited (default)
     - ``None`` (was ``500``)
     - Inert for KNN-style predictions; removed for mechanical identity
   * - ``ia``
     - t0 ``IA.tsv``
     - payload ``ia_file``
     - Must point at the same IA artefact as LAFA used for the band
   * - ``toi``
     - ``groundtruth_terms_of_interest.txt``
     - the window's terms, by its gain rule (a request has not named them since 2026-11-20)
     - LAFA's frame derives LAFA's own terms (:doc:`lafa-frame`); the residual up to 0.004 on MFO was the pivot's terms
   * - ``prop``
     - ``fill``
     - ``fill``
     - Already matched
   * - ``norm``
     - ``cafa``
     - ``cafa``
     - Already matched
   * - ``no_orphans``
     - on
     - on
     - Already matched

**How to run a LAFA-comparable evaluation.** In the ``run_cafa_evaluation``
payload, leave ``th_step`` and ``max_terms`` at their defaults (``0.01`` and
``None``) and name the IA table LAFA's frame uses. The terms of interest and
the population are the window's: a window of LAFA's frame derives LAFA's own
terms (:doc:`lafa-frame`), and every evaluation scores all the window's targets,
an unpredicted one as a failure, with coverage beside the number.

**Persisted metrics.** ``run_cafa_evaluation`` persists, per aspect, the
IA-weighted ``f_micro_w`` (headline), ``fmax_w``, ``f_micro``, and the
weighted micro ``precision_w`` / ``recall_w`` / ``coverage_w_at_tau``. The
last is the coverage at the threshold ``f_micro_w`` was reported at, cafaeval's
``cov_w`` and the coverage LAFA prints beside it. ``coverage_w`` is the largest
coverage over every threshold, cafaeval's ``cov_max``, as ``coverage`` is for
the unweighted ``coverage_at_tau``; results made before 2026-11-21 carry only
``coverage_w``. Each cell also records its size in each variant, ``n_targets``
and ``n_targets_w``: the proteins with ground truth among its terms of
interest, and those with positive weighted ground truth. They are the
denominators of the cell and of its coverage, counted from the rows of the
per-protein grid, so the record and the file agree. ``n_proteins`` is
cafaeval's ``n``, the proteins with a prediction at the reported threshold, and
it moves with the threshold. The ``_w``
keys appear only when a real IA file was supplied; under the uniform IC=1
fallback they are omitted. The ``/v1/benchmark/matrix`` endpoint ranks every
cell by ``f_micro_w`` (falling back to ``fmax`` for legacy IC=1 rows, flagged
via ``primary_metric``) and exposes a ``per_task`` mean and 95% CI.

**Where each figure was read on the threshold grid.** cafaeval reports each F at
the threshold where its curve peaks, on a grid of step ``th_step``. Each cell
records, per F variant, that threshold and whether it is an edge:
``maxima.f_micro_w.tau`` is the threshold of the headline, and
``maxima.f_micro_w.edge`` is ``"top"`` when the curve peaks at the highest
threshold that kept a prediction, ``"bottom"`` at the lowest, and null for an
interior maximum. A figure at an edge is not a maximum and no verdict is read
from it (ADR-D55, section 6); the window page marks it beside the figure and
says so in its tooltip. The cell's own ``tau`` is the threshold of the
unweighted ``fmax``, not of the headline: on 211 -> 227, LK's cellular component
reads 0.97 there and 0.99 for ``f_micro_w``. How the scores were read before
the grid (``results.reading``): ``score_reading``, ``raw`` (LAFA's reading) or
``percentile_rank`` (ADR-D55's comparison reading), and the grid's ``th_step``.
Two results of one scored set can differ in these alone, and the window page
names each result's reading; a result written before 2026-11-21 takes it from
the payload of the job that evaluated it.

**The files behind each figure.** A result records every file the run wrote:
``results.artifacts.keys`` lists their keys in the artifact store, and since
2026-11-21 ``results.artifacts.sha256`` holds the digest of each file as it was
uploaded. A cell's ``f_micro_w`` and ``cov_w`` are read from its category's
``evaluation_best_f_micro_w.tsv``, and its size from the category's
``per_protein_grid.parquet``. ``GET
/annotations/evaluation-sets/{id}/results/{rid}/artifacts/{path}`` serves one
recorded file by itself, with the recorded digest in the ``X-Recorded-SHA256``
header, so whoever downloads it can check that the bytes are the ones the
figures were read from. The window page names the file and the digest behind
each cell. Results made before 2026-11-21 record the keys only.

**Obsolete metrics.** Unweighted ``fmax`` / ``f_micro`` (equal weight on all
GO terms) are superseded by IA-weighted ``f_micro_w``; they are kept for
history but are not LAFA-comparable. Numbers from the v226 evaluation window
are also kept for history but should not be compared head-to-head with LAFA
v227 results (different band, different ground truth shape).

Checked against a second implementation
---------------------------------------

The evaluator is checked against the lab's: ``sfjepa.eval.cafa.evaluate_cell``
in the thesis' lab repository, a separate call of ``cafaeval-protea`` with
LAFA's flags, written for the lab's own campaigns. Both pin ``cafaeval-protea``
203af0a, the commit that names each NK and LK per-protein row by the truth
protein it came from.

The check reads one evaluation result back through the API: the files the run
stored (``artifacts.zip``), the information accretion table it was given, and
the ontologies of the window's pivot and two ends. It evaluates every cell with
the lab's evaluator on those files, and compares, protein by protein:

- the IA-weighted true-positive and predicted mass at every threshold, and the
  truth mass. The platform stores the per-threshold grid in float32 and the
  scalars in float64 (the contract in
  ``protea/core/operations/_paired_panels_artifact.py``), so the grid is
  compared with the lab's float64 masses cast to float32. That is equality
  when both computed the same values;
- each protein's masses at the cell's chosen threshold, in float64;
- the published ``f_micro_w``, which the platform stores to four decimals.

It also asks, input by input, whether a reader outside could rerun every cell
from the public API alone. On the development store, for the ESM-2 650M
nearest-neighbour set at cut 211 scored with ``embedding_only``, on the window
211 -> 227 of LAFA's frame:

.. literalinclude:: ../_data/lab-evaluator-parity-211-227.json
   :language: json

Every cell agrees, protein by protein.

Two cells, PK's biological process and cellular component, are compared at
the chosen threshold only. When the record was made, the platform did not store
their grid, because its weighted and unweighted branches score different
proteins: 38 and 4 PK proteins whose remaining truth carries no information
accretion. The grid held one row per protein for both variants, so it refused
those namespaces and recorded the refusal (``per_protein_grid_namespace_dropped``,
``variant_populations_differ``). Since 2026-11-21 each variant has its own
population in the grid. The rows are the unweighted population, a protein
outside the weighted one carries zeros in its weighted columns, and every
reader of the weighted variant keeps the rows with positive weighted ground
truth, so evaluations made since then store both cells.

Every input has a door:

- The CAFA export restricted to the window, and the three ground-truth
  downloads, are byte-identical to the files the run read.
- The known-terms download is a superset of the PK cell's known terms. Its
  extra rows lie in aspects where the protein has no PK truth, and it gives
  the same PK numbers.
- The terms of interest are the window's rule applied to the two ends'
  ontologies.

The record names the platform's revision, the lab's, and the
``cafaeval-protea`` commit on both sides. The check is repeated whenever the
evaluator or that commit changes.

Implementation reference
-------------------------

- Core logic: :mod:`protea.core.evaluation` (``EvaluationData``,
  ``compute_evaluation_data``)
- Band registry: ``protea/core/band_registry.py`` (``BANDS``,
  ``assert_band_consistency``); CI guard ``scripts/check_band_registry.py``
- Operations: :mod:`protea.core.operations.generate_evaluation_set`,
  :mod:`protea.core.operations.run_cafa_evaluation`
- API router: ``protea/api/routers/annotations.py`` (download endpoints,
  generate and run routes)

.. seealso::

   - :doc:`/results`: the sealed board (``f_micro_w`` = 0.40765 on the
     v227 to v230 frame) produced by following this protocol.
   - :doc:`/historical/pre-v227`: the superseded GOA 220 to 229 Fmax figures,
     retained for provenance only.
   - :doc:`/operate/reproduce-the-sealed-board`: the ordered path that reproduces the
     sealed board.
   - :doc:`operations`: the ``generate_evaluation_set`` and
     ``run_cafa_evaluation`` operations that implement the protocol live.
     Booster training has moved out-of-tree to ``protea-reranker-lab``
     and is registered through ``POST /reranker-models/import``; see
     :ref:`Register a reranker from protea-reranker-lab
     <howto-register-reranker>`.

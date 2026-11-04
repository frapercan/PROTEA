ADR-D55: Evaluated explorations, one unit for a hypothesis, its rules, its arms, its report and its review
=========================================================================================================

:Status: Accepted in principle by the author on 2026-11-04 (the lab's decision D27); the details are open for review
:Date: 2026-11-04
:Author: Francisco Miguel Pérez Canales
:Phase: platform consolidation (the lab's research enters the platform)
:Depends on: ADR-D51 (per-residue codes), ADR-D53 (lab method runs), ADR-D54 (model weights registry)

Context: results live outside the platform, and nothing says what a number compares
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The campaign has produced hundreds of numbers, and most of them live outside PROTEA:
lab caches, measurement files and copies on two machines and an HPC cluster. Their
provenance is unclear, so nobody can delete them safely. The author put it plainly on
2026-11-04: "No queremos que nos pase como la otra vez... terminamos con un montón de datos
fuera de la plataforma que no podemos borrar. Lo queremos todo dentro." Then: "Siempre todo
dentro de la plataforma. Menos el entrenamiento."

The same night showed four ways a number can mislead even when it is computed correctly:

* Two banks compared as a pair differed in six axes at once: length limit, pooling,
  normalisation, width, population and code revision. Their delta was attributable to none
  of them.
* A pull request asserted a property that had not been measured. Measured, it was false for
  sequences above 5,000 residues.
* A figure measured on one configuration was quoted for another: 64 active features per
  residue, where the store holds about 150.
* Heavy computation fell on the server: a neighbour sweep took the laptop to 94 C. A node
  also drifted from the server in interpreter, taxonomy dump and GPU driver.

PROTEA already has the parts of a sound measurement:

* evaluation sets with a frame identity;
* ``run_cafa_evaluation`` and ``stratify_evaluation``;
* ``compare_paired_panels``, with the method seal, a declared axis, a protein-level paired
  bootstrap and a floor of 30 proteins;
* the model weights registry;
* ``run_lab_method``.

What it lacks is the unit that ties them to a question. The experimentation graph
(``/instrument/graph``, 7,245 lines counted on 2026-11-04) and the benchmark table
(``/instrument/benchmark``, 2,613 lines) try to be that unit and are too complex. The
benchmark table also crowns a champion by maximum, which its own code calls the winner's
curse (``protea/api/metrics.py``).

Decision
~~~~~~~~

1. The exploration is the unit
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An exploration ties a falsifiable hypothesis to:

* one evaluation window;
* rules frozen with a fingerprint before any number is read;
* the arms and contrasts that answer it;
* a report generated from rows;
* a review in two steps;
* everything it uses and produces, each with an owner and an expiry date.

It has one of three kinds:

* ``one_axis``: two arms that differ only in the declared field;
* ``full_method``: two different methods, declared by the author and marked as not
  attributable to any component;
* ``instrument``: a check of the platform itself, against a predicate written beforehand.

Repeating an exploration on another window is another exploration, which records that it
builds on the first. An exploration is never deleted; it is archived.

2. Four tables, and no new column on an existing one
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* ``exploration``: its fields are:

  * number, title, question and hypothesis;
  * kind, state, and the archived mark with its reason;
  * the evaluation set, with a ``RESTRICT`` foreign key;
  * the rules (JSONB), their sha256, the database time they were frozen and who froze them;
  * a ``post_hoc`` flag, set when the candidate already had results in the window when the
    rules were frozen;
  * the grade and verdict, both computed by the server;
  * proposer, launcher and reviewer;
  * the accepted report's sha256 and the checklist version.

* ``exploration_link``: one row per linked object, as an exclusive arc. The target is a job,
  prediction set, evaluation result, model weights row, embedding config, query set,
  annotation set, information accretion set, external reference, another exploration, or a
  store key with its sha256.

  * Each link has a role (input, arm, result, strata, comparison, job, temporary, reference,
    supports or supersedes), the arm's name, whether the exploration owns the target, an
    expiry date (mandatory for a temporary target) and its size.
  * A link is created in the transaction that enqueues its step, never inside a payload,
    since payloads forbid extra keys.

* ``exploration_event``: an append-only log; a trigger refuses ``UPDATE`` and ``DELETE``.
  Each event records the database time, the actor and the actor's type (user, API key or
  server), and the event: created, launched, amendment, step failed, report, check answered,
  objection, reply, reviewed, decision, deployed, archived, marked for deletion, deleted.

* ``external_reference``: published figures by others, reusable across explorations. It
  holds the title, kind, URL and citation, when and by whom it was captured, a copy in the
  store with its sha256, and its evaluation profile (window, targets, evidence codes,
  protein-binding rule, ontology closure, metric and aggregation).

3. Four states and a mark
^^^^^^^^^^^^^^^^^^^^^^^^^

``draft``
    The rules can be edited. The server computes the receipt live as the arms are chosen,
    and refuses to launch while the arms differ in more than the declared axis.

``running``
    Launching freezes the rules: it records their canonical sha256 and the database time.
    After that, only amendments are possible, each as an event with a new fingerprint.

    * Each step is launched from the page, which enqueues the frozen payload and creates its
      link in the same transaction. The first version has no orchestrator.
    * The launcher must be a principal other than the proposer.

``reported``
    A light server operation generates the report from rows and records its sha256. The
    reviewer answers the fixed checklist and opens objections that point at a link or a
    cell. The reviewer's ``reviewed`` event enables the decision.

``decided``
    Accepted or rejected, with a text, by the author only, from a user session and never with
    an API key. Accepting fixes the report's fingerprint and protects everything it cites
    from deletion. Accepting means the measure and its reading hold, not that the hypothesis
    is true: an accepted "against" is a result.

Archived is a mark with a reason (abandoned, closed, superseded or withdrawn). Superseded
and withdrawn explorations stay visible, struck through. Archiving triggers retention.

4. The search is registered, not only its winner
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Choosing one of N configurations and declaring only the winner conditions the published
number on a search that leaves no trace. The frame's seal covers the recipe, not the
search that picked it. So selection and confirmation are separate explorations:

* A **selection** exploration declares its whole search space in its rules before
  launching: how many configurations, which seeds and in what order. Every arm it launches
  is linked to it, the losers included.
* A **confirmation** exploration is one arm against the reference, on another window. It
  links to the search with the role ``supports``, and its report states "chosen among N".

Today the platform can declare recipes (``embedding_config``) and populations (query sets
with a published sha256). This section adds a way to declare a search.

5. The receipt is computed, never written by hand
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It has five layers. It is computed when the arms are chosen and again in the report, and
what is not recorded appears as "not recorded".

* **Recipe:** the twelve identity fields of ``embedding_config``, compared field by field,
  and the rederivation of each id.
* **Population:** the number of donors in the materialised bank, the sha256 of their sorted
  accessions, and how many entered the corpus after the window's start
  (``first_admitted_release``).

  This layer is not hypothetical. On 2026-11-04, 30,128 proteins admitted to the corpus
  after release 211 carried annotations in the 211 set, none of them experimental. 29,922
  of their sequences are donors in bank ``f3a7fe16``, and 16,981 of the proteins were
  admitted at release 228 or later. A neighbour transfer on the window from 211 to 227
  therefore uses donors that are in the corpus only because of what happened later.

* **Frame:** the frame digest, written when each result is created rather than sealed
  afterwards.
* **Coverage:** queries and donors without a vector, and proteins scored per panel.
* **Execution:** for every job, the machine, interpreter, library versions, code revision
  and whether the tree was clean, dependency revisions, image by id, weights by sha256, and
  duration.

6. One comparison, one decision rule, a deeper grid
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**One comparison.** ``compare_paired_panels`` with the method seal. Its states per panel
are the only verdict vocabulary: candidate greater, base greater, equal with power, without
power, null unread, unresolvable. A plain mean of the nine panels never decides, and no
table crowns a maximum.

**One decision rule.** It is read on the panels the rules declared as deciding, with the
comparison's confidence adjusted for how many panels decide (Bonferroni through the
existing ``confidence`` field). The rules are frozen at launch, but a free reading of the
validation window from 211 to 227 is not blocked: an exploration frozen after such a
reading carries ``post_hoc`` and the grade ``exploratory``.

**Standard strata.** Every report crosses four axes (``protea/core/strata.py``), with n in
every cell:

* the nine panels: prior-knowledge category by aspect;
* homology: the identity of each query to its nearest annotated donor at the window's
  start, in five bands (no donor, up to 30 %, 30 to 60 %, 60 to 90 %, above 90 %), crossed
  with whether that donor's annotation is experimental. It is computed once per window, on
  the GPU node, with MMseqs2 pinned by commit, so that it does not move with the bank of
  each arm;
* length, in new bands: up to 512, 513 to 1,021, 1,022 to 3,000, 3,001 to 5,000, above
  5,000, and without a vector. The bands move from 512, 1,024 and 2,048 for three reasons:
  1,021 is where a 1,022-token limit actually truncates, since the limit counts the end
  token; 3,000 is the author's line of caution on long proteins; and 5,000 separates the
  longest proteins, where the attention route was measured to reorder neighbours. This is a
  change to the band registry and goes through its check;
* cells below the floor of 30 proteins are drawn hatched with their n, never hidden.

Strata not declared as deciding are described, and are never read as verdicts.

**The scale of the score is part of comparability.** Without a re-ranker or a
``ScoringConfig``, a neighbour prediction is scored ``1 - cosine_distance / 2``. Its floor is
then set by how anisotropic the bank is. The server session measured the cosine between
random pairs (4,000 per bank, seed 20261104) on 2026-11-04:

=========  ================================================  ============  ==========
Bank       Recipe                                            Random pair   Score floor
=========  ================================================  ============  ==========
f3a7fe16   ankh-base, full length, mean and max, raw           0.950        0.975
e7ba37da   ankh-base, cut at 1,022, mean, normalised           0.562        0.781
76792dc2   learned sparse code over e7ba37da, cut 211          0.138        0.569
9d72dc44   learned sparse code over e7ba37da, cut 227          0.199        0.600
=========  ================================================  ============  ==========

Two unrelated proteins already score 0.975 in the first bank. The useful range is 0.025 there
and 0.431 in the third, and on a threshold grid of step 0.01 the first bank has two steps.

The maximum it reports is not a maximum. On the panel of no prior knowledge in biological
process, the F curve of both neighbour arms rises to the last point of the grid, 0.99, where
99.7 % of the first bank's predictions sit at 0.98 or above. A difference between two such
readings compares a censored edge with another, not two maxima. So:

* The receipt gains a sixth layer, the scale. For each arm it records:

  * the random-pair similarity and the score floor it implies;
  * the share of predictions in the grid's top bin;
  * whether the F curve's maximum is interior.

* A censored maximum blocks a verdict.
* For a comparison, each prediction set's scores are read through their own empirical
  distribution, as a percentile rank within the set. The transform is monotone, so it
  preserves each arm's ordering and every threshold metric in principle. What it changes is
  where a uniform grid falls: on the arm's own quantiles, so the grid resolves the curve.
  The raw reading is kept as a secondary check.
* Two consequences of that reading are fixed here, because each one changes a number:

  * A metric at a fixed threshold becomes relative to the arm. Under the percentile
    reading, precision at 0.5 is precision at that arm's median score, not at a similarity
    of 0.5, so two arms compared at one threshold are compared at the same quantile of
    each.
  * Ties take the average of the ranks they span. Tied scores then read alike, and a
    threshold keeps or drops a tie group whole, as it did on the raw scale. Another tie
    method would be another reading, so the method is written into the receipt.

* The reading is part of the frame seal, like the threshold grid: a result read raw and one
  read by percentile rank never seal alike. ``raw`` is left out of the sealed material, so
  no seal minted before the reading existed moves.
* Both arms use the same scoring, and the receipt records it.

7. Review in two steps, and the grade
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The fixed checklist mixes automatic checks with human review:

* one axis, from the receipt (automatic, blocking);
* one frame, the same frame digest on every cited result (automatic, blocking);
* one population, the same query FASTA and donor fingerprint or a declared difference
  (automatic, blocking);
* rules before numbers (automatic; when it fails, the grade drops to exploratory);
* machine: no heavy step ran on the server (automatic, blocking);
* no leak: the weights' cut is not after the window's start, and the bank's corpus
  membership is declared (automatic, blocking);
* everything inside: each input is a platform row, registered weights or a cache declared
  with a manifest and an expiry (automatic, blocking);
* figures with an origin: free text admits no result figure, and every figure is drawn from
  the report's snapshot;
* the server session's review, and the author's acceptance.

The server computes the grade from the window's role and the checklist: thesis,
development, check or exploratory. A thesis figure comes from a new window read once.

8. Retention
^^^^^^^^^^^^

* A temporary target cannot be created without an expiry date.
* Deletion takes two steps. A collection marks the expired objects, which can still be
  restored; after a grace period it deletes them, with one event per object.
* It refuses to delete anything an accepted or deployed exploration cites, and anything
  registered as weights.
* A cache on the GPU node is deleted only after the author confirms.
* Links use ``RESTRICT`` because today identity is lost silently: deleting a window or a
  prediction set cascades to its evaluation results.

9. Training outside, everything else inside
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* A lab training reads an export of the platform: a content-addressed package with a
  manifest and an expiry.
* Its weights come back as a ``model_weights`` row with kind, sha256, cut and training
  record, and ``run_lab_method`` takes a ``model_weights_id`` and checks the sha256 before
  it mounts anything.
* This is the boundary the re-ranker already draws: ``export_research_dataset``, training
  in ``protea-reranker-lab``, and an import validated by ``feature_schema_sha``. It is
  reused here, not duplicated.

10. Where the work runs
^^^^^^^^^^^^^^^^^^^^^^^

* Heavy steps run on the GPU node, with one consumer per compute queue: batch predictions,
  evaluations, training exports and embeddings. The server keeps the database, the store,
  the write queues and light reads.
* Queries go through indexes:

  * filters on indexed columns come first;
  * ``EXISTS`` semi-joins replace joins that multiply rows;
  * only the columns needed are projected;
  * no whole table is streamed to a node when a disk cache or a small materialised set
    answers the question.

  The corpus-membership count above is two index probes and took 13 s.
* Nodes stay in parity with the server:

  * the same interpreter, libraries and taxonomy dump for every queue both could serve;
  * a node sync that covers every environment a worker uses;
  * driver and kernel upgrades applied on both machines deliberately, never through
    unattended upgrades.

11. Pages
^^^^^^^^^

* **The list** shows the active window and its two read counters (the historical one and
  the one since the clean start), the deployment pointer, and an inbox of what only the
  author can unblock. Explorations are grouped by question and never sorted by a figure.
* **The detail** has eight blocks:

  1. header and frame;
  2. frozen rules;
  3. receipt;
  4. result per panel;
  5. the deep grid of the nine panels by homology and length, with its marginals;
  6. references by others, shown as context and never on the same axis;
  7. review and decision;
  8. provenance, footprint and actions.

* **The window board** replaces the graph and the benchmark table: questions in rows, the
  current base (set by the last accepted exploration, never by the best value), and
  one-axis steps with their panel strips. An axis never measured on its own shows as a gap,
  not a zero.
* All text is in the five interface languages.

Consequences
~~~~~~~~~~~~

* The experimentation graph, the benchmark table and ``experiment_run`` become read only when
  the list ships, and are removed after the first accepted exploration. The findings
  written as markdown, ``scripts/run_experiments.py`` and ``scripts/bootstrap_fmax_ci.py``
  are retired.
* **Size budget.** The first version must take fewer lines than the graph it replaces, and
  is counted before it is accepted.
* **Prerequisites,** each its own change:

  * ``run_cafa_evaluation`` writes the frame digest when it creates a result;
  * ``job.started`` records the machine and interpreter;
  * the method seal classifies the keys of an ingested method and records its revisions;
  * ``run_lab_method`` takes ``model_weights_id``;
  * the artifact store gains ``list(prefix)``;
  * the band registry takes the new length bands;
  * an operation computes per-query identity per window;
  * evaluation offers the percentile-rank reading of section 6 (``score_reading``) and
    reports, per namespace and F variant, where the maximum sits and whether it is at an
    edge (``maxima``);
  * the residue code configuration ``22166300`` is repaired, together with the learned code
    configurations ``76792dc2`` and ``9d72dc44``. Their producers left ``chunk_size``,
    ``chunk_overlap`` and ``embedding_scale`` out of the recipe. The identity hook hashed
    the three as None and the INSERT stored the column defaults, so none of the three ids
    is the one its own row derives. The hook now resolves every omitted identity field
    before it hashes, and the producers state their recipes in full. ``22166300`` also
    records ``max_length`` 1022, which ``encode_residue_vocabulary`` never applies: its
    codes reach 8,259 residues. Its target, ``43193409``, is derived with an explicit null,
    and the repair moves its rows onto it.

* **Ordering.** The clean start closes the pipeline with four explorations:

  1. the instrument repeats;
  2. the bank's corpus membership at the window's start;
  3. the cut-211 recipe computed inside, against its own neighbours, as a full method;
  4. its promotion to the deployment cut.

  The value of the corpus as a donor bank is measured afterwards, as a ladder of donor
  policies, one field per rung.

Open points for review
~~~~~~~~~~~~~~~~~~~~~~

* **The author's signature.** Today a null principal counts as an administrator, and an API
  key passes the same role check as the author. Accepting will require a live user session
  with the author's id. Until then, the page warns that the two signatures are a convention.
* **The rule vocabulary.** It is closed and validated server side; its first version will be
  published with the migration.
* **Identity per window.** It costs one MMseqs2 search per window, about 18 minutes on twelve
  threads for the window from 211 to 227 (measured by the lab). It is cached, and its
  sha256 enters the frame.

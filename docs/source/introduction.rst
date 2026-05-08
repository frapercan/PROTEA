Introduction
============

Motivation
----------

The gap between the number of protein sequences deposited in public databases
and the number that carry experimentally verified functional annotations has
grown by several orders of magnitude over the past decade. UniProtKB/TrEMBL
stores more than 250 million unreviewed sequences, while UniProtKB/Swiss-Prot —
the manually curated subset — contains fewer than 600 000. Closing this gap by
wet-lab experiments is economically infeasible, and the community has
therefore invested in *automated* functional annotation: computational
pipelines that transfer Gene Ontology (GO) terms :cite:`cafa2013` from a small
set of well-characterised reference proteins to the much larger set of
unannotated targets.

Automated GO term prediction is now a mature research area with well-defined
benchmarks (the CAFA challenges :cite:`cafa2013,cafa2019,cafa2023`), a
reference scoring tool (``cafa-evaluator`` :cite:`cafaeval2023`), and a rich
catalogue of methods surveyed in :doc:`related_work`. Most open-source
pipelines, however, were released as *research artefacts* — single-purpose
scripts optimised for the paper that accompanies them. When integrated into a
production setting (a web server, a shared lab platform, a recurring batch
job) they expose a cluster of engineering issues that are rarely discussed in
the original publications:

1. **Reproducibility under versioned data.** Annotation transfer depends on
   three versioned inputs: the GO ontology release, the reference annotation
   set, and the embedding or similarity model. A prediction that is
   reproducible today often cannot be replayed one year later because none
   of these versions is recorded alongside the prediction.
2. **Temporal integrity of evaluation.** Benchmarking a tool against its own
   reference database — the default configuration for most open-source
   predictors — exposes the evaluation to data leakage: the tool has already
   seen the annotations that the benchmark treats as ground truth. This
   problem is acknowledged by the CAFA protocol but ignored by many method
   papers.
3. **Architectural coupling.** Pipelines that conflate database sessions,
   message-broker connections, orchestration logic, and domain computation
   in the same worker class are hard to extend, hard to unit-test, and
   fragile under the partial failures typical of long-running jobs against
   external APIs.

PROTEA is designed to address all three problems jointly, as a single system,
so that each concern is enforced by construction rather than by convention.

The legacy coupling problem
---------------------------

The **Protein Information System (PIS)** and **FANTASIA** codebases were
developed at the Andalusian Centre for Biomedical Bioinformatics (CBBIO) and
established foundational infrastructure for protein data ingestion and
functional annotation at scale. Both systems share a structural limitation
that directly motivates PROTEA: their workers conflate multiple concerns into
single classes.

A typical PIS/FANTASIA worker manages its own database session, connects
directly to the message broker, orchestrates task sequencing, *and* executes
domain logic — all in the same class. This coupling produces code that is
difficult to unit-test (because all infrastructure must be mocked at once),
hard to extend (because adding a new operation requires understanding the
entire execution context), and fragile under failure (because a queue
disconnect or database error can leave jobs in ambiguous states with no audit
trail). The consequences are well-documented in the enterprise software
literature :cite:`fowler2002patterns`.

Research questions
------------------

This thesis investigates whether the engineering issues above can be resolved
by architectural discipline *without* sacrificing prediction quality or
computational efficiency. It is organised around three research questions:

**RQ1 — Reproducible architecture.**
   Can a protein functional annotation pipeline be architected so that every
   prediction is *exactly reproducible* given the same input data, without
   sacrificing horizontal scalability on a single GPU and a modest number
   of CPU workers?

**RQ2 — Temporal integrity of evaluation.**
   To what extent does temporal data leakage inflate the apparent performance
   of homology-based GO prediction tools (Pannzer2, InterProScan,
   eggNOG-mapper) when they are benchmarked against their own current
   reference databases, and can a fair temporal holdout protocol be enforced
   at the data model level?

**RQ3 — Feature engineering on top of KNN.**
   Does a learned re-ranker that exploits classical pairwise alignment metrics
   (Needleman–Wunsch, Smith–Waterman) and taxonomic distance features on top
   of embedding-based KNN results consistently outperform the baseline
   embedding similarity score, across the full CAFA NK/LK/PK partitioning and
   all three GO namespaces?

Hypotheses
----------

The three questions are paired with three falsifiable hypotheses:

**H1.** A strict separation between an ``Operation`` protocol (pure domain
logic), a two-session job lifecycle (claim → execute), and a typed
infrastructure layer is sufficient to express every existing PIS/FANTASIA
workflow. No domain operation requires direct access to the message broker
or to the session-management layer.

**H2.** Open-source homology-based tools evaluated against their current
reference databases exhibit *measurable* exact-match overlap with the
ground-truth annotations of a temporal holdout. This overlap is quantifiable
and large enough to account for a significant fraction of their apparent
Fmax advantage over strictly temporal methods.

**H3.** A LightGBM binary classifier trained on 20 numeric and 3 categorical
features derived from alignment and taxonomy can outperform the baseline
cosine similarity score of an embedding-only pipeline across all 9 cells of
the NK/LK/PK × BPO/MFO/CCO grid.

Contributions
-------------

.. admonition:: Provisional numbers in C2 and C3
   :class: warning

   The specific figures cited below (62.4 % NK leakage in C2, "improves Fmax
   across all 9 cells" in C3) come from the pre-2026-04-10 experimental run
   and will be regenerated for the Zenodo deposit accompanying the thesis.
   The *direction* of the findings — large data-leakage overlap for Pannzer2,
   and the re-ranker surpassing the heuristic — is stable; only the exact
   values may move slightly when the chapter is re-rendered. See
   :doc:`/results` for the full provisional notice.

The thesis makes three contributions, one per research question:

**C1 — A reproducible platform for protein functional annotation** built on
a typed operation protocol, a two-session job lifecycle, a RabbitMQ job queue
with nine routed queues, and a PostgreSQL + pgvector data model that
versions every input (OBO release, annotation set source, embedding config)
by UUID. The platform is released as open source and runs end-to-end on a
single workstation with one GPU. PROTEA currently consolidates sixteen
registered operations covering ingestion, embedding, prediction, evaluation,
and re-ranking, as well as a one-click ``/annotate`` endpoint that takes a
FASTA upload and returns ranked GO predictions.

**C2 — A quantitative data-leakage analysis** of Pannzer2, InterProScan, and
eggNOG-mapper against a GOA 220 → 229 temporal holdout. The analysis measures
exact-match overlap between each tool's predictions and the ground-truth
annotations and shows that up to 62.4 % of the NK ground truth is already
present in the Pannzer2 reference database, fully explaining its apparent
advantage over temporally strict methods. The chapter :doc:`results` presents
the full numbers and discusses the interpretation.

**C3 — A temporal-holdout re-ranking pipeline** trained on 13 historical GOA
splits (releases 160 through 220) using alignment and taxonomy features on
top of ESM-C 300M KNN results. The final re-ranker (``v3``) is shown to
improve Fmax over the embedding-only baseline across all 9 evaluation cells
of the NK/LK/PK × BPO/MFO/CCO grid, while keeping the training signal
interpretable (per-feature importances are reported in :doc:`results`).

The PROTEA approach
-------------------

PROTEA realises these contributions through a deliberate separation of three
layers:

**Infrastructure layer** (``protea/infrastructure/``)
   Manages database sessions, connection factories, configuration loading, and
   the RabbitMQ transport. This layer knows nothing about domain logic.

**Execution layer** (``protea/workers/``)
   Orchestrates the job lifecycle: claiming a job, dispatching it to the
   correct operation, and recording the outcome. The ``BaseWorker`` uses two
   independent sessions by design — one to claim (``QUEUED → RUNNING``) and
   one to execute — ensuring that even a mid-execution crash leaves the
   database in a consistent, inspectable state.

**Domain layer** (``protea/core/``)
   Pure domain logic. Each ``Operation`` receives an open session and an
   ``emit`` callback; it returns an ``OperationResult``. Operations do not
   manage sessions, queues, or HTTP routing. They are individually testable
   with a mocked session and a noop emit function.

The three layers communicate only through well-defined interfaces. Chapter
:doc:`architecture/system_overview` describes the runtime stack, chapter
:doc:`architecture/job_lifecycle` documents the two-session lifecycle in
detail, and chapter :doc:`architecture/operations` lists every registered
operation together with its payload schema, execution flow, and side effects.

An incremental migration
------------------------

The goal of PROTEA is not a complete rewrite. PIS tables (``protein``,
``sequence``, ``protein_uniprot_metadata``) and FANTASIA computation
workflows are progressively migrated into the new architecture as new
capabilities are added. Each migration step must preserve or improve
computational efficiency and must not introduce regressions in the data model.
The discipline that makes this incremental evolution safe is the combination
of a typed operation protocol, an append-only audit log (``JobEvent``), and
database migrations managed by Alembic.

Current capabilities
---------------------

PROTEA currently provides sixteen registered operations spanning the full
protein functional annotation pipeline:

- **Data ingestion** — ``insert_proteins``, ``fetch_uniprot_metadata``,
  ``load_ontology_snapshot``, ``load_goa_annotations``,
  ``load_quickgo_annotations``.
- **Embedding computation** — ``compute_embeddings`` (coordinator),
  ``compute_embeddings_batch``, ``store_embeddings``.
- **GO term prediction** — ``predict_go_terms`` (coordinator),
  ``predict_go_terms_batch``, ``store_predictions``.
- **Evaluation** — ``generate_evaluation_set``, ``run_cafa_evaluation``.
- **Re-ranker dataset publishing** — ``export_research_dataset`` (LightGBM
  training itself lives in ``protea-reranker-lab``; PROTEA only produces
  the frozen train/eval parquets and serves the registered boosters).
- **Diagnostics** — ``ping``.

A scoring engine applies weighted formulas or trained LightGBM re-rankers to
prediction sets. The one-click ``/annotate`` endpoint automates the entire
workflow from FASTA upload to ranked GO term prediction. The full operation
catalogue is documented in :doc:`architecture/operations`, including the
four ephemeral consumer operations that fan out GPU and KNN work across
batch queues.

.. admonition:: Design principle
   :class: note

   New operations are added by implementing the ``Operation`` protocol and
   registering the instance at worker startup. No changes to the
   infrastructure or execution layers are required. This is the property
   that makes Hypothesis **H1** testable: if a new workflow requires
   modifications outside the domain layer, the architectural claim is
   falsified.

Thesis outline
--------------

The remainder of this thesis is organised as follows.

- :doc:`related_work` situates PROTEA against existing workflow engines, the
  CAFA evaluation tradition, homology- and embedding-based GO prediction
  methods, and the protein language models that supply its embedding
  backends.
- :doc:`architecture/index` describes the system architecture in five
  chapters: the runtime stack and requirements (``system_overview``), the
  two-session job lifecycle (``job_lifecycle``), the versioned data model
  (``data_model``), the operation catalogue (``operations``), and the
  evaluation protocol with formal NK/LK/PK definitions (``evaluation``).
- :doc:`results` presents the experimental evaluation: ablations over ``k``
  and the scoring function, the three re-ranker iterations, the external
  benchmark against Pannzer2 / InterProScan / eggNOG-mapper, and the
  quantitative data-leakage analysis.
- :doc:`appendix/index` contains installation and quickstart instructions,
  a configuration reference, how-to guides, operational runbooks, and
  architectural decision records (ADRs) for every non-obvious design choice.
- :doc:`references` lists every cited work.

Readers interested in architecture should start with
:doc:`architecture/system_overview`; readers interested in empirical results
should jump directly to :doc:`results`; readers interested in reproducing the
pipeline end-to-end should follow :doc:`appendix/installation_and_quickstart`
and then the how-to guides.

Related work
============

PROTEA sits at the intersection of three lines of research that have so far
evolved largely in isolation: (i) workflow and pipeline engineering for
large-scale bioinformatics, (ii) automated Gene Ontology term prediction from
sequence, and (iii) protein language models as general-purpose feature
extractors. This chapter situates the system against each of these bodies of
work and articulates the specific gap that motivates the platform.

Workflow systems for bioinformatics
-----------------------------------

General-purpose workflow engines such as Galaxy :cite:`galaxy2018`,
Snakemake :cite:`snakemake2012`, and Nextflow :cite:`nextflow2017` provide
reproducible pipeline execution, DAG-style task scheduling, and containerised
tool encapsulation. They target the *"compose existing tools"* workflow
pattern: each step wraps a CLI executable, inputs and outputs are files on
disk, and provenance is captured by pinning container digests.

This pattern is poorly suited to PROTEA's domain for three reasons. First,
UniProt ingestion and GO annotation are not one-shot file conversions: they
are stateful, long-running, partial-progress-tolerant processes that must
survive broker disconnects and resume without re-downloading. Second,
embedding computation and KNN prediction operate on shared in-memory state
(the reference cache) that cannot be serialised to files between steps
without prohibitive I/O overhead. Third, the consumers of the output are
interactive users through an HTTP API, not batch scripts; they need
sub-second status queries, structured event timelines, and the ability to
cancel in-flight jobs.

PROTEA is therefore architected as an application with an internal job queue,
not as a pipeline expressed in a DSL. The design draws on the *job queue*
pattern familiar from web applications (Celery, Sidekiq, RQ), adapted to the
scientific computing setting through a strict separation between domain
operations and infrastructure (see :doc:`architecture/system_overview`).

The PIS and FANTASIA precursors
-------------------------------

The **Protein Information System** (PIS) and **FANTASIA** codebases were
developed at CBBIO as end-to-end systems for protein data management and
functional annotation transfer. PIS established the ingestion side: a
PostgreSQL schema, a RabbitMQ-backed job queue, and Python workers that
paginate the UniProt REST API. FANTASIA extended this with GPU embedding
computation and KNN-based annotation transfer using ProtT5 and ESM models.

Both systems proved that the pipeline was tractable at UniProtKB/Swiss-Prot
scale (500 000+ reviewed proteins), but share a structural weakness: each
worker conflates a database session, the AMQP channel, orchestration logic,
and domain code in a single class. The consequences are well-known from
enterprise software :cite:`fowler2002patterns`: unit-testable pieces are hard
to isolate, new operations inherit boilerplate from an unrelated base class,
and partial failures (a broker reconnect mid-job) leave jobs in ambiguous
states because state transitions and business logic share the same session.
PROTEA is an explicit response: it keeps the data model, the queue topology,
and the empirical lessons from PIS/FANTASIA, but rebuilds the execution layer
around an ``Operation`` protocol that is pure domain logic and testable with
a mocked session.

Automated GO term prediction
----------------------------

The Critical Assessment of Functional Annotation (CAFA) challenges
:cite:`cafa2013,cafa2019,cafa2023` have established the reference benchmark
for automated GO term prediction: given a set of target proteins whose
experimental annotations are known at time ``t1`` but not at time ``t0``,
methods submit scored (protein, GO term) predictions and are evaluated
against the ``t1 − t0`` delta using Information Accretion (IA) weighting.

Published methods span three families:

- **Homology-based transfer.** BLAST-style search against a reference set,
  followed by annotation transfer through sequence identity thresholds. The
  canonical open-source tools are **Pannzer2** :cite:`pannzer2018`,
  **InterProScan** :cite:`interproscan2014` (domain-level signatures), and
  **eggNOG-mapper** :cite:`eggnog2021` (orthology groups).
- **Embedding-based transfer.** Replace BLAST similarity with cosine
  distance in the embedding space of a protein language model, then transfer
  annotations from nearest neighbours. **DeepGOPlus** :cite:`deepgoplus2020`
  combines a CNN over sequence with DIAMOND hits; **SPROF-GO**
  :cite:`sprofgo2023` uses ProtT5 embeddings and a learned aggregator.
- **Deep-learning classifiers.** End-to-end networks that predict per-GO-term
  probabilities directly from sequence or embeddings, e.g. **GoFormer** and
  related transformer-based models.

PROTEA falls into the embedding-based family but makes three deliberate
choices that distinguish it from prior work. First, KNN search is performed
in Python (numpy or FAISS :cite:`faiss2021`) rather than in the database, a
design decision motivated by the observed latency and memory behaviour of
pgvector on 500 000+ vectors (documented in ADR-001). Second, the reference
set is *frozen at t0* by construction; the ingestion pipeline records the
``OntologySnapshot`` OBO version and the ``AnnotationSet`` source version of
every reference annotation, so that a prediction produced today is exactly
reproducible against the same references tomorrow. Third, the LightGBM
re-ranker (trained offline in ``protea-reranker-lab`` and registered into
PROTEA via ``POST /reranker-models/import``) operates on hand-engineered
features on top of KNN results (Needleman–Wunsch and Smith–Waterman
alignment metrics via
``parasail`` :cite:`parasail2016`, taxonomic distance via ``ete3``
:cite:`ete32016`, and neighbour-aggregate signals) rather than on raw
embeddings, keeping the training signal interpretable.

Information-theoretic evaluation with ``cafaeval``
--------------------------------------------------

A recurring source of confusion in the GO prediction literature is the
difference between *naive* Fmax (averaged over predictions, independent of
term specificity) and the CAFA *weighted* Fmax that uses Information
Accretion :cite:`ia2013` to down-weight trivially correct predictions of root
or near-root terms. The open-source ``cafaeval`` package :cite:`cafaeval2023`
is the reference implementation of the CAFA scoring protocol, including IA
propagation, NK/LK/PK partitioning, and per-namespace Fmax reporting.

PROTEA delegates scoring entirely to ``cafaeval``: the ``run_cafa_evaluation``
operation writes CAFA-format TSVs, invokes ``cafaeval`` as a subprocess, and
parses the resulting per-namespace metrics into an ``EvaluationResult`` row.
This design decision avoids the temptation of reimplementing the scoring
logic: any bug in IA computation would invalidate every reported number, and
the literature has already converged on a single trusted implementation. See
:doc:`architecture/evaluation` for the full protocol.

Protein language models
-----------------------

The embedding backends supported by PROTEA are all publicly released,
pre-trained protein language models:

- **ProtT5** :cite:`prottrans2022`. Encoder/decoder transformer trained on
  UniRef50 with a T5 denoising objective. The ``prot_t5_xl_uniref50`` checkpoint
  produces 1024-dimensional residue embeddings, which are mean-pooled to one
  vector per sequence in the default PROTEA configuration.
- **ESM-2** :cite:`esm2_2023`. Decoder-only transformer from Meta AI trained
  on UniRef50 with a masked-language-modelling objective. Checkpoints of
  35 M, 150 M, 650 M, 3 B, and 15 B parameters are available; PROTEA
  benchmarks use the 650 M ``esm2_t33_650M_UR50D`` variant.
- **ESM-C** :cite:`esmc2024`. Compressed ESM family released by EvolutionaryScale
  in 2024. The 300 M checkpoint produces 960-dimensional embeddings at a
  fraction of the inference cost of ESM-2 650 M while preserving most of the
  downstream task performance. ESM-C is the default backend in PROTEA's
  benchmarks because of this favourable cost/accuracy trade-off.
- **Ankh.** Encoder/decoder T5-style protein model from
  ElnaggarLab, available as ``ankh-base`` and ``ankh-large`` checkpoints.
  Loaded via the same ``T5EncoderModel`` path as ProtT5, but the backend
  forces ``bfloat16`` on CUDA (FP16 overflows to ``NaN``) and tokenises
  char-by-char with ``is_split_into_words=True`` because Ankh's
  SentencePiece vocabulary maps literal spaces to ``<unk>``; the
  ``<AA2fold>`` prefix is never injected. Not yet used in the benchmark
  tables (see :doc:`/results`); included for parity with the upstream
  protein-PLM survey.

All four backends are wrapped by a single ``EmbeddingConfig`` row that
records the model checkpoint, layer selection, pooling strategy, and any
post-processing (L2 normalisation). This discipline is necessary for
reproducibility: a prediction run annotated with ``embedding_config_id`` can
always be replayed against the exact same model weights and pooling recipe.

Positioning PROTEA
------------------

Against this backdrop, PROTEA's contribution is not a new prediction
algorithm but a **reproducible, auditable, and extensible platform** that
turns the existing literature into an executable system. The three
architectural invariants (typed operations, two-session job lifecycle, and
versioned reference data) are specifically designed so that an
embedding-based GO predictor can be benchmarked against external tools
(Pannzer2, InterProScan, eggNOG-mapper) under a *fair* temporal holdout:
reference annotations frozen at ``t0``, ground truth computed from
``t1 − t0``, and every data source tagged with an immutable version.
Chapter :doc:`results` quantifies the effect of this discipline through a
data-leakage analysis of the external tools and a set of ablation studies
over the PROTEA pipeline itself.

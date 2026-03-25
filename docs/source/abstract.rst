Abstract
========

Large-scale protein analysis requires reliable pipelines capable of ingesting, enriching, and
organizing data from public repositories such as UniProt. Existing systems like the
**Protein Information System (PIS)** and **FANTASIA** have demonstrated the feasibility of
these workflows at scale, but their monolithic worker design conflates infrastructure concerns
(database sessions, queue management) with domain logic (sequence deduplication, metadata
enrichment), making the codebase difficult to extend, test, and maintain.

**PROTEA** is a platform designed to address this structural debt through an incremental
migration strategy. Rather than a complete rewrite, it introduces a clean separation of
concerns: a typed *Operation protocol* encapsulates domain logic, a *job queue* (RabbitMQ)
decouples HTTP ingestion from computation, and a *two-session worker pattern* ensures
robust, auditable state transitions. A React/Next.js frontend provides real-time visibility
into job progress through structured event logs.

The platform implements the full protein functional annotation pipeline: UniProt sequence
ingestion, GO ontology and annotation loading, GPU-accelerated embedding computation
(ESM-2, ESM3c, T5), KNN-based GO term prediction with optional pairwise alignment and
taxonomic features, CAFA-style temporal holdout evaluation (NK/LK/PK), and LightGBM
re-ranking. A scoring engine and one-click annotation endpoint make the system accessible
to researchers without machine-learning infrastructure expertise.

The platform is designed to accommodate continuous extension — new operations, new data
sources, new models — without architectural regression. Computational efficiency is preserved
at each migration step, with sequence deduplication by MD5 hash, cursor-based pagination,
and exponential backoff against upstream rate limits.

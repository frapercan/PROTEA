Quality Engineering
===================

PROTEA's quality strategy is multi-layered. Hard gates (CI workflows that
must be green before a PR can merge) sit alongside soft gates (the smell
budget, which ratchets complexity without blocking emergency fixes).
Both are complemented by structural patterns (refactoring slices, plugin
architecture) that keep the codebase intrinsically testable, and by
reproducibility invariants (SHA fingerprints, Alembic migrations, statistical
regression benchmarks) that make every experiment result traceable to the
exact code and data that produced it.

The inventory below covers every practice actually implemented in the
stack. Items are grouped by concern. Each entry carries a one- or
two-sentence description and a link to the canonical source: a file path,
an ADR number, a CI workflow name, or a slice identifier from the plan
store.

.. contents:: On this page
   :local:
   :depth: 2

.. toctree::
   :maxdepth: 1

   mutation-testing

Testing layers
--------------

Unit tests
~~~~~~~~~~

126 test modules and more than 2 200 test functions live under ``tests/``.
Every subpackage has a corresponding test file; the suite runs with
``poetry run pytest`` and requires no external services (database or broker
calls are mocked).

.. code-block:: text

   tests/
     test_core.py, test_api.py, test_base_worker.py, ...
     (126 test modules total)

Integration tests
~~~~~~~~~~~~~~~~~

Tests that need a real PostgreSQL instance are activated via the
``with-postgres`` pytest flag. The ``conftest.py`` session fixture spins up a
``pgvector/pgvector:pg16`` Docker container, enables the ``vector``
extension, and tears it down after the session. Key integration modules:

* ``tests/test_infrastructure.py`` (session, engine, app factory)
* ``tests/test_pair_feature_perf_equivalence.py`` (parallelised vs serial
  parity for the export pipeline, PR #421)
* ``tests/test_predict_go_terms.py`` and ``test_predict_go_terms_coverage.py``
* ``tests/test_auth_session_revocation.py``
* ``tests/test_predict_go_terms_from_interpro_pg.py``
* ``tests/test_datasets_and_reranker_import_smoke.py``

The integration workflow (``.github/workflows/integration.yml``) runs
pytest with the Postgres flag enabled on every PR.

End-to-end tests (Playwright)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Critical user flows are covered by Playwright specs in
``apps/web/e2e/flows/``. Flows include landing, jobs, annotate submission,
reranker, scoring, stack, maintenance, and support pages. Specs use
fully hermetic per-test API mocks (``e2e/flows/fixtures/mock-api.ts``)
so no live backend is required. CI workflow: ``.github/workflows/playwright.yml``.

Property tests (F6.2)
~~~~~~~~~~~~~~~~~~~~~

`Hypothesis <https://hypothesis.readthedocs.io/>`_-based tests in
``tests/property/`` exercise algorithmic invariants (output ranges,
ancestor max-monotonicity, sort order, parquet roundtrip) over
randomised inputs. Three modules are currently covered:

* ``tests/property/test_scoring_property.py``
* ``tests/property/test_contract_payloads_property.py``
* ``tests/property/test_parquet_roundtrip_property.py``

A fixed seed (``derandomize=True``) in ``tests/conftest.py`` makes runs
reproducible across CI workers.

Mutation tests (F6.3)
~~~~~~~~~~~~~~~~~~~~~

`Cosmic Ray <https://cosmic-ray.readthedocs.io/>`_ rewrites
``protea/core/scoring.py`` (and any PR-touched module in
``protea/core/``) one AST operator at a time and checks whether the
Hypothesis kill criterion catches each mutant. Configuration:
:file:`cosmic-ray.toml`. Reference baseline for ``scoring.py``:
227 mutants, 76.21 % mutation score (83.17 % effective after 19
type-hint equivalent mutants are excluded). Full description:
:doc:`mutation-testing`.

Boundary validation (T1.8)
~~~~~~~~~~~~~~~~~~~~~~~~~~

``tests/test_parquet_export_boundary.py`` and
``tests/test_contracts_invariants.py`` enforce the
producer-consumer invariant on the canonical feature column set: every
column declared in ``ALL_FEATURES`` must have an unconditional producer
wired to the ``CanonicalFeatureRegistry``, and the parquet exporter must
emit exactly that column set. Adding a column without a producer crashes
the export pipeline at the T1.8 invariant check before the job reaches
compute-intensive stages.

Cross-repo invariant tests (T1.7)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``tests/test_contracts_invariants.py`` additionally validates that the
``protea-contracts`` ABC surface (``Operation`` protocol, payload
dataclasses) is satisfied by all registered operations in
``build_operation_registry``. This prevents plugin repos
(``protea-runners``, ``protea-backends``) from shipping operations that
silently break the consumer interface.

Smoke tests
~~~~~~~~~~~

``scripts/smoke.sh`` probes a running stack (``$PROTEA_API_URL``,
default ``http://127.0.0.1:8000``): health endpoint, ping job dispatch,
and response validation. Runs in the Deploy E2E workflow after the
compose stack is brought up.

Reproducibility and statistical regression
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``scripts/bootstrap_fmax_ci.py`` runs a paired bootstrap (N = 10 000
iterations) comparing a champion prediction set against the KNN baseline.
Slice LB.3 established that six of six NK+LK confidence intervals are
strictly positive at 95 % on ``bench-v1-K5-v226-lineage``, providing a
publishable statistical claim for Chapter 6. The multi-seed binary
classifier recipe (3 independent seeds) produced ``NK+LK cafaeval 0.7291 +/- 0.0028``,
confirming the benchmark is not a one-seed artefact.

Code-smell budget
-----------------

``scripts/check_smells.py`` enforces four structural thresholds:

============  ===========
Threshold     Limit
============  ===========
File LOC      800
Class LOC     500
Method LOC    60
Parameter #   6
============  ===========

Enforcement uses a ratchet model: a baseline JSON records existing
offenders at their current size. A run fails only if a *new* offender
appears or an existing one grows larger. Removing an offender
silently shrinks the baseline when the ``write-baseline`` flag is passed. The CI lint job
runs the script as part of the ``make lint`` step; the gate name in CI
is "smell budget OK". The same script is distributed to all eight stack
repos for cross-repo consistency.

Lint and static analysis
------------------------

Python
~~~~~~

* **ruff** (``>=0.15.5``): fast linting and auto-fix, configured in
  ``[tool.ruff]`` in ``pyproject.toml``. Runs as a pre-commit hook and
  in the lint workflow (``.github/workflows/lint.yml``).
* **ruff format**: canonical formatter replacing black.
* **mypy** (``>=1.19.1``): type checking, configured in ``[tool.mypy]``
  in ``pyproject.toml``. Strict on new modules. Runs in the lint workflow.
* **bandit**: security static analysis at severity level ``high`` and
  confidence level ``high`` on ``protea/``. Configuration in
  ``[tool.bandit]`` in ``pyproject.toml``. Promoted to a blocking check
  (T5.7) in ``.github/workflows/security.yml``.
* **pip-audit**: dependency vulnerability scanning. Also a blocking check
  (T5.7) in the security workflow.

Frontend
~~~~~~~~

* **ESLint** (``apps/web/eslint.config.mjs``): Next.js / TypeScript
  rules on the ``apps/web/`` application.
* **tsc** (TypeScript compiler): type checking runs as part of
  ``npm run build`` in the Playwright and docs CI jobs.

Dataset naming convention (FARM-EXP.6)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The reranker-token-lint workflow (``.github/workflows/reranker-token-lint.yml``)
delegates to the shared linter in
``frapercan/agent-farm/.github/workflows/reranker-token-lint.yml``.
It blocks PRs that introduce shorthand reranker-recipe version tokens
in publishable prose under ``docs/`` and ``README.md``. Only the
nine canonical GOA snapshot tokens are allowed. CHANGELOG.md is
structurally excluded (per ADR D36 and FARM-EXP.6 decision).

OpenAPI spec drift
~~~~~~~~~~~~~~~~~~

``.github/workflows/openapi-drift.yml`` generates a fresh
``docs/openapi.json`` from the live FastAPI app and diffs it against the
committed copy. A non-zero diff fails the check "``docs/openapi.json``
matches code", preventing the API spec from silently diverging from the
implementation.

Pre-commit hook bundle
----------------------

The project ships a ``.pre-commit-config.yaml`` that installs the
following hooks:

* **ruff** and **ruff-format**: lint and format on every staged Python file.
* **trailing-whitespace**, **end-of-file-fixer**, **check-yaml**,
  **check-toml**, **check-added-large-files** (500 KB cap),
  **debug-statements**, **check-merge-conflict**: standard hygiene
  guards from ``pre-commit/pre-commit-hooks``.

Additional project-specific guards are enforced at multiple levels.

Em-dash guard
~~~~~~~~~~~~~

``scripts/check_no_em_dashes.py`` greps for em-dashes (both the ASCII
double-hyphen form and the Unicode character) after stripping
backtick-quoted spans. The hook is stricter than a simple string search:
it fires even when the pattern appears inside fenced code blocks, so CLI
flag documentation must use inline backticks rather than fenced bash
blocks. Prose in RST section underlines uses ``=``, ``~``, and ``^``
throughout this page for that reason.

Stash audit
~~~~~~~~~~~

``.github/workflows/stash-audit.yml`` runs ``git stash list`` on the
checked-out PR branch and fails if any stash entries exist. This
enforces the project-wide policy of using ``git restore`` plus commits
on a WIP branch instead of stash (eight violations tracked across three
sessions before the CI gate was added).

CI gates (GitHub Actions)
--------------------------

Required checks on ``develop``-targeted PRs:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Workflow file
     - What it checks
   * - ``test.yml``
     - pytest unit suite (Python 3.12, no DB required)
   * - ``integration.yml``
     - pytest with Postgres flag (Docker Postgres container)
   * - ``lint.yml``
     - ruff, ruff-format, mypy, check_smells.py
   * - ``security.yml``
     - pip-audit (blocking) + bandit (blocking)
   * - ``docs.yml``
     - Sphinx HTML build, zero warnings policy
   * - ``openapi-drift.yml``
     - ``docs/openapi.json`` matches live app
   * - ``playwright.yml``
     - Playwright critical user flows
   * - ``deploy-e2e.yml``
     - Build Dockerfile + compose smoke
   * - ``deploy-e2e-skip.yml``
     - Branch protection canary: fires when deploy paths are NOT changed,
       reports the same check name so the required check is always present
   * - ``mutation.yml``
     - Cosmic Ray on PR-touched core modules (informational, not blocking)
   * - ``reranker-token-lint.yml``
     - Dataset naming convention in docs and README prose
   * - ``stash-audit.yml``
     - No git stash entries in the checked-out branch
   * - ``auto-merge.yml``
     - Enables squash auto-merge once all required checks pass on
       non-draft PRs targeting develop

Refactoring patterns applied
-----------------------------

The following slices applied named refactoring patterns to reduce
complexity and improve testability. Each is traceable to an ADR or plan
slice.

**T-CONTEXTS (Introduce Parameter Object)**
    The large ``_KnnTransferRunner`` dict-builder was refactored to
    accept a single parameter object instead of 14 positional arguments
    (``protea/core/_knn_transfer_runner.py``, referenced in
    ``docs/source/reference/core.rst``).

**T2B.1 (FeatureRegistry / Strategy pattern)**
    ``CanonicalFeatureRegistry`` (``protea/core/features/registry.py``)
    registers named feature-compute callables as strategies. Both the
    parquet exporter (T2B.2) and the batch predictor (T2B.3) drive
    feature generation through the registry singleton, decoupling feature
    definition from call sites.

**T2B.3 (decompose ``_predict_batch``)**
    The monolithic batch prediction path was decomposed into collaborating
    objects, with feature generation driven through
    ``CanonicalFeatureRegistry`` rather than inline dispatch.

**T2B.4 (RerankerScorer as compositive class)**
    The re-ranker scoring logic was extracted as ``RerankerScorer``
    (``protea/core/operations/predict_go_terms/_reranker_scorer.py``),
    composed into ``PredictGOTermsBatchOperation`` rather than inherited
    as a mixin. This makes the scorer independently testable and
    swappable. See :doc:`/adr/D34-selective-rerank-resurrection`.

**T2B.5 (Method Object for 300+ LOC methods)**
    Long methods in the KNN transfer runner were extracted into
    ``_KnnTransferRunner`` (``protea/core/_knn_transfer_runner.py``) and
    supporting phase modules (``protea/core/_anc2vec_phases.py``,
    ``protea/core/_leaf_record_builder.py``). See
    :doc:`/adr/D31-t2b5-method-object-reframe`.

**T2B.6 (module split)**
    ``predict_go_terms`` and ``training_dump_helpers`` were split into
    focused sub-modules:
    ``protea/core/operations/predict_go_terms/`` (package with
    ``_batch_op.py``, ``_reranker_scorer.py``, ``_batch_op_reranker.py``,
    ``_common.py``) and ``protea/core/training_dump_helpers.py``.

**Plugin entry_points architecture**
    All eight stack repos (``protea-sources``, ``protea-runners``,
    ``protea-backends``, ``protea-contracts``, etc.) expose operations
    through Python ``[project.entry-points]`` declarations.
    ``build_operation_registry()``
    (``protea/core/operation_catalog.py``) resolves them at startup. This
    hexagonal-style boundary keeps the core independent of any specific
    annotation source, embedding backend, or runner implementation.

Reproducibility guardrails
---------------------------

SHA fingerprinting
~~~~~~~~~~~~~~~~~~

Every ``RerankerModel`` row stores a ``feature_schema_sha``: a
content-addressable fingerprint of the feature family and column order
used when the booster was trained. The inference path in
``protea/core/reranker.py`` refuses to score with a booster whose schema
SHA does not match the live pipeline, preventing silent feature drift.
Similarly, ``Dataset`` rows carry ``schema_sha`` and ``manifest_sha``.
These provenance fields are surfaced in the UI via the ``ProvenancePanel``
component on the reranker page (``apps/web/app/[locale]/reranker/page.tsx``).

Dataset naming convention
~~~~~~~~~~~~~~~~~~~~~~~~~

The canonical naming form (ADR D36) is
``bench-v1-K{K}-v{band}-lineage-{plm}``, where ``K`` is the KNN
neighbourhood size, ``band`` is the GOA temporal snapshot, and ``plm``
is the protein language model identifier. The reranker-token-lint
workflow enforces this pattern in prose; the field is stored in the
``Dataset.name`` column with a UNIQUE constraint.

Alembic versioned migrations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``alembic/versions/`` directory contains 56 migration scripts that
are the authoritative source of truth for the database schema. Schema
changes that affect parquet column layout increment the
``dataset_schema_sha`` version (T1.6 migration). The
``alembic upgrade head`` step is required before any operation that reads
or writes ORM models, and is part of the disaster recovery runbook
(:doc:`/runbooks/disaster-recovery`).

Operational guardrails
-----------------------

Pre-warm and serve-stale-on-error
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Slow aggregate endpoints (``/v1/proteins/stats``) use a background
cache with ``prewarm_all`` (``protea/api/routers/proteins_stats.py``)
and ``serve_stale_on_error=True``. On a transient DB error or a cold
miss, the last-known value is returned rather than blocking the UI.
This prevents a 30-second DISTINCT-over-JOIN query from timing out
ngrok connections on cache cold-starts.

SIGTERM handler and force-fail gating
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``protea/workers/shutdown.py`` registers a SIGTERM handler on worker
startup. When the deploy-keeper sends SIGTERM during a redeploy, the
handler waits for the current job to finish (up to a configurable grace
period), calls ``force_fail`` to mark any in-flight job as ``FAILED``,
and exits with code 143. This prevents orphaned ``RUNNING`` rows in the
``Job`` table.

Stale job reaper
~~~~~~~~~~~~~~~~

``protea/workers/stale_job_reaper.py`` periodically queries for ``Job``
rows that have been in ``RUNNING`` state beyond their expected
wall-clock budget and transitions them to ``FAILED``. This is the last
line of defence against jobs whose workers died without sending a final
state transition. Runbook: :doc:`/runbooks/stale-job-reaper`.

Database and ORM patterns
--------------------------

**3NF schema**
    The ORM models (``protea/infrastructure/orm/models/``) are in third
    normal form. Foreign keys, UNIQUE constraints, and partial indexes
    are declared in model metadata and materialised via Alembic migrations.

**ENUM case consistency**
    A pre-existing bug (``ExperimentRun`` status stored as lowercase in
    Postgres but the ORM ``Enum`` sending uppercase names) was caught and
    fixed in the FIX-EXP-RUN-ENUM slice. A similar latent shape was
    identified in ``Job.status``
    (``protea/infrastructure/orm/models/job.py``) and flagged for
    diagnosis before any ORM-level fix.

**Idempotent UNIQUE constraints**
    The ``EvaluationSet`` UNIQUE constraint (T-INFRA.EVAL-SET-UNIQUE)
    prevents duplicate ground-truth snapshots from being inserted by
    concurrent evaluation jobs. INSERT paths use ``ON CONFLICT DO NOTHING``
    or manager-level deduplication.

**Producer-consumer invariant (T1.8)**
    Described in the Testing section above. The invariant is also
    documented in ``tests/test_contracts_invariants.py`` with an explicit
    assertion message referencing the memory note
    ``project_canonical_feature_producer_consumer``.

**Two-session worker pattern**
    ``BaseWorker.handle_job`` uses two separate SQLAlchemy sessions: one
    to claim the job (``QUEUED -> RUNNING``), and a second to run it and
    record the outcome. This prevents a crashed execute session from
    rolling back the claim transition. See
    :doc:`/adr/002-two-session-worker-pattern`.

Architectural patterns
-----------------------

**Repository pattern**
    ORM model classes in ``protea/infrastructure/orm/models/`` and
    corresponding manager/query helpers implement the repository pattern:
    domain logic calls typed query helpers rather than constructing raw SQL.

**Operation registry**
    ``build_operation_registry()`` in ``protea/core/operation_catalog.py``
    is the single source of truth for which operations are available.
    Every ``POST /v1/jobs`` dispatch resolves the ``operation`` field
    through this registry. Ad-hoc endpoint calls outside the registry are
    a hard constraint violation per the project's hard-constraints
    document (``CLAUDE.md`` in the repo root).

**Event-driven architecture**
    ``protea.jobs``, ``protea.embeddings``, ``protea.predictions``,
    ``protea.training``, and ``protea.evaluations`` queues are powered by
    RabbitMQ and Pika consumers. Messages are durable, survive broker
    restarts, and support per-queue scaling via ``manage.sh scale``.

**Lifespan-managed services**
    ``protea/api/app.py`` uses FastAPI's lifespan handler
    (``_build_lifespan``) to open and close the session factory,
    initialise the operation registry, and call ``prewarm_all`` during
    startup. This ensures resources are released cleanly on shutdown and
    avoids global state initialised at import time.

**Plugin architecture via Python entry_points**
    The eight stack repos expose their operations through
    ``[project.entry-points]`` declarations. The PROTEA core does not
    import plugin code directly; it discovers it at runtime through
    ``importlib.metadata``.

Security
--------

**JWT + API key auth**
    ``protea/api/bearer.py`` implements JWT bearer authentication.
    ``assert_bearer_config()`` (called from ``create_app``) aborts startup
    if ``JWT_SECRET`` is missing, preventing silent anonymous access.
    API key snapshots store the ``role`` field so that the inference gate
    correctly applies viewer / annotator / administrator policies. A
    one-line bug where ``role`` was dropped from the snapshot (PR #504)
    was caught and fixed before any annotator credentials were issued in
    production. See :doc:`/adr/D06-authentication`.

**bandit + pip-audit**
    Both run as blocking checks in the security workflow
    (``.github/workflows/security.yml``, T5.7). bandit targets
    severity level ``high`` and confidence level ``high``; pip-audit
    scans the production dependency closure.

**md5 with usedforsecurity=False**
    Hash calls that use ``md5`` for caching (not cryptographic) purposes
    pass ``usedforsecurity=False``
    (``protea/core/reranker.py``, ``protea/core/_pair_feature_compute.py``).
    This satisfies bandit's ``B324`` rule without a ``# noqa`` suppression.

Coverage gates
--------------

**pytest-cov** is wired into the unit-test workflow
(``.github/workflows/test.yml``). Coverage is computed against
``protea/`` and emitted as ``coverage.xml`` for the GitHub Actions
summary. The current target is **80 % branch coverage** for
``protea/core`` and ``protea/api`` (the domain-critical layers); the
overall repo target is **70 % statement coverage**. Coverage
regressions on a PR diff are surfaced as a comment by the
``coverage-comment`` action but are advisory: a drop alone does not
block the merge. The hard requirement is the underlying test suite
passing; coverage is the visible side-channel that prevents silent
test-coverage erosion.

**Mutation score** (cosmic-ray) is a complementary signal: it answers
"are my tests actually killing bugs?" rather than "did my tests
execute the line?". The 76 % / 83 % effective baseline on
``protea/core/scoring.py`` (see :doc:`mutation-testing`) is the
quality bar; the workflow runs on every PR that touches
``protea/core/`` modules but is informational, never blocking.

Type checking with mypy
------------------------

The ``lint.yml`` workflow runs ``mypy --config-file pyproject.toml``
against the full ``protea/`` package. Strict-mode is enabled module-
by-module (``[tool.mypy.overrides]`` in ``pyproject.toml``);
new modules are added in strict mode as a default. SQLAlchemy 2.0
mapped types are typed via ``Mapped[...]`` annotations across the ORM
layer, so the static analysis catches refactors that would otherwise
fail only at runtime when a relationship name changes.

Third-party libraries without published stubs are stubbed in
``stubs/`` (project-local) or marked ``ignore_missing_imports`` per
module. The escape hatch is logged in ``pyproject.toml`` rather than
sprinkled as ``# type: ignore`` comments in source.

Schema migration testing
-------------------------

Alembic migrations under ``alembic/versions/`` are reversible by
contract. Every migration must define both ``upgrade()`` and
``downgrade()``, paired with an ``op.create_index`` / ``op.drop_index``
or ``op.add_column`` / ``op.drop_column`` mirror. The integration
workflow exercises this by performing a full ``alembic upgrade head``
on a fresh Postgres container at the start of the run; selected PRs
that touch schema additionally run ``alembic downgrade -1`` followed
by ``alembic upgrade head`` to verify the round-trip.

The unique partial index added for the ``job.dedup_key`` column
(F-OPS-JOBS.1a, migration ``a7b3c8d2e1f4``) is a recent example of a
migration that ships a round-trip test in the same PR.

Branch protection and auto-merge policy
----------------------------------------

The ``develop`` branch is protected on GitHub with the following
configuration:

* **Required status checks**: every workflow listed in the
  *CI gates* table above must pass before a PR can be merged. The
  exact list is enforced by GitHub branch-protection rules and is
  duplicated in ``.github/branch-protection.json`` for audit.
* **Required approvals**: PRs must have at least one approving
  review.
* **Linear history**: merge commits are disabled; the merge button
  squashes the PR commits into a single commit on develop.
* **Dismiss stale approvals**: any push after an approval invalidates
  it.

The auto-merge workflow (``.github/workflows/auto-merge.yml``) enables
GitHub's native auto-merge feature on non-draft PRs that target
``develop``. Once all required status checks pass and any required
reviews are submitted, the PR squash-merges automatically.

Two notes about the auto-merge policy:

* **Advisory checks** (bandit at low severity, reranker-token linter
  on certain doc files, cosmic-ray mutation score) can be red without
  blocking the merge. The required checks list is conservative on
  purpose; advisory checks are visible signals that get fixed in
  follow-up PRs.
* **Hotfixes** to ``main`` (production) follow a separate flow:
  cherry-picked from develop, gated by the same checks, manually
  merged by an operator.

Observability and SLO
----------------------

Three observability surfaces are instrumented:

**Structured logs**
    Every worker and the API emit JSON-line logs with structured
    fields (``timestamp``, ``level``, ``logger``, ``job_id``,
    ``operation``, ``stage``, ``fields``). Logs are written to
    ``logs/`` locally and shipped to Loki via the
    ``loki-docker-driver`` (T5.4) in deployed environments. The
    queue-consumer middleware emits one event per state transition
    and one per significant stage boundary inside an operation,
    creating an audit trail that survives worker restarts.

**Metrics**
    A ``/metrics`` endpoint (T5.2) exposes Prometheus counters and
    histograms: ``protea_job_state_transitions_total{from,to}``,
    ``protea_operation_duration_seconds{op}``,
    ``protea_queue_depth{queue}``, ``protea_http_requests_total``
    plus latency histograms. The Grafana dashboards (T5.3) graph
    these against an SLO of *95 % of jobs reach SUCCEEDED or FAILED
    within 6 h of dispatch*, with a separate panel per long-running
    operation (``export_research_dataset``, ``compute_embeddings``,
    ``run_cafa_evaluation``).

**Traces (OpenTelemetry)**
    FastAPI + SQLAlchemy + pika are instrumented via OTel
    auto-instrumentation (T5.1). Spans are emitted to an OTLP
    collector; the collector is configured for Tempo in deployed
    environments and stdout in dev. A request that crosses the
    API → queue → worker → DB → artifact-store boundary produces a
    single trace tree, which is essential when diagnosing a slow
    export.

Definition of done and PR checklist
------------------------------------

A PR is considered ready to merge when:

1. **All required CI checks are green** (the workflows listed in
   *CI gates*).
2. **Local CI was run before pushing** (``ruff``, ``mypy``,
   ``pytest``, ``check_smells.py``). The pre-commit hook bundle
   enforces the smallest set automatically; the full suite is the
   author's responsibility.
3. **The PR description references the slice id** (e.g.
   ``F-OPS-JOBS.1a``) and includes a one-line summary of what
   changed and why.
4. **No new offenders** in the smell budget. If new offenders are
   intentional (legitimate feature growth), the baseline is updated
   in the same PR via ``python scripts/check_smells.py --write-baseline``.
5. **OpenAPI is in sync** if the change touched router code:
   ``poetry run python scripts/generate_openapi.py`` updates
   ``docs/openapi.json``.
6. **The plan store is updated** if the PR closes a slice: the
   slice's status moves to ``done`` and the ``pr:`` field records
   the PR number.
7. **Documentation is updated** when the change introduces or
   removes a public surface: Sphinx pages, ADR if architectural,
   runbook if operational.

Documentation hygiene
----------------------

**ADR registry**
    ``docs/source/adr/`` contains 38+ Architecture Decision Records
    numbered D01-D38, plus nine legacy numbered ADRs (001-009). Each ADR
    documents context, decision, and consequences in a consistent RST
    template. Recent entries: D34 (selective rerank resurrection),
    D35 (canonical 8-PLM embedding configs), D36 (PLM axis explicit in
    dataset naming), D37 (auth users/roles multi-instance),
    D38 (neural head deferred, dataset-pack pivot).

**Hard constraints document**
    The repo-root ``CLAUDE.md`` and ``agent-farm/CLAUDE.md`` list
    non-negotiable constraints for every session: no pgvector
    for KNN at scale, no direct push to main/develop, no stash, no
    em-dashes in prose, no force push, no skipping pre-commit hooks.
    These are read by all contributors and any automated tooling at
    session start.

**Plan store**
    ``agent-farm/plans/<loop>/PLAN.md`` is the canonical slice catalog.
    Each slice is a self-contained unit of work with a unique ID
    (e.g. ``FARM-EXP.13``, ``F-DATA-PACK.1``), a clear objective, and
    acceptance criteria. Completed slices are marked with a status and
    linked to the PR that delivered them.

**Sphinx docs build (zero warnings)**
    ``.github/workflows/docs.yml`` runs ``sphinx-build`` with ``-W``
    (treat warnings as errors). Every autodoc cross-reference must
    resolve; orphaned RST files are detected.

How to read this page
----------------------

**Link conventions used above:**

* ``:file:`` or inline code paths (e.g. ``scripts/check_smells.py``) refer
  to files in the PROTEA repository root.
* ``:doc:`` cross-references point to other pages in this Sphinx site.
* Workflow names in the CI table (e.g. ``test.yml``) refer to files under
  ``.github/workflows/`` in the PROTEA repository.
* Slice identifiers (e.g. ``FARM-EXP.6``, ``T2B.4``) refer to entries in
  ``agent-farm/plans/`` or the master plan v3.2. Use them as search keys
  in the plan store to find the full specification and linked PR.
* ADR identifiers (e.g. ``D34``, ``002``) refer to files under
  ``docs/source/adr/`` and are cross-referenced with ``:doc:``.

**What "hard gate" vs "soft gate" means in this context:**

A *hard gate* is a required CI status check: a PR cannot be merged to
``develop`` until it passes. A *soft gate* (the smell budget, mutation
score, bandit advisory) produces a visible signal and blocks the
conversation without blocking the merge. Soft gates are intended to
prompt a fix-or-accept decision rather than to stop work cold.

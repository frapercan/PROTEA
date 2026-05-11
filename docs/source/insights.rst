Operational Insights and Lessons Learned
=========================================

.. contents:: On this page
   :local:
   :depth: 2

This appendix captures non-obvious gotchas that surfaced during the PROTEA
F-EXP campaigns and operational history. Each entry is grounded in a real
incident: an ADR, a commit record, an experiment run, or an operational
failure that triggered a fix. The goal is to keep the lessons alive across
handovers and to give future maintainers a head start on problems that were
only obvious in retrospect.

The material is a companion to thesis chapter 6 (F-EXP evaluation) and
appendix B (reproduction guide). Where numbers appear, they come from
named experiment runs; cross-references to ADRs point to the decision record
that closed each issue.

.. seealso::

   - :doc:`/appendix/reproduction_guide` for the ordered procedure that
     regenerates every figure from a clean database.
   - :doc:`/adr/index` for the full decision log.
   - :doc:`/runbooks/index` for on-call operational procedures.


Schema SHA drift
----------------

**What it is.**
``schema_sha`` is a 12-hex-char fingerprint computed over the sorted list of
feature families active at training time. It is the load-bearing safety check
that prevents inference from scoring with a booster trained against a different
feature schema. The predict-time batch worker recomputes it from its own active
flags and compares for strict equality; a mismatch emits
``reranker.schema_mismatch`` and falls back to KNN ordering rather than
silently producing miscalibrated scores.

**How it broke.**
Two separate definitions of ``compute_schema_sha`` accumulated across the
codebase: one in PROTEA and one in ``protea-reranker-lab``. For the first
several study generations they happened to agree, because the feature lists
were short. Around the study_v9 timeframe (2026-05-01), a feature-family
refactor in the lab changed the sort order of one family group. The PROTEA
copy used a different normalization, producing a different fingerprint for the
same logical feature set. The result was a non-reproducible run: the booster
import script accepted the model (because it read the lab's own fingerprint),
but the batch worker refused to activate re-ranking (because it recomputed
from PROTEA's definition). The fallback to KNN ordering was silent enough
that the mismatch went unnoticed for one full study iteration.

**How it was caught.**
A side-by-side comparison of Fmax between the lab's internal evaluation and
PROTEA's ``run_cafa_evaluation`` output on the same prediction set revealed
an unexplained gap. Tracing the scoring path surfaced the
``reranker.schema_mismatch`` event log entries that had been logged but not
surfaced to the developer's attention.

**The fix.**
ADR-D10 introduced a parallel ``schema_sha_v2`` column on both ``Dataset``
and ``RerankerModel``. The ``v2`` value is computed exclusively from
``protea_contracts.compute_schema_sha``, which is the shared, versioned
implementation in the contracts package (see :doc:`/adr/D10-schema-sha-v2`).
A backfill script recomputed ``v2`` from historical rows; a regression test
asserted that any ``v1/v2`` discrepancy on historical rows is expected and
documented rather than silently fixed. Production inference reads ``v2``.

**Prevention.**
The single source of truth rule: anything that both sides of the
platform-lab boundary need to agree on must live in ``protea-contracts``,
not duplicated in each repo. Drift between two copies is not detectable
until a run fails for an unrelated-looking reason. The ``feature_schema_sha``
strict-equality check was the right design; the mistake was implementing it
twice. See :doc:`/adr/007-contract-first-lab-integration` for the full
contract-first rationale.


Annotation leakage in temporal evaluation
------------------------------------------

**What it is.**
The CAFA evaluation protocol requires that the re-ranker is trained on
annotations that were *known at t0* and evaluated on annotations that became
known only between t0 and t1. This temporal boundary is straightforward for
direct GO annotations but breaks down as soon as GO lineage propagation enters
the picture.

**How it broke.**
The ``anc2vec_query`` feature family computes ancestry-based embeddings for
each candidate GO term relative to the query protein's existing annotation
profile. When the training rows were generated, the annotation profile used
was the *t1* profile rather than the *t0* profile. This meant that terms the
protein acquired between t0 and t1 (the labels the re-ranker is supposed to
predict) were already visible in the feature values. The label and the feature
shared information: the model was, in effect, learning to recognize its own
answers.

The effect was dramatic and initially invisible. In the study_v9 leave-one-out
ablation, dropping ``anc2vec_query`` cost 0.2565 Fmax on nk-bpo and 0.1524 on
lk-cco, mean delta +0.1449 across three representative cells. Those are
signals three to four times larger than any legitimate feature family in the
study. At the time, the size of the delta was misread as evidence that ancestry
features were particularly informative, not as a red flag for leakage.

**How it was caught.**
During the study_v9 cafaeval re-validation phase (see runs/study_v9/SUMMARY.md,
section 5), the ratio between lab Fmax and cafaeval Fmax for lk-cco reached
3.00x and for lk-mfo 2.57x. Those ratios were plausible given that cafaeval
propagates predictions through the GO DAG and the lab evaluator does not, but
the magnitude prompted a closer look at feature construction. Inspecting the
``anc2vec_query`` construction code revealed that it called into a utility
that resolved the protein's annotation set against the full database view,
not against the t0 snapshot materialised for each training pair.

**The fix.**
The bench-v1-K5-filtered dataset was generated by re-running the feature
pipeline with the annotation snapshot pinned to the relevant t0 for each
training pair. The ``filter_provenance`` block in the manifest records the
rule: keep only rows where the (category, protein_accession, aspect) tuple
has at least one label=1 row in that category, removing cross-category
replicas introduced by the earlier export. After retraining on the filtered
dataset, nk-bpo Fmax dropped from 0.4649 (study_v9 replication) to a
corrected evaluation under strict temporal holdout. The cafaeval re-validation
ratios returned to the 1.1-1.6x range that is expected from GO propagation
alone.

**Prevention.**
Every feature family that touches the protein's annotation state must be
constructed against an explicitly time-stamped annotation snapshot, passed
in as a parameter rather than resolved at construction time from the live
database view. The ``export_research_dataset`` operation now materialises a
frozen annotation table per snapshot pair and passes its id as a payload
parameter. Any new feature family that requires annotation-side context must
accept this id and resolve against it. See thesis chapter 6 and appendix B
for the full reproducible setup.

.. note::

   This discovery is one of the methodological contributions of the doctoral
   work. Most CAFA-style methods in the literature publish an Fmax number
   without disclosing the exact t0/t1 annotation snapshot used for feature
   construction. The leakage is easy to introduce inadvertently. The
   PROTEA campaign discovered it empirically; the corrected evaluation
   protocol is the version reported in chapter 6. See the
   :ref:`reproducibility-section` entry below for the broader context.


cafaeval PK coverage bug
------------------------

**What it is.**
Upstream ``cafaeval`` (the CAFA evaluator fork at
``claradepaolis/CAFA-evaluator-PK``) computes a ``coverage`` metric for the
Partial-Knowledge (PK) evaluation branch. Coverage is the fraction of eligible
proteins for which the predictor emits at least one scored term at a given
threshold. By definition it is bounded in [0, 1].

**How it broke.**
On every PROTEA benchmark run against the 220 to 230 temporal window, PK
coverage values of 1.3 to 1.9 were observed. The upstream bug is an asymmetry
between numerator and denominator inside
``compute_confusion_matrix_exclude_sparse``: the denominator ``ne`` correctly
excludes proteins whose TOI annotations were all pre-excluded (already known
at t0), but the numerator ``metrics['n']`` counted those same proteins
whenever the predictor emitted any non-excluded term for them. The visible
symptom was coverage greater than one. The silent secondary effect was that
``precision`` used ``metrics['n']`` as its denominator, so precision was
under-divided. On the 220 to 230 benchmark this dragged PK Fmax by 30-40%.

.. list-table:: Effect on the 220 to 230 PK cells (from ADR-008)
   :header-rows: 1
   :widths: 20 25 25 25

   * - Cell
     - Fmax before fix
     - Fmax after fix
     - Delta
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
     - -0.97

NK and LK cells were unchanged within float noise.

**The fix.**
The ``cafaeval-protea`` fork (commit ``cec8ccd``) applies a one-line semantic
correction inside the PK confusion matrix kernel: restrict the numerator row
count to proteins that still have at least one GT annotation in the TOI after
the per-protein exclude mask. The NK/LK path was not touched. The fork's
parity test suite was updated to xfail only the PK variants (intentional
divergence), while NK/LK variants continue to enforce bit-exact parity with
upstream. See :doc:`/adr/008-cafaeval-pk-coverage-fix` for the full root-cause
analysis and patch.

**Operational fallout.**
Three operational implications followed directly from this fix.

First, ``cafaeval-protea`` is installed as a ``file://`` path dependency.
After pulling a new fork commit, the venv must be force-reinstalled with
``pip install --force-reinstall --no-deps``, because ``poetry install`` treats
the local path as satisfied once the lockfile hash matches and will silently
leave the old module in ``site-packages/``.

Second, running workers hold the ``cafaeval`` module in memory. Reinstalling
the package does not hot-patch running workers; the evaluation worker must be
restarted to pick up the fix.

Third, every ``EvaluationResult`` row persisted before the fix carries the
buggy ``cov`` and ``precision`` values and must be discarded. The delete
endpoint cascades to MinIO artifacts and the launcher re-fires new evaluation
runs automatically for any prediction set that loses its result.

**Prevention.**
Coverage outside [0, 1] is a useful invariant assertion. Adding a post-hoc
check on ``run_cafa_evaluation`` output that logs a warning for any out-of-range
coverage value would have surfaced this earlier. The upstream bug exists verbatim
in ``claradepaolis/CAFA-evaluator-PK`` and has been flagged for an upstream
report with the minimal reproducer from the fork's regression test.


Deploy infrastructure fragility
--------------------------------

**What it is.**
The public PROTEA endpoint is served via a static ngrok domain that tunnels to
the Next.js frontend on port 3000. This arrangement has two single points of
failure: the ngrok process can die silently, and the deploy slot worktree
(``~/Thesis2/worktrees/protea-deploy``) can disappear while the
``deploy.sh`` script continues to assume it exists.

**The ngrok tunnel death pattern.**
The ngrok process runs in the foreground under ``scripts/expose.sh``. If the
host goes to sleep, the network stack drops, or the process is killed by an
OOM event, the public domain becomes unreachable while the local stack
continues running normally. The symptom from the outside is a 502 Bad Gateway
or a "Tunnel not found" page. The symptom from the inside is that
``curl -sf http://localhost:8000/jobs`` returns 200 but
``curl -sf https://protea.ngrok.app`` fails.

**The deploy-keeper worktree-not-existing pattern.**
The ``deploy.sh`` script manages the production-mode stack inside the
``protea-deploy`` git worktree. On 2026-05-11, a supervisor session reached
an agent that could not recreate the worktree because the parent branch had
been fast-forwarded past the branch tip the worktree was pinned to. The
script exited immediately with ``deploy slot does not exist`` on every
invocation, leaving the demo endpoint dead until a manual ``git worktree add``
restored the slot.

**The fix for each pattern.**
Tunnel deaths: restart ``expose.sh`` from the deploy slot, or from
``repositories/PROTEA`` if the slot is missing. If the script exits
immediately, diagnose the local stack first. If ngrok authentication is
missing, run ``ngrok config add-authtoken``.

Worktree loss: recreate the worktree with a ``git worktree add`` against the
current ``origin/develop`` tip, then run a full deploy cycle before
re-opening the tunnel. See :doc:`/runbooks/ngrok-deploy-recovery` for the
exact command sequence.

**Prevention.**
Run ``expose.sh`` under a process supervisor (a ``manage.sh``-tracked
background process or a systemd unit) rather than in a terminal tab that can
be closed. Add a pre-flight check to ``deploy.sh`` that detects a missing
worktree and either recreates it automatically or emits an actionable error
message instead of exiting with a bare path error. The deploy slot must be
created manually once; it is not recreated automatically if the directory is
deleted.


Smell-budget enforcement and the Method Object reframe
------------------------------------------------------

**What it is.**
PROTEA inherits from the PIS and FANTASIA codebases, where workers conflated
database sessions, queue management, orchestration, and business logic into
single classes. The smell-budget campaign (T2B.5) targeted the four largest
method bodies in the new codebase for Method Object extraction.

**Why the spec was stale.**
By the time PR #267 landed (2026-05-09), AST analysis revealed that three of
the four listed execute methods had already been reduced to well below the
60 LOC budget by prior PRs (#162, #169, #170, #177). A plan entry that
listed four Method Object classes as pending work had become misleading:
most of the work had shipped across 8 partial merges discovered only during
a shepherd scan.

**What actually shipped.**
The remaining smell on ``predict_go_terms.py`` was the size of the parent
class (1589 LOC), not any single method. The reframe extracted the largest
cohesive sub-cluster of methods that share mutable state into
``_AspectSeparatedKnnRunner`` (347 LOC, 10 methods, all below budget),
reducing the parent class from 1589 to 1283 LOC and closing T2B.5 without
the redundant Method Object class that the original spec had prescribed for
an already-budgeted entrypoint.

See :doc:`/adr/D31-t2b5-method-object-reframe` for the full decision, the
AST audit numbers, and the criterion for treating future oversized methods
in the same files.

**The lesson.**
Plan entries that list specific class or method names can become stale faster
than the execution cadence. A shepherd scan before opening a task is cheaper
than opening a PR against work that has already landed in a different form.
The T2B.5 closure also validated that the acceptance criterion (no method
over 60 LOC) is meaningful as a target, but the mechanism (Method Object vs.
sub-cluster extraction) should be chosen based on the actual shape of the
code at the time of the refactor, not on the shape it had when the spec was
written.


Plan-store vs reality drift
----------------------------

**What it is.**
The canonical plan store at ``agent-farm/plans/`` records the status of every
slice. When a slice lands via PR but the plan file is not updated in the same
pass, the plan store and the repository diverge. Subsequent conductor sessions
may dispatch executor agents to work that has already shipped.

**How it happened.**
Multiple slices (T-OPS.12, T2B.1, T2B.2, and others) were marked as
``pending`` in the plan store after their corresponding PRs had merged to
``develop``. The conductor had no mechanism to cross-check PR merge history
against plan status. Executor agents opened worktrees, read the plan, and
began implementing work that was already present in the codebase.

The downstream cost was low in each individual case (the agent discovered the
existing code and exited cleanly), but the pattern accumulated: over several
loops, a non-trivial fraction of conductor decisions were based on stale
status data.

**The fix.**
Two safeguards were added. A ``render.py`` script generates a human-readable
status table from the plan files and exits non-zero if any slice marked
``done`` or ``in_review`` lacks a corresponding PR reference in its metadata.
A lint guard in the CI workflow blocks plan files from being merged without
an explicit ``pr:`` field once status is set to ``done``. The conductor was
also updated to check ``git log`` for recent merges before dispatching
against a slice with no recent heartbeat.

**Prevention.**
Every slice closure must include a plan-store update in the same PR. The rule
is: one PR closes one slice; the PR description contains the slice id; the
plan file is updated as part of the PR diff. Slices that land across multiple
PRs (like T2B.5) are closed in the final PR of the sequence, not in the first
one. A post-merge hook that checks plan consistency takes the discipline
requirement off the developer and puts it into the tooling.


Cross-repo plugin coordination
-------------------------------

**What it is.**
The PROTEA stack is split across eight repositories. Any change to
``protea-contracts`` (ABCs, payloads, schema) must propagate to every
consumer repo before the consumer can be tested or released. Doing this in
the wrong order causes integration failures that are hard to diagnose because
the error surfaces in the consumer, not in the contract definition.

**The MIL.1a incident.**
The MIL.1a delivery shipped new contract symbols in ``protea-contracts`` and
new backend implementations in ``protea-backends`` in the same release window.
The backends repo was pinned to the contracts feature branch rather than a
released tag. During integration testing, a third consumer (``protea-runners``)
that had not been updated picked up the old contracts tag and failed to
import the new symbols. The failure appeared as an import error in the runner
worker, not in the contracts or backends repos.

**The protocol.**
Merge contracts first, tag and release a semver patch or minor version.
Consumer repos update their dependency pin to the new tag in a separate PR,
with CI running against the released tag rather than a branch ref. A
consumer that has not updated its contracts pin will fail loudly at import
time (the ``Operation`` protocol version check raises on mismatch) rather
than running with a silently wrong schema. This fail-loud design was
intentional (see :doc:`/adr/007-contract-first-lab-integration`): the batch
worker emits ``reranker.contracts_unavailable`` and falls back to KNN ordering
rather than crashing, but it does log the fallback reason at warning level.

**Prevention.**
The release checklist for any change that touches ``protea-contracts`` is:
(1) merge and tag contracts, (2) open consumer PRs that bump the pin, (3)
merge consumer PRs in dependency order (platform before plugins), (4) verify
the integration test suite against the new tags. Skipping step 1 and
opening consumer PRs against a branch ref is the failure mode to avoid.


.. _reproducibility-section:

Reproducibility as a methodological contribution
-------------------------------------------------

**Context.**
Protein function prediction has a reproducibility problem that is rarely
acknowledged. Most CAFA-style methods publish a single Fmax number computed
against a benchmark dataset, but the accompanying code does not specify which
GO annotation release served as t0 for feature construction, which proteins
were in the query set, or whether the GO DAG propagation was applied before
or after the temporal split. Two groups running nominally the same method
against the same CAFA benchmark can produce different numbers for legitimate
reasons that are invisible in the paper.

**What the PROTEA campaign discovered empirically.**
During the F-EXP campaign, running the same trained booster against two
slightly different annotation snapshots (differing only in which GOA release
was used to resolve the training pairs) produced Fmax differences of 0.03 to
0.05 across NK cells. These differences are not noise: they are deterministic
given the snapshot pair, reproducible to four decimal places. But they would
look like unexplained variance to a researcher who had not pinned the snapshot
version in their run record.

The leakage discovery (see above) made this concrete. The ``anc2vec_query``
features produced dramatically inflated Fmax numbers because they used t1
annotations in the training rows. The inflation looked like a strong feature
signal. The corrected evaluation required pinning the annotation snapshot
explicitly in the ``export_research_dataset`` payload and verifying that every
feature family respected the pin.

**The contribution.**
PROTEA's experiment infrastructure was designed to make the full snapshot pair
list, the annotation source, the embedding config id, the schema_sha, and the
producer git sha part of every ``Dataset`` row. Every ``EvaluationResult`` row
is linked back through ``PredictionSet`` to the specific prediction run and
its parameters. The thesis (chapter 6) reports numbers that can be reproduced
from the commands in appendix B because the snapshot versions are part of the
record, not implicit.

This is not a novel insight in the machine learning literature, but it is
novel in the protein function prediction literature, where the dominant
publication format does not include this level of provenance. The PROTEA
campaign's methodological angle is that temporal holdout correctness is not
a detail: it is the thing being measured, and it requires explicit
infrastructure to get right.

**Prevention for future campaigns.**
Any evaluation that involves a temporal split must specify: (1) the annotation
release used as t0, (2) the annotation release used as t1, (3) whether GO
lineage propagation was applied before or after splitting, (4) which proteins
were in the query set, and (5) whether the same protein could appear in both
training and evaluation sets (a common source of leakage in methods that
aggregate across multiple snapshot pairs). The ``EvaluationSet`` row in PROTEA
captures (1), (2), (3), and (4). The training dataset manifest captures the
snapshot pair list. Cross-checking that the eval snapshot pair does not
overlap with any training pair is enforced by the ``export_research_dataset``
payload validator. See :doc:`/appendix/reproduction_guide` for the full
ordered procedure.

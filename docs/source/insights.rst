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

   - :doc:`/operate/reproduce-0.4063` for the ordered path that reproduces
     the sealed board.
   - :doc:`/adr/index` for the full decision log.
   - :doc:`/runbooks/index` for on-call operational procedures.


Feature-schema SHA drift across the platform-lab boundary
----------------------------------------------------------

**What it is.**
``feature_schema_sha`` is a deterministic fingerprint over the sorted
list of feature families active at training time. It is the load-bearing
safety check that prevents inference from scoring with a booster trained
against a different feature schema: the predict-time batch worker
recomputes it from its own active flags and falls back to KNN ordering
on a mismatch rather than producing miscalibrated scores.

**The lesson.**
For one study iteration in 2026-05, two independent implementations of
``compute_schema_sha`` (one in PROTEA, one in
``protea-reranker-lab``) used different normalisations of the same
feature list. The booster import accepted the model but the batch
worker rejected it at scoring time. The fallback to KNN was silent
enough to mask the drift for a full sweep.

The fix moved the canonical implementation to ``protea-contracts`` and
backfilled a parallel column (``schema_sha_v2``) so historical rows
could be compared without disturbing the live ``schema_sha``. Production
inference reads ``schema_sha_v2``. See
:doc:`/adr/D10-schema-sha-parallel-migration` for the dual-write rollout
and :doc:`/adr/007-contract-first-lab-integration` for the broader
contract-first rationale.

**Prevention rule.**
Anything that both sides of the platform-lab boundary must agree on
lives in ``protea-contracts``, not duplicated in each repo. The
strict-equality check was the right design; the only mistake was
implementing it twice.


Replication artefact in the anc2vec_query feature family
---------------------------------------------------------

.. note::

   Earlier drafts of this section described the incident as "temporal label
   leakage". That framing was wrong and has been corrected. The CAFA
   temporal partition itself (NK / LK / PK in :doc:`/architecture/evaluation`)
   is mathematically clean: PK simply records that the protein already had
   experimental annotations in some namespace at t0, which is a legitimate
   evaluation split, not a leak. The incident below was a feature-construction
   artefact in one feature family, not a flaw in the temporal protocol.

**What it is.**
``anc2vec_query_known_count`` counts the protein's t0 annotations in the
query namespace. As a stand-alone feature this is unproblematic. The issue
arose from how the training table was assembled in the early
``export_research_dataset`` pipeline.

**How it broke.**
The export materialised the training parquet by replicating each
``(protein, aspect)`` row across categories (NK, LK, PK) so the booster
could see all three label streams in one pass. The replication step did
not filter on whether the protein actually belonged to that category in
that aspect: a protein that was genuinely NK in F appeared as a synthetic
negative row in P and C, and a protein that was PK in P got a synthetic
NK row in aspects where it had no terms. Because ``anc2vec_query_known_count``
is deterministic in the t0 annotation profile, its value silently became
a perfect bucket identifier: low value → "this is a genuine NK row", high
value → "this is a synthetic negative replicated from another category".
The booster learned the bucket, not the biology.

The effect was dramatic and initially invisible. In the study_v9 leave-one-out
ablation, dropping ``anc2vec_query`` cost 0.2565 Fmax on nk-bpo and 0.1524 on
lk-cco, mean delta +0.1449 across three representative cells. Those are
signals three to four times larger than any legitimate feature family in the
study. At the time, the size of the delta was misread as evidence that
ancestry features were unusually informative.

**How it was caught.**
During the study_v9 cafaeval re-validation phase, the ratio between lab
Fmax and cafaeval Fmax for lk-cco reached 3.00x and for lk-mfo 2.57x.
Those ratios were plausible given that cafaeval propagates predictions
through the GO DAG and the lab evaluator does not, but the magnitude
prompted a closer look at the training table. Inspecting the row
distribution per ``(protein, aspect, category)`` exposed the
cross-category replication.

**The fix.**
PROTEA commit ``223299c`` filters ``(protein, aspect)`` by category
membership before the replication step: a row is emitted in category
:math:`c` only if there is at least one label-1 row for that
``(protein, aspect)`` in category :math:`c`. The synthetic-negative
buckets disappear; ``anc2vec_query_known_count`` reverts to a normal
feature with normal importance. After the rebuild, the cafaeval
re-validation ratios returned to the 1.1-1.6x range expected from GO
propagation alone, and per-category Fmax values aligned across
evaluators.

**Prevention.**
Any cross-category replication in dataset assembly must be gated by an
explicit per-category membership filter. The ``filter_provenance``
block in the manifest records the rule. Generally, any feature derived
from the t0 annotation profile must be constructed against an
explicitly time-stamped snapshot, passed as a payload parameter rather
than resolved at construction time against the live database view, so
its values cannot accidentally encode metadata about the row that
carries them.


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

.. list-table:: Effect on the 220 to 230 PK cells (from :doc:`/adr/008-cafaeval-pk-coverage-fix`)
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

The ``anc2vec_query`` artefact (see above) made this concrete. The
cross-category replication in the early training-table assembly turned
``anc2vec_query_known_count`` into a bucket identifier and inflated its
apparent importance. The corrected pipeline filters
``(protein, aspect)`` by category membership before replication, so each
feature carries only the information its definition implies and not
metadata about how its row was constructed.

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
payload validator. See :doc:`/operate/reproduce-0.4063` for the ordered
reproduction path.

The served last layer is a weak retrieval base, and standardisation is the lever
--------------------------------------------------------------------------------

PROTEA's retrieval encoder stores learned, GO-aligned codes rather than a raw
protein-language-model vector. A controlled ablation on the ankh-base substrate
motivates that choice, and its lesson is not the one a reader expects.

**The finding.**
Scored board-faithfully (cosine top-30 KNN GO transfer into the 15,000-protein
reference, ``f_micro_w`` over the nine cells), the learned k-WTA encoder
``d8979601`` reaches mean 0.21500, versus 0.14597 for the best fixed
representation (a standardised mid layer, L10, k-WTA) and 0.13356 for the served
last-layer dense baseline. That is plus 47.3 percent over the best fixed choice
and plus 61.0 percent over the served baseline, winning all nine cells, with the
largest gains on molecular function.

**Why the last layer is a poor base.**
Mean-pooled activations of ankh-base's final layer are compressed to a peak
absolute value near 0.6 by the model's closing LayerNorm, while the mid layers
reach magnitudes above 400,000. The final layer's flattened geometry is a weak
substrate for cosine retrieval, which is one reason the served last layer sat at
the bottom of the ranking.

**The lever is standardisation, not depth.**
Among fixed representations the dominant lever is per-dimension standardisation
(z-score, statistics fit on the reference pool only, non-transductive), not the
index of the layer. Choosing a different raw layer does not beat the served base;
standardising a mid layer does. A rerun at the champion's declared 100,000
protein pool confirms this at scale with high significance: a standardised L10
beats both the raw L10 and the served last layer, while the raw layer choice is
statistically null. The same rerun shows that training-pool size, the
hard-negative objective, and a learned multi-layer mixture are all null in this
harness, so standardisation is the single lever the fixed-representation family
exposes.

**The caveat, and what resolved it.**
A controlled re-training of the encoder inside the offline lab harness, on a
local mean-pool of the ankh-base last layer, reaches only the fixed-representation
band (about 0.14 to 0.16). The resolution is the base embedding, not the training
procedure: the identical head recipe, trained on the production-stored embedding
for this backbone, reproduces the served encoder (mean 0.220 against 0.215). The
lab arms fell short only because their local extraction is a weaker base than the
production one. The precise extraction difference (pooling, normalisation, or which
tensor is read as the last layer) is the one detail still to pin down. These are all KNN-only retrieval numbers and are
distinct from the sealed 0.4063 reranked board in :doc:`results`; they explain
why the champion stores learned GO-aligned codes. See
:doc:`/adr/D35-canonical-8plm-embedding-configs` for the embedding config
registry and :doc:`/adr/D38-neural-head-deferred-dataset-pack-pivot` for the
neural-head decision this evidence informs.

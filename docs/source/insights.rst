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

   - :doc:`/operate/reproduce-the-sealed-board` for the ordered path that reproduces
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
payload validator. See :doc:`/operate/reproduce-the-sealed-board` for the ordered
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
distinct from the sealed 0.40765 reranked board in :doc:`results`; they explain
why the champion stores learned GO-aligned codes. See
:doc:`/adr/D35-canonical-8plm-embedding-configs` for the embedding config
registry and :doc:`/adr/D38-neural-head-deferred-dataset-pack-pivot` for the
neural-head decision this evidence informs.

.. _insight-representation-matters-only-in-twilight:

The representation earns its place at retrieval and loses it at scoring
-----------------------------------------------------------------------

The ablation above measures the encoding where the encoding is the only
evidence available, and finds it worth a great deal. Measured again at the
other end of the same pipeline, with the rest of the evidence switched on, the
same axis is worth nothing. Both numbers are correct, and the distance between
them is the most useful thing this project has measured about its own
representation.

**The grid.** Four encodings (pretrained ankh-base, a dense fitted map, a
sparse pooled map at 128 of 2048 atoms, and a sparse per-residue code) crossed
with six neighbourhood sizes, nine score weightings and three knowledge
regimes, banded by sequence identity, scored board-faithfully on the nine
category-by-aspect cells. 104 arms per cell.

**No arm beats any other.** The margin between first and second never exceeds
0.0015 in any of the nine cells, and in five of them the second place is the
same encoding under a different weighting. The per-cell winner this grid was
built to find cannot be determined, and that is the result rather than a gap in
it.

**What separates arms is the channel, not the encoding.** Holding the weighting
fixed, the four encodings spread 0.0540 under ``embedding_only`` at K=30 and
0.0025 under ``composite_no_embedding``. The second of those carries weight
exactly 0.0 on embedding similarity and wins 72.7 percent of cells.

The 0.0540 is almost entirely the instrument, and this can be shown rather
than asserted. It is measured under ``embedding_only``, the channel that
:ref:`insight-capacity-is-read-through-one-channel` shows is scored on a
population that moves with the arm. Here the correlation between population and
score is -0.809, and the shape is specific: the un-encoded baseline is scored
on the most proteins and scores worst.

.. list-table:: The four arms at K=30 under ``embedding_only``
   :header-rows: 1
   :widths: 34 22 22

   * - Arm
     - Score
     - Mean population
   * - sparse pooled
     - 0.23656
     - 2,261
   * - sparse per-residue
     - 0.22580
     - 2,218
   * - dense fitted
     - 0.21353
     - 2,237
   * - pretrained ankh-base (un-encoded)
     - 0.18251
     - 2,350

Of the 0.0540, some 0.0313 is the baseline against the encoded arms across a
5.9 percent population gap, and 0.023 separates the three encoded arms across a
1.9 percent one.

**Restricted to scorers whose populations agree, the encoding axis at K=30 is
what it was at K=1.** Across four composite scorers whose arms sit within 1.4
percent of each other in population, the four encodings spread 0.00194,
0.00227, 0.00248 and 0.00326. Two to three thousandths, against a headline of
0.0540 from the same arms at the same budget.

**No sentence here should name a winning encoding.** Which one wins at K=30
flips with the scorer, four scorers to four: the dense fitted map wins under
one set and the sparse pooled map under the other. That is the same
self-inconsistency the backbone ordering shows under its winning weighting, and
it means the ordering inside these numbers is not reportable at any budget. The layer
axis reproduces the pattern independently: the last layer beats depth 38 in all
sixteen comparisons, by 0.0307 through ``embedding_only`` and 0.0026 through
the winner, an attenuation of about twelve.

**In the regime this project names as its frontier, the representation does not
participate at all.** In half the prior-knowledge cells the four encodings score
identically to six decimal places. That is a stronger statement than a small
difference, because it is falsifiable and it failed to be falsified: the four
encodings retrieve the same neighbours and transfer the same terms.

**The ties are smaller than chance would produce**, which is what makes the
absence of a winner an explanation rather than only an observation. A maximum
taken over many arms has a floor of roughly ``sigma * sqrt(2 * ln N)``; at the
study's measured spread that is about 0.0093 for the 104 arms in one cell and
0.0108 for the 528 in the grid. Every margin here is five to seven times below
it, including the 0.0021 by which ProtST leads ankh-base on the board. A
comparison decided before it ran is held to the resolution floor of 0.0013
instead, which is why a 0.0099 loss on a two-arm test counts and a 0.0021
margin over a search does not. The discriminator is the search budget, not the
size of the number.

**Depth and encoding are not the same lever seen twice.** Two instruments
sharing no code, one scoring ``f_micro_w`` on the task and one measuring
reachability on a retrieval bank, agree by identity band on what changing depth
costs:

.. list-table:: Depth 38 minus the last layer, by sequence identity band
   :header-rows: 1
   :widths: 20 25 25

   * - Identity band
     - Task, ``f_micro_w``
     - Retrieval, reachability
   * - <= 30 percent
     - +0.0068
     - +0.0010
   * - 30 to 60 percent
     - -0.0142
     - -0.0158
   * - 60 to 90 percent
     - -0.0129
     - -0.0099
   * - > 90 percent
     - -0.0010
     - -0.0162

Same sign in three of four bands and close in magnitude in the two middle ones.
Both instruments say depth is inert in the twilight zone and costs elsewhere,
which is the opposite shape to the encoding axis, whose effect was largest in
twilight and zero above ninety percent identity. Depth costs where the answer
was already easy; the encoding matters only where it was hard.

**How to read this against the section above.** The retrieval ablation reports
the learned encoder at 0.21500 against 0.13356 for the served last-layer dense
baseline, a 61.0 percent gain. That measurement gives the encoding the whole
job: cosine top-30 transfer, with no identity signal, no neighbour consensus and
no taxonomic prior in the score. The grid here gives it the job it actually has
in the served pipeline, alongside those other channels, and the winning
weighting reads it at zero. Neither number is wrong and neither supersedes the
other. The learned encoder earns its place by retrieving a better candidate set,
and it does not additionally earn one by scoring it, because by the time the
candidates are scored the evidence that orders them is coming from somewhere
else.

The practical consequence is that the retrieval axis is closed by measurement.
What limits the board is the ordering of candidates already retrieved, which is
the same conclusion the BP wall reaches from the other direction below.


.. _insight-capacity-is-read-through-one-channel:

An 8M-parameter backbone is within the noise of a 650M one, once the score stops asking
----------------------------------------------------------------------------------------

The section above measures one axis, the encoding, at two points in the
pipeline. The backbone axis, measured on a different grid in an earlier rung,
does the same thing, and putting the three axes side by side shows the property
is the pipeline's rather than any one axis's.

Eight pretrained protein language models were scored on the nine cells across
neighbourhood sizes and score weightings, on the GOA 226 to 227 frame. That is
not the sealed board's window and these are not board numbers; the comparison
between the two columns is the finding, not their level. Read through the
channel that asks the embedding for everything, the backbones separate. Read
through a weighting that also has identity and neighbour consensus available,
they do not separate at all. The second of those is the result; the first, as
set out below, is not safe to attribute to capacity.

.. list-table:: Best arm per backbone, mean ``f_micro_w`` over the nine cells
   :header-rows: 1
   :widths: 34 11 22 22

   * - Backbone
     - Params
     - ``embedding_only``
     - ``composite``
   * - ``facebook/esm2_t33_650M_UR50D``
     - 650M
     - 0.30826
     - **0.35728**
   * - ``Rostlab/prot_t5_xl_half_uniref50-enc``
     - 1.2B
     - **0.34194**
     - 0.35673
   * - ``ElnaggarLab/ankh-large``
     - 1.15B
     - 0.33509
     - 0.35647
   * - ``Rostlab/ProstT5``
     - 1.2B
     - 0.32803
     - 0.35632
   * - ``esmc_600m``
     - 600M
     - 0.32034
     - 0.35628
   * - ``facebook/esm2_t6_8M_UR50D``
     - 8M
     - 0.29610
     - 0.35617
   * - ``mila-intel/ProtST-esm1b``
     - 652M
     - 0.34039
     - 0.35608
   * - ``ElnaggarLab/ankh-base``
     - 450M
     - 0.32838
     - 0.35487
   * - **spread**
     -
     - **0.04584**
     - **0.00241**

**The 8-million-parameter model is the clearest case.** Against the best
composite arm it is eighty-one times smaller, and through ``composite`` that
costs 0.00111, which is below the study's 0.0013 resolution floor: on this
evidence the platform cannot tell the two apart.

.. warning::

   **The ``embedding_only`` column is confounded and must not be read as a
   capacity effect.** The arms are not scored on the same proteins.
   ``cafaeval`` counts a protein in a cell only where the arm predicted
   something there, so an arm covering more of the population is scored on
   more of it, including the proteins for which no good neighbour exists. The
   eight ``embedding_only`` maxima are scored on populations spanning 7.5
   percent, from 6,193 to 6,655 proteins summed over the nine cells, and the
   rank correlation between population size and score is **-0.976**. Sorted by
   population, the column is very nearly sorted by score.

   A quality difference and a population difference are collinear here, and
   nothing in these summaries separates them: ``cafaeval`` reports per cell,
   not per protein, so the arms cannot be restricted to a common population
   after the fact. The 0.04584 spread is therefore an upper bound on what
   backbone choice buys through this channel, not a measurement of it, and the
   0.04584 that appeared in earlier drafts of the attenuation ratio inherits
   that.

   The ``composite`` column does not have the problem. Its populations span
   2.4 percent, 6,116 to 6,264, and the population-to-score correlation is
   -0.143. That is the column the finding rests on.

**There is no ordering to report in the composite column.** Its eight entries
span 0.00241, and each entry is itself a maximum taken over eight to eleven
arms sharing the neighbourhood sizes 3, 5, 10 and 30, so the spread between the
backbones is smaller than the selection noise carried by any one of them. The
unequal budgets do not explain the order either: the backbone searched hardest,
at eleven arms, placed third, and the one that won had nine. The rank
correlation between the two columns is 0.000, which
is not a reversal but the signature of an absent signal: the composite column
has no order for the ``embedding_only`` order to agree or disagree with. Any
claim that one of these backbones is the right one, made on this evidence,
would be a claim about which arm won a coin toss.

**The flatness is not an artefact of the operating point.** Every one of the
sixteen maxima above is at K=3, so the table is a balanced cut of two arms per
backbone rather than a maximum over unequal budgets. Repeating the comparison
at the other neighbourhood sizes keeps the result:

.. list-table:: Backbone spread under ``composite``, by neighbourhood size
   :header-rows: 1
   :widths: 12 26 26 36

   * - K
     - Spread
     - Population range
     - Note
   * - 3
     - 0.00241
     - 2.4 percent
     - the cut published above
   * - 5
     - 0.04579
     - 551 percent
     - unusable, see below
   * - 10
     - 0.00943
     - 2.9 percent
     - nothing removed
   * - 30
     - 0.04354
     - 2.9 percent
     - 0.00740 excluding one arm, see below

Two budgets need their exceptions named rather than quietly dropped. At K=5
one arm is scored on 954 proteins against a norm near 6,100, which is a
truncated evaluation and not a backbone result; the spread at that budget
measures the truncation. At K=30 the spread is one outlier: seven backbones
span 0.00740 and ``ankh-base`` sits alone 0.036 below the next worst, on a
normal population, and only under this scorer. That arm is a suspect data
point, not evidence that the backbone matters at K=30.

With those two named, the backbone axis is flat under the winning weighting at
every budget where the evaluation is sound.

**The same shape appears on every representation axis measured**, and it is
the right-hand column that carries it.

.. list-table:: Spread under the winning weighting, against an upper bound on
                the spread through the embedding channel alone
   :header-rows: 1
   :widths: 28 16 22 22

   * - Axis
     - Rung
     - Winning weighting
     - ``embedding_only``
   * - Backbone (8 pretrained PLMs)
     - 1
     - **0.00241**
     - at most 0.04584
   * - Encoding (4 representations)
     - 2
     - **0.0025**
     - 0.0540
   * - Layer depth (3 depths)
     - 2
     - **0.0026**
     - 0.0307

Three axes measured in different rungs, on different grids and on different
frames, spanning an eighty-one-fold range of model capacity, four ways of
encoding a protein and three depths of the same network. Under the weighting
that wins, all three collapse to between 0.0024 and 0.0026, which is under
twice the study's resolution floor and far under the selection floor that
applies to a maximum taken over a grid. That is one property of the pipeline
observed three times, not three findings, and it is the reason this project's
remaining headroom is argued for at the ranking stage rather than the
representation stage.

The left-hand column is deliberately not converted into an attenuation ratio.
The backbone entry is an upper bound for the reason given above, and the three
rows sit on different frames and different populations, so a ratio computed
down that column would be arithmetic performed on numbers that are not
commensurable. What the three rows share is the right-hand column, where the
populations are tight and the axis is gone.

**What it does not say.** None of this shows that the representation is
unimportant in general, and none of it licenses picking the cheapest backbone
for a different pipeline. It says that in a pipeline whose winning score
weighting reads identity and neighbour consensus, those channels are already
carrying what the embedding would otherwise have supplied, and the embedding is
consulted for the part they cannot reach. The twilight-zone result in the
section above is that part, and it is where the representation still pays.


.. _insight-bp-wall-is-a-ranking-limit:

The BP wall is a ranking limit, not an evidence ceiling
-------------------------------------------------------

The sealed board (:doc:`results`) is first in seven of nine cells. The two it does
not win are LK-BPO and PK-BPO, the Biological Process branch for the proteins with
limited or no prior knowledge. This section locates that limit. Every figure below is
measured on one harness, against the full ground truth, on the PK-BPO cell.

**The evidence is present.**
97.0 percent of the true protein to Biological Process term pairs the pipeline misses
use a term that some protein in the same cohort already carried before the target
window opened. Weighted by information accretion, which is what the metric scores,
the figure is 95.2 percent. Only 3 percent of the pairs, and 4.8 percent of the
weight, are genuinely novel. Nothing is missing from the vocabulary.

**Retrieval is not the binding constraint.**
Candidate recall is 0.322. Submitting every pool cell that belongs to the propagated
ground truth, which is what a perfect ordering of the candidates already retrieved
would be worth, yields ``f_micro_w`` **0.7519** at precision 1.000, verified through
the evaluator itself. Allowing that ordering to also keep the pool cells whose own
ancestors are true, which a real ranker may do and an oracle has no reason to refuse,
reaches **0.7764**, and the ceiling of the pool lies in [0.7764, 0.8326]. The deployed
re-ranker delivers **0.2131** on that same pool: **27.4 percent** of what its own
shortlist allows.

**So adding candidates does not pay.**
A co-occurrence expansion lifting recall from 0.322 to 0.480 moves the score by a
small fraction of the gap. More candidates do not help a ranker that cannot order the
ones it already holds.

**Nor is it where the list is cut.**
A global threshold cannot express a per-protein term count: every protein is cut at
the same ``tau`` whether it deserves three terms or thirty. Freezing the pipeline's
own ordering and granting each protein its true count, an oracle no method could
have, moves ``f_micro_w`` from 0.2017 to 0.2379. That is plus 0.036 of a 0.406 gap,
**about a tenth**. The other nine tenths is ordering.

**It is ordering, and no feature carries it.**
On PK-BPO no feature the pipeline carries exceeds AUC 0.68 (``classifier_present``;
then ``protst_text`` at 0.64 on 41 percent coverage, ``classifier_score`` 0.63,
alignment near 0.60) against a 2.47 percent positive rate.

**The deployed recipe is the best technique we have.**
Every variation tested scores below it:

.. list-table:: PK-BPO, one harness, full ground truth
   :header-rows: 1
   :widths: 60 20 20

   * - Recipe
     - ``f_micro_w``
     - vs deployed
   * - **deployed: per-category ``lambdarank``, aspects pooled**
     - **0.2131**
     - reference
   * - trained per cell instead of pooling aspects
     - 0.2017
     - minus 0.011
   * - ``binary`` objective instead of ``lambdarank``
     - 0.1518
     - minus 0.061
   * - plus within-protein rank and z-score features
     - 0.1465
     - minus 0.067
   * - plus class weighting
     - 0.1441
     - minus 0.069
   * - classifier-proposed candidates dropped from the pool
     - flat
     - coverage 0.978 to 0.846

The ``binary`` result is worth stating twice, because it is counterintuitive: it
carries a **better** AUC than the deployed recipe (0.8227 against 0.7903) and a
**worse** ``f_micro_w``. AUC ranks these recipes in the opposite order to the metric
that decides. Do not triage ranking levers by AUC.

**Ruled out by measurement.**
GO-DAG hierarchical proximity as a feature (AUC 0.5501, decorrelated from the
re-ranker yet adding plus 0.0002 when blended); a text-aligned scorer as a re-ranker
feature (plus 0.0016); an InterPro graft (negative on BP); and a larger base
representation, which reorders the same candidates and so cannot help where recall is
not the constraint.

**Where that leaves the two cells.**
The gap to the leading external method is plus 0.072 (LK-BPO) and plus 0.076
(PK-BPO). The ranking headroom inside the pool already retrieved is several times
that, so the work is a ranker and not a retriever. The technique levers available are
exhausted: the deployed recipe sits at their optimum. The signal that would close the
gap is not identified, and the two structural candidates testable with these
resources, term co-occurrence and ontology proximity, are both dead. "Improve the
ranker" is a direction, not yet a plan.

**Method note.**
Two rules this cell earned, both cheap to apply and both load-bearing here. A recall
number does not identify what binds a pipeline; the ceiling of the pool does, and
obtaining it costs one evaluation with the labels used as the score. And a monotone
rescaling of a score is **not** free under a threshold-swept metric: ``f_micro_w``
sweeps ``tau`` on a fixed grid, so remapping the score distribution changes which
cuts the sweep can reach. Scoring one booster with its raw output and with a global
rank-percentile of that output differs by 0.088 on this cell. Any transform applied
before evaluation is part of the measurement.

.. note::
   These figures come from a retrained booster on an exported dataset rather than the
   sealed board, and they track it closely: the deployed recipe measures 0.2131 here
   against the board's 0.2181 for PK-BPO. Every figure shares one ground truth and one
   harness.

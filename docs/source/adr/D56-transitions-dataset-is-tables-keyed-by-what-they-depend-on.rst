ADR-D56: The transitions dataset is tables keyed by what they depend on
=======================================================================

:Status: Proposed
:Date: 2026-11-07
:Author: Francisco Miguel Pérez Canales
:Phase: single world model of a protein's knowledge (D29)

Context: a feature family cannot add a grain
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

D29 makes the thesis one model: a JEPA that predicts, in latent space, the
representation of a protein's future state of knowledge from its present. It
needs a dataset of transitions, and the design names five things per query
protein: its sequence identity and per-residue vocabulary code, its own state at ``t``, its
neighbours at ``t`` with each neighbour's state, its gains at ``t'``, and the
term table of the cut.

``export_research_dataset`` publishes one parquet row per
``(protein_accession, go_term_id)``: a table of PAIRS, with the label,
``category`` and ``snapshot_pair`` as reserved columns and every feature
arriving through one of the twenty-one families in
``protea_contracts.FEATURE_FAMILIES``, each recorded in the manifest as
``produced`` or ``declared_absent`` (ADR-D45).

Measured against that schema, the gains at ``t'`` are already there: the label
is the truth, ``category`` is the stratum and ``snapshot_pair`` is the
transition. Most of the neighbour description is there too, spread over nine
families (``alignment_nw``, ``alignment_sw``, ``length``, ``taxonomy_pair``,
``taxonomy_voters``, ``knn``, ``knn_distance``, ``knn_vote``,
``k_neighborhood``). ``annotation_meta`` carries ``qualifier`` and
``evidence_code``, though not ``assigned_by``.

What is missing is not data. **A family is a set of columns on the row that
already exists, so it cannot add a grain.** A query's own state is N rows per
query, a neighbour's state is N by M, and a vocabulary key is one per sequence.
None of the three is a column on a pair.

Decision: ten tables, each keyed by what it actually depends on
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One ``Dataset`` row, one manifest, ten parquets. Eight when this record was
accepted; ``window_terms_out`` and ``residue_refs`` were added on 2026-11-08
after the laboratory read the writer's design, and each has its own section
below saying what it is for.

**The principle is one sentence: a table is stored by what it depends on.** It
came up three times before it was named. A protein's neighbours at ``t`` do not
depend on ``t'``. Neither does a neighbour's state at ``t``. Nor does a feature
token depend on the window at all, only on the sequence and the vocabulary. Key
any of those by the window and the rows are copied once per horizon; key them
by what they are a function of and they are written once and read by every
horizon that reaches them. Every table below is keyed that way, and the volume
of a cut then grows with its number of ``t`` values rather than with its number
of windows. The existing pair table stays
as it is and is not extended: it is good for what it is, and the model does not
read it at that grain.

.. list-table::
   :header-rows: 1

   * - table
     - grain
     - holds
   * - ``queries``
     - ``(window, query)``
     - ``sequence_id``, the per-residue vocabulary code's ``shard_key``,
       ``position`` and ``shard_sha256``, ``length``, ``stratum``,
       ``version_t``, ``snapshot_pair`` and ``horizon_days``
   * - ``query_state``
     - ``(window, query, go_id, moment)``
     - ``moment`` is ``t`` or ``t'``; ``evidence_code``, ``qualifier``,
       ``assigned_by``. At ``t'`` the non-experimental annotations go in too
   * - ``neighbourhood``
     - ``(version_t, query, neighbour)``
     - ``rank``, then ``distance``, ``similarity``, ``identity``, ``coverage``
       and ``taxonomy_distance``
   * - ``neighbour_state``
     - ``(version_t, neighbour, go_id)``
     - ``evidence_code``, ``qualifier``, ``assigned_by``.
       **No moment column**
   * - ``gains``
     - ``(window, query, aspect, go_id)``
     - the platform's truth as the evaluator uses it: ``closure``, ``stratum``,
       and in PK the already-known excluded. **Which closure** is a column,
       not an assumption
   * - ``feature_tokens``
     - ``(embedding_config_id, sequence_id, feature)``
     - one row per distinct vocabulary feature a sequence lights:
       ``max_value`` de-scaled, then ``max_position``, ``fraction`` and
       ``span`` RELATIVE TO THE LENGTH, in [0, 1], and ``rarity`` in the
       corpus
   * - ``terms``
     - ``(cut, go_id)``
     - ``aspect``, and the text vectors BY REFERENCE: ``vector_uri`` and
       ``vector_sha256``, never the 2560 floats themselves
   * - ``term_ancestors``
     - ``(cut, go_id, ancestor)``
     - the relation to each strict ancestor
   * - ``window_terms_out``
     - ``(window, go_id)``
     - the axis terms this window's rule leaves OUT, so negatives are drawn
       from the axis minus this
   * - ``residue_refs``
     - ``(embedding_config_id, sequence_id)``
     - ``shard_key``, ``shard_sha256``, ``position``: the per-residue codes
       NAMED and never copied

The keys are the whole decision. A protein's neighbours at ``t`` and a
protein's state at ``t`` do not depend on ``t'``, so neither table is keyed by
the window. One write per ``t``, read by every horizon that starts there.

**``version_t`` is why ``queries`` carries two version columns.**
``snapshot_pair`` names the window, and ``version_t`` is the column that joins
to ``neighbourhood`` and ``neighbour_state``, which are keyed by it and not by
the window. Deriving one from the other at load time would mean reconstructing
a join key from a label, so both are stored.

**Three keys name a field that varies, and that is the point of them.**
``feature_tokens`` carries ``embedding_config_id`` because two configurations
summarise the same sequence differently; ``terms`` and ``term_ancestors`` carry
``cut`` because two cuts disagree about which terms exist, by 10,4 % between
this campaign's validation and test pivots; and ``gains`` carries ``closure``
because the same window under two closures is two different truths. Drop any
one of the three and rows from two levels occupy one key, which is the defect
class this project keeps paying for: a level must be named by every field that
varies.

The terms a window leaves out are stored, not the ones it keeps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Added 2026-11-08. The loader draws negatives from the term axis, and without
this table it draws them from the WHOLE axis. A term created after an early
window then appears as a negative there, and the model learns that new terms
are never gained, which is an artefact of the rule and not biology. What the
loader needs is exactly the axis terms it must not treat as negatives.

**It is stored as the complement because of the size, measured.** Over the 64
windows of revision ``d5416224``, on one axis of 43.558 live terms:

.. list-table::
   :header-rows: 1

   * - quantity
     - min
     - median
     - max
     - total
   * - ``axis - TOI``, which is this table
     - 207
     - 1.339
     - 4.087
     - 106.010
   * - ``TOI - axis``, which the loader never sees
     - 0
     - 2.481
     - 3.286
     -

Storing each window's terms of interest instead would be 64 times 43.558, about
2,79 million rows, for the same information.

**And it is ``axis - TOI``, not "the complement of the TOI".** The terms of
interest are what is live in both ENDS, and a term can be live in both ends
while being obsolete in the pivot's era. So the set is not contained in the
axis: in **42 of those 64 windows it holds more terms than the axis does**. A
reader told only two of the three counts would compute the third wrongly, which
is why the writer emits ``toi_terms_off_axis`` as its own number instead of
leaving it to be inferred.

**A window whose rule does not trim gets an EMPTY table, not the
intersection.** The function that computes the terms of interest does not look
at ``toi_rule``: it always intersects the two ends' live sets, and a separate
condition decides whether that intersection is applied. ``pivot_terms`` is the
DEFAULT of the rule vocabulary, so a writer that computed the intersection and
forgot the condition would remove terms from the loader's negatives that the
window's gain never excluded. Every window of this campaign is
``live_in_both_ends``, so nothing in the data would have shown it. The
condition therefore lives in ONE function that both the evaluation and the
writer call, with a test that asserts the identity and that the old inline
comparison is gone.

**And one invariant is asserted across tables, per window.** No gained term may
appear in its own window's ``window_terms_out``. A gain is ``new - old``
restricted to ``(new & TOI) | (new & old)``; a gained term is by definition not
in the old side, so it can only come from ``new & TOI`` and lies inside the
terms of interest. This is the only check that can see IN THE DATA what sharing
the function cannot: if the two rules ever diverge, an identity test on the
import still passes. Measured on 2026-11-08 over four windows: zero violations
against 3.346 to 5.241 gained terms each. The gain is read through the single
verified door the truth has, so the check cannot drift from the read.

The axis is a snapshot and the cut is a release, and they are two fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Added 2026-11-08, correcting a design that had them as one. ``cut`` is the
``ontology_snapshot_id`` that indexes ``terms`` and ``term_ancestors``: the term
AXIS. ``cut_version`` is the GOA release the leakage rule compares against,
``t' <= cut_version``. Two different things, and the first draft of the writer's
payload carried one field for both.

Neither is derived from the other, in the payload or in the manifest. Deriving
one would be the step that makes a number stop being attributable: a bundle
whose leakage rule was evaluated against a release nobody declared cannot say
what it is leak-free with respect to.

The leakage rule is applied when the bundle is BUILT
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Added 2026-11-08, after it was needed. A window whose ``t'`` is after
``cut_version`` cannot train a model cut there: its gain is in the model's
future. The laboratory's loader checks that rule and refuses the whole bundle,
so this is not the only guard. It is the one that stops the bundle existing.

**It is not hypothetical.** The first run of the writer declared
``cut_version`` 211 over 64 windows, and **fifteen of them ended after 211**:
every transition from 211 to 212 through 225 to 226. All fifteen fall INSIDE
the span of the long validation window, 211 to 227, so their gains are part of
the truth that window scores. The table that run published holds ontology terms
rather than annotations, so nothing leaked; but the bundle declared a scope
that was not its own, and the next table written for those windows would have
leaked the validation truth into a training set.

So the writer refuses the window, the way it already refuses one on another
pivot, and for the same reason: a manifest that is wrong about its own scope is
worse than a run that fails.

Which windows train the model is a separate question and not this record's. The
constraint this section states is only the bound: for a bundle that must not
leak into the long validation window, ``cut_version`` has to be at or before
the release that window opens on.

The window is the join key, and it is the full id
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The loader joins ``window_terms_out`` to ``queries`` on ``window`` as text. If
one table carried the full ``evaluation_set_id`` and the other a semantic name
or an eight-character prefix, the negatives restriction would simply not apply
and **nothing would fail**: no error, no empty result, just a model trained
against negatives that were never excluded.

So both tables write the full id, and the writer asserts that the two tables
agree about their window set before publishing either. Both are built from one
payload in one pass, so the assertion should be trivially true, which is
exactly why it is cheap to keep for the day one table is written from a
different list. The loader counts ``windows_with_terms_out`` on its side, so a
disagreement is visible from both ends.

The truth is exported, never re-derived
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``gains`` carries the label the reading head trains against, and it is the
platform's own: the per-side closure, the stratum, and in PK the exclusion of
what ``t`` already knew. It would be derivable in the laboratory from the two
states, and that is exactly why it is exported instead. Two derivations of one
truth mean a model trained against one label and scored against another, and
the difference would not announce itself.

**Which closure is a column.** The two frames do not agree everywhere: NK and
LK coincide, and PK differs by graph-only gain, 28.333 entries that gain an
ancestor without gaining an annotation. A ``gains`` table that said only
"truth" would hide that PK's truth depends on the graph it is closed in, so the
frame is recorded per row.

**``closure`` takes two values, ``pivot`` and ``native``, and it is DERIVED.**
``pivot`` is a window whose two native snapshots are both the pivot, so both
ends close under one graph. ``native`` is a window whose ends close under their
own eras. The value comes from comparing the window's two natives against its
pivot and is never a field anybody writes, for the same reason the frame digest
is computed rather than declared: a written value can disagree with the row it
describes and nothing would see it. Measured over the 131 evaluation sets in
the database, two values are enough and a third is not needed: **122 are
``native`` and 9 are ``pivot``**.

**It is NOT ``truth_profile``.** That field says how the truth is derived,
``projected`` or ``as_closed``; ``closure`` says which graphs the two ends
close in. They vary independently, and the probe that settles the open decision
below is the proof: both of its windows are ``projected`` and they differ only
in ``closure``. Copying one into the other would leave the column unable to
record the one distinction it exists for.

**And ``closure`` alone does not make two ``native`` windows comparable.** Of
the 122, every one pivots on ONE OF ITS OWN TWO ENDS, which loses almost
nothing on intersection. A window that closes natively and pivots on a distant
third snapshot loses 7,6 % of its old end's terms and 8,9 % in BPO, by the
table below. Same ``closure``, different objects. Comparability needs
``closure`` AND
``pivot_snapshot_id``, which is already a column of its own, and this is the
defect class the three keys above exist to prevent: a level must be named by
every field that varies. **No window of that third shape existed before this
design**, which is worth knowing when its first numbers look unfamiliar.

The leak is a missing column, not a rule
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A neighbour's state at ``t'`` is leakage at inference: at prediction time the
future of the neighbourhood is exactly what is not known. Two ways to keep it
out were considered.

**Enforced on read.** Normalise state by ``(protein, version)`` and let one
shared reading function return, for a window, only entries at ``t`` or earlier
plus the query's own ``t'``. This is the smaller schema, and it was rejected: a
parquet is a file. It is read by ``pd.read_parquet`` in a training repo, in a
notebook, by a collaborator, none of which import this project's contracts. A
protein that is a query of one window and a neighbour of another has its
``t'`` rows in the file, and whoever joins on protein and version sees them.
The guarantee would hold only while every reader is the right one.

**Enforced by the schema**, which is what this ADR decides.
``neighbour_state`` has no ``moment`` column. There is nowhere to write a
neighbour's future, so the illegal state is not forbidden, it is
unrepresentable. A reader cannot make the mistake because the file cannot hold
the mistake.

This is the same choice the frame design already makes: the holdout guard
refuses a prospective block rather than trusting a convention about it
(ADR-D40).

What the keys cost and save, measured
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Measured on release 227: **9.940.364 annotations over 691.942 proteins, 14,37
per protein**. The valid 211 to 227 window declares **40.531** queries. At
``N = 100`` neighbours:

.. list-table::
   :header-rows: 1

   * - per window
     - rows
   * - the query's own state at ``t`` and ``t'``
     - 1.164.861
   * - the neighbours' state at ``t``
     - **58.243.047**
   * - the neighbourhood
     - 4.053.100

The neighbour states are fifty times the query states, and they are precisely
the part that does not depend on ``t'``. Keying them by ``version_t`` instead
of by window is therefore the whole volume decision:

.. list-table::
   :header-rows: 1

   * - horizons from one ``t``
     - keyed by window
     - keyed by what it depends on
     - saving
   * - 1
     - 59.407.908
     - 59.407.908
     - 1,0x
   * - 5
     - 297.039.540
     - 64.067.352
     - 4,6x
   * - 10
     - 594.079.079 (~47 GB)
     - 69.891.656 (~5,6 GB)
     - **8,5x**
   * - 20
     - 1.188.158.159 (~95 GB)
     - 81.540.266
     - **14,6x**

So the normalisation that matters is by version, which the rejected option also
offered. What this decision adds is that the saving does not cost the
guarantee: splitting state by ROLE gets both.

The term tables are two for the same reason as the state
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A term's aspect and its text vector are properties of the term. Its relation to
an ancestor is a property of a PAIR. Those are two grains, so they are two
tables, and ``term_ancestors`` holds the strict ancestors only: a term is not
its own ancestor, and a table that admitted the reflexive pair would make every
consumer decide whether to filter it.

The feature tokens are summarised where the shards are
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The model does not read the per-residue code. It reads, per query protein, one
token per distinct vocabulary feature that protein lights. The shards hold the
code and nothing else can: 5.344 of them, 16,9 MB each, 90,6 GB over
296.944.034 residues and 43.240.843.072 entries, and they live in this
machine's object store. Reading them from the compute node over its wifi is
hours; recomputing the vocabulary pass there duplicates the work. So a platform
operation summarises them where they are, and the dataset names its table by
sha.

**Every descriptor is relative, and that is a correctness requirement.** Three
of the five are ratios in [0, 1]: ``max_position``, the position of the maximum
as ``index / (length - 1)``; ``fraction``, the share of residues where the
feature fires; and ``span`` over the length. The other two are ``max_value``,
the de-scaled maximum activation, and ``rarity``, the feature's share of the
corpus, also in [0, 1]. **None is an absolute residue count**, because the rows
ship as float16, and float16 represents integers exactly only to 2048: 2049
becomes 2048 and 8259 becomes 8256. The corpus holds
sequences past 8.000 residues, so an absolute position stored this way would
give a plausible wrong answer for a long protein and nothing would say so. The
length itself is an integer, ``length`` in ``queries``.

**The de-scaling is verified, not assumed.** The maximum must be the value the
quantiser means, not the raw level, because a level is a bin index and is not
comparable across features. That table is a laboratory artefact, but its
fingerprint is already in the platform: the configuration's ``model_name`` is
``residue-vocabulary:06fed75ca76d17be:f3a7fe16``, and ``model_name`` is one of
the twelve identity fields of an embedding configuration's uuid5. So the
operation takes the quantiser as a declared input and REFUSES unless its
fingerprint is that one. Summarising with the wrong table stops being unlikely
and becomes impossible.

**Nothing is filtered.** All distinct features are stored. Measured over the
40.016 sequences of the 211 to 227 window with this vocabulary: **3.287
distinct features per protein on average**, 40 % of the 8.192, with quantiles
1.490 / 2.318 / 2.888 / 3.336 / 3.771 / 4.112 / 4.712 and a maximum of 5.644,
rising with length from a median of 1.760 below 100 residues to 4.264 above
2.000. That is 131,5 million rows for those sequences, about 1,3 GB with the
feature as int16 and the five values as float16.

And 2.924 features appear in more than half of all proteins, 1.329 in more than
90 % and **489 in more than 99 %**: six per cent of the vocabulary is a nearly
constant background. Dropping them would raise the signal and lose the
derivation's fidelity, so the platform keeps them and ``rarity`` says which
they are. It is the feature's share of the CORPUS and never of the exported
population, so the same feature carries the same rarity in every dataset. A
frequency filter is a modelling choice and belongs in the loader as a declared
ablation, where it can be turned off; baked into the export it could not be
ablated without producing the dataset again.

The vocabulary code is referenced, never copied
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``residue_code`` already carries the reference: ``sequence_id``,
``embedding_config_id``, ``shard_key``, ``shard_sha256``, ``position``,
``residue_count`` and ``entry_count``. ``queries`` stores
``(shard_key, position)`` and verifies with ``shard_sha256``.

Copying is not an option at this scale: the repaired population
``43193409-de70-526b-ae8d-43892a744049`` holds 683.887 sequences in 5.344
shards over 296.944.034 residues (ADR-D51, and the three identities repaired on
2026-11-04).

Many horizons in one export
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The train side already supports it and needs no change:
``dataset.train_snapshot_pairs`` is ``jsonb``, ``snapshot_pair`` is a reserved
per-row column, and ``_stream_train_shards`` returns the pairs it saw. One
export per cut with every ``(t, t')`` inside it is what that column is for.

``dataset.eval_snapshot_pair`` is a single ``varchar``, so the held-out side is
one window. That is left as it is: the official evaluation remains
``run_cafa_evaluation`` over the platform's window, and the dataset's held-out
side serves internal selection during training, where held-out proteins inside
the same windows are enough.

The residue count is denormalised, and that is deliberate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``residue_code`` already holds it, so ``queries`` repeats it as ``length``. The
feature tokens the loader builds divide by it to get a relative position, a
fraction and a span, so without the column it would open a shard to read one
integer.
One int64 against a file open is a trade worth naming rather than discovering.

The population is the gainers, and that is a declared bias
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``queries`` holds the proteins that GAIN annotation between ``t`` and ``t'``:
the union of NK, LK and PK, which is what ``_training_dump_loaders`` derives
today and what the evaluator scores. For the reading head that is right, since
the thing trained on is the thing scored.

For a world model it is a bias, and it is written here rather than left to be
found. Every example gains something, so the model never sees a protein whose
knowledge did not change, which in the corpus is the common case. A later
export adds a declared sample of non-gainers with a column that distinguishes
them. Until then the manifest says the population is the gainers, because a
population that lives only in a loader is the same defect
``export_evaluation_targets`` was written to end: the operation still has no
field with which to declare one.

The term axis is one pivot, and it is not the test windows'
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every window in this dataset pivots on one snapshot, ``releases/2022-07-01``,
and that is what ``cut`` names in ``terms`` and ``term_ancestors``. One pivot
is what makes 53 windows comparable to each other: the 130 evaluation sets
already in the database use 62 distinct pivots, each window closing on one of
its own two ends, and a delta read under a different graph is a different
delta.

**The pivot predates the cut, which is the property that matters.** Release 211
was published 2022-09-16 and the pivot's OBO is 2022-07-01, so a model cut at
211 is entitled to that graph: it is anachronistic for the old side of an early
window, never a graph from the model's future. And it is the construction the
validation window uses. ``evaluation_set ea0f4334``, the long window that
reproduces all ten of the laboratory's counters, carries the pivot and BOTH
native snapshots at ``releases/2022-07-01``.

**What one pivot costs, measured.** The share of each era's live terms that has
no non-obsolete row in the pivot, which is the share a native-era closure would
lose on intersection:

.. list-table::
   :header-rows: 1

   * - era
     - CCO
     - MFO
     - BPO
     - all
   * - 2016-05
     - 4,8 %
     - 5,3 %
     - 8,9 %
     - 7,7 %
   * - 2018-04
     - 3,9 %
     - 4,1 %
     - 7,2 %
     - 6,1 %
   * - 2021-10
     - 0,2 %
     - 0,6 %
     - 1,4 %
     - 1,1 %
   * - 2022-07
     - 0 %
     - 0 %
     - 0 %
     - 0 %
   * - 2025-10
     - 2,8 %
     - 4,3 %
     - 1,4 %
     - 2,3 %

It is not uniform and it does not point the same way on both sides. Before the
pivot the loss concentrates in BPO, twice what it is in CCO, which is the same
thinning recorded elsewhere: BPO lost 26 % of its edges over the series while
raw annotations grew. After the pivot it inverts and MFO is the aspect that
suffers. A window's thinning depends on which side of 2022-07 it sits.

**And the largest discontinuity is not inside this dataset.** Of the pivot's
43.558 live terms, **10,4 % have no non-obsolete row in the test window's own
pivot** ``releases/2025-07-22``, 13,4 % of them in MFO; against the prospective
band's ``releases/2026-01-23`` it is 13,3 %, and 14,0 % in MFO. That gap is
larger than anything between the training windows and the validation window,
it is closed because rebuilding a held-out window on another pivot would spend
a pass that is spent, and it applies to any future confirmation window for a
model cut at 211, whose axis is 2022's for good. The manifest records each
window's survival against the pivot per aspect, three numbers a window, so the
asymmetry is a covariate a declared ablation can weigh or exclude on rather
than a bias nobody can see.

The neighbourhood is declared in the manifest
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Not just the rows: the manifest records the embedding configuration, the search
backend, ``N``, and the self-exclusion rule. The backend is ``numpy`` and only
``numpy``, because it is the only member of ``ORDER_INVARIANT_BACKENDS``; the
payload's own default is ``faiss``, which is approximate and order dependent,
and a neighbourhood that moves between runs would carry into the dataset the
exact class of defect exploration 3 closed.

**The self-exclusion rule is ``accession``**, declared in the manifest as
``self excluded by accession; twins kept``. The query is not its own donor: its
state arrives through ``query_state``, and as a neighbour at 100 % identity it
would be a shortcut to copying itself. Its twins stay, because an accession
sharing a sequence is a separate entry annotated by other people and its
annotation is real evidence.

That distinction is a quarter of the corpus and not a corner case. Measured:
629.895 sequences carry one accession, **54.153 carry more, and 162.640
accessions are involved**, which is 23,8 % of the 684.048 with a sequence; the
largest group is 127 accessions over one sequence. Excluding by sequence would
take, from those queries, two real annotated entries on average.

**The margin and the drop resolve the same rule, and that is load-bearing.**
The search asks for ``k`` plus a margin so that ``k`` survive the drop. Mixing
the two rules has already cost this project a measured defect: a pre-search
asking ``k+1`` and dropping by accession against a method dropping by sequence
returned short lists for 264 of 14.032 queries, 887 donor slots, and the method
filled the difference from whatever else its bank held.

**And this rule is not the method's.** ``predict_go_terms`` runs with
``exclude_self_neighbour`` false, which is ``none``: the kNN method keeps its
own vote, which is what axis C decided over 387 of 387 panels. The dataset's
rule is a third value, chosen for a table where the same information has a
column of its own, and it changes nothing about the method.

Open decisions
~~~~~~~~~~~~~~~

- **Which closure the training windows use.** OPEN, and the measurements
  above are what it turns on. Two constructions reach the same code path,
  since ``_compute_delta`` takes both natives and the pivot separately: give
  every window the pivot as both natives, as ``ea0f4334`` does, and nothing is
  lost on intersection but a 2016 old side is closed under a 2022 graph, so
  ancestors the graph added later count as already known and the delta shrinks;
  or give each side its own era's snapshot, which is faithful to the era and
  pays the loss in the table above. The first matches the validation window and
  isolates annotation dynamics from graph dynamics, which is worth something
  given the 28.333 graph-only gain entries measured in PK. The second keeps the
  old side honest about what its era could express. One early window generated
  both ways settles it on ``delta_proteins`` and the strata instead of on
  mechanism, and until it is settled ``gains`` carries ``closure`` so a dataset
  says which one it is.
- **A plural held-out side.** Deferred, for the reason above. It becomes a
  schema change the day internal selection needs several horizons.
- **Importing the text vectors or referencing them.** SETTLED: by reference.
  The laboratory holds them for the cut's snapshot, 43.558 terms by 2560 in
  float16, which is that snapshot's non-obsolete term count exactly. ``terms``
  carries ``vector_uri`` and ``vector_sha256`` and never the floats, so the
  224 MB are not copied into every export, for the same reason the vocabulary
  code is referenced. A loader therefore fetches the table once and aligns it
  on ``go_id``, which is also why the vectors are not part of this dataset's
  row count. It changes the day a dataset has to be self-contained to be
  published outside.

  **The sha256 is the identity and the uri is only a locator**, the same split
  as ``obo_sha256`` against ``obo_uri``: a loader must refuse a file whose
  hash is not the one ``terms`` declares, and must refuse one missing any term
  of the axis, because a vector table silently short of the axis trains on a
  smaller vocabulary than the dataset says. ``vector_uri`` is an artifact-store
  URI when the file has been published there and NULL until it has, never a
  machine-local path: a path that resolves on one machine and not on the next
  is a column that promises what it cannot deliver, and the sha travels while
  the path does not.

  **One file per cut, and a loader may refuse a second.** ``vector_sha256``
  repeats down a cut's rows and every row of one cut must name the same file,
  because the cut IS the term axis: two files for one axis would be two
  vocabularies behind one key, which is the defect the three keys above exist
  to prevent. A cut whose rows name two shas is malformed, not a choice, and
  the loader that rejects it is right to.
- **A second neighbour list by sparse code.** It needs a protein-level grouped
  per-residue vocabulary configuration, which does not exist. The schema leaves room:
  ``neighbourhood`` is keyed by ``(version_t, query, neighbour)`` with the
  source of the ranking as a column, so a second list is more rows and not a
  new shape.
- **``assigned_by``.** It travels in ``query_state`` and
  ``neighbour_state`` although the minimal model does not read it yet,
  because a column nobody asked for is cheaper to carry than an export to
  re-run. In the PAIR table it stays missing, where it is a family change. ``annotation_meta`` has three columns
  and this is a fourth. It is a family change, unlike everything else here.

Before building on it: this path has never run here
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``export_research_dataset`` has never run in this instance. ``dataset`` holds
zero rows and ``job`` holds no record of the operation in any state. So
publishing to the artifact store, inserting the ``Dataset`` row and reconciling
``schema_sha`` with ``manifest_sha`` is untravelled ground in this database.

The first step is therefore not these five tables. It is one small run of the
export that already exists, over a single window, to see a ``Dataset`` row land
with its manifest and its two digests. A break in that path is cheaper to find
under the old schema than under five new tables standing on it.

Consequences
~~~~~~~~~~~~~

- The pair table and its twenty-one families are untouched. Nothing that reads
  them today changes.
- A neighbour's future cannot be written, so no reader has to be trusted not to
  read it.
- The volume of a cut grows with its number of ``t`` values and not with its
  number of horizons.
- A ``Dataset`` now means one of two shapes, so the manifest states which.

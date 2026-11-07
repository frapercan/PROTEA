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

Decision: seven tables, each keyed by what it actually depends on
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One ``Dataset`` row, one manifest, seven parquets. The existing pair table stays
as it is and is not extended: it is good for what it is, and the model does not
read it at that grain.

.. list-table::
   :header-rows: 1

   * - table
     - grain
     - holds
   * - ``queries``
     - ``(window, query)``
     - sequence identity, the per-residue vocabulary code's
       ``(shard_key, position)`` and ``shard_sha256``, the residue count, the
       stratum, ``snapshot_pair`` and the horizon in days
   * - ``query_state``
     - ``(window, query, term, moment)``
     - ``moment`` is ``t`` or ``t'``; term, evidence code, qualifier,
       ``assigned_by``. At ``t'`` the non-experimental annotations go in too
   * - ``neighbourhood``
     - ``(version_t, query, neighbour)``
     - distance, identity, similarity, coverage, taxonomy, rank
   * - ``neighbour_state``
     - ``(version_t, neighbour, term)``
     - term, evidence code, qualifier, ``assigned_by``. **No moment column**
   * - ``gains``
     - ``(window, query, aspect, term)``
     - the platform's truth as the evaluator uses it: the per-side closure,
       the stratum, and in PK the already-known excluded. **Which closure**
       is a column, not an assumption
   * - ``terms``
     - ``(cut, term)``
     - aspect and the text vector
   * - ``term_ancestors``
     - ``(cut, term, ancestor)``
     - the relation to each strict ancestor

The keys are the whole decision. A protein's neighbours at ``t`` and a
protein's state at ``t`` do not depend on ``t'``, so neither table is keyed by
the window. One write per ``t``, read by every horizon that starts there.

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

``residue_code`` already holds it, so ``queries`` repeats it. The feature cards
the loader builds divide by the length to get a relative position, a fraction
and a span, so without the column it would open a shard to read one integer.
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

- **A plural held-out side.** Deferred, for the reason above. It becomes a
  schema change the day internal selection needs several horizons.
- **Importing the text vectors or referencing them.** The laboratory holds
  them for the cut's snapshot, 43.558 terms by 2560 in float16, which is that
  snapshot's non-obsolete term count exactly. The decision here is to reference
  them by sha256 rather than copy 224 MB into every export, for the same reason
  the vocabulary code is referenced. It changes the day a dataset has to be
  self-contained to be published outside.
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

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

Decision: five tables, each keyed by what it actually depends on
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One ``Dataset`` row, one manifest, five parquets. The existing pair table stays
as it is and is not extended: it is good for what it is, and the model does not
read it at that grain.

.. list-table::
   :header-rows: 1

   * - table
     - grain
     - holds
   * - ``queries``
     - ``(window, query)``
     - sequence identity, the per-residue vocabulary code's ``(shard_key, position)`` and
       ``shard_sha256``, the stratum, ``snapshot_pair`` and the horizon in days
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
   * - ``terms``
     - ``(cut, term)``
     - ontology position, text, vector

The keys are the whole decision. A protein's neighbours at ``t`` and a
protein's state at ``t`` do not depend on ``t'``, so neither table is keyed by
the window. One write per ``t``, read by every horizon that starts there.

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

Open decisions
~~~~~~~~~~~~~~~

- **A plural held-out side.** Deferred, for the reason above. It becomes a
  schema change the day internal selection needs several horizons.
- **A second neighbour list by sparse code.** It needs a protein-level grouped
  per-residue vocabulary configuration, which does not exist. The schema leaves room:
  ``neighbourhood`` is keyed by ``(version_t, query, neighbour)`` with the
  source of the ranking as a column, so a second list is more rows and not a
  new shape.
- **``assigned_by`` in the pair table.** ``annotation_meta`` has three columns
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

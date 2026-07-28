ADR-D45: The producer seam in the reranker feature export
=========================================================

:Status: Accepted
:Date: 2026-07-10
:Author: Francisco Miguel Pérez Canales
:Phase: T-GOBERNANZA

.. note::

   This ADR was drafted on 2026-06-22 with the seam framed as a
   train/serve VALUE skew (the incident recorded below). On 2026-07-10 the
   mechanism was traced to code and data, and the true shape is narrower and
   sharper: it is a PRODUCER seam, not a value skew and not a storage
   problem. Three declared feature families shipped semantically null through
   the export while every fingerprint matched. This record now states that
   verified mechanism and the decisions that follow from it. The
   value-skew framing and the warn-only mitigation already shipped for it are
   retained below as supporting context, because both the incident and the
   code are real.

Context: the fingerprint pins names, not values or producers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
PROTEA fingerprints its reranker feature set and refuses to score with a
booster whose feature schema has drifted. The fingerprint is computed in
``protea-contracts`` (``src/protea_contracts/feature_schema.py``):

- ``compute_schema_sha(columns)`` (line 253) hashes ``"|".join(sorted(columns))``,
  UTF-8 encoded, through ``short_sha`` (``_hashing.py`` line 29, which returns
  ``sha256(blob).hexdigest()[:12]``).
- ``compute_feature_schema_sha(families, drop)`` (line 273) builds one part
  per family as ``f"{fam}={','.join(sorted(cols))}"`` (lines 292 to 294),
  joins the parts with ``"|"``, and hashes that (lines 297 to 298).

Both digests are functions of column NAMES and, for the family-aware digest,
family membership. Neither can see a value, which producer wrote a column, or
whether a producer ran at all. Identical names plus identical family
membership yield an identical sha regardless of what was actually written into
the cells.

The six LAFA signals are already declared in the same file. In
``NUMERIC_FEATURES`` they appear as ``classifier_score`` (line 113),
``classifier_present`` (line 114), ``self_prior_score`` (line 116),
``association_total`` (line 119), ``association_cross`` (line 120) and
``association_present`` (line 121). In ``FEATURE_FAMILIES`` they are grouped
as ``classifier`` (line 232), ``self_prior`` (line 233) and ``association``
(line 234). They are first-class members of the canonical schema. A booster
that selects those families passes the schema-sha guard by construction.

The export writes a well-defined zero, not the truth
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The leaf-record builder that materialises each ``(query, candidate-GO)`` row
fills all six columns with a constant ``0.0``. The static method
``_lafa_default_fields()`` in ``protea/core/_leaf_record_builder.py`` (defined
at line 342) returns:

.. code-block:: python

   return {
       "classifier_score": 0.0,
       "classifier_present": 0.0,
       "self_prior_score": 0.0,
       "association_total": 0.0,
       "association_cross": 0.0,
       "association_present": 0.0,
   }

Its own docstring states the intent plainly. ``self_prior_score`` is
overwritten by the native compute when the ``compute_self_prior`` payload flag
is set, and, in the docstring's words, "the ``classifier_*`` and
``association_*`` columns stay zero until later lafa-integrate slices wire
their producers. A well-defined zero, not NaN, matching the lineage
convention." The producers for the ``classifier`` and ``association`` families
were never wired into the export path, so those five columns are emitted as a
constant ``0.0`` for every row of every shard.

(Note on naming: the six columns are filled by ``_lafa_default_fields()``. A
separate method ``_reranker_default_fields()`` at line 34 of the same file
fills the KNN reranker block for non-KNN rows and does NOT touch the six LAFA
columns. Earlier prose that attributed the zero-fill to
``_reranker_default_fields()`` named the wrong method.)

The sealed run recorded the consequence. The frozen comparison for the
consolidated champion,
``protea-reranker-lab/results/clean_227230/comparison.json``, carries
``feature_exclusions`` = "association_* and classifier_* (zero-filled in
export) + id/label/category/aspect/snapshot_pair/qualifier/evidence_code/
taxonomic_relation". The lab excluded the two families precisely because the
export shipped them as constant zeros.

The live database is not the problem
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This is a producer seam in the EXPORT, not a storage defect. A 50,000-row
sample of ``go_prediction.features`` taken on 2026-07-10 shows 0.0% zeros for
``association_total`` and 0.0% zeros for ``classifier_score``: the production
prediction path writes real values into the JSONB blob
(``protea/infrastructure/orm/models/embedding/go_prediction.py``, the
``features`` column at line 115). The seam is that the frozen research dataset,
the artefact the lab trains on, receives a well-defined zero where the live
row holds a real value. The fingerprint matched throughout, so nothing failed.

Honest scope: the sealed number is intact
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The sealed champion (``f_micro_w`` 0.40765, frame v227 to v230,
config ``d8979601``) was trained WITHOUT the ``classifier`` and
``association`` families, exactly because they were zero-filled and excluded.
The seam therefore did not corrupt the sealed number. What it did is quieter
and worth stating without softening: it silently removed three declared
feature families from the model's reach. The champion was chosen from a
feature space three families smaller than the schema advertised, and no check
ever flagged the absence.

Decision
~~~~~~~~
D45 is a PRODUCER seam. Three declared feature families shipped semantically
null through the export while every fingerprint matched. The governance that
follows treats a produced value, its absence, and its provenance as
first-class, rather than trusting name-level identity to imply value-level
truth.

1. **No silent default.** When no producer ran for a declared column, emit
   ``NaN`` (LightGBM already reads ``NaN`` as missing) or refuse to write the
   row. A zero is a claim ("this signal was measured and it is zero"); missing
   is the truth ("no producer measured this"). The current
   ``_lafa_default_fields()`` zero convention makes an absent producer
   indistinguishable from a genuine zero, and that is the exact confusion the
   seam exploits.

2. **A degeneracy check at export.** A declared feature family that is
   constant across a shard fails the export job loudly instead of shipping. A
   family that is present in the schema but never varies is, by definition,
   carrying no information, and that condition must be an error at write time,
   not a discovery made months later when a comparison file happens to record
   it.

3. **Provenance per family.** Which producer, at which version, over which
   snapshot, recorded alongside the dataset. This is the ``SignalConfig.source``
   of the planned signal store: the family's value provenance travels with the
   artefact so a consumer can see not only that a column is present but who
   produced it and under what inputs.

Evidence that the seam fired
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This is not a hypothetical. Two datasets in the reranker lab carry the **same**
``feature_schema_sha``, ``775611822dd9``, and differ in exactly these families.
``results/clean_227230/comparison.json`` records the sealed run at that
fingerprint with ``feature_exclusions: association_* and classifier_*
(zero-filled in export)``. ``results/clean_227230/clfassoc/SUMMARY.md`` records a
second arm at the same fingerprint, run with ``compute_classifier`` and
``compute_association`` switched **on**, where the five previously zero-filled
features carry real values. One fingerprint, two semantically different
datasets. That is precisely the confusion the fingerprint exists to prevent, and
it had been recorded in the project's own results directory for months.

Implementation status
~~~~~~~~~~~~~~~~~~~~~
All three consequences above are implemented as of PR #710.

1. **No silent default.** ``_LeafRecordBuilder._lafa_default_fields()`` returns
   ``float("nan")`` for all six columns. LightGBM already reads NaN as missing,
   so a family with no producer is missing rather than zero. When a producer
   **is** wired, ``apply_export_parity_features`` first resets that family to a
   true-zero baseline, because the producers only overwrite the candidates they
   hit: a non-hit of a produced family is a genuine ``0``, while a non-hit of an
   absent family stays ``NaN``.

2. **The degeneracy check.** ``parquet_export._assert_no_degenerate_families()``
   raises when a family recorded as produced is constant across a split. It is
   evaluated at the consolidated split level rather than per per-category shard,
   because a per-shard check would false-fire: no-knowledge proteins carry no
   experimental known terms, so ``association_*`` is a legitimate all-zero on
   the NK shard. Only families recorded as produced are checked, so a
   declared-absent family never fails an export.

3. **Provenance per family.** The dump manifest now carries a
   ``feature_family_provenance`` key, written by
   ``_training_dump_loaders._dump_family_provenance``: one
   ``{family, state, producer}`` record per family, where ``state`` is
   ``produced`` or ``declared_absent``. A reader learns a family's status from
   metadata instead of inferring it from a column of zeros. ``SignalConfig``
   remains the destination for the fuller provenance (producer version, input
   snapshot) once the signal store lands; the manifest key is its first surface.

The same three states are mirrored, for human readers, in the
``protea_contracts.feature_docs`` registry (``FeatureStatus.PRODUCED`` /
``DECLARED_ABSENT``), which is the single source the Sphinx feature reference
renders from.

What D45 is NOT
~~~~~~~~~~~~~~~
Promoting the six columns from the JSONB blob to typed columns is a separate,
additive change (on the order of 2.5 GB) that would let the redundant
``features`` JSONB be dropped in a reviewed follow-up step. The blob currently
holds roughly 75 GB of ``go_prediction``'s 101 GB, and about 54 of its 60 keys
duplicate typed columns on the same row (the model mirrors typed columns into
the blob; see the ``build_feature_jsonb`` note next to the ``features``
column). Typing the columns and dropping the duplicated blob is a real storage
win. It does NOT close D45. A typed ``classifier_score`` column with no
producer wired into the export would still be filled with a default and would
still ship semantically null under a matching fingerprint. D45 is closed by
the three decisions above (no silent default, degeneracy check, per-family
provenance), not by a storage migration.

Supporting context: the value-skew incident and the shipped mitigation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The same seam produced a train/serve VALUE skew earlier in the campaign. On
2026-06-21 an INT-8 native re-run regressed the reranked LAFA ``f_micro_w``
from 0.3745 to 0.3462: a booster trained while the ``association`` producer was
effectively a constant column was then served against real ``association``
values. The column names were unchanged, so ``feature_schema_sha`` matched and
the guard passed silently. That incident and this export producer seam are two
faces of the one invariant the name-level fingerprint cannot enforce: that the
same producers, at the same configuration, filled the same columns.

A conservative, warn-only mitigation for the value-skew face already ships:

- ``protea.core.operations.predict_go_terms._blob_provenance`` computes a
  stable value-provenance descriptor and a 12-hex digest over the blob families
  from the live predict payload (which producers are active plus reachable
  config markers, plus a per-family ``BLOB_PRODUCER_VERSIONS`` constant). It is
  the value-provenance counterpart to ``feature_schema_sha``.
- ``RerankerScorer.record_blob_provenance`` (in
  ``protea/core/operations/predict_go_terms/_reranker_scorer.py``) runs after
  the schema-sha guard. It always emits a ``reranker.blob_provenance`` event,
  and, when the payload carries an expected provenance that disagrees with the
  live one, emits a loud ``reranker.blob_provenance_mismatch`` warning and
  proceeds (the expected marker is nullable, so it never refuses). Scoring
  numbers are byte-identical when provenance matches or is absent.

That mitigation makes the seam OBSERVABLE at serve. The three decisions in this
ADR make it a hard error at EXPORT, which is where the three families were lost.

Consequences
~~~~~~~~~~~~~
- A declared feature family can no longer ship as a constant default while a
  matching fingerprint reports health. The export fails loudly (decision 2),
  or writes ``NaN`` the booster reads as missing (decision 1).
- The absence of a producer is now recoverable from the artefact itself
  (decision 3): the dataset records who produced each family and under what
  inputs, so a stranger auditing the frozen dataset can tell a measured zero
  from an unwired producer.
- The sealed 0.40765 champion is unaffected: it was trained without the two
  zero-filled families, so re-homing them behind real producers is an additive
  expansion of the feature space, not a correction to the sealed number.
- The storage question (typed columns, dropping the ~75 GB duplicated blob) is
  decoupled from the governance question and can proceed on its own reviewed
  schedule without being mistaken for a fix.

Resolution
~~~~~~~~~~
Open (partially implemented). The mechanism is verified against code and data
as of 2026-07-10. The warn-only blob-provenance observability described above
is in place. The three decisions (no silent default, export degeneracy check,
per-family provenance via the signal store's ``SignalConfig.source``) are the
governance this ADR adopts; they are additive and land as the signal store is
built. Until they land, treat any ``classifier`` or ``association`` column read
from a legacy frozen dataset as unwired-in-export, not as a genuine zero.

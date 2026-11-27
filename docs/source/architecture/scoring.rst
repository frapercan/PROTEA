How candidates become scores
============================

.. contents:: On this page
   :local:
   :depth: 2

A transfer method does not store scores. The nearest-neighbour transfer and the
InterPro mirror store **candidates**: one row per (protein, term) with the
evidence for it, such as the distance to the donor, the alignment identities
and the share of neighbours that voted for the term. Turning that evidence into
one number is a step of the method, ``score_prediction_set``, and its result is
a prediction set of its own, a **scored set**, whose rows carry the score.

The evaluator scores what a method stored and nothing else. Every prediction
file it reads is a set's ``method_score``, as stored, and a set that stores
candidates without scores is refused before anything is staged, with this step
named.

What the step does
------------------

``score_prediction_set`` reads a candidate set protein by protein, in chunks,
and for each protein, in this order:

1. keeps the nearest row per term and, when a depth is named, only the
   candidates within it, with the neighbour aggregates recounted at the cut;
2. checks that the cut was applied;
3. drops what the bank denies (:mod:`protea.core.denials`), the bank being the
   candidate set's annotation set and the ontology its own;
4. scores each row with a ``ScoringConfig``: a weighted average of evidence
   signals in [0, 1], optionally times the evidence weight and an information
   prior (:mod:`protea.core.operations._candidate_scoring`);
5. writes the scores into the scored set: one ``prediction_score`` row per
   protein, with its terms, ascending, and their scores. The candidates and
   their evidence stay in the candidate set.

Proteins are independent at every step, so the scores do not depend on the
chunk size. A failed run leaves nothing behind: the worker rolls its
transaction back.

.. code-block:: json

   {
     "operation": "score_prediction_set",
     "queue_name": "protea.jobs",
     "payload": {
       "prediction_set_id": "<the candidate set>",
       "scoring_config_id": "<the arithmetic>",
       "max_k_position": null
     }
   }

The arithmetic is always named: there is no default formula. The fallback the
evaluator used when it was given none, ``1 - distance / 2``, computes what the
configuration ``embedding_only`` computes for every candidate that carries a
distance, as a neighbour transfer's candidates all do. The ``method_score``
formula is refused, since it returns a score candidates do not have, and so is
a set that already carries its own scores.

When a reranker scores the candidates
-------------------------------------

The nearest-neighbour method can score its candidates with a reranker while it
predicts, with one booster for every candidate or one per knowledge category.
The reranker's score is then the method's score, stored as each row's
``method_score``, so that set holds scores already and ``score_prediction_set``
refuses it. The InterPro BP graft, a deploy setting
(``serve.interpro_bp_graft``), combines InterPro's evidence with that score as
the method predicts, and only then, and the prediction set's record says so
(``interpro_bp_graft``).

How the scores are stored
-------------------------

A scored set holds three things per (protein, term), the protein, the term and
the score, and nothing else: they are simple transformations of evidence the
candidate set already stores. So it does not hold ``go_prediction`` rows. On the
development store a ``go_prediction`` row costs about 593 bytes (2026-11-20:
24,2 million rows, 7,1 GB of table and 6,6 GB of its eight indexes), most of it
columns a score leaves empty and indexes a scored set never uses.
``prediction_score`` holds one row per protein with two aligned arrays, the term
ids and the scores. Measured on the same store, the 691.226 pairs of one
nearest-neighbour set scored with ``embedding_only`` take 13,97 MB, 20,2 bytes
per pair, about 29 times less than that average, and a sweep over several
configurations and every language model stays small.

The scores are ``double precision``, as a method's own ``method_score`` is, so
the prediction file the evaluator writes from either form is the same to the
last digit. One query reads both forms (``_method_score_predictions``): a set
stores one form or the other, and a set holding both is refused.

One door for scores
-------------------

A set's scores have one way in and one way out. In: the method that predicts
(``score_prediction_set`` for a candidate set, a reranker as the method
predicts, an ingested method's own scores). Out: the reader the evaluator uses
(``_method_score_predictions``), which also serves the CAFA export
``GET /embeddings/prediction-sets/{id}/predictions-cafa.tsv``: the same rows,
the same order, the same four decimals, and for a window the same targets,
read from the window's own truth.

``score_prediction_set`` refuses a configuration that weights a signal the
candidates do not carry, such as an alignment score over candidates retrieved
without alignments, which would otherwise score every row with nothing.

Retired on 2026-11-20 as parallel doors: the API's on-the-fly scoring and
reranking streams (``score.tsv``, ``rerank.tsv``) and single-pair score, its
metrics endpoints (``metrics``, ``reranker-metrics``) with ``protea.core.metrics``,
which computed a Fmax of its own with exact term matching and no propagation,
and the CAFA export's own arithmetic, ``max(0, 1 - distance)``, which no other
part of the platform used.

Retired on 2026-11-21, the last of them: the detail page of a prediction set
computed a score in the browser (``computeScore``), a copy of the arithmetic
that knew five signals and not the neighbour vote fraction, so under
``vote_fraction`` it showed 0 on every row. A scored set's page shows the
scores it stored, and a candidate set's page lists the scored sets made from
it, with their configurations; neither computes a score. Configurations are
made on the scoring page and applied by ``score_prediction_set``.

Votes weighted by distance
--------------------------

Two signals weigh each neighbour's vote by how close the neighbour is. Neither
is stored: both are taken at the cut from each candidate's donor ledger
(``protea.core.operations._donor_recount``), so a configuration that weights
either is refused unless the run names a depth (``max_k_position``, or
``max_sequence_rank`` with every count made in sequences). Both are fractions
in [0, 1].

``neighbor_similarity_vote``
   For a term, the summed similarity ``1 - d/2`` of the neighbours within the
   depth that carry it, over the summed similarity of every neighbour within
   the depth: a similarity-weighted vote, the form of DiamondScore with
   similarities for bit scores. A term every neighbour carries scores 1.

``neighbor_kernel_fraction``
   For a term, the sum of ``h / (h + d)`` over the neighbours within the depth
   that carry it, divided by the depth: goPredSim's reliability index over
   ``k`` neighbours, with its bandwidth ``h = 0.5``.

No row holds the similarity vote's denominator, the neighbourhood: a ledger
names only the donors of its own term. The step rebuilds it as the union of
the ledgers of one protein's candidates, in one aspect when the set's record
says its search ranked each aspect on its own (``aspect_separated_knn``), and
across aspects when it ranked one list; a set whose record does not say is
refused for this signal. The union is the neighbourhood when every neighbour
left a candidate, as every neighbour of an aspect-separated search donates a
term of its aspect, and the step asserts it: within the depth the ledgers must
name the positions 1 to ``min(k, n)``, where ``n`` is the deepest position any
of them reaches, since no row records how many neighbours the search returned.
A protein whose ledgers skip a position stops the run, with the protein, the
aspect and the missing positions in the message. A neighbour leaves no
candidate when every term it donated was dropped, for instance as a denial of
the query.

Two presets weight one signal each, ``similarity_vote`` and
``kernel_fraction``. They are defined beside the others
(``NEIGHBOURHOOD_VOTE_PRESETS`` in ``protea/services/_scoring_models.py``) and
created one at a time through ``POST /scoring/configs``. They are not in the
list the seed-all endpoint ``POST /scoring/configs/presets`` creates, which
creates every preset an instance lacks, the ones it deleted included.

What a scored set records
-------------------------

Its columns are the candidate set's: the same annotation set, ontology,
embedding configuration, query set and retrieval depth. Its ``meta`` is the
candidate set's record, so the scored set still says how its candidates were
retrieved, with these keys set by the step:

``scored_from``
   The candidate set's id.
``scoring``
   The arithmetic (formula, weights, evidence weights, params), the depth cut,
   and the code revision and library versions that applied them.
``scoring_config_id``
   The ``ScoringConfig`` row the arithmetic was read from.
``denials``, ``denied_removed``
   The denial policy, and how many candidates it removed.
``job_id``
   The job that scored the set.

The method seal (:mod:`protea.core.method_seal`) reads ``scoring`` as part of
the method: two scored sets compared on another axis must agree in it.
``scored_from``, ``scoring_config_id`` and ``denied_removed`` are execution:
a method re-run and scored again has another candidate set, the same arithmetic
may sit in two rows, and how many candidates a policy removes depends on the
queries.

How it was checked
------------------

While the evaluator still scored candidates itself, the scored set's prediction
file, the file ``cafaeval`` reads, was compared with the file the evaluator
wrote from the candidates with the same configuration, byte for byte:

- in the test suite, against Postgres, under two configurations, with and
  without a cut;
- on the development store, over every protein of the nearest-neighbour
  candidate sets it had scored, each case inside a transaction that was rolled
  back. The script that ran it compared against the evaluator's path and was
  retired with it; the record below names the code by commit and by file
  digest.

Since then the test suite pins the prediction file the evaluator writes from a
scored set against scores worked by hand, under the same two configurations,
with and without a cut (``tests/test_scoring_a_candidate_set.py``).

.. literalinclude:: ../_data/scored-set-parity-development-store.json
   :language: json

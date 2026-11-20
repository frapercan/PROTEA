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
5. writes one row per (protein, term) into the scored set: the score as
   ``method_score``, the donor of the nearest row as ``ref_protein_accession``.

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

The benchmark
=============

.. contents:: On this page
   :local:
   :depth: 2

The benchmark is four windows of :doc:`lafa-frame`, the frame that builds a
window's truth the way LAFA builds its own windows: three windows to train on
and one to validate on, each scored with LAFA's information accretion at its
start. Methods are compared on it during development. The held-out test is
LAFA's own window, scored once, at the end.

The windows
-----------

Each window runs between two cuts with a photo: a GOA release, the GO release
its corpus was loaded under, and the Swiss-Prot release paired with it. The
numbers are GOA releases.

==============  ==========  ==========================================================
window          role        what the role allows
==============  ==========  ==========================================================
160 -> 194      training    a method may be fitted on its gains
177 -> 205      training    a method may be fitted on its gains
194 -> 211      training    a method may be fitted on its gains
211 -> 227      validation  a method is selected and read on it, never fitted on it
==============  ==========  ==========================================================

The roles are the ones the windows these replace carried, and
:mod:`protea.core.window_declarations` defines them.

The test is LAFA's window ``Sep_2025_Mar_2026``, GOA 227 -> 230, whose truth
LAFA published. It is the project's held-out test: it is read once, at the end,
by the author's word, and no store in development builds it.

How a window is built
---------------------

Name the rule set and the role, and nothing else; the rule set fixes the four
rules, the closure and the population (:doc:`lafa-frame`):

.. code-block:: json

   {
     "operation": "generate_evaluation_set",
     "queue_name": "protea.jobs",
     "payload": {
       "old_annotation_set_id": "<the start's corpus>",
       "new_annotation_set_id": "<the end's corpus>",
       "truth_rule_set": "lafa-frame",
       "window_role": "training"
     }
   }

The information accretion LAFA scores a window with is a table over the
window's start, with LAFA's thirteen codes and only its Swiss-Prot cohort:

.. code-block:: json

   {
     "operation": "compute_information_accretion",
     "queue_name": "protea.jobs",
     "payload": {
       "ontology_snapshot_id": "<the start's ontology>",
       "annotation_set_id": "<the start's corpus>",
       "evidence_regime": "lafa",
       "cohort": "swissprot"
     }
   }

How a method is scored on it
----------------------------

A method is scored on what it stored. A set whose method scores its own
candidates (a reranker at prediction time, a method run in the laboratory and
ingested, the own-annotations baseline) is evaluated as it is. A
nearest-neighbour or InterPro set stores candidates, and is scored first with
``score_prediction_set`` (:doc:`scoring`), which names the configuration and
the depth; the scored set is what is evaluated.

PROTEA's evaluator, ``cafaeval-protea``, runs ``cafaeval`` with LAFA's flags
(``-prop fill -norm cafa -no_orphans``, a threshold step of 0.01, and
``-known`` with PK's known terms for PK), at the window's pivot, which for this
frame is the start's ontology. Three settings are the benchmark's and have to
be named:

.. code-block:: json

   {
     "operation": "run_cafa_evaluation",
     "queue_name": "protea.evaluations",
     "payload": {
       "evaluation_set_id": "<the window>",
       "prediction_set_id": "<the method's predictions>",
       "information_accretion_set_id": "<the table over the window's start>",
       "toi_rule": "live_in_both_ends",
       "restrict_gt_to_predicted": false
     }
   }

- ``information_accretion_set_id``: the table above, the one over the
  window's start with the regime ``lafa`` and the cohort ``swissprot``.
- ``toi_rule``: ``live_in_both_ends``, the terms live in both ontologies,
  which are the terms the window's truth was trimmed to.
- ``restrict_gt_to_predicted``: ``false``. LAFA scores every target, whether
  the method predicted it or not; the evaluator's default drops the targets a
  method left out, which measures a different quantity.

What it replaces
----------------

Before this benchmark, each pair of corpora had two windows of the October
frame, one over every protein and one over the Swiss-Prot list of the start
(``superset_at_t0``). The eight are marked superseded
(``supersede_evaluation_sets``), each pointing to the window of LAFA's frame
over the same corpora, with the date and the reason. Nothing was deleted: the
results computed on them stay records of what was measured.

Read with care
--------------

These windows span between two and a half and three and a half years of GO,
by the dates of their two ontologies; LAFA's span about six months. Each
end is closed under its own ontology, as LAFA does, so a term that gained a
parent between the two ontologies is a gain at t1 with no new annotation, and
that is large on windows this long. LAFA counts these gains and so does the
frame, which records how many on every window:
``lafa_pk_proteins_made_by_the_ontology`` and
``lafa_pk_pairs_made_by_the_ontology``, counted over every protein before the
population. :doc:`lafa-frame` lists it with the frame's other known flaws.

The record
----------

The windows, their information accretion tables and the windows they replace,
as the development store holds them, written by
``scripts/benchmark/write_record.py``:

.. literalinclude:: ../_data/benchmark-development-store.json
   :language: json

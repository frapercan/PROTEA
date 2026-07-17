PROTEA at a glance
==================

This page is the whole system in one read, for someone who just arrived. It
assumes no background: every term is explained in place, and each part links on
to the chapter that develops it. If you clone this repository and run it, this
is the map.

What this is
------------

A protein is a chain of amino acids that does a job in the cell. Its *function*
is that job, written in a shared vocabulary called the Gene Ontology, or GO:
terms like *binds DNA* or *located in the mitochondrion*. Most proteins ever
sequenced carry no such labels; there are more than 250 million unreviewed
sequences against fewer than 600,000 manually curated ones. PROTEA reads a
protein's sequence and proposes its GO terms, with the evidence for each one in
plain view.

How it works, in one breath
---------------------------

PROTEA does not guess a function from the sequence directly. It *retrieves*. It
turns each protein into a compact code, finds the most similar proteins that are
already labelled, and lets those neighbours vote for GO terms; this is
*k-nearest-neighbours*, kNN. A second stage, a *reranker*, weighs that vote
against other clues (how strong each match is, how a candidate term relates to
what we already know) and calibrates a final score. The full flow, from UniProt
sequence to a scored prediction, is in :doc:`architecture/index`.

What it achieves
----------------

On a fair test, PROTEA reaches ``f_micro_w`` 0.40765 (a weighted,
information-aware F-measure) and ranks first in seven of the nine evaluation
cells. The nine cells are three knowledge regimes, from proteins we know nothing
about to proteins we already know something about, crossed with the three
branches of GO. The board and its reproduction path are in :doc:`results` and
:doc:`operate/reproduce-the-sealed-board`.

Why the test is fair
--------------------

The hard part of this field is not the model, it is not cheating. PROTEA is
scored on a *temporal holdout*: we freeze what was known on one date and ask
only about function that was discovered afterwards, so the answer cannot leak
into the question. The metric definition, the scoring recipe, and the per-band
ontology and information-content registry live in one place,
:doc:`architecture/evaluation`. The sealed board never moves; regenerated
numbers are candidates until reviewed against it.

Where it stops, honestly
------------------------

Two of the nine cells are not won: predicting the Biological Process branch for
the least-studied proteins. The evidence available does not reach there yet. We
name it the biological-process wall and we show it, rather than hide it, and we
point to the first crack in it (a representation trained on written descriptions
of function). The design lessons behind all of this, including why standardising
a representation matters more than its depth, are collected in :doc:`insights`.

Where to go next
----------------

- New here and want to run something? The quickstart gets you from a clone to a
  first job in about ten minutes: :doc:`operate/reproduce-the-sealed-board`.
- Want the architecture, the queues, and the operation catalogue?
  :doc:`architecture/index`.
- Want the design decisions and their trade-offs? The ADRs in
  :doc:`appendix/index`, and the operational lessons in :doc:`insights`.

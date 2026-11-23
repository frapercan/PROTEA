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

PROTEA does not guess a function from the sequence directly. It gathers
*candidate* terms from several sources and learns how far to trust each one.
A protein already says something about itself: its own annotations that no
experiment has confirmed yet. Similar proteins say more: PROTEA turns each
protein into a vector with a protein language model, finds the closest
proteins that are already labelled, and lets them vote; this is
*k-nearest-neighbours*, kNN. Terms that usually appear together say the rest.
A learned reader then weighs every candidate against all of these clues and
gives it a calibrated score. The full flow, from UniProt sequence to a scored
prediction, is in :doc:`architecture/index`.

How it is studied
-----------------

Everything the project has explored sits in one line of research, by stages:
representations of the protein, sources of candidates, signals, two learners
that read the same evidence (a world model that predicts how a protein's
knowledge will change, and decision trees), evaluation by strata, and routing
per stratum. It is laid out in :doc:`research_line`. The corpus each method may
use is fixed per cut, as a photo whose contents are shown in :doc:`corpus`.

What it achieves
----------------

Two results, on two frames that should not be compared with each other.

- **The sealed board (October 2026).** On the earlier temporal frame, PROTEA
  reached ``f_micro_w`` 0.40765 (a weighted, information-aware F-measure) and
  ranked first in seven of the nine evaluation cells. The nine cells are three
  knowledge regimes, from proteins we know nothing about to proteins we already
  know something about, crossed with the three branches of GO. The board and
  its reproduction path are in :doc:`results` and
  :doc:`operate/reproduce-the-sealed-board`.
- **The current frame.** The windows now follow the LAFA benchmark's rules
  exactly, over the corpus photo of each cut. On the validation window 211->227
  the champion today orders the protein's own annotations with a model fitted
  on the earlier window and reaches 0.3543 in the same measure, mean of the nine
  cells; transfer from the nearest neighbour with the best of eight language
  models reaches 0.3241. The current state is on :doc:`news`.

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
of function). The design lessons behind all of this, including
which of the choices turn out not to have mattered, are collected in
:doc:`ablation`.

Where to go next
----------------

- Want the research behind it, and where it stands? :doc:`research_line` and
  :doc:`news`.
- Want to see the data each cut holds? :doc:`corpus`.
- New here and want to run something? The quickstart gets you from a clone to a
  first job in about ten minutes: :doc:`appendix/installation_and_quickstart`.
- Want to rebuild the board itself? That is a different and longer road, and
  :doc:`operate/reproduce-the-sealed-board` is honest about how long: there is
  no single-command reproduction, and two of the artifacts it needs are not
  currently located.
- Want the architecture, the queues, and the operation catalogue?
  :doc:`architecture/index`.
- Want to know how the method was arrived at, and which parts of it the
  measurements say could have been left alone? That is the ablation:
  :doc:`ablation`.
- Want the design decisions and their trade-offs? The ADRs in
  :doc:`appendix/index`, and the operational lessons in :doc:`insights`.

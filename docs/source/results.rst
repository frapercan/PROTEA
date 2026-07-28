Results
=======

.. contents:: On this page
   :local:
   :depth: 1

The sealed board
----------------

PROTEA's headline result on the leakage-free temporal frame is a weighted,
IA-aware micro F-measure (``f_micro_w``) of **0.40765**, first in seven of the
nine evaluation cells (NK/LK/PK by BPO/MFO/CCO).

- **Frame.** Test window v227 to v230; validation window v225 to v227. The
  window is temporal by construction: the reference pool is frozen at t0 and
  the ground truth is the delta gained by t1, so the score is free of the
  data leakage that inflates tools scored against a current database.
- **Champion.** The learned k-WTA retrieval encoder (config ``d8979601``,
  which stores GO-aligned codes rather than a raw PLM vector, a choice the
  representation ablation in :doc:`insights` motivates) for candidate
  generation, followed by a stacked per-category re-ranker (see
  :doc:`/adr/D43-stacked-meta-reranker`).
- **Metric and scoring.** ``f_micro_w`` is the headline metric, scored
  board-faithfully with ``cafaeval`` on the sealed settings (OBO
  ``releases/2025-07-22``, the t0 IA artefact, the release
  terms-of-interest, ``prop=fill``, ``norm=cafa``, ``no_orphans``, and
  ``-known`` on PK only so Partial-Knowledge excludes already-known terms).
  The protocol, the per-band OBO/IA registry, and the LAFA parity mapping
  are documented in full in :doc:`/architecture/evaluation`, which is the
  home of the metric definition and the scoring recipe.

The two cells not won are LK-BPO and PK-BPO: the Biological Process wall. It is a
limit of the ranking stage, not of the available evidence. 97.0 percent of the true
terms missed on those cells already exist in the pre-window vocabulary, 95.2 percent
by the information accretion the metric weights by, and the candidate pool the
pipeline already retrieves is worth ``f_micro_w`` 0.7519 at precision 1.000 to a
perfect ranker, and up to 0.7764 to the best ordering of it, while the re-ranker
delivers 0.2131 of that. First in seven of nine is
the honest standing on this frame, and the wall is a characterised limit that the
measurement, its method, and the levers it rules out set out in
:ref:`insight-bp-wall-is-a-ranking-limit`.

.. admonition:: One number, one home
   :class: note

   This chapter states the board once. The metric definition and the
   ``cafaeval`` recipe live in :doc:`/architecture/evaluation`; the
   re-ranker design lives in :doc:`/adr/D43-stacked-meta-reranker`; the
   step-by-step reproduction path lives in
   :doc:`/operate/reproduce-the-sealed-board`. Numbers are not restated elsewhere in
   the book; the other chapters cross-reference this board.

Reproducing the board
----------------------

The board is produced on-platform, every result carrying the job id that
produced it, on a frame that reproduces bit-identically across two
independent runs. The ordered path (stand up the stack, load the v227
snapshot, compute the learned-encoder codes, retrieve, re-rank, and score
with ``cafaeval`` on the sealed settings) is documented in
:doc:`/operate/reproduce-the-sealed-board`, which is explicit about which stages are
job-backed today and which are not yet automated.

Provenance of the earlier figures
----------------------------------

An earlier version of this chapter reported an Fmax board on the GOA 220 to
229 window with ESM-C 300M embeddings and a three-generation LightGBM
progression. That frame was superseded (different metric, different backbone,
different window) and its numbers were never the sealed board. The full text
is retained for provenance only in :doc:`/historical/pre-v227` and must not be
quoted as a current result.

.. seealso::

   - :doc:`/architecture/evaluation`: the CAFA temporal-holdout protocol, the
     ``f_micro_w`` definition, the per-band OBO/IA registry, and the LAFA
     scoring parity mapping.
   - :doc:`/adr/D43-stacked-meta-reranker`: the stacked per-category
     re-ranker design.
   - :doc:`/operate/reproduce-the-sealed-board`: the ordered reproduction path.
   - :doc:`/historical/pre-v227`: the superseded pre-v227 figures, retained
     for provenance.

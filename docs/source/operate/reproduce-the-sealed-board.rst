Reproduce the sealed board
==========================

This page names the ordered path to reproduce PROTEA's sealed board (the
``f_micro_w`` headline stated in :doc:`/results`). It is deliberately honest
about which stages are automated on-platform today and which are not. It does
not restate the number and it does not print commands that have not been
verified against the codebase; each stage links to the runbook or ADR that
carries the exact, verified payloads.

.. admonition:: There is no single-command reproduction
   :class: warning

   The board is produced stage by stage, every result carrying the job id
   that produced it, on a frame that reproduces bit-identically across two
   independent runs (see :doc:`/runbooks/reproducible-eval-frame`). There is
   no one orchestrator job that runs all stages end-to-end, and the
   re-ranker itself is trained out of this repository (see step 5). Treat the
   steps below as the map, not as a script.

The ordered path
----------------

1. **Stand up the stack.** Bring up the API and workers from a checkout with
   ``bash scripts/manage.sh start`` (Postgres and RabbitMQ must already be
   running). Installation and first-run details are in
   :doc:`/appendix/installation_and_quickstart`. All work below is dispatched
   as jobs via ``POST /jobs`` with an ``{operation, payload}`` body; never use
   ad-hoc curl against internal endpoints.

2. **Load the v227 snapshot.** Load the GO ontology snapshot and the GOA
   annotation sets for the frame with the ``load_ontology_snapshot`` and
   ``load_goa_annotations`` operations. The canonical OBO for band v227 is
   ``releases/2025-07-22`` and the IA artefact is the t0 IA for that band;
   the authoritative per-band OBO and IA pins are in the band registry,
   documented in :doc:`/architecture/evaluation` (per-band registry) and
   enforced at runtime and in CI.

3. **Compute the learned-encoder codes.** The champion is the k-WTA retrieval
   encoder (config ``d8979601``), which stores GO-aligned codes rather than a
   raw PLM vector. Codes are materialised over the base embeddings by the
   ``apply_learned_encoder`` operation offline, and a novel query is embedded
   on the fly by ``compute_embeddings`` when its config uses the
   ``learned-code`` backend. Serving requires the head artifact to be provided
   through the ``PROTEA_LEARNED_ENCODER_ARTIFACT`` (or
   ``PROTEA_LEARNED_ENCODER_DIR``) environment variable; the exact resolution
   rules and failure modes are in :doc:`/runbooks/serve-learned-code-retrieval`.

4. **Retrieve.** Run KNN GO transfer over the learned codes with the
   ``predict_go_terms`` operation, producing a ``PredictionSet``. The
   retrieval and prediction operations and their payload schemas are in
   :doc:`/architecture/operations`.

5. **Re-rank.** The candidates are re-ranked by a stacked per-category
   re-ranker (evidence scorers plus a shallow per-category combiner), designed
   in :doc:`/adr/D43-stacked-meta-reranker`. Booster *training* is not part of
   PROTEA: it runs in the ``protea-reranker-lab`` sibling repository over a
   frozen parquet dataset that PROTEA publishes via ``export_research_dataset``.
   The trained booster is brought back into PROTEA through the
   ``POST /reranker-models/import`` endpoint (or the
   ``scripts/register_reranker.py`` helper). This step is therefore only
   partly automated on-platform: the dataset export and the import are jobs
   and endpoints here, the training is not.

6. **Score with cafaeval on the sealed settings.** Generate the cross-OBO
   evaluation set with ``generate_evaluation_set`` and score the re-ranked
   ``PredictionSet`` with ``run_cafa_evaluation``, using the board-faithful
   recipe: band ``v227``, ``prop=fill``, ``norm=cafa``, ``no_orphans``,
   ``th_step=0.01``, ``max_terms=None``, the t0 IA file, the release
   terms-of-interest file, and ``-known`` on PK only. The exact job payloads,
   the asymmetric cross-OBO pins, and the bit-identical reproduction check are
   in :doc:`/runbooks/reproducible-eval-frame`; the metric definition and the
   LAFA parity mapping are in :doc:`/architecture/evaluation`.

What a clean platform does not have
------------------------------------

The steps above name two artifacts by identity: the learned-encoder head behind
config ``d8979601`` (step 3) and a trained booster (step 5). **Neither is
produced by this repository, and neither is present in a platform brought up
from scratch.**

The dependency is wider than those two steps. ``two_tower_classifier.py`` pins
``d8979601`` as its protein-tower input, so that one config is the substrate
under the classifier as well as the head behind the board, and it names three
further artifact families it resolves from the store by environment variable:
seven ``head_seed*.pt`` checkpoints, ``go_sparse_codes*.npz``, and ``vocab_go``,
which is the trained term ORDER and therefore not reconstructible by sorting.
Five families in total, and the same answer for each.

Checked against the running instance on 2026-08-23, which was stood up clean on
2026-07-30:

.. list-table::
   :header-rows: 1
   :widths: 52 24

   * - Artifact family
     - Present
   * - embedding config ``d8979601`` (head ``ankh_base_hardneg.pt``)
     - no
   * - any config on the ``learned-code`` backend
     - no
   * - any registered ``RerankerModel``
     - no, zero rows
   * - two-tower checkpoints, ``head_seed*.pt``
     - no
   * - GO sparse codes, ``go_sparse_codes*.npz``
     - no
   * - trained term order, ``vocab_go*``
     - no

That is the clean run behaving as intended rather than a fault, but it means a
reader following this page against a fresh platform stops at step 3, and the
board in :doc:`/results` is a sealed historical result rather than something the
platform will recompute on request.

.. admonition:: Where those artifacts are is an open question, not a closed one
   :class: warning

   Neither artifact has been located on the host or on the compute node's
   system disk. That is not the same as saying they are gone. A 391 GB archive
   partition on the compute node, which the project's records describe as
   holding the pre-inversion assets, **has not been searched**: it is present
   on the device and not mounted, and mounting it needs a privilege neither
   machine's automation holds.

   So the correct reading of the table above is that the artifacts are not
   *here*, and that the one place they would most plausibly be has not been
   looked in. Until it has, this page states a precondition rather than a loss.

The head artifact has to be supplied from outside, by the environment variable
named in step 3; the booster has to be trained in ``protea-reranker-lab`` and
imported, as step 5 says. Those two sentences are the reproduction's real
precondition, and they were previously readable only by inference.

What is not yet automated
-------------------------

- There is no single end-to-end job that runs all six stages; each is
  dispatched and its output UUID threaded into the next.
- Re-ranker training lives in ``protea-reranker-lab`` (step 5), outside this
  repository. PROTEA automates the dataset export and the booster import, not
  the training.
- The IA and terms-of-interest artefacts (step 6) and the learned-encoder head
  artifact (step 3) are supplied by path or environment variable; they are not
  fetched automatically.

.. seealso::

   - :doc:`/results`: the sealed board this page reproduces.
   - :doc:`/runbooks/reproducible-eval-frame`: the verified job payloads and
     the bit-identical reproduction check.
   - :doc:`/runbooks/serve-learned-code-retrieval`: the learned encoder
     ``d8979601`` and its head-artifact configuration.
   - :doc:`/architecture/evaluation`: the metric definition, the per-band
     OBO/IA registry, and the cafaeval recipe.
   - :doc:`/adr/D43-stacked-meta-reranker`: the re-ranker design.
   - :doc:`/historical/reproduction_guide`: the superseded pre-v227 guide,
     retained for provenance only.

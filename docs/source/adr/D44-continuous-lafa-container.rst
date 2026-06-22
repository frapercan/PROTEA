ADR-D44: Continuous local deployment of the LAFA container at the current best
===============================================================================

:Status: Proposed
:Date: 2026-06-22
:Author: Francisco Miguel Pérez Canales
:Phase: T-PRODUCTO

Context
~~~~~~~
The LAFA submission and the local product are the SAME ``predict()`` that
runs in the worker (ADR-D23, ADR-D15): one method, not two. The container
is a thin wrapper over ``protea-method`` (``method_main.py``) on top of the
``protea-method-runtime`` image; the KNN, scoring, priors and reranker live
in PROTEA core, not in standalone scripts.

The product requirement (FR-3) is a runnable local inference product whose
benchmark is live on ``/benchmark`` and exposed via ngrok (NFR-PROCESS).
NFR-PERF states the LAFA ``f_micro_w`` is a floor, not a ceiling: as the
beat-lafa-1 campaign improves the number, the deployed product must follow.
Today the gap is that the best system is improved *offline* (the reranker
lab, the sealed champion) while the deployed container + the
``/benchmark`` row drift from the current best, and the native seal itself
is redeploy-gated (slice INT-8; see
:doc:`/runbooks/lafa-native-parity`).

The deployable thing is **container + data**, and the data is the
load-bearing half. "The data we put in" is a set of **versioned entities
with provenance**, each already a first-class ORM row:

- the **t0-clean reference pool** (``AnnotationSet`` + its frozen reference
  ``SequenceEmbedding`` matrix, cutoff-dated);
- the **per-category booster trio** (``RerankerModel`` rows, with
  ``feature_schema_sha`` / ``dataset_id`` / ``external_source``);
- the **OBO snapshot** (``OntologySnapshot``, ``obo_version``);
- the **IA file** (the t0 ``IA.tsv``, the ``f_micro_w`` weighting).

Decision
~~~~~~~~
Define a continuous-deployment loop that rebuilds the LAFA container and
re-pins its data to the current best whenever beat-lafa-1 lands a validated
improvement, redeploys it locally, and surfaces the new benchmark live on
``/benchmark``. The container code and the data entities version
independently and are pinned together by a single deployed manifest.

**1. The container channel (code).** The image is the existing thin wrapper
(ADR-D23: ``ghcr.io/frapercan/protea/knn-v1`` over
``protea-method-runtime``). It rebuilds only when the *method code* changes
(a new scorer, a feature, a propagation step). Image release follows the
existing release-please / tag pipeline (ADR-D29): a SemVer tag dispatches
the image build + push (ADR-D27) and the cross-repo integration smoke. No
ad-hoc rebuilds.

**2. The data channel (entities).** Each data entity is rebuilt by an
existing on-platform operation, every one UI-actionable (FR-1) and tracked
(``Job`` / ``JobEvent`` + MLflow):

- reference pool / embeddings: the export + embedding pipeline;
- boosters: trained in ``protea-reranker-lab`` from a published
  ``Dataset``, registered via ``POST /reranker-models/import`` (the
  ``feature_schema_sha`` guard refuses a drifted booster, NFR-REPRO);
- OBO snapshot: ``load_ontology_snapshot``;
- IA: ``compute_ia_for_snapshot`` over the snapshot + corpus.

The data does NOT live inside the image (ADR-D23 container contract: data
is bind-mounted, ``-v /host/data:/app/data:ro``). The deployed bundle is a
read-only directory of these versioned artifacts plus a manifest that pins
the exact entity ids / shas.

**3. The deployed manifest (the pin).** A single manifest names the current
deployed configuration by entity id + content sha: the image tag, the
reference ``AnnotationSet`` + embedding ``SequenceEmbedding`` config, the
booster trio ``RerankerModel`` ids and their ``feature_schema_sha``, the
``OntologySnapshot`` ``obo_version``, and the IA artifact. This is the
"current best" record; promoting a new best is editing this manifest and
redeploying. Provenance is end to end: every entity carries its producer
and fingerprint, so a third party reconstructs the same number (NFR-REPRO).

**4. The promotion loop (CD).** On a validated beat-lafa-1 improvement:

a. the improving lever lands on-platform (a scorer / feature / booster), the
   way the campaign already ships them (one ``EvidenceScorer`` or post-proc
   per lever, ADR-D43, no refactor);
b. the affected data entity is rebuilt by its on-platform operation and
   registered with provenance;
c. the new configuration is scored over the fixed 7401 LAFA frame by the
   parity runbook (:doc:`/runbooks/lafa-native-parity`); the new
   ``f_micro_w`` must clear the deployed best on the SAME frame + recipe at
   the converged seed band (no lucky seeds, NFR-PERF floor discipline);
d. the deployed manifest is bumped to the new entity ids / image tag and
   the local stack is redeployed (``scripts/deploy.sh`` slot at
   ``~/Thesis2/worktrees/protea-deploy``); the container picks up the new
   bind-mounted data and the new image;
e. ``/benchmark`` surfaces the new frame-stamped ``EvaluationResult``
   automatically (it is a read-only aggregator); the ngrok tunnel keeps the
   live number reachable for review (NFR-PROCESS).

**5. The redeploy is the gate, by design.** Step (d) is the only
maintainer-gated step (the hard constraint: no unattended stack restart /
redeploy). Everything upstream (rebuild data, register, parity-score) is
unattended and tracked; the loop stages a ready-to-promote manifest and the
maintainer authorizes the redeploy. This makes "continuous" mean
*continuously prepared and one authorized step from live*, not
*auto-restarting*.

Consequences
~~~~~~~~~~~~~
- The product is always one authorized redeploy from the current best; the
  deployed ``/benchmark`` number stops drifting from the campaign.
- Code and data version independently; a data-only improvement (a better
  booster, a fresher reference pool) needs no image rebuild, only a manifest
  bump + redeploy. A code improvement rides the existing tag pipeline.
- The ``feature_schema_sha`` guard plus the deployed manifest make a wrong
  booster / schema pairing impossible to deploy silently (the v6 seal-vs-serve
  skew that caused the 0.315 collapse is caught at registration, not at
  serve).
- The native == offline parity claim (NFR-REPRO) becomes maintainable: every
  promotion is parity-scored on the fixed frame before it can be promoted,
  so "every number is live-verifiable in the app" stays true over time, not
  just once.
- The redeploy gate is explicit and small (one manifest bump + one
  authorized ``deploy.sh`` run), which keeps the hard no-unattended-restart
  constraint while still delivering continuity.

Resolution
~~~~~~~~~~
Open. The container strategy (ADR-D23), the data entities (the ORM rows
above), the parity check (:doc:`/runbooks/lafa-native-parity`), and the
release pipeline (ADR-D29) all exist; D44 records the loop that binds them
and names the deployed manifest as the "current best" pin. The first
promotion is INT-8 (the native seal of the 0.3745 best), blocked only on the
maintainer-authorized redeploy described in the parity runbook.

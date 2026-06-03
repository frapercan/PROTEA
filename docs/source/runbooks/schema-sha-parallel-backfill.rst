schema_sha_v2 backfill
======================

``schema_sha`` is the feature-set fingerprint that prevents inference from
running with a re-ranker booster trained against a different feature
schema. Two definitions of ``compute_schema_sha`` coexisted (lab and
PROTEA) and caused silent drift before parity was fixed (see
:doc:`../adr/D10-schema-sha-parallel-migration`). ADR D10 decided to add
a parallel ``schema_sha_v2`` column to ``Dataset`` and ``RerankerModel``,
backfill it from ``protea_contracts.compute_schema_sha``, and switch
inference to read the new ``schema_sha_v2`` column.

As of the time this runbook was written, the T1.6 Alembic migration and
backfill script described in ADR D10 have **not yet shipped**. This
runbook documents the expected fix sequence for when T1.6 lands.
Until then, escalate to the T1.6 owner if you observe the symptoms
below.

The current ``schema_sha`` column is a 12-hex SHA-256 of the
registry feature list computed by
``protea.core.parquet_export._compute_schema_sha`` and stored in both
the ``dataset`` and ``reranker_model`` tables. The scoring router in
``protea/core/operations/predict_go_terms.py`` compares the live hash
from ``protea_contracts.compute_feature_schema_sha`` against the booster's
stored ``feature_schema_sha``; a mismatch causes the reranker to be
skipped (the ``reranker.schema_mismatch`` event is emitted at ``error``
level and inference falls back to unranked KNN output).

Symptoms
--------

- ``GET /jobs/{id}/events`` for a ``predict_go_terms`` job returns an
  event of the form::

      "event": "reranker.schema_mismatch"
      "fields": {
        "expected_sha": "<stored-sha>",
        "live_sha": "<computed-sha>",
        "reranker_model_id": "<uuid>"
      }

- The scoring router endpoints (``GET /scoring/prediction-sets/{id}/score.tsv``,
  ``…/reranker-metrics``) return scores without reranker contribution or
  return 422 with a ``schema_mismatch`` detail.

- Reranker metrics are absent from an ``EvaluationResult`` that you
  expect to carry them.

- The ``dataset`` table has rows where ``schema_sha`` was written by an
  older exporter and no ``schema_sha_v2`` column exists yet (visible once
  T1.6 ships).

Diagnosis
---------

1. **Identify the stored vs. live SHA for the affected booster:**

   .. code-block:: sql

       -- Stored SHA on the RerankerModel row.
       SELECT id, name, feature_schema_sha
       FROM reranker_model
       WHERE id = '<reranker-model-uuid>';

2. **Compute the live SHA from the running pipeline:**

   .. code-block:: bash

       # The live hash is logged in the reranker.schema_mismatch event.
       # To reproduce it manually:
       poetry run python3 - <<'EOF'
       from protea_contracts import compute_feature_schema_sha
       from protea_contracts import ALL_FEATURE_FAMILIES
       print(compute_feature_schema_sha(list(ALL_FEATURE_FAMILIES)))
       EOF

3. **Check which Datasets the affected booster references:**

   .. code-block:: sql

       SELECT d.id, d.name, d.schema_sha, rm.name AS booster_name,
              rm.feature_schema_sha AS booster_sha
       FROM dataset d
       JOIN reranker_model rm ON rm.dataset_id = d.id
       WHERE rm.id = '<reranker-model-uuid>';

4. **Confirm whether the schema_sha_v2 column exists** (only after T1.6
   ships):

   .. code-block:: sql

       SELECT column_name
       FROM information_schema.columns
       WHERE table_name = 'dataset'
         AND column_name LIKE 'schema_sha%';

   If only ``schema_sha`` appears and no ``schema_sha_v2`` column is
   present, the T1.6 migration has not run yet. Escalate to the T1.6
   owner.

5. **For each dataset, compare the original and parallel SHAs** (after T1.6 ships):

   .. code-block:: sql

       SELECT id, name, schema_sha AS sha_v1, schema_sha_v2 AS sha_v2,
              (schema_sha = schema_sha_v2) AS matches
       FROM dataset
       ORDER BY created_at;

   Rows where ``matches = false`` represent datasets written by an older
   exporter before the parity fix and must be backfilled.

Fix
---

.. note::

   The backfill script described here will ship as part of T1.6. Until
   T1.6 lands, escalate to the T1.6 owner. Do not attempt to UPDATE
   ``schema_sha`` or ``feature_schema_sha`` manually in production: an
   incorrect value will silence the mismatch guard and allow drift to
   propagate undetected.

**When T1.6 ships, follow these steps:**

1. **Apply the Alembic migration** that adds ``schema_sha_v2`` to
   ``dataset`` and ``feature_schema_sha_v2`` to ``reranker_model``:

   .. code-block:: bash

       alembic upgrade head

2. **Run the backfill script** (path to be confirmed when T1.6 ships;
   expected location based on ADR D10):

   .. code-block:: bash

       poetry run python scripts/backfill_schema_sha_v2.py \
           --dry-run        # inspect rows to update before writing

       poetry run python scripts/backfill_schema_sha_v2.py
           # apply; writes schema_sha_v2 via protea_contracts.compute_schema_sha

   The script will iterate every ``Dataset`` row and compute
   ``schema_sha_v2`` from ``protea_contracts.compute_schema_sha`` applied
   to the canonical feature list at the time the backfill runs. Rows
   where ``schema_sha_v2`` is already populated are skipped.

3. **Validate the backfill via the golden-parquet gate** (T1.8 invariant):

   .. code-block:: bash

       poetry run pytest tests/ -k "golden_parquet or schema_sha" -v

4. **Re-register affected re-ranker models** if their ``feature_schema_sha``
   no longer matches the live pipeline after the backfill. Use the import
   endpoint to update the stored SHA without re-training:

   .. code-block:: bash

       # Example: update feature_schema_sha on an existing booster row.
       # Consult POST /reranker-models/import or PATCH (if available in
       # the version that ships T1.6) for the exact payload.
       curl -s -X POST http://localhost:8000/reranker-models/import-by-reference \
           -H "Content-Type: application/json" \
           -d '{
             "name": "<booster-name>",
             "artifact_uri": "<existing-uri>",
             "run": {
               "dataset": {"schema_sha": "<new-schema_sha_v2-value>"},
               "families": ["<family1>", "<family2>"]
             }
           }' | python3 -m json.tool

5. **Verify that inference no longer raises ``reranker.schema_mismatch``:**

   .. code-block:: bash

       curl -s -X POST http://localhost:8000/jobs \
           -H "Content-Type: application/json" \
           -d '{
             "operation": "predict_go_terms",
             "payload": {
               "embedding_config_id": "<config-uuid>",
               "reranker_model_id": "<model-uuid>"
             }
           }' | python3 -m json.tool

       # Then poll the job events for reranker.schema_mismatch vs reranker.applied.
       curl -s http://localhost:8000/jobs/<job-uuid>/events \
           | python3 -m json.tool | grep -E '"event".*reranker'

Prevention
----------

**Dual-write enforced in code**

ADR D10 mandates that once T1.6 ships, ``export_research_dataset``
(``protea/core/operations/export_research_dataset.py``) and the
``ParquetExportContext`` helper (``protea/core/parquet_export.py``)
must write both the original ``schema_sha`` column (kept until F3) and
``schema_sha_v2`` on every new ``Dataset`` row. The inference path in
``predict_go_terms.py`` reads the ``schema_sha_v2`` column. No new
Dataset should be created without both fields populated.

**CI gate on schema_sha drift**

A regression test (to be added as part of T1.6) will:

1. Load a synthetic golden parquet produced by the current exporter.
2. Assert that ``_compute_schema_sha()`` (original) equals
   ``protea_contracts.compute_schema_sha(feature_names)`` (parallel
   column) for the same feature set.

The test will be run as part of ``poetry run pytest`` with no special
flags. It fails loudly when the two hash functions diverge, surfacing
drift before it reaches any database row.

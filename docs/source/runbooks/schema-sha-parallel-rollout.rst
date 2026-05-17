schema_sha_v2 rollout (T1.6)
============================

This runbook walks the operator through applying the T1.6 (ADR D10)
``schema_sha_v2`` migration in production: pre-flight backup, schema
upgrade, idempotent backfill, verification, and rollback. It is the
production counterpart of the dry-run validated by the T1.6
``schema_sha_v2`` dry-run branch against a clone of the live database.

Pre-flight checklist
--------------------

1. **Take a fresh pg_dump backup** before any schema change.

   .. code-block:: bash

       backup_dir="$HOME/Thesis2/backups"
       ts=$(date -u +%Y%m%d-%H%M%S)
       out="$backup_dir/pre-t16-$ts.dump"
       time docker exec protea-postgres-1 pg_dump \
           -U protea -F c -d protea > "$out"
       ls -lh "$out"

   The drill clone restored a 1.9G ``-F c`` dump in roughly ten
   minutes; the equivalent production dump completes in the same order
   of magnitude on the same hardware. Hold the dump path for the
   rollback step.

2. **Confirm the alembic head matches what the migration expects.**

   .. code-block:: bash

       cd ~/Thesis2/repositories/PROTEA
       poetry run alembic current
       poetry run alembic heads

   Both must print ``9d2fb5a07e4c (head)`` immediately before the
   upgrade. If they differ, stop and reconcile with the deployer.

3. **Quiesce writes to the affected tables.** Pause the experiment
   worker and any pipeline that issues ``export_research_dataset``,
   ``register_reranker``, or the ``reranker-models/import`` and
   ``reranker-models/import-by-reference`` endpoints (under the
   ``/v1/`` prefix) until the backfill finishes. The migration itself
   is additive and
   non-blocking, but the backfill needs a consistent view of every
   row.

Apply the migration
-------------------

The migration is the file
``alembic/versions/a3c1d8e2f4b6_t1_6_add_schema_sha_v2_columns.py``.
It is strictly additive: two ``ADD COLUMN`` + two ``CREATE INDEX``
statements. Postgres can perform both without rewriting either table,
so wall time stays in the low seconds even on production-scale data
(measured: under three seconds against the drill clone, which carried
the same row counts as production at the time of the dry run).

.. code-block:: bash

    cd ~/Thesis2/repositories/PROTEA
    export PROTEA_DB_URL="postgresql+psycopg://<user>:<pw>@<host>:<port>/<db>"
    poetry run alembic upgrade head

Verify both columns and their indexes landed:

.. code-block:: bash

    docker exec protea-postgres-1 psql -U protea -d protea -c "\d dataset" \
        | grep -E "schema_sha"
    docker exec protea-postgres-1 psql -U protea -d protea -c "\d reranker_model" \
        | grep -E "schema_sha"

You should see ``schema_sha`` and ``schema_sha_v2`` on both tables,
plus ``ix_dataset_schema_sha_v2`` and ``ix_reranker_model_schema_sha_v2``
in the index list.

Backfill
--------

The backfill script computes the canonical ``schema_sha_v2`` value via
``protea_contracts.compute_schema_sha`` applied to the running feature
registry, writes it to every row, and commits every ``--batch-size``
rows for resumability. Idempotent: rows where ``schema_sha_v2`` is
already populated are skipped unless ``--force`` is supplied.

Step 1: dry-run on production. Prints what would be written without
mutating any row.

.. code-block:: bash

    cd ~/Thesis2/repositories/PROTEA
    time poetry run python scripts/backfill_schema_sha_v2.py --dry-run

Step 2: apply.

.. code-block:: bash

    time poetry run python scripts/backfill_schema_sha_v2.py --batch-size 100

Wall time measured on the drill clone (single-digit row counts at the
time of the dry run): under one second end to end. Even a production
load of a few thousand rows fits well under one minute because the
script reads the canonical SHA once and writes a single column value
per row.

Verification
------------

The backfill is complete when every row has a populated ``schema_sha_v2`` column.

.. code-block:: bash

    docker exec protea-postgres-1 psql -U protea -d protea -tAc \
        "SELECT count(*) FROM dataset WHERE schema_sha_v2 IS NULL"
    # expected: 0

    docker exec protea-postgres-1 psql -U protea -d protea -tAc \
        "SELECT count(*) FROM reranker_model WHERE schema_sha_v2 IS NULL"
    # expected: 0

Spot-check that the original and parallel SHAs agree for fresh exports
(rows where the two should match because they were produced after the
parity fix):

.. code-block:: bash

    docker exec protea-postgres-1 psql -U protea -d protea -c \
        "SELECT id, name, schema_sha, schema_sha_v2,
                (schema_sha = schema_sha_v2) AS matches
         FROM dataset ORDER BY created_at DESC LIMIT 10;"

Rows produced before the parity fix will show ``matches = false``;
this is expected, and inference will pick up the ``schema_sha_v2``
column. Drift in rows where both algorithms should have agreed is a
regression and must be investigated before the next export.

Rollback
--------

The migration is safe to roll back mid-deploy: dropping the indexes
and columns has no effect on inference, which falls back to the legacy
``schema_sha`` / ``feature_schema_sha`` columns when ``schema_sha_v2``
is absent or NULL.

.. code-block:: bash

    cd ~/Thesis2/repositories/PROTEA
    poetry run alembic downgrade -1

Verify the columns are gone:

.. code-block:: bash

    docker exec protea-postgres-1 psql -U protea -d protea -tAc \
        "SELECT count(*) FROM information_schema.columns
         WHERE table_name='dataset' AND column_name='schema_sha_v2'"
    # expected: 0

If the rollback itself fails (it should not, since the migration is
additive), restore from the pre-flight pg_dump:

.. code-block:: bash

    docker exec protea-postgres-1 psql -U protea -d protea -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
    time docker exec -i protea-postgres-1 pg_restore \
        -U protea -d protea < "$out"
    poetry run alembic stamp 9d2fb5a07e4c

Measured wall times (drill clone)
---------------------------------

These numbers come from the dry-run executed by the T1.6 branch
against a 1.9G ``-F c`` dump of production restored into a
``pgvector/pgvector:pg16`` container on the same host. Re-measure on
production hardware before quoting them in a change ticket.

* ``pg_restore`` of the 1.9G dump: ~10m35s (one-shot, only relevant
  for dry-run reproducibility).
* ``alembic upgrade head`` through the T1.6 migration: ~2.85s
  (covers three pending revisions; the T1.6 step itself is the last
  one).
* ``backfill_schema_sha_v2.py --dry-run`` for 2 datasets + 51 reranker
  models: ~3.66s.
* ``backfill_schema_sha_v2.py`` (apply): ~1.53s for the same 53 rows.
  Idempotent second run: ~1.55s (no writes, all rows skipped).
* ``alembic downgrade -1``: ~0.61s.

Idempotency note
----------------

Running the backfill twice without ``--force`` is a no-op: the script
prints ``skipped=<n>`` for each table and exits zero. ``--force``
overwrites existing values; only use it to repair a partial run that
wrote the wrong digest (e.g. against a stale feature registry).

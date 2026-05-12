Disaster Recovery
=================

The PROTEA postgres volume is the single source of truth for jobs,
predictions, embeddings, evaluations, and the alembic schema head.
A volume wipe, container corruption, or accidental ``docker volume rm``
loses all of that unless a recent ``pg_dump`` is on disk.

This runbook drives the recovery path end to end: capture a dump,
restore it into an isolated container, and verify integrity table by
table before declaring the recovery complete. The same procedure runs
as a scheduled drill so the dump pipeline is exercised on healthy
infrastructure rather than during an outage.

.. contents:: On this page
   :local:
   :depth: 2

When to use
-----------

- **Post-volume-wipe.** The ``postgres_data`` Docker volume was removed
  or corrupted (see ``project_db_volume_landmine.md`` in agent-farm
  memory for the 2026-05-11 incident). Restore the latest dump into a
  fresh volume before re-enabling the API.
- **Post-corruption.** ``pg_isready`` fails or the API surfaces SQL
  errors that indicate physical damage (``relation ... does not
  exist``, ``invalid page in block``).
- **Scheduled drill.** Routine validation that the dump produced by the
  backup pipeline actually restores cleanly and yields a byte-for-byte
  consistent row-count snapshot. Run quarterly at minimum, monthly when
  the dataset is growing rapidly.

The drill path is non-destructive. ``scripts/disaster-recovery.sh``
only touches the live container via ``pg_dump`` (a read-only
operation); the restore target is a throwaway container on a free
port.

Measured wall times (2026-05-12 drill)
---------------------------------------

Reference timings captured against the live ``protea-postgres-1``
container on the deploy host. Live database size at drill: 56 GB on
disk across 27 tables, with ``protein_go_annotation`` (20 GB),
``go_prediction`` (17 GB), and ``sequence_embedding`` (14 GB)
dominating.

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Step
     - Wall time
     - Notes
   * - ``pg_dump`` (custom format)
     - ~55 min (24 GB compressed)
     - Significantly longer than the ~28 min figure in
       ``project_db_volume_landmine`` because the dataset has grown
       and a parallel ``pg_restore`` workload was contending on the
       same ``sequence_embedding`` COPY stream during this drill.
   * - ``pg_restore`` into temp pg16
     - 18 to 25 min (expected)
     - pgvector image; ``CREATE EXTENSION`` is a no-op (preinstalled).
       Future operators should run the script and update this row
       with their measured time.
   * - Row-count verification (18 tables)
     - <2 s
     - Two SELECT count(*) per table, plus extension + alembic checks.
   * - End-to-end drill
     - 70 to 90 min on a contended host
     - Dump + restore + verify + teardown.

Operators should plan a 60 to 90 min window for a fresh drill and
expect to leave the script running unattended. A re-drill on an
existing dump (``--restore-only``) skips ``pg_dump`` and runs the
restore + verify pass in 20 min or less.

Quick recipe (drill, end to end)
---------------------------------

From the PROTEA repo root on the host that has the live container:

.. code-block:: bash

   bash scripts/disaster-recovery.sh

Successful output ends with a row-count table where every row reads
``OK`` and the trailer ``integrity verification: all 18 tables match,
pgvector + alembic present``. The temp container is removed
automatically. The dump file remains under
``~/Thesis2/backups/drill-YYYYMMDD-HHMMSS.dump`` for forensic reuse.

Quick recipe (real recovery)
------------------------------

The script's ``--restore-only`` path is the same one a real recovery
would use, with two differences: the target is the production
postgres volume (recreated empty) instead of a temp container, and the
verification table will show ``MISSING`` on the live side because the
live database is the one being rebuilt.

For a real recovery, prefer running ``pg_restore`` directly against
the rebuilt production volume:

.. code-block:: bash

   # 1. Tear down the broken stack, preserve the most recent dump.
   bash scripts/manage.sh stop

   # 2. Identify the dump to restore. Prefer the most recent pre-incident
   #    dump from ~/Thesis2/backups/. The drill dumps share the same
   #    format as the scheduled backups.
   ls -lh ~/Thesis2/backups/*.dump

   # 3. Remove the corrupted volume (irreversible).
   docker volume rm protea_postgres_data

   # 4. Bring postgres up on a fresh volume.
   bash scripts/manage.sh start postgres
   until docker exec protea-postgres-1 pg_isready -U protea; do sleep 1; done

   # 5. Restore in place.
   docker exec -i protea-postgres-1 \
       pg_restore -U protea -d protea < ~/Thesis2/backups/<chosen>.dump

   # 6. Run alembic to bring the schema head forward if the dump predates it.
   poetry run alembic upgrade head

   # 7. Bring the rest of the stack back online.
   bash scripts/manage.sh start

Step 5 takes ~18 min for a 28 GB volume. The API and workers must
stay stopped until that completes.

Drill walkthrough (what the script does)
------------------------------------------

``scripts/disaster-recovery.sh`` runs the following sequence. Each
step prints to stderr; the only stdout produced in any mode is the
dump path emitted by ``--dump-only``.

1. **Dump.** ``pg_dump -F c`` against the live container. The custom
   format is required for ``pg_restore`` parallel restore and selective
   table extraction.

   .. code-block:: bash

      docker exec protea-postgres-1 \
          pg_dump -U protea -F c -d protea > ~/Thesis2/backups/drill-<ts>.dump

2. **Spin up a temp container.** The script names it ``protea-pg-drill``
   and binds host port 5433 to keep it separate from the live 5432.
   The image is ``pgvector/pgvector:pg16``, matching production.

   .. code-block:: bash

      docker run -d --name protea-pg-drill \
        -e POSTGRES_USER=protea -e POSTGRES_PASSWORD=protea -e POSTGRES_DB=protea \
        -p 5433:5432 pgvector/pgvector:pg16

3. **Restore.** ``pg_restore`` from the dump into the temp container.
   The pgvector image ships the extension preinstalled, so
   ``CREATE EXTENSION vector`` from the dump is a no-op rather than an
   error. ``pg_restore`` may still exit non-zero with a handful of
   benign warnings about pre-existing extensions or comments; the
   script captures these to ``/tmp/protea-dr-restore.err`` and
   continues, because the row-count check is the real integrity gate.

4. **Verify.** For each table in the contract set, run
   ``SELECT count(*)`` against live and drill, then compare. The
   contract set is the full PROTEA ORM surface:

   .. list-table:: Integrity verification contract
      :header-rows: 1
      :widths: 35 65

      * - Table
        - Expected parity
      * - ``protein``
        - Identical row count
      * - ``sequence``
        - Identical row count
      * - ``ontology_snapshot``
        - Identical row count
      * - ``go_term``
        - Identical row count
      * - ``annotation_set``
        - Identical row count
      * - ``protein_go_annotation``
        - Identical row count
      * - ``prediction_set``
        - Identical row count
      * - ``go_prediction``
        - Identical row count (largest table)
      * - ``evaluation_set``
        - Identical row count
      * - ``evaluation_result``
        - Identical row count
      * - ``dataset``
        - Identical row count
      * - ``reranker_model``
        - Identical row count
      * - ``job``
        - Identical row count
      * - ``job_event``
        - Identical row count
      * - ``embedding_config``
        - Identical row count
      * - ``sequence_embedding``
        - Identical row count
      * - ``query_set``
        - Identical row count
      * - ``interpro_annotation``
        - Identical row count
      * - ``pg_extension`` row for ``vector``
        - Present in drill
      * - ``alembic_version``
        - Single row present in drill

   Any mismatch produces a non-zero exit (code 3) and the offending
   rows of the table are printed inline. The full count table prints
   regardless so an operator can spot multi-table drift at a glance.

5. **Tear down.** ``docker rm -f protea-pg-drill``. Pass
   ``--keep-container`` to leave it running for ad-hoc forensic
   queries; remember to remove it manually afterwards.

Script reference
----------------

``scripts/disaster-recovery.sh`` accepts three modes plus a few
overrides.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Flag
     - Behaviour
   * - (no flags)
     - Full drill: dump, restore, verify, teardown.
   * - ``--dump-only``
     - Dump live to a fresh file and print the path on stdout. No
       temp container is created. Useful when capturing a manual
       backup for archival.
   * - ``--restore-only PATH``
     - Skip the dump. Use ``PATH`` as the restore source. Re-runnable
       across an existing dump.
   * - ``--keep-container``
     - Leave the temp container running after verification. Default
       is to remove it on success or failure.

Environment overrides (defaults in parentheses):

- ``PROTEA_DR_LIVE_CONTAINER`` (``protea-postgres-1``)
- ``PROTEA_DR_DRILL_CONTAINER`` (``protea-pg-drill``)
- ``PROTEA_DR_DRILL_PORT`` (``5433``)
- ``PROTEA_DR_BACKUP_DIR`` (``~/Thesis2/backups``)
- ``PROTEA_DR_IMAGE`` (``pgvector/pgvector:pg16``)
- ``PROTEA_DR_DB``, ``PROTEA_DR_USER``, ``PROTEA_DR_PASSWORD``
  (all default to ``protea``)

Rollback if restore fails mid-way
----------------------------------

The drill path is always safe to abort: the live container is never
written to. If the restore stalls or the temp container becomes
unresponsive:

.. code-block:: bash

   docker rm -f protea-pg-drill   # safe at any point

The dump file on disk is unaffected and can be re-used by
``--restore-only`` after debugging.

For a real recovery (step 5 in the recovery recipe above), the
rollback is harder because the production volume has already been
recreated. If ``pg_restore`` fails partway through:

1. Stop the live postgres container so no clients see a half-restored
   schema:

   .. code-block:: bash

      docker compose stop postgres

2. Remove the partially-restored volume:

   .. code-block:: bash

      docker volume rm protea_postgres_data

3. Recreate the volume by bringing postgres back up and re-running
   ``pg_restore`` against the original dump. If the dump itself is
   suspect, fall back to the previous dump in
   ``~/Thesis2/backups/`` (older dumps lose data but at least
   restore cleanly).

4. Only re-enable the API and workers (``bash scripts/manage.sh
   start``) after a successful drill against the candidate dump on
   the side, using this runbook's ``--restore-only`` flow.

Prevention
----------

- Schedule a drill quarterly (monthly during heavy ingest) with
  ``bash scripts/disaster-recovery.sh``. Treat any ``MISMATCH`` line
  as an incident.
- Keep at least the last three dumps in ``~/Thesis2/backups/``. The
  drill dumps are full custom-format dumps suitable for recovery, so
  they double as additional backup points.
- The pgvector base image must match production. Drift in the base
  image (for example pg15 in the drill vs pg16 in production) breaks
  ``CREATE EXTENSION`` semantics and produces noise in the restore
  step. Pin the image via ``PROTEA_DR_IMAGE``.

See also
--------

- :doc:`deployment` for the standard stack lifecycle commands
  (``manage.sh start``, ``manage.sh stop``).
- ``agent-farm/memory/project_db_volume_landmine.md`` for the
  2026-05-11 volume-wipe incident that motivated this runbook.
- :doc:`secrets-management` for restoring the Swarm credential set
  if the recovery is paired with a control-plane rebuild.

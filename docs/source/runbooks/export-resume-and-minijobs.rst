Resumable export + parallel minijobs
====================================

This runbook covers the ``export_research_dataset`` job: how it now
resumes after a cancel / kill / reboot, and how to run the per-snapshot
cuts in parallel.

Symptoms this addresses
-----------------------

* A long export (several hours) was cancelled, the worker rebooted, or
  the job was re-claimed, and the run restarted from cut 1, losing every
  completed snapshot-pair cut.
* ``PROTEA_EXPORT_MINIJOBS`` was pinned to ``0`` because the parallel
  path assembled the final parquet with a whole-split ``pd.concat`` that
  spiked memory (~54 GB) and OOM-killed the worker.

How resume works (default, no flag needed)
------------------------------------------

The serial dump runner stages each snapshot-pair cut's per-category
parquet shards under a STABLE directory keyed by the dataset name plus a
fingerprint of the cut-affecting config (embedding config, ontology,
version set, ``k``, feature flags). As each cut finishes it writes a
small JSON done-marker next to its shards.

* On (re-)start the runner restores every completed cut from its marker
  and resumes at the first unfinished cut.
* A marker whose shard file is missing (a cut that died mid-flush) is
  recomputed, not trusted.
* The staging directory is removed only after the consolidated dataset
  is assembled successfully. A failure or a kill leaves it in place so
  the next run resumes.

Base location: ``<repo_root>/storage/export_resume/`` (survives a
reboot, unlike ``/tmp``). Override with ``PROTEA_EXPORT_RESUME_DIR``.

To force a clean restart, delete the dataset's staging directory under
that base before re-queuing the job.

Streaming write (default)
-------------------------

Both producers now stream shards into the consolidated ``train.parquet``
/ ``eval.parquet`` through a ``pyarrow`` ``ParquetWriter`` (one ~200k-row
batch resident), so memory stays bounded regardless of dataset size:

* monolithic path: ``protea.core.parquet_export`` (the ``_SplitWriter``).
* minijobs assembler: ``protea.core.operations.export_minijobs._export_write``
  (the ``_stream_assemble`` helper, replacing the old ``pd.concat``).

The published schema is unchanged: the canonical per-record schema plus
the trailing ``snapshot_pair`` column.

Enabling parallel minijobs (opt-in)
-----------------------------------

With the streaming write the concat-OOM is gone, so the coordinator path
is viable. Enable it on the ``protea.training`` worker:

.. code-block:: bash

   export PROTEA_EXPORT_MINIJOBS=1

When set, ``export_coordinator`` partitions the cell into one
``export_knn_batch`` per train version plus one for the eval version and
dispatches them to ``protea.training.knn-batch``; each feeds
``export_features_batch`` then ``export_write``. Cuts that ran serially
(~45 min each, ~N x 45 min total) now run in parallel across the batch
workers, bounded by the worker count on those queues. The terminal write
delivery streams every per-pair shard into the final dataset and inserts
the ``Dataset`` row exactly as the serial path does.

Leave ``PROTEA_EXPORT_MINIJOBS`` unset (or ``0``) to keep the serial
path; both paths produce the same dataset contract.

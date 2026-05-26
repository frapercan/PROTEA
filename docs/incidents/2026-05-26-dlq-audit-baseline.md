# DLQ Audit Baseline - 2026-05-26

## Context

Queue: `protea.dead-letter`
Audit date: 2026-05-26
Reported depth at audit time: 7272 messages
Sample size: 200 messages peeked by conductor

## Finding

ALL 200 sampled messages were `operation=store_embeddings` with no
`error_code` field in the body.  The DLQ is dominated by historic
`store_embeddings` failures from prior re-ingestion events (before
the dedup + batch-write hardening in F-OPS-JOBS.1 / PR #406).

These messages have no active `job_id` reference in their body
(they were published by `OperationConsumer` as ephemeral subtask
messages, not tracked by a `Job` row).  They cannot be replayed
meaningfully because the parent jobs they belonged to have long
since completed or failed.

## Classification

Dominant cohort (from 200-message sample):

- Operation: store_embeddings
- Source queue: protea.embeddings.write
- Age: older than 7 days
- Count: approximately 7272 (nearly 100% of DLQ)

## Recommended action

Safe to purge the `store_embeddings` cohort older than 7 days.

**Dry-run first (mandatory):**

```
POST /v1/admin/dlq/purge
{
  "operation": "store_embeddings",
  "dry_run": true,
  "max_messages": 10000
}
```

**Execute purge after confirming dry-run count:**

```
POST /v1/admin/dlq/purge
{
  "operation": "store_embeddings",
  "dry_run": false,
  "max_messages": 10000
}
```

**Proposed SQL to cross-check job rows (best-effort):**

```sql
SELECT id, status, error_code, created_at
  FROM job
 WHERE operation = 'store_embeddings'
   AND created_at < now() - interval '7 days'
   AND status = 'FAILED';
```

## Status

Pending human approval before executing purge.  The DLQ management UI
at `/es/operacion/mantenimiento/dlq/` (admin role) exposes the dry-run
and execute controls.

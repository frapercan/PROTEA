-- reap_ghost_e0fbdf5f.sql
-- One-off recovery script for the ghost RUNNING jobs incident
-- documented in the orphan-jobs-2026-05-18 memory entry.
--
-- Root cause: InFailedSqlTransaction on session.get() inside
-- _execute_with_session bypassed the FAILED transition; the SIGTERM
-- handler ran too late (ShutdownGuard not yet armed); rows were left
-- in RUNNING indefinitely.
--
-- This script is intentionally NOT auto-run.  Apply manually after
-- verifying the target rows are genuine ghosts (no live worker):
--
--   1. Check that no worker process holds the job:
--        SELECT id, started_at, leased_until, error_code
--        FROM job WHERE status = 'running'
--        ORDER BY started_at;
--
--   2. Cross-reference with `bash scripts/manage.sh status` to confirm
--      no worker is actually processing these job IDs.
--
--   3. Run this script inside a transaction so you can ROLLBACK if
--      the WHERE clause matches more rows than expected:
--        BEGIN;
--        <run script>
--        SELECT id, status, error_code FROM job WHERE status = 'failed'
--          AND error_code = 'GhostJobRecovered' ORDER BY finished_at DESC;
--        COMMIT;  -- or ROLLBACK;
--
-- Criteria for a ghost job:
--   * status = 'running'
--   * started_at older than 24 hours (no legitimately long job should
--     exceed 24h; the production reaper catches them at 6h anyway)
--   * leased_until IS NULL or leased_until < now()  (no active heartbeat)
--   * no JobEvent in the last 2 hours  (no active progress reporting)
--
-- Adjust the interval thresholds to match your deployment.

BEGIN;

WITH ghost_candidates AS (
    SELECT j.id
    FROM job j
    WHERE
        j.status = 'running'
        AND j.started_at < now() - INTERVAL '24 hours'
        AND (j.leased_until IS NULL OR j.leased_until < now())
        AND NOT EXISTS (
            SELECT 1 FROM job_event je
            WHERE je.job_id = j.id
              AND je.ts > now() - INTERVAL '2 hours'
        )
)
UPDATE job
SET
    status       = 'failed',
    finished_at  = now(),
    error_code   = 'GhostJobRecovered',
    error_message = 'Manually recovered by reap_ghost_e0fbdf5f.sql: '
                    'job was stuck in RUNNING with no active worker or heartbeat.'
WHERE id IN (SELECT id FROM ghost_candidates);

-- Show how many rows were updated before you COMMIT or ROLLBACK.
SELECT COUNT(*) AS rows_updated
FROM job
WHERE status = 'failed' AND error_code = 'GhostJobRecovered';

-- COMMIT;  -- uncomment once you have verified the count is correct
ROLLBACK;   -- safe default: review output, then re-run with COMMIT

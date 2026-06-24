-- backfill_select_window_provenance.sql
-- One-off, idempotent backfill for the re-staged SELECT evaluation set
-- (FIX-UI-PROVENANCE, 2026-06-11).
--
-- Symptom: the eval job context renders "eval=?->?" because the re-staged
-- SELECT set (eval_set a3be0a6d) has a blank evaluation_set.window_role, and
-- its evaluation_result rows have blank frame / temporal_window / leakage_role,
-- so the UI cannot resolve the rolling-origin window label.
--
-- This stamps the ADR D40 protocol provenance for the SELECT window
-- 220 -> 227 (role = valid):
--   * evaluation_set.window_role     = 'valid'
--   * evaluation_result.temporal_window = 'SELECT_220_227'
--   * evaluation_result.leakage_role    = 'select'
--   * evaluation_result.frame           = 'internal'  (220->227 is PROTEA's
--       internal selection window, not the parity-locked LAFA 227->230 frame)
--   * evaluation_result.arms_enabled    = {knn, reranker, mlp_tower, interpro}
--       derived per row (reranker on when reranker_model_id is set)
--
-- Idempotent: every UPDATE is guarded by "IS NULL" so re-running is a no-op
-- and an already-stamped value is never overwritten.
--
-- NOT auto-run. The conductor applies it manually against the live DB after
-- the SELECT grid finishes. The set is targeted by its short-id prefix; set
-- :prefix to the full id if more than one set shares the prefix.
--
-- The forward path (newly evaluated sets) is stamped at the source by
-- run_cafa_evaluation (window_role on the set, frame/temporal_window/
-- leakage_role/arms_enabled on the result); this script only repairs rows
-- written before that fix landed.

\set prefix 'a3be0a6d'

BEGIN;

-- 1. Stamp the protocol window binding on the EvaluationSet(s).
UPDATE evaluation_set
SET window_role = 'valid'
WHERE id::text LIKE :'prefix' || '%'
  AND window_role IS NULL;

-- 2. Stamp the per-result provenance markers (frame / window / role).
UPDATE evaluation_result er
SET temporal_window = COALESCE(er.temporal_window, 'SELECT_220_227'),
    leakage_role    = COALESCE(er.leakage_role, 'select'),
    frame           = COALESCE(er.frame, 'internal')
FROM evaluation_set es
WHERE er.evaluation_set_id = es.id
  AND es.id::text LIKE :'prefix' || '%'
  AND (er.temporal_window IS NULL OR er.leakage_role IS NULL OR er.frame IS NULL);

-- 3. Derive arms_enabled per row (only where unset).
UPDATE evaluation_result er
SET arms_enabled = jsonb_build_object(
        'knn', true,
        'reranker', er.reranker_model_id IS NOT NULL,
        'mlp_tower', false,
        'interpro', false
    )
FROM evaluation_set es
WHERE er.evaluation_set_id = es.id
  AND es.id::text LIKE :'prefix' || '%'
  AND er.arms_enabled IS NULL;

-- Review before committing: confirm the set + result counts look right.
SELECT es.id AS eval_set_id,
       es.window_role,
       count(er.id)                         AS results,
       count(er.temporal_window)            AS with_window,
       count(er.leakage_role)               AS with_role,
       count(er.frame)                      AS with_frame,
       count(er.arms_enabled)               AS with_arms
FROM evaluation_set es
LEFT JOIN evaluation_result er ON er.evaluation_set_id = es.id
WHERE es.id::text LIKE :'prefix' || '%'
GROUP BY es.id, es.window_role;

-- COMMIT;  -- uncomment once the counts above are verified
ROLLBACK;   -- safe default: review output, then re-run with COMMIT

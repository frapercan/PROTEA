# Broken tests inventory

Snapshot 2026-05-05 tras T0.1 cobertura baseline. Los siguientes tests fallan en HEAD `7e8de14` (rama `refactor/post-lab-stabilization`). Corresponden a deuda preexistente o regresiones que se arreglan como parte del trabajo de F0/F2 del master plan.

## A. Resueltos en este pase (T0.1)

- `tests/test_queue.py::TestPublishJob::test_exponential_backoff_delays`
- `tests/test_queue.py::TestPublishJob::test_closes_connection_on_exception`

Causa: regresión directa del commit `e299672` (publisher retry 5 to 12). Tests actualizados a las nuevas constantes; verde.

## B. Pendientes: payload schema drift

14 tests en `tests/test_predict_go_terms.py` fallan porque sus fixtures no pasan `ontology_snapshot_id`, ahora campo requerido en `PredictGOTermsPayload` y `PredictGOTermsBatchPayload` (`predict_go_terms.py:165, 237`). Previo a este snapshot.

Tests afectados:

- `TestPredictBatch::test_transfers_go_annotations_from_nearest_neighbor`
- `TestPredictBatch::test_includes_self_as_first_reference`
- `TestPredictBatch::test_distance_threshold_filters_far_neighbors`
- `TestPredictBatch::test_limit_per_entry_caps_neighbors`
- `TestPredictBatchParentCancellation::test_skips_when_parent_cancelled`
- `TestPredictBatchParentCancellation::test_skips_when_parent_failed`
- `TestPredictGOTermsBatchPayload::test_valid_payload`
- `TestPredictGOTermsBatchPayload::test_feature_flags_default_false`
- `TestPredictBatchRerankerFeatures::test_reranker_features_included_when_enabled`
- `TestPredictBatchRerankerFeatures::test_reranker_features_excluded_when_disabled`
- `TestPredictGOTermsBatchReranker::test_skipped_when_artifact_context_missing`
- `TestPredictGOTermsBatchReranker::test_schema_mismatch_falls_back`
- `TestPredictGOTermsBatchReranker::test_applies_when_schema_matches`
- `TestPredictGOTermsBatchReranker::test_no_reranker_leaves_dicts_untouched`

Reparación: añadir `"ontology_snapshot_id": str(uuid.uuid4())` (o usar el existente del fixture) en cada payload de test. Dejado para T0.2 (safe_emit) que toca tests, o como hard-prerequisite a T2B.4 (extract class de `PredictGOTermsBatchOperation`) que reescribirá estas suites.

## C. Pendientes: scoring router metrics

- `tests/test_scoring_router.py::TestRerankerMetrics::test_returns_metrics`
- `tests/test_scoring_router.py::TestRerankerMetrics::test_empty_predictions_returns_zero_metrics`

Causa pendiente de diagnóstico. Si es deuda similar a B, mismo tratamiento.

## Política

- Cada test en B y C que pasa por una refactor de F0-F2, se arregla a la vez.
- Antes de cerrar F2, este fichero debe estar vacío o con explicación de exclusión por test.
- Cualquier nueva regresión se añade aquí con su causa documentada.

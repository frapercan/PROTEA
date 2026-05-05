# Broken tests inventory

Snapshot 2026-05-05 tras T0.1b. Suite unit verde end-to-end: **1056 passed, 10 skipped, 0 failed**.

Política: este fichero registra cualquier test que esté roto en `master` o `refactor/*`. Si está vacío, la suite está limpia.

## Histórico de resoluciones

### T0.1b — 2026-05-05

Resueltos en este pase los 16 tests rotos detectados en T0.1:

**A. Regresiones de los 5 commits previos (2 tests)**

- `test_queue.py::TestPublishJob::test_exponential_backoff_delays`
- `test_queue.py::TestPublishJob::test_closes_connection_on_exception`

Causa: commit `e299672` subió publisher retry de 5 a 12. Tests actualizados a las nuevas constantes.

**B. Payload schema drift en `test_predict_go_terms.py` (12 tests)**

- `TestPredictBatch::*` (4 tests)
- `TestPredictBatchParentCancellation::*` (2 tests)
- `TestPredictGOTermsBatchPayload::test_valid_payload`
- `TestPredictGOTermsBatchPayload::test_feature_flags_default_false`
- `TestPredictBatchRerankerFeatures::*` (2 tests)
- `TestPredictGOTermsBatchReranker::*` (4 tests)

Causa: `ontology_snapshot_id` añadido como campo requerido en `PredictGOTermsPayload` y `PredictGOTermsBatchPayload`. Fixtures y payloads inline no se actualizaron al añadirlo. Patch: 5 ediciones añadiendo `"ontology_snapshot_id": _SNAPSHOT_ID` (constante ya existente) en `_payload()` helpers e inline.

**C. EvaluationSet mock fields drift en `test_scoring_router.py` (2 tests)**

- `TestRerankerMetrics::test_returns_metrics`
- `TestRerankerMetrics::test_empty_predictions_returns_zero_metrics`

Causa: `EvaluationSet.groundtruth_uri` añadido al modelo; el helper `_make_eval_set()` lo dejaba como MagicMock truthy, lo que enrutaba el handler hacia el path persisted-artifact (que no estaba mockeado) en lugar del path on-the-fly (sí mockeado). Patch: `_make_eval_set()` setea explícitamente `groundtruth_uri = None` y `stats = None`.

## Hard rule

Antes de cerrar cualquier fase mayor del master plan, este fichero debe estar vacío o con explicación de exclusión por test.

# Slow tests inventory

Snapshot 2026-05-05 tras T0.1. Cobertura unit `--durations=30`.

Ningún test supera el umbral del master plan v3 (`5s`).

Top 5 tests más lentos:

| Test | Duración | Razón |
|------|---------:|-------|
| `tests/test_compute_embeddings.py::TestBatchSizeConsistency::test_esm_batch_size_consistency` | 3.21s | Carga ESM-2 8M para chequeo de consistencia (legítimo) |
| `tests/test_compute_embeddings.py::TestValidateLayers::test_valid_reverse_index` | 2.03s | Validación numérica intensiva |
| `tests/test_real_models.py::TestESM2_8M::test_output_shape_and_finite` | 0.45s setup | Marker `slow`, opt-in |
| `tests/test_knn_streaming_smoke.py::test_list_vs_stream_equivalence` | 0.36s | KNN smoke OK |
| `tests/test_infrastructure.py::TestCreateApp::*` | ~0.12s cada | App boot suite OK |

Política: si un test cruza 5s en ejecuciones futuras, añadir aquí con causa raíz y plan de aceleración (mock pesado, fixture de sesión, marker `slow` opt-in, etc.).

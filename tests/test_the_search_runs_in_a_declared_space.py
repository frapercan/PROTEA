"""The neighbour search runs in a declared space: the transform, where it is applied, what it keeps.

``embedding_transform`` maps the bank and the queries with the label-free
statistics of the configuration's space at the run's cut
(``protea.core.embedding_space``). The pool is mapped in the float32 copy the
search reads, in place and in chunks, after the half-precision vectors are
widened and before the cosine normalisation; the half-precision pool and its disk
cache stay raw. The queries are mapped with the same statistics, so a bank
vector asked as a query lands on its own row of the transformed pool.

These pin: an unset transform changes no byte; whitening by the platform's
symmetric form and by the principal-axes form gives the same neighbours; a query
lands on its pool row within 1e-6; the half-precision bank and the disk cache
stay raw; the in-process key carries the space and the disk key does not; the
in-place transform of a 2,560-dimension pool holds a few chunks beside it, never
a second copy; and what the search cannot run is refused.
"""

from __future__ import annotations

import io
import json
import tracemalloc
import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from protea_contracts import PredictGOTermsBatchPayload, PredictGOTermsPayload
from scipy.sparse import csr_array

from protea.core import disk_cache
from protea.core._artifact_identity import ArtifactMismatch
from protea.core.embedding_space.transforms import (
    CHUNK_ROWS,
    TRANSFORMS,
    SpaceParameters,
    fixed_count,
    transformed_copy,
    whitening_gains,
)
from protea.core.knn_search import search_knn
from protea.core.method_seal import METHOD_IDENTITY_FIELDS, assert_one_axis
from protea.core.operations.predict_go_terms import _batch_op_feature as feature_module
from protea.core.operations.predict_go_terms import _search_space as space_module
from protea.core.operations.predict_go_terms._batch_op import PredictGOTermsBatchOperation
from protea.core.operations.predict_go_terms._batch_op_reference import _PoolRequest
from protea.core.operations.predict_go_terms._receipt import run_receipt
from protea.core.operations.predict_go_terms._search_space import SearchSpace, search_space

CFG = uuid.UUID("11111111-1111-1111-1111-111111111111")
ANN = uuid.UUID("22222222-2222-2222-2222-222222222222")
PHOTO = uuid.UUID("33333333-3333-3333-3333-333333333333")
STATISTICS = uuid.UUID("44444444-4444-4444-4444-444444444444")


def _quiet(*_args: Any, **_kwargs: Any) -> None:
    return None


def _sample(n: int, d: int, seed: int = 3) -> np.ndarray:
    """An anisotropic cloud off the origin: one dominant direction, a slow spectrum."""
    rng = np.random.default_rng(seed)
    scales = np.geomspace(8.0, 0.05, d)
    rows = rng.standard_normal((n, d)) * scales
    rotation, _ = np.linalg.qr(rng.standard_normal((d, d)))
    shifted: np.ndarray = rows @ rotation.T + np.linspace(2.0, -1.0, d)
    return shifted


def _parameters(rows: np.ndarray, *, shrinkage: float = 0.05, removed: int = 2) -> SpaceParameters:
    """The statistics of ``rows`` as the transforms read them (eigenvalues descending)."""
    mean = rows.mean(axis=0)
    centred = rows - mean
    covariance = centred.T @ centred / rows.shape[0]
    values, vectors = np.linalg.eigh(covariance)
    return SpaceParameters(
        mean=mean,
        variances=np.diag(covariance).copy(),
        eigenvalues=values[::-1].copy(),
        eigenvectors=vectors[:, ::-1].copy(),
        shrinkage=shrinkage,
        removed_directions=removed,
    )


def _space(transform: str, parameters: SpaceParameters, sha: str = "a" * 64) -> SearchSpace:
    return SearchSpace(transform, STATISTICS, sha, 500, parameters.dimension, parameters)


def _unit(rows: np.ndarray) -> np.ndarray:
    normalised: np.ndarray = rows / np.linalg.norm(rows, axis=1, keepdims=True)
    return normalised


def _batch_payload(**over: Any) -> PredictGOTermsBatchPayload:
    return PredictGOTermsBatchPayload.model_validate(
        {
            "embedding_config_id": str(CFG),
            "annotation_set_id": str(ANN),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": ["P1"],
            "compute_alignments": False,
            "compute_taxonomy": False,
            **over,
        }
    )


# Unset, nothing moves -----------------------------------------------------------------


@pytest.mark.parametrize(("need_cos", "need_plain"), [(True, True), (True, False), (False, True)])
def test_an_unset_transform_changes_no_byte(need_cos: bool, need_plain: bool) -> None:
    bank = _sample(64, 12).astype(np.float16)
    raw, cosine = disk_cache._dense_f32_views(bank, need_cos=need_cos, need_plain=need_plain)
    widened = bank.astype(np.float32)
    norms = np.linalg.norm(widened, axis=1, keepdims=True)
    if need_plain:
        assert raw.tobytes() == widened.tobytes()
    if need_cos:
        assert cosine.tobytes() == (widened / (norms + 1e-9)).tobytes()


def test_an_unset_transform_asks_nothing_of_the_store_and_writes_no_block() -> None:
    session = MagicMock()
    assert search_space(session, _batch_payload()) is None
    assert session.mock_calls == []
    p = PredictGOTermsPayload(
        embedding_config_id=str(CFG), annotation_set_id=str(ANN), ontology_snapshot_id=str(ANN)
    )
    assert "embedding_transform" not in run_receipt(p, uuid.uuid4())


# Where the transform is applied ------------------------------------------------------------


@pytest.mark.parametrize("transform", TRANSFORMS)
def test_both_views_are_of_the_transformed_space(transform: str) -> None:
    rows = _sample(400, 16)
    space = _space(transform, _parameters(rows))
    bank = rows[:50].astype(np.float16)
    expected = transformed_copy(bank.astype(np.float32), transform, space.parameters)

    raw, cosine = disk_cache._dense_f32_views(
        bank, need_cos=True, need_plain=True, transform=space.apply
    )

    assert raw.dtype == cosine.dtype == np.float32
    assert np.allclose(raw, expected, atol=1e-5)
    assert np.allclose(cosine, _unit(expected), atol=1e-6)


def test_the_cosine_only_route_transforms_in_place_too() -> None:
    rows = _sample(400, 16)
    space = _space("whiten", _parameters(rows))
    bank = rows[:50].astype(np.float16)
    raw, cosine = disk_cache._dense_f32_views(
        bank, need_cos=True, need_plain=False, transform=space.apply
    )
    expected = transformed_copy(bank.astype(np.float32), "whiten", space.parameters)
    assert raw is None
    assert np.allclose(cosine, _unit(expected), atol=1e-6)


def test_the_half_precision_bank_stays_raw() -> None:
    rows = _sample(400, 16)
    space = _space("center", _parameters(rows))
    bank = rows[:50].astype(np.float16)
    before = bank.tobytes()
    views = disk_cache._derive_reference_views(
        [f"P{i}" for i in range(50)], bank, transform=space.apply
    )
    assert views["embeddings"] is bank
    assert bank.tobytes() == before


def test_the_symmetric_and_the_principal_axes_whitening_find_the_same_neighbours() -> None:
    """The platform whitens with ``W = U g U^T``; ``(x - mu) U g`` is that rotated by ``U^T``.

    A rotation keeps every cosine, so the two forms rank every donor alike.
    """
    rows = _sample(600, 16, seed=5)
    parameters = _parameters(rows, shrinkage=0.02)
    pool, queries = rows[:500], rows[500:540] + 0.01
    gains = whitening_gains(parameters)

    def principal_axes(vectors: np.ndarray) -> np.ndarray:
        mapped: np.ndarray = ((vectors - parameters.mean) @ parameters.eigenvectors) * gains
        return mapped

    symmetric_pool = transformed_copy(pool, "whiten", parameters)
    symmetric_queries = transformed_copy(queries, "whiten", parameters)
    accessions = [f"P{i:03d}" for i in range(pool.shape[0])]

    def neighbours(bank: np.ndarray, asked: np.ndarray) -> list[list[str]]:
        hits = search_knn(
            _unit(asked).astype(np.float32),
            _unit(bank).astype(np.float32),
            accessions,
            k=10,
            backend="numpy",
            metric="cosine",
            pre_normalized=True,
        )
        return [[accession for accession, _ in row] for row in hits]

    assert neighbours(symmetric_pool, symmetric_queries) == neighbours(
        principal_axes(pool), principal_axes(queries)
    )


class _Half:
    """What a halfvec column returns: a vector with ``to_list``."""

    def __init__(self, values: np.ndarray) -> None:
        self._values = values

    def to_list(self) -> list[float]:
        return [float(v) for v in self._values]


@pytest.mark.parametrize("transform", TRANSFORMS)
def test_a_query_lands_on_its_row_of_the_transformed_pool(transform: str) -> None:
    rows = _sample(800, 64)
    space = _space(transform, _parameters(rows))
    bank = rows[:300].astype(np.float16)
    accessions = [f"P{i:03d}" for i in range(bank.shape[0])]
    pool = disk_cache._derive_reference_views(
        accessions, bank, need_cos=False, need_plain=True, transform=space.apply
    )
    asked = [accessions[7], accessions[250]]
    found = [(accession, _Half(bank[accessions.index(accession)])) for accession in asked]

    op = PredictGOTermsBatchOperation()
    p = _batch_payload(query_accessions=asked, embedding_transform=transform)
    with (
        patch.object(feature_module, "_vectors_by_accession", return_value=found),
        patch.object(feature_module, "search_space", return_value=space),
    ):
        queries, valid = op._load_query_embeddings(MagicMock(), asked, CFG, p, _quiet)

    assert valid == asked
    expected = pool["embeddings_f32"][[7, 250]]
    assert np.max(np.abs(queries - expected)) <= 1e-6


# Keys ------------------------------------------------------------------------------------


class TestTheInProcessKeyCarriesTheSpace:
    def _key(self, space: SearchSpace | None) -> tuple[Any, ...]:
        p = _batch_payload()
        return PredictGOTermsBatchOperation._process_cache_key(
            p, _PoolRequest(CFG, ANN, None, space), need_cos=True, need_plain=False
        )

    def test_a_space_moves_the_key_and_no_space_keeps_it(self) -> None:
        parameters = _parameters(_sample(400, 16))
        raw = self._key(None)
        whiten = self._key(_space("whiten", parameters))
        assert raw[-1] is None
        assert whiten != raw
        assert whiten[:-1] == raw[:-1]

    def test_the_transform_and_the_statistics_each_move_it(self) -> None:
        parameters = _parameters(_sample(400, 16))
        whiten = self._key(_space("whiten", parameters))
        assert whiten != self._key(_space("center", parameters))
        assert whiten != self._key(_space("whiten", parameters, sha="b" * 64))

    def test_the_disk_key_does_not_move(self) -> None:
        """The disk cache holds the raw pool, which every space of it reuses."""
        space = _space("whiten", _parameters(_sample(400, 16)))
        assert _PoolRequest(CFG, ANN, None, space).cache_key == _PoolRequest(CFG, ANN).cache_key


def test_a_transformed_load_writes_the_raw_pool_to_disk(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(disk_cache, "_DISK_CACHE_DIR", tmp_path)
    rows = _sample(400, 16)
    space = _space("whiten", _parameters(rows))
    bank = rows[:40].astype(np.float16)
    accessions = [f"P{i:03d}" for i in range(40)]
    op = PredictGOTermsBatchOperation()
    query = MagicMock()
    query.count.return_value = len(accessions)
    with (
        patch(
            "protea.core.operations.predict_go_terms._batch_op_reference.bank_is_sparse",
            return_value=False,
        ),
        patch.object(op, "_reference_pool_query", return_value=query),
        patch.object(
            op, "_stream_pool", return_value=(accessions, bank.copy(), "database", "binary")
        ),
    ):
        views = op._load_reference_data(
            MagicMock(), _PoolRequest(CFG, ANN, None, space), _quiet, need_plain=False
        )

    written, _ = disk_cache._disk_cache_paths(CFG, ANN)
    assert np.load(written).tobytes() == bank.tobytes()
    assert views["embeddings"].tobytes() == bank.tobytes()
    expected = transformed_copy(bank.astype(np.float32), "whiten", space.parameters)
    assert np.allclose(views["embeddings_f32_cos"], _unit(expected), atol=1e-6)


# Memory ------------------------------------------------------------------------------------


@pytest.mark.parametrize("transform", ["center", "remove_dominant_directions"])
def test_the_in_place_transform_of_a_2560_dimension_pool_holds_a_few_chunks(
    transform: str,
) -> None:
    """The peak of mapping a pool in place is a few float64 chunks, whatever its rows.

    Measured with ``tracemalloc``, which numpy reports its buffers to. A transform
    of the whole array at once would hold a float64 copy of the pool; this one
    holds at most the chunk it centred, the projection and the result: three
    chunks of ``CHUNK_ROWS`` rows, under a bound a second float32 copy of the
    pool would not fit in.
    """
    d, n = 2560, 12 * CHUNK_ROWS
    parameters = SpaceParameters(
        mean=np.full(d, 0.25),
        variances=np.ones(d),
        eigenvalues=np.linspace(2.0, 1.0, d),
        eigenvectors=np.eye(d),
        shrinkage=0.0,
        removed_directions=10,
    )
    space = _space(transform, parameters)
    pool = np.random.default_rng(1).standard_normal((n, d), dtype=np.float32)
    first_row = pool[0].astype(np.float64)

    tracemalloc.start()
    try:
        space.apply(pool)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    chunk = CHUNK_ROWS * d * 8
    assert peak <= 4 * chunk, f"{peak / 2**20:.0f} MiB to map a {pool.nbytes / 2**20:.0f} MiB pool"
    assert 4 * chunk < pool.nbytes, "the bound must be below a second float32 copy"
    expected = transformed_copy(first_row[None, :], transform, parameters)[0]
    assert np.allclose(pool[0], expected, atol=1e-5)


def test_the_hook_maps_the_very_array_it_returns() -> None:
    """``_dense_f32_views`` hands the transform its float32 copy and keeps that buffer."""
    bank = _sample(40, 8).astype(np.float16)
    seen: list[np.ndarray] = []
    _raw, cosine = disk_cache._dense_f32_views(
        bank, need_cos=True, need_plain=False, transform=seen.append
    )
    assert len(seen) == 1 and seen[0] is cosine


# Refusals -----------------------------------------------------------------------------------


def _artifact(parameters: SpaceParameters) -> bytes:
    """An artifact ``read_statistics_artifact`` reads: the arrays and the document."""
    document = {"spectrum": {"fixed_count": fixed_count(parameters.dimension)}}
    buffer = io.BytesIO()
    np.savez(
        buffer,
        format_version=np.asarray(1),
        mean=parameters.mean,
        variances=parameters.variances,
        eigenvalues=parameters.eigenvalues,
        eigenvectors=parameters.eigenvectors,
        shrinkage=np.asarray(parameters.shrinkage),
        removed_directions=np.asarray(parameters.removed_directions),
        document=np.frombuffer(json.dumps(document).encode(), dtype=np.uint8),
    )
    return buffer.getvalue()


@pytest.fixture()
def a_cut_with_statistics(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """A photo's run whose configuration has statistics at the photo, in a store in memory."""
    import hashlib

    parameters = _parameters(_sample(400, 16))
    blob = _artifact(parameters)
    store = MagicMock()
    store.get.return_value = blob
    row = MagicMock(
        id=STATISTICS,
        embedding_config_id=CFG,
        artifact_uri="memory://statistics.npz",
        artifact_sha256=hashlib.sha256(blob).hexdigest(),
        n=400,
        d=16,
    )
    monkeypatch.setattr(space_module, "_PARAMETERS", {})
    monkeypatch.setattr(space_module, "_store", lambda: store)
    monkeypatch.setattr(space_module, "photo_of", lambda _s, _a: PHOTO)
    monkeypatch.setattr(space_module, "bank_is_sparse", lambda _s, _c: False)
    monkeypatch.setattr(space_module, "_statistics_row", lambda _s, _c, _p: row)
    return {"store": store, "row": row, "parameters": parameters}


class TestWhatTheSearchCannotRunIsRefused:
    def test_the_statistics_are_read_once_and_verified(self, a_cut_with_statistics) -> None:
        p = _batch_payload(embedding_transform="whiten")
        first = search_space(MagicMock(), p)
        second = search_space(MagicMock(), p)
        assert first is not None and second is not None
        assert first.statistics_id == STATISTICS and first.transform == "whiten"
        assert np.array_equal(first.parameters.mean, a_cut_with_statistics["parameters"].mean)
        assert a_cut_with_statistics["store"].get.call_count == 1

    def test_bytes_that_are_not_the_rows_are_refused(self, a_cut_with_statistics) -> None:
        a_cut_with_statistics["row"].artifact_sha256 = "0" * 64
        with pytest.raises(ArtifactMismatch):
            search_space(MagicMock(), _batch_payload(embedding_transform="center"))

    def test_a_row_of_another_dimension_is_refused(self, a_cut_with_statistics) -> None:
        a_cut_with_statistics["row"].d = 32
        with pytest.raises(ValueError, match="dimensions where the row says 32"):
            search_space(MagicMock(), _batch_payload(embedding_transform="center"))

    def test_the_v6_features_are_refused_beside_a_transform(self, a_cut_with_statistics) -> None:
        p = _batch_payload(embedding_transform="whiten", compute_v6_features=True)
        with pytest.raises(ValueError, match="compute_v6_features"):
            search_space(MagicMock(), p)

    def test_a_sparse_bank_is_refused(
        self, a_cut_with_statistics, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(space_module, "bank_is_sparse", lambda _s, _c: True)
        with pytest.raises(ValueError, match="sparse bank"):
            search_space(MagicMock(), _batch_payload(embedding_transform="center"))

    def test_an_annotation_set_no_photo_loaded_is_refused(
        self, a_cut_with_statistics, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(space_module, "photo_of", lambda _s, _a: None)
        with pytest.raises(ValueError, match="loaded by no cut photo"):
            search_space(MagicMock(), _batch_payload(embedding_transform="center"))

    def test_a_cut_without_statistics_is_refused(
        self, a_cut_with_statistics, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(space_module, "_statistics_row", lambda _s, _c, _p: None)
        with pytest.raises(ValueError, match="compute_embedding_space_statistics"):
            search_space(MagicMock(), _batch_payload(embedding_transform="center"))

    def test_a_sparse_bank_never_takes_a_transform_in_the_batch(self) -> None:
        space = _space("center", _parameters(_sample(400, 16)))
        codes = csr_array(np.eye(3, 16, dtype=np.float32))
        with pytest.raises(ValueError, match="sparse bank"):
            disk_cache._derive_reference_views(["A", "B", "C"], codes, transform=space.apply)

    def test_the_coordinator_refuses_before_writing_the_prediction_set(
        self, a_cut_with_statistics, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from protea.core.operations.predict_go_terms import _coordinator as coordinator

        monkeypatch.setattr(space_module, "_statistics_row", lambda _s, _c, _p: None)
        monkeypatch.setattr(
            coordinator.PredictGOTermsOperation,
            "_validate_inputs",
            staticmethod(lambda _s, _p: (CFG, ANN, uuid.uuid4(), MagicMock(model_name="m"))),
        )
        session = MagicMock()
        payload = {
            "embedding_config_id": str(CFG),
            "annotation_set_id": str(ANN),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "query_accessions": ["P1"],
            "embedding_transform": "whiten",
            "_job_id": str(uuid.uuid4()),
        }
        with pytest.raises(ValueError, match="none are computed"):
            coordinator.PredictGOTermsOperation().execute(session, payload, emit=_quiet)
        session.add.assert_not_called()


# The record ---------------------------------------------------------------------------------


class TestTheReceiptNamesTheSpace:
    def _payload(self, transform: str | None) -> PredictGOTermsPayload:
        return PredictGOTermsPayload(
            embedding_config_id=str(CFG),
            annotation_set_id=str(ANN),
            ontology_snapshot_id=str(ANN),
            embedding_transform=transform,
        )

    def test_the_block_names_the_transform_and_the_statistics(self) -> None:
        parameters = _parameters(_sample(400, 16), shrinkage=0.125, removed=3)
        space = _space("remove_dominant_directions", parameters, sha="c" * 64)
        block = run_receipt(self._payload("remove_dominant_directions"), uuid.uuid4(), space)[
            "embedding_transform"
        ]
        assert block == {
            "name": "remove_dominant_directions",
            "statistics_id": str(STATISTICS),
            "statistics_sha256": "c" * 64,
            "n": 500,
            "d": 16,
            "shrinkage": 0.125,
            "removed_directions": 3,
        }
        json.dumps(block)

    def test_a_transform_without_its_statistics_is_refused(self) -> None:
        with pytest.raises(ValueError, match="needs the statistics"):
            run_receipt(self._payload("whiten"), uuid.uuid4())
        center = _space("center", _parameters(_sample(400, 16)))
        with pytest.raises(ValueError, match="needs the statistics"):
            run_receipt(self._payload("whiten"), uuid.uuid4(), center)

    def test_it_is_an_axis_of_the_method(self) -> None:
        parameters = _parameters(_sample(400, 16))
        whiten = run_receipt(self._payload("whiten"), uuid.uuid4(), _space("whiten", parameters))
        raw = run_receipt(self._payload(None), uuid.uuid4())
        assert "embedding_transform" in METHOD_IDENTITY_FIELDS

        def row(meta: dict[str, Any]) -> dict[str, Any]:
            return {
                "id": str(uuid.uuid4()),
                "embedding_config_id": str(CFG),
                "annotation_set_id": str(ANN),
                "ontology_snapshot_id": str(ANN),
                "query_set_id": str(PHOTO),
                "limit_per_entry": 30,
                "distance_threshold": None,
                "method_id": None,
                "meta": meta,
            }

        recorded = {"code_revision": "a" * 40, "dependency_revisions": {"protea-method": "b" * 40}}
        assert assert_one_axis(
            ["embedding_transform"], row({**whiten, **recorded}), row({**raw, **recorded})
        ) == ("embedding_transform",)

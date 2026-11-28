"""The search runs the donor set, the evidence channels and the exclusion after retrieval.

protea-contracts 1.8.0 named three things the predict path refused until this
revision could run them: a declared set of photo members as the donors
(``donor_policy.donor_set_id``), the evidence channels a donation may come
through (``donor_policy.evidence_channels``), and the query leaving its own
neighbourhood after retrieval (``self_exclusion`` ``"accession_after_retrieval"``,
LAFA's own search). With them the pool admits a donor only through a positive
row, so a donor whose admitted rows are all NOT rows takes no neighbour slot,
and the pool cache's epoch moved with that meaning.

These tests need no database: the coordinator's refusals read a mocked store,
the pool's predicates are read off the compiled query, and the searches run on
synthetic pools through the batch's own pre-searches and protea-method's
pipeline. ``tests/test_donor_set_and_channels_on_postgres.py`` runs the same
predicates against Postgres.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from protea_contracts import PredictGOTermsBatchPayload
from protea_contracts.payloads import DonorPolicy
from sqlalchemy.orm import Session

from protea.config.tuning import get_tuning
from protea.core import disk_cache
from protea.core.domain.aspect import ASPECT_CODES
from protea.core.operations.predict_go_terms import _coordinator as coordinator
from protea.core.operations.predict_go_terms import _photo_sequences as photo_sequences
from protea.core.operations.predict_go_terms import _pool_admission as admission
from protea.core.operations.predict_go_terms._batch_op import PredictGOTermsBatchOperation
from protea.core.operations.predict_go_terms._batch_op_reference import (
    _PoolRequest,
    _ReferenceMixin,
    _restrict_annotations,
)
from protea.core.operations.predict_go_terms._common import (
    _UNIFIED_REF_KEY,
    AspectSeparatedKnnContext,
    PredictGOTermsPayload,
    _UnifiedPredictContext,
)
from protea.core.operations.predict_go_terms._reference_aspect import aspect_bank
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation

CFG = uuid.UUID("11111111-1111-1111-1111-111111111111")
ANN = uuid.UUID("22222222-2222-2222-2222-222222222222")
PHOTO = uuid.UUID("33333333-3333-3333-3333-333333333333")
DONOR_SET = "44444444-4444-4444-4444-444444444444"
RECIPE = [
    "truth",
    "curated",
    "phylogenetic",
    "orthology_electronic",
    "reviewed_field_electronic",
]


def _quiet(*_args: Any, **_kwargs: Any) -> None:
    return None


@pytest.fixture()
def disk_write_off(monkeypatch: pytest.MonkeyPatch) -> Any:
    """The tuning switch ``reference_pool_disk_write`` off, for one test."""
    monkeypatch.setenv("PROTEA_TUNING__operation__reference_pool_disk_write", "false")
    get_tuning.cache_clear()
    yield
    monkeypatch.delenv("PROTEA_TUNING__operation__reference_pool_disk_write", raising=False)
    get_tuning.cache_clear()


# The coordinator ---------------------------------------------------------------


def _coordinator_payload(**policy: Any) -> PredictGOTermsPayload:
    return PredictGOTermsPayload.model_validate(
        {
            "embedding_config_id": str(CFG),
            "annotation_set_id": str(ANN),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "query_accessions": ["P00001"],
            "donor_policy": policy,
        }
    )


def _store_holding(donor_set: Any) -> MagicMock:
    session = MagicMock()
    session.get.return_value = donor_set
    return session


class TestTheCoordinatorAdmitsOnlyThePhotosOwnMembers:
    """A donor set restricts the corpus at the run's cut, so it must be that cut's members."""

    def test_a_photo_members_set_of_the_runs_photo_passes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(coordinator, "photo_of", lambda _s, _a: PHOTO)
        donor_set = SimpleNamespace(source_kind="photo_members", cut_photo_id=PHOTO)
        coordinator._refuse_donors_the_cut_does_not_declare(
            _store_holding(donor_set), _coordinator_payload(donor_set_id=DONOR_SET)
        )

    @pytest.mark.parametrize(
        ("kind", "photo"),
        [
            ("photo_members", uuid.UUID(int=9)),
            ("window_sample", None),
            ("evaluation_targets", None),
        ],
        ids=["another photo's members", "a window sample", "a window's targets"],
    )
    def test_any_other_set_is_refused(
        self, monkeypatch: pytest.MonkeyPatch, kind: str, photo: uuid.UUID | None
    ) -> None:
        monkeypatch.setattr(coordinator, "photo_of", lambda _s, _a: PHOTO)
        donor_set = SimpleNamespace(source_kind=kind, cut_photo_id=photo)
        with pytest.raises(ValueError, match="only a photo_members set of that photo"):
            coordinator._refuse_donors_the_cut_does_not_declare(
                _store_holding(donor_set), _coordinator_payload(donor_set_id=DONOR_SET)
            )

    @pytest.mark.parametrize("named", [DONOR_SET, "not-a-set-id"])
    def test_a_set_the_store_does_not_hold_is_refused(
        self, monkeypatch: pytest.MonkeyPatch, named: str
    ) -> None:
        monkeypatch.setattr(coordinator, "photo_of", lambda _s, _a: PHOTO)
        with pytest.raises(ValueError, match="is not a query set"):
            coordinator._refuse_donors_the_cut_does_not_declare(
                _store_holding(None), _coordinator_payload(donor_set_id=named)
            )

    @pytest.mark.parametrize(
        ("policy", "field"),
        [
            ({"donor_set_id": DONOR_SET}, "donor_policy.donor_set_id"),
            ({"evidence_channels": ["truth"]}, "donor_policy.evidence_channels"),
        ],
    )
    def test_an_annotation_set_no_photo_loaded_is_refused(
        self, monkeypatch: pytest.MonkeyPatch, policy: dict[str, Any], field: str
    ) -> None:
        monkeypatch.setattr(coordinator, "photo_of", lambda _s, _a: None)
        with pytest.raises(ValueError, match=f"{field} restricts the donors at a cut"):
            coordinator._refuse_donors_the_cut_does_not_declare(
                MagicMock(), _coordinator_payload(**policy)
            )

    def test_channels_alone_ask_for_the_photo_and_nothing_else(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(coordinator, "photo_of", lambda _s, _a: PHOTO)
        session = MagicMock()
        coordinator._refuse_donors_the_cut_does_not_declare(
            session, _coordinator_payload(evidence_channels=RECIPE)
        )
        session.get.assert_not_called()

    def test_a_policy_without_either_reads_nothing(self) -> None:
        session = MagicMock()
        coordinator._refuse_donors_the_cut_does_not_declare(
            session, _coordinator_payload(evidence_codes=["EXP"])
        )
        assert session.mock_calls == []

    def test_execute_refuses_before_creating_the_prediction_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(coordinator, "photo_of", lambda _s, _a: None)
        monkeypatch.setattr(
            coordinator.PredictGOTermsOperation,
            "_validate_inputs",
            staticmethod(lambda _s, _p: (CFG, ANN, uuid.uuid4(), SimpleNamespace(model_name="m"))),
        )
        session = MagicMock()
        payload = _coordinator_payload(donor_set_id=DONOR_SET).model_dump(mode="json")
        with pytest.raises(ValueError, match="no cut photo"):
            coordinator.PredictGOTermsOperation().execute(
                session, {**payload, "_job_id": str(uuid.uuid4())}, emit=_quiet
            )
        session.add.assert_not_called()


def test_a_photos_run_may_compute_alignments(monkeypatch: pytest.MonkeyPatch) -> None:
    """The alignments read the cut's sequences now, so the old refusal is gone."""
    monkeypatch.setattr(coordinator, "photo_of", lambda _s, _a: PHOTO)
    p = PredictGOTermsPayload.model_validate(
        {
            "embedding_config_id": str(CFG),
            "annotation_set_id": str(ANN),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "compute_alignments": True,
        }
    )
    session = MagicMock()
    session.query.return_value.join.return_value.order_by.return_value.all.return_value = [
        ("P00001",)
    ]
    assert coordinator._accessions_with_a_vector(session, p, CFG, _quiet) == [("P00001",)]


def test_the_job_summary_names_the_new_fields() -> None:
    summary = coordinator.PredictGOTermsOperation().summarize_payload(
        {
            "donor_policy": {"donor_set_id": DONOR_SET, "evidence_channels": RECIPE},
            "self_exclusion": "accession_after_retrieval",
        }
    )
    assert "donors=44444444" in summary
    assert "channels=5" in summary
    assert "query-left-after-retrieval" in summary


# The pool's predicates -----------------------------------------------------------


def _sql(request: _PoolRequest) -> str:
    query = _ReferenceMixin()._reference_pool_query(Session(), request)
    return str(query.statement.compile(compile_kwargs={"literal_binds": True})).lower()


@pytest.fixture()
def a_photos_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """The pool of a photo's run, with the photo's channel table, and no store behind it."""
    monkeypatch.setattr(photo_sequences, "photo_of", lambda _s, _a: PHOTO)
    monkeypatch.setattr(
        admission, "channel_source", lambda _s, _a: admission.ChannelSource(PHOTO, "c" * 64)
    )


class TestThePoolAdmitsThroughAPositiveRow:
    def test_the_permissive_pool_reads_positive_rows_only(self) -> None:
        with patch.object(photo_sequences, "photo_of", lambda _s, _a: None):
            sql = _sql(_PoolRequest(CFG, ANN))
        assert "protein_go_annotation.qualifier is null" in sql
        assert "not like '%not%'" in sql

    def test_the_three_readers_share_one_predicate(self) -> None:
        """The pool, the aspect build and the donation loader read one definition."""
        import inspect

        from protea.core.operations.predict_go_terms import _batch_op_feature, _batch_op_reference

        for source in (
            inspect.getsource(_batch_op_reference._ReferenceMixin._reference_pool_query),
            inspect.getsource(_batch_op_reference._ReferenceMixin._collect_aspect_annotations),
            inspect.getsource(_batch_op_feature._FeatureLoadingMixin._fetch_annotation_chunk),
        ):
            assert "positive_rows()" in source
            assert 'like("%NOT%")' not in source

    def test_the_cache_key_carries_the_epoch_it_moved_to(self) -> None:
        assert disk_cache._CACHE_EPOCH >= 5
        assert disk_cache._cache_key(CFG, ANN).startswith(f"e{disk_cache._CACHE_EPOCH}__")


class TestTheDonorSetIsASemiJoin:
    def test_it_keeps_the_listed_members_by_the_members_accession(self, a_photos_run) -> None:
        sql = _sql(_PoolRequest(CFG, ANN, DonorPolicy(donor_set_id=DONOR_SET)))
        assert "exists (select" in sql
        assert "query_set_entry.query_set_id = '44444444444444444444444444444444'" in sql
        assert "query_set_entry.accession = member_sequences.accession" in sql

    def test_unset_it_adds_nothing(self, a_photos_run) -> None:
        assert "query_set_entry" not in _sql(_PoolRequest(CFG, ANN, DonorPolicy()))

    def test_it_keys_its_own_pool(self) -> None:
        restricted = _PoolRequest(CFG, ANN, DonorPolicy(donor_set_id=DONOR_SET))
        assert restricted.cache_key != _PoolRequest(CFG, ANN).cache_key
        assert restricted.discriminator.endswith(f"donors={DONOR_SET}")


class TestTheChannelsAreReadAtTheCut:
    def test_the_predicate_reads_the_photos_table_and_its_review_status(self, a_photos_run) -> None:
        sql = _sql(_PoolRequest(CFG, ANN, DonorPolicy(evidence_channels=RECIPE)))
        assert "evidence_channel_of('" + "c" * 64 + "'" in sql
        assert "channel_member.superset" in sql
        assert "channel_member.photo_id = '33333333333333333333333333333333'" in sql
        for channel in RECIPE:
            assert f"'{channel}'" in sql
        assert "'signature_electronic'" not in sql

    def test_unset_they_ask_nothing_of_the_store(self) -> None:
        session = MagicMock()
        assert admission.channels_of_the_cut(session, ANN, DonorPolicy()) is None
        assert admission.channels_of_the_cut(session, ANN, None) is None
        assert session.mock_calls == []

    def test_an_annotation_set_no_photo_loaded_has_no_channels(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(admission, "channel_source", lambda _s, _a: None)
        with pytest.raises(ValueError, match="loaded by no cut photo"):
            admission.channels_of_the_cut(
                MagicMock(), ANN, DonorPolicy(evidence_channels=["truth"])
            )

    def test_a_reader_that_does_not_say_where_is_refused(self) -> None:
        """Forgetting the source cannot read every channel silently."""
        rows = Session().query(ProteinGOAnnotation.protein_accession)
        with pytest.raises(ValueError, match="no cut photo was resolved"):
            _restrict_annotations(rows, DonorPolicy(evidence_channels=["truth"]))

    def test_codes_and_channels_both_apply(self, a_photos_run) -> None:
        policy = DonorPolicy(evidence_codes=["EXP", "IEA"], evidence_channels=["truth"])
        sql = _sql(_PoolRequest(CFG, ANN, policy))
        assert "protein_go_annotation.evidence_code in ('exp', 'iea')" in sql
        assert "evidence_channel_of(" in sql

    def test_they_key_their_own_pool_whatever_order_they_were_given_in(self) -> None:
        one = _PoolRequest(CFG, ANN, DonorPolicy(evidence_channels=RECIPE))
        other = _PoolRequest(CFG, ANN, DonorPolicy(evidence_channels=list(reversed(RECIPE))))
        assert one.cache_key == other.cache_key != _PoolRequest(CFG, ANN).cache_key


# The exclusion after retrieval ------------------------------------------------------


def _batch_payload(**over: Any) -> PredictGOTermsBatchPayload:
    return PredictGOTermsBatchPayload.model_validate(
        {
            "embedding_config_id": str(CFG),
            "annotation_set_id": str(ANN),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": ["Q1", "Q9"],
            "search_backend": "numpy",
            "metric": "cosine",
            "compute_alignments": False,
            "compute_taxonomy": False,
            **over,
        }
    )


#: A pool of four donors where Q1 is a donor too and Q9 is not; R1 shares Q1's
#: direction, so Q1's own row is its nearest neighbour.
_POOL = ["Q1", "R1", "R2", "R3"]
_POOL_VECTORS = np.array(
    [[1.0, 0.0, 0.0], [0.99, 0.1, 0.0], [0.5, 0.8, 0.1], [0.1, 0.2, 0.95]], dtype=np.float32
)
_QUERY_VECTORS = np.array([[1.0, 0.0, 0.0], [0.6, 0.75, 0.0]], dtype=np.float32)
_TERMS = {101: "GO:0000101", 102: "GO:0000102", 103: "GO:0000103", 104: "GO:0000104"}


def _unit(rows: np.ndarray) -> np.ndarray:
    normalised: np.ndarray = rows / np.linalg.norm(rows, axis=1, keepdims=True)
    return normalised.astype(np.float32)


def _annotations(accessions: set[str]) -> dict[str, list[dict[str, Any]]]:
    terms = dict(zip(_POOL, _TERMS, strict=True))
    return {
        acc: [{"go_term_id": terms[acc], "qualifier": None, "evidence_code": "EXP"}]
        for acc in accessions
    }


def _donors_per_query(predictions: list[dict[str, Any]]) -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for row in predictions:
        found.setdefault(row["protein_accession"], set()).add(row["ref_protein_accession"])
    return found


class TestTheQueryLeavesAfterRetrieval:
    """LAFA's search: retrieve the asked depth from the pool as it stands, then drop the query."""

    def test_through_the_unified_path_a_query_in_the_pool_keeps_one_fewer(self) -> None:
        op = PredictGOTermsBatchOperation()
        p = _batch_payload(
            limit_per_entry=2,
            exclude_self_neighbour=True,
            self_exclusion="accession_after_retrieval",
            aspect_separated_knn=False,
        )
        ctx = _UnifiedPredictContext(
            p=p,
            annotation_set_id=ANN,
            prediction_set_id=uuid.uuid4(),
            valid_accessions=["Q1", "Q9"],
            query_embeddings=_unit(_QUERY_VECTORS),
            ref_data={
                "accessions": list(_POOL),
                "embeddings": _POOL_VECTORS.astype(np.float16),
                "embeddings_f32": _POOL_VECTORS,
                "embeddings_f32_cos": _unit(_POOL_VECTORS),
            },
        )
        identities = {acc: acc for acc in [*_POOL, "Q9"]}
        with (
            patch.object(
                op, "_load_annotations_for", side_effect=lambda *a, **k: _annotations(a[2])
            ),
            patch.object(
                op, "_load_go_term_metadata", return_value=(_TERMS, dict.fromkeys(_TERMS, "F"))
            ),
            patch(
                "protea.core.operations.predict_go_terms._unified_path.load_sequence_identities",
                return_value=identities,
            ),
        ):
            result = op._unified_predict_via_pipeline(MagicMock(), ctx)

        donors = _donors_per_query(result.predictions)
        assert donors["Q1"] == {"R1"}, "Q1 is in the pool: two retrieved, itself dropped"
        assert len(donors["Q9"]) == 2, "Q9 is not: both retrieved neighbours stay"

    def test_the_pre_search_asks_for_no_margin(self) -> None:
        """The retrieved depth is the asked depth; only the drop follows it."""
        op = PredictGOTermsBatchOperation()
        p = _batch_payload(
            limit_per_entry=3,
            exclude_self_neighbour=True,
            self_exclusion="accession_after_retrieval",
            aspect_separated_knn=False,
        )
        ctx = _UnifiedPredictContext(
            p=p,
            annotation_set_id=ANN,
            prediction_set_id=uuid.uuid4(),
            valid_accessions=["Q1"],
            query_embeddings=np.zeros((1, 3), dtype=np.float32),
            ref_data={
                "accessions": list(_POOL),
                "embeddings_f32": np.zeros((4, 3), dtype=np.float32),
                "embeddings_f32_cos": np.zeros((4, 3), dtype=np.float32),
            },
        )
        asked: dict[str, int] = {}

        def fake_search(_q: Any, _refs: Any, _accs: Any, *, k: int, **_kw: Any) -> Any:
            asked["k"] = k
            return [[(a, float(i)) for i, a in enumerate(_POOL[:k])]]

        with (
            patch("protea.core.knn_search.search_knn", side_effect=fake_search),
            patch.object(op, "_load_annotations_for", return_value={}),
        ):
            _anns, neighbours = op._unified_load_annotations(MagicMock(), ctx, None)

        assert asked["k"] == 3
        assert neighbours == {"R1", "R2"}

    def test_the_aspect_pre_search_drops_it_the_same_way(self) -> None:
        from protea_method._self_by_sequence import Queries

        from protea.core.operations.predict_go_terms._aspect_helpers import _AspectKnnPreSearch

        asked: dict[str, int] = {}

        def fake_search(_queries: Any, _refs: Any, k: int, _p: Any, _use_cos: bool) -> Any:
            asked["k"] = k
            return [[(a, float(i)) for i, a in enumerate(_POOL[:k])]]

        p = _batch_payload(
            limit_per_entry=2,
            exclude_self_neighbour=True,
            self_exclusion="accession_after_retrieval",
            aspect_separated_knn=True,
        )
        with patch.object(_AspectKnnPreSearch, "_search_aspect", side_effect=fake_search):
            _aspect, neighbours = _AspectKnnPreSearch._knn_one_aspect(
                "F", Queries(["Q1"], np.zeros((1, 3))), {"F": {"accessions": _POOL}}, p, False
            )

        assert asked["k"] == 2
        assert neighbours == [[("R1", 1.0)]]

    def test_it_reads_no_sequence_map(self) -> None:
        from protea.core.operations.predict_go_terms._self_exclusion import needs_sequence_keys

        p = _batch_payload(exclude_self_neighbour=True, self_exclusion="accession_after_retrieval")
        assert needs_sequence_keys(p) is False


# Aspect-separated retrieval is unchanged, unified retrieval is not ----------------------


#: Donors by aspect, as the aspect build reads them: positive rows only. N1 holds
#: NOT rows alone, so it is in no aspect, and it sits right on Q1's direction.
_ASPECT_DONORS = {"P": {"R1", "R2"}, "F": {"R2", "R3"}, "C": {"R1", "R3"}}
_ASPECT_TERMS = {"P": 201, "F": 202, "C": 203}


def _aspect_rows() -> tuple[dict[str, set[str]], dict[str, dict[str, list[dict[str, Any]]]]]:
    go_maps = {
        aspect: {
            acc: [{"go_term_id": _ASPECT_TERMS[aspect], "qualifier": None, "evidence_code": "IDA"}]
            for acc in sorted(donors)
        }
        for aspect, donors in _ASPECT_DONORS.items()
    }
    return {a: set(d) for a, d in _ASPECT_DONORS.items()}, go_maps


def _unified(with_not_only_donor: bool) -> dict[str, Any]:
    rows = {
        "N1": [1.0, 0.0, 0.0],
        "R1": [0.95, 0.2, 0.0],
        "R2": [0.4, 0.9, 0.1],
        "R3": [0.1, 0.3, 0.9],
    }
    accessions = sorted(a for a in rows if with_not_only_donor or a != "N1")
    vectors = np.array([rows[a] for a in accessions], dtype=np.float16)
    return disk_cache._derive_reference_views(accessions, vectors)


def _aspect_run(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, *, with_not_only_donor: bool
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """The aspect-separated path over one pool: its views and its candidate rows."""
    monkeypatch.setattr(disk_cache, "_DISK_CACHE_DIR", tmp_path)
    op = PredictGOTermsBatchOperation()
    request = _PoolRequest(CFG, ANN)
    with (
        patch.object(op, "_load_reference_data", return_value=_unified(with_not_only_donor)),
        patch.object(
            PredictGOTermsBatchOperation,
            "_collect_aspect_annotations",
            staticmethod(lambda *a, **k: _aspect_rows()),
        ),
    ):
        ref_data = op._load_reference_data_per_aspect(MagicMock(), request, _quiet)
    p = _batch_payload(limit_per_entry=2, aspect_separated_knn=True)
    ctx = AspectSeparatedKnnContext(
        valid_accessions=["Q1", "Q9"],
        query_embeddings=_unit(_QUERY_VECTORS),
        ref_data_by_aspect=ref_data,
        annotation_set_id=ANN,
        prediction_set_id=uuid.UUID(int=1),
        payload=p,
    )
    terms = {term: f"GO:{term:07d}" for term in _ASPECT_TERMS.values()}
    aspects = {term: aspect for aspect, term in _ASPECT_TERMS.items()}
    with (
        patch.object(op, "_load_feature_engineering_data", return_value=({}, {}, {}, {})),
        patch.object(op, "_load_go_term_metadata", return_value=(terms, aspects)),
        patch(
            "protea.core.operations.predict_go_terms._aspect_helpers.load_sequence_identities",
            return_value={a: a for a in ["N1", "R1", "R2", "R3", "Q1", "Q9"]},
        ),
    ):
        predictions, _n, _g, _f = op._run_aspect_separated_knn(MagicMock(), ctx)
    return predictions, ref_data


class TestOnlyUnifiedRetrievalChanges:
    def test_aspect_separated_candidate_rows_are_byte_identical_before_and_after(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        before, before_views = _aspect_run(tmp_path / "e4", monkeypatch, with_not_only_donor=True)
        after, after_views = _aspect_run(tmp_path / "e5", monkeypatch, with_not_only_donor=False)

        assert before, "the comparison is empty"
        assert before == after
        for aspect in ASPECT_CODES:
            assert before_views[aspect]["accessions"] == after_views[aspect]["accessions"]
            assert np.array_equal(
                aspect_bank(before_views[aspect], use_cos=True),
                aspect_bank(after_views[aspect], use_cos=True),
            )

    def test_a_not_only_donor_no_longer_takes_a_unified_slot(self) -> None:
        """Before, Q1's nearest neighbour was N1, which donates nothing; now it is R1."""
        from protea.core.knn_search import search_knn

        queries = _unit(_QUERY_VECTORS[:1])
        nearest = {}
        for label, with_not_only_donor in (("before", True), ("after", False)):
            pool = _unified(with_not_only_donor)
            hits = search_knn(
                queries,
                pool["embeddings_f32_cos"],
                pool["accessions"],
                k=1,
                backend="numpy",
                metric="cosine",
                pre_normalized=True,
            )
            nearest[label] = hits[0][0][0]
        assert nearest == {"before": "N1", "after": "R1"}


# The disk switch ------------------------------------------------------------------------


class TestTheDiskSwitch:
    """Off, a miss is built in memory and nothing is written; the same pool is served."""

    def test_the_unified_pool_is_served_and_not_written(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch, disk_write_off: None
    ) -> None:
        monkeypatch.setattr(disk_cache, "_DISK_CACHE_DIR", tmp_path)
        pool = (["R1", "R2"], np.ones((2, 3), dtype=np.float16))
        events: list[str] = []
        served = disk_cache._load_reference_pool_cached(
            disk_cache.RefPoolKey(CFG, ANN),
            lambda: pool,
            emit=lambda name, _fields: events.append(name),
            write=get_tuning().operation.reference_pool_disk_write,
        )
        assert served[0] == pool[0] and np.array_equal(served[1], pool[1])
        assert list(tmp_path.iterdir()) == []
        assert events == ["refpool.cache_not_written"]

    def test_a_cache_already_on_disk_is_still_read(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch, disk_write_off: None
    ) -> None:
        monkeypatch.setattr(disk_cache, "_DISK_CACHE_DIR", tmp_path)
        disk_cache._save_to_disk_cache(CFG, ANN, ["R1"], np.ones((1, 3), dtype=np.float16))
        never = MagicMock(side_effect=AssertionError("the database was read"))
        served = disk_cache._load_reference_pool_cached(
            disk_cache.RefPoolKey(CFG, ANN), never, write=False
        )
        assert served[0] == ["R1"]

    def test_the_load_reads_the_switch(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch, disk_write_off: None
    ) -> None:
        monkeypatch.setattr(disk_cache, "_DISK_CACHE_DIR", tmp_path)
        op = PredictGOTermsBatchOperation()
        query = MagicMock()
        query.count.return_value = 2
        pool = (["R1", "R2"], np.ones((2, 3), dtype=np.float16), "database", "binary")
        with (
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reference.bank_is_sparse",
                return_value=False,
            ),
            patch.object(op, "_reference_pool_query", return_value=query),
            patch.object(op, "_stream_pool", return_value=pool),
        ):
            accessions, _vectors, source, _transfer = op._load_ref_pool(
                MagicMock(), _PoolRequest(CFG, ANN), _quiet
            )
        assert (accessions, source) == (["R1", "R2"], "database")
        assert list(tmp_path.iterdir()) == []

    def test_the_aspect_views_are_the_same_and_nothing_is_written(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch, disk_write_off: None
    ) -> None:
        off, off_views = _aspect_run(tmp_path / "off", monkeypatch, with_not_only_donor=False)
        assert not (tmp_path / "off").exists() or list((tmp_path / "off").iterdir()) == []
        monkeypatch.delenv("PROTEA_TUNING__operation__reference_pool_disk_write")
        get_tuning.cache_clear()
        on, on_views = _aspect_run(tmp_path / "on", monkeypatch, with_not_only_donor=False)
        assert any((tmp_path / "on").iterdir()), "with the switch on the aspects are written"

        assert off == on
        for aspect in ASPECT_CODES:
            assert off_views[aspect]["accessions"] == on_views[aspect]["accessions"]
            assert off_views[aspect]["acc_to_anno_idx"] == on_views[aspect]["acc_to_anno_idx"]
        assert off_views[_UNIFIED_REF_KEY]["accessions"] == on_views[_UNIFIED_REF_KEY]["accessions"]

    def test_it_is_on_by_default(self) -> None:
        get_tuning.cache_clear()
        assert get_tuning().operation.reference_pool_disk_write is True

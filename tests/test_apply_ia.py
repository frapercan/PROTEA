"""Unit tests for the per-candidate ``IA`` information-accretion feature.

Targets ``apply_ia`` / ``_load_ia_feature_map`` in
``protea.core.operations.predict_go_terms._ia_feature``. The IA-table resolver
and the go_id resolver are mocked so the test exercises only the
lookup-and-attach: a candidate whose snapshot-invariant go_id is in the IA map
gets ``IA`` set to the raw accretion value; an unknown candidate is left unset
(LightGBM missing branch); an empty map no-ops with the prediction list intact.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from protea.core.operations.predict_go_terms import _ia_feature as ia
from protea.core.operations.predict_go_terms import _post_knn_pipeline as pkp


def _emit(*_args, **_kwargs) -> None:
    return None


def _run(prediction_dicts, *, ia_map, gid_by_go):
    """Drive ``apply_ia`` with both resolvers mocked.

    ``_load_go_id_and_aspect`` is lazy-imported by ``apply_ia`` from
    ``_post_knn_pipeline``, so it is patched at its definition site there.
    """
    with (
        patch.object(ia, "_load_ia_feature_map", return_value=ia_map),
        patch.object(pkp, "_load_go_id_and_aspect", return_value=(gid_by_go, {})),
    ):
        ia.apply_ia(MagicMock(), MagicMock(), prediction_dicts, _emit)
    return prediction_dicts


def test_candidate_in_ia_map_gets_raw_accretion() -> None:
    rec = {"protein_accession": "Q1", "go_term_id": 11}
    out = _run([rec], ia_map={"GO:0000011": 7.5}, gid_by_go={11: "GO:0000011"})
    assert out[0]["IA"] == 7.5


def test_prefers_inline_go_id_over_int_resolution() -> None:
    rec = {"protein_accession": "Q1", "go_term_id": 11, "go_id": "GO:0000011"}
    # gid_by_go intentionally wrong; the inline go_id must win.
    out = _run([rec], ia_map={"GO:0000011": 3.2}, gid_by_go={11: "GO:9999999"})
    assert out[0]["IA"] == 3.2


def test_candidate_absent_from_ia_map_is_left_unset() -> None:
    rec = {"protein_accession": "Q1", "go_term_id": 22}
    out = _run([rec], ia_map={"GO:0000011": 7.5}, gid_by_go={22: "GO:0000022"})
    assert "IA" not in out[0]


def test_empty_ia_map_noops() -> None:
    rec = {"protein_accession": "Q1", "go_term_id": 11}
    with patch.object(ia, "_load_ia_feature_map", return_value={}):
        ia.apply_ia(MagicMock(), MagicMock(), [rec], _emit)
    assert rec == {"protein_accession": "Q1", "go_term_id": 11}


def test_resolver_priority_env_over_snapshot(tmp_path, monkeypatch) -> None:
    """``PROTEA_IA_FEATURE_PATH`` is honoured ahead of the snapshot ia_url."""
    ia_file = tmp_path / "ia.tsv"
    ia_file.write_text("GO:0000011\t5.0\nGO:0000022\t9.0\n", encoding="utf-8")
    monkeypatch.setenv("PROTEA_IA_FEATURE_PATH", str(ia_file))
    ia._cached_ia_map.cache_clear()
    session = MagicMock()
    out = ia._load_ia_feature_map(session, MagicMock(), None, _emit)
    assert out == {"GO:0000011": 5.0, "GO:0000022": 9.0}
    session.get.assert_not_called()  # never fell through to the snapshot
    ia._cached_ia_map.cache_clear()


def test_resolver_warns_when_nothing_resolves(monkeypatch) -> None:
    monkeypatch.delenv("PROTEA_IA_FEATURE_PATH", raising=False)
    session = MagicMock()
    session.get.return_value = None  # no snapshot / no ia_url
    out = ia._load_ia_feature_map(session, MagicMock(), None, _emit)
    assert out == {}

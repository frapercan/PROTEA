"""serve-offline-reconciliation S3 unit tests.

Covers the four serve-wiring changes, each asserting both the activated
behaviour AND the behaviour-preserving default:

1. ``active_or_latest_reranker`` — active-first, latest-by-created_at fallback.
2. ``_pinned_embedding_config`` / ``_best_embedding_config`` — pinned config
   with safe fallback to the smallest-param auto-pick.
3. ``_predict_payload`` — config-driven serve schema flags (defaults preserved).
4. ``noisy_or_graft_bp`` / ``apply_interpro_bp_graft`` — InterPro2GO BP graft
   post-step (math + gate + default-off no-op).

No real DB or queue: the session is a small fake / MagicMock.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from protea.config.tuning import ServeTuning, TuningSettings, get_tuning

# ---------------------------------------------------------------------------
# 1. active_or_latest_reranker
# ---------------------------------------------------------------------------


class _Terminal:
    """Tail of a query chain: ``.order_by(...).first()`` yields a fixed row."""

    def __init__(self, row: object) -> None:
        self._row = row

    def order_by(self, *_a: object, **_k: object) -> _Terminal:
        return self

    def first(self) -> object:
        return self._row


class _FakeRerankerQuery:
    """Fake ``session.query(RerankerModel)`` chain for the selector helper.

    ``.filter(...)`` models the ``is_active is True`` branch (returns the
    active row); ``.order_by(...)`` directly models the fallback branch
    (returns the latest row). The helper is called with no category/aspect in
    these tests, so the single ``filter`` call is unambiguously the active one.
    """

    def __init__(self, active_row: object, latest_row: object) -> None:
        self._active = active_row
        self._latest = latest_row

    def filter(self, *_a: object, **_k: object) -> _Terminal:
        return _Terminal(self._active)

    def order_by(self, *_a: object, **_k: object) -> _Terminal:
        return _Terminal(self._latest)


def _selector():
    from protea.infrastructure.orm.models.embedding.reranker_model import (
        active_or_latest_reranker,
    )

    return active_or_latest_reranker


def test_active_reranker_is_preferred() -> None:
    active = SimpleNamespace(id=uuid4(), name="active")
    latest = SimpleNamespace(id=uuid4(), name="latest")
    session = MagicMock()
    session.query.return_value = _FakeRerankerQuery(active, latest)
    assert _selector()(session) is active


def test_falls_back_to_latest_when_none_active() -> None:
    latest = SimpleNamespace(id=uuid4(), name="latest")
    session = MagicMock()
    session.query.return_value = _FakeRerankerQuery(None, latest)
    assert _selector()(session) is latest


def test_returns_none_when_no_rows() -> None:
    session = MagicMock()
    session.query.return_value = _FakeRerankerQuery(None, None)
    assert _selector()(session) is None


# ---------------------------------------------------------------------------
# 2. pinned embedding config (with fallback)
# ---------------------------------------------------------------------------


def _patch_serve(monkeypatch: pytest.MonkeyPatch, **serve_kwargs: object) -> None:
    """Patch tuning so ``get_tuning().serve`` reflects ``serve_kwargs``."""
    settings = TuningSettings(serve=ServeTuning(**serve_kwargs))
    monkeypatch.setattr("protea.config.tuning.get_tuning", lambda: settings)


def test_pinned_config_none_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    from protea.api.routers import annotate

    _patch_serve(monkeypatch)  # default: no pin
    session = MagicMock()
    assert annotate._pinned_embedding_config(session) is None
    session.get.assert_not_called()


def test_pinned_config_invalid_uuid_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    from protea.api.routers import annotate

    _patch_serve(monkeypatch, default_embedding_config_id="not-a-uuid")
    session = MagicMock()
    assert annotate._pinned_embedding_config(session) is None


def test_pinned_config_missing_row_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    from protea.api.routers import annotate

    _patch_serve(monkeypatch, default_embedding_config_id=str(uuid4()))
    session = MagicMock()
    session.get.return_value = None
    assert annotate._pinned_embedding_config(session) is None


def test_pinned_config_without_embeddings_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from protea.api.routers import annotate

    cid = uuid4()
    _patch_serve(monkeypatch, default_embedding_config_id=str(cid))
    config = SimpleNamespace(id=cid)
    session = MagicMock()
    session.get.return_value = config
    session.query.return_value.scalar.return_value = False  # no embeddings
    assert annotate._pinned_embedding_config(session) is None


def test_pinned_config_with_embeddings_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    from protea.api.routers import annotate

    cid = uuid4()
    _patch_serve(monkeypatch, default_embedding_config_id=str(cid))
    config = SimpleNamespace(id=cid)
    session = MagicMock()
    session.get.return_value = config
    session.query.return_value.scalar.return_value = True
    assert annotate._pinned_embedding_config(session) is config


def test_best_config_prefers_pin_over_auto_pick(monkeypatch: pytest.MonkeyPatch) -> None:
    from protea.api.routers import annotate

    cid = uuid4()
    _patch_serve(monkeypatch, default_embedding_config_id=str(cid))
    pinned = SimpleNamespace(id=cid)
    session = MagicMock()
    session.get.return_value = pinned
    session.query.return_value.scalar.return_value = True
    # If the pin is honoured, the smallest-param scan is never consulted.
    assert annotate._best_embedding_config(session) is pinned


def test_best_config_falls_back_to_smallest_when_unpinned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from protea.api.routers import annotate

    _patch_serve(monkeypatch)  # no pin
    smallest = SimpleNamespace(id=uuid4())
    session = MagicMock()
    q = session.query.return_value
    q.order_by.return_value.all.return_value = [smallest]
    q.scalar.return_value = True  # smallest has embeddings
    assert annotate._best_embedding_config(session) is smallest


# ---------------------------------------------------------------------------
# 3. serve-flag config-driven predict payload
# ---------------------------------------------------------------------------


def _build_payload():
    from protea.api.routers.annotate import AnnotateFormOptions, _predict_payload

    return _predict_payload(uuid4(), uuid4(), uuid4(), uuid4(), AnnotateFormOptions())


def test_predict_payload_defaults_preserve_behaviour(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_serve(monkeypatch)  # all defaults
    payload = _build_payload()
    assert payload["compute_alignments"] is True
    assert payload["compute_taxonomy"] is True
    assert payload["compute_v6_features"] is False
    assert payload["compute_lineage_features"] is False


def test_predict_payload_reflects_serve_config(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_serve(
        monkeypatch,
        compute_v6_features=True,
        compute_lineage_features=True,
    )
    payload = _build_payload()
    assert payload["compute_v6_features"] is True
    assert payload["compute_lineage_features"] is True
    # Untouched flags keep their (defaulted) values.
    assert payload["compute_alignments"] is True
    assert payload["compute_taxonomy"] is True


# ---------------------------------------------------------------------------
# 4. InterPro2GO BP noisy-OR graft
# ---------------------------------------------------------------------------


def test_noisy_or_updates_existing_candidate() -> None:
    from protea.core.operations.predict_go_terms._interpro_graft import noisy_or_graft_bp

    preds = [{"protein_accession": "P1", "go_id": "GO:1", "reranker_score": 0.5}]
    interpro = [{"protein_accession": "P1", "go_id": "GO:1", "go_term_id": 9, "prob": 0.5}]
    out, stats = noisy_or_graft_bp(preds, interpro)
    # 1 - (1 - 0.5)(1 - 0.5) = 0.75
    assert out[0]["reranker_score"] == pytest.approx(0.75)
    assert out[0]["interpro_graft_present"] == 1.0
    assert stats == {"updated": 1, "added": 0}


def test_noisy_or_appends_new_bp_candidate() -> None:
    from protea.core.operations.predict_go_terms._interpro_graft import noisy_or_graft_bp

    preds = [{"protein_accession": "P1", "go_id": "GO:1", "reranker_score": 0.5}]
    interpro = [{"protein_accession": "P1", "go_id": "GO:2", "go_term_id": 3, "prob": 0.8}]
    out, stats = noisy_or_graft_bp(preds, interpro)
    assert stats == {"updated": 0, "added": 1}
    new = out[-1]
    assert new["go_id"] == "GO:2"
    assert new["aspect"] == "P"
    assert new["reranker_score"] == pytest.approx(0.8)
    # Existing candidate untouched.
    assert out[0]["reranker_score"] == 0.5


def test_noisy_or_missing_base_treated_as_zero() -> None:
    from protea.core.operations.predict_go_terms._interpro_graft import noisy_or_graft_bp

    preds = [{"protein_accession": "P1", "go_id": "GO:1"}]  # no reranker_score
    interpro = [{"protein_accession": "P1", "go_id": "GO:1", "go_term_id": 1, "prob": 0.6}]
    out, _ = noisy_or_graft_bp(preds, interpro)
    assert out[0]["reranker_score"] == pytest.approx(0.6)


def test_noisy_or_clamps_out_of_range() -> None:
    from protea.core.operations.predict_go_terms._interpro_graft import noisy_or_graft_bp

    preds = [{"protein_accession": "P1", "go_id": "GO:1", "reranker_score": 1.5}]
    interpro = [{"protein_accession": "P1", "go_id": "GO:1", "go_term_id": 1, "prob": 0.5}]
    out, _ = noisy_or_graft_bp(preds, interpro)
    # base clamps to 1.0 -> noisy-or stays 1.0
    assert out[0]["reranker_score"] == pytest.approx(1.0)


def test_noisy_or_skips_entries_without_go_or_prob() -> None:
    from protea.core.operations.predict_go_terms._interpro_graft import noisy_or_graft_bp

    preds: list[dict] = []
    interpro = [
        {"protein_accession": "P1", "prob": 0.9},  # no go_id
        {"protein_accession": "P1", "go_id": "GO:1"},  # no prob
    ]
    out, stats = noisy_or_graft_bp(preds, interpro)
    assert out == []
    assert stats == {"updated": 0, "added": 0}


def test_noisy_or_applies_weight() -> None:
    """``weight`` scales the InterPro contribution: 1-(1-base)(1-w*prob)."""
    from protea.core.operations.predict_go_terms._interpro_graft import noisy_or_graft_bp

    preds = [{"protein_accession": "P1", "go_id": "GO:1", "reranker_score": 0.5}]
    interpro = [{"protein_accession": "P1", "go_id": "GO:1", "go_term_id": 9, "prob": 0.5}]
    out, stats = noisy_or_graft_bp(preds, interpro, weight=0.4)
    # 1 - (1 - 0.5)(1 - 0.4 * 0.5) = 1 - 0.5 * 0.8 = 0.6
    assert out[0]["reranker_score"] == pytest.approx(0.6)
    assert stats == {"updated": 1, "added": 0}


def test_noisy_or_skips_zero_weighted_contribution() -> None:
    """``weight * prob <= 0`` contributes nothing (no update, no new row)."""
    from protea.core.operations.predict_go_terms._interpro_graft import noisy_or_graft_bp

    preds = [{"protein_accession": "P1", "go_id": "GO:1", "reranker_score": 0.5}]
    interpro = [
        {"protein_accession": "P1", "go_id": "GO:1", "go_term_id": 9, "prob": 0.8},
        {"protein_accession": "P1", "go_id": "GO:2", "go_term_id": 7, "prob": 0.9},
    ]
    out, stats = noisy_or_graft_bp(preds, interpro, weight=0.0)
    assert out == preds
    assert out[0]["reranker_score"] == 0.5
    assert stats == {"updated": 0, "added": 0}


def test_compute_interpro_bp_preds_graded_bp_only() -> None:
    """Graded score support/n, BP-only, with the denominator counting all
    GO-mapped InterPro entries (MF entries inflate n but are not emitted)."""
    from protea.core.operations.predict_go_terms._interpro_graft import (
        compute_interpro_bp_preds,
    )

    prot2iprs = {"P1": {"IPR1", "IPR2", "IPR3"}}
    ipr2go_direct = {"IPR1": {"GO:bp"}, "IPR2": {"GO:bp"}, "IPR3": {"GO:mf"}}
    go_meta = {"GO:bp": (1, "P"), "GO:mf": (2, "F")}

    def ancestors(_g: str) -> frozenset[str]:
        return frozenset()

    out = compute_interpro_bp_preds(prot2iprs, ipr2go_direct, ancestors, go_meta)
    # n = 3 (all three IPRs carry a GO mapping); support[GO:bp] = 2 -> 2/3.
    # GO:mf is dropped (BP-only) but still counted in n.
    assert out == [{"protein_accession": "P1", "go_id": "GO:bp", "go_term_id": 1, "prob": 2 / 3}]


def test_compute_interpro_bp_preds_propagates_ancestors() -> None:
    """Each InterPro entry's GO set is propagated up the DAG before scoring."""
    from protea.core.operations.predict_go_terms._interpro_graft import (
        compute_interpro_bp_preds,
    )

    prot2iprs = {"P1": {"IPR1", "IPR2"}}
    ipr2go_direct = {"IPR1": {"GO:leaf"}, "IPR2": {"GO:leaf"}}
    go_meta = {"GO:leaf": (10, "P"), "GO:root": (11, "P")}

    def ancestors(go_id: str) -> frozenset[str]:
        return frozenset({"GO:root"}) if go_id == "GO:leaf" else frozenset()

    out = compute_interpro_bp_preds(prot2iprs, ipr2go_direct, ancestors, go_meta)
    by_go = {r["go_id"]: r["prob"] for r in out}
    # Both IPRs imply leaf -> root, so support = 2, n = 2 -> prob 1.0 each.
    assert by_go == {"GO:leaf": pytest.approx(1.0), "GO:root": pytest.approx(1.0)}


def test_compute_interpro_bp_preds_unmapped_ipr_counts_in_denominator() -> None:
    """An InterPro entry whose terms fall outside the snapshot still counts in n."""
    from protea.core.operations.predict_go_terms._interpro_graft import (
        compute_interpro_bp_preds,
    )

    prot2iprs = {"P1": {"IPR1", "IPRX"}}
    ipr2go_direct = {"IPR1": {"GO:bp"}, "IPRX": {"GO:obsolete"}}
    go_meta = {"GO:bp": (1, "P")}  # GO:obsolete absent from snapshot

    def ancestors(_g: str) -> frozenset[str]:
        return frozenset()

    out = compute_interpro_bp_preds(prot2iprs, ipr2go_direct, ancestors, go_meta)
    # IPRX still counts toward n (n = 2) even though its term is dropped.
    assert out == [{"protein_accession": "P1", "go_id": "GO:bp", "go_term_id": 1, "prob": 0.5}]


class _EmptyResult:
    """Tail of a fake ``session.execute(...)`` that yields nothing."""

    def all(self) -> list:
        return []

    def scalar(self) -> None:
        return None


class _EmptySession:
    """Fake session whose every query returns an empty result set."""

    def execute(self, *_a: object, **_k: object) -> _EmptyResult:
        return _EmptyResult()


def test_load_interpro_bp_predictions_noop_without_cached_signatures() -> None:
    """No cached InterPro signature for any query -> graceful empty source."""
    from protea.core.operations.predict_go_terms._interpro_graft import (
        load_interpro_bp_predictions,
    )

    out = load_interpro_bp_predictions(_EmptySession(), uuid4(), ["P1", "P2"])
    assert out == []


def test_apply_graft_is_noop_when_signatures_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no cached signatures the post-step must not mutate preds."""
    from protea.core.operations.predict_go_terms._interpro_graft import (
        apply_interpro_bp_graft,
    )

    _patch_serve(monkeypatch, interpro_bp_graft=True)
    preds = [{"protein_accession": "P1", "go_id": "GO:1", "reranker_score": 0.5}]
    events: list[tuple] = []

    def emit(name, pct, fields, level):  # noqa: ANN001
        events.append((name, fields))

    out = apply_interpro_bp_graft(_EmptySession(), uuid4(), ["P1"], preds, emit)
    assert out == preds
    assert out[0]["reranker_score"] == 0.5
    assert events and events[0][0] == "predict_go_terms_batch.interpro_bp_graft_done"
    assert events[0][1]["candidates_updated"] == 0
    assert events[0][1]["interpro_bp_predictions"] == 0


def test_graft_gate_default_off(monkeypatch: pytest.MonkeyPatch) -> None:
    from protea.core.operations.predict_go_terms._post_knn_pipeline import (
        _interpro_bp_graft_enabled,
    )

    _patch_serve(monkeypatch)  # default
    assert _interpro_bp_graft_enabled() is False


def test_graft_gate_on_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    from protea.core.operations.predict_go_terms._post_knn_pipeline import (
        _interpro_bp_graft_enabled,
    )

    _patch_serve(monkeypatch, interpro_bp_graft=True)
    assert _interpro_bp_graft_enabled() is True


# ---------------------------------------------------------------------------
# ServeTuning defaults + env override
# ---------------------------------------------------------------------------


def test_serve_tuning_defaults_are_behaviour_preserving() -> None:
    s = ServeTuning()
    assert s.default_embedding_config_id is None
    assert s.compute_alignments is True
    assert s.compute_taxonomy is True
    assert s.compute_v6_features is False
    assert s.compute_lineage_features is False
    assert s.interpro_bp_graft is False
    assert s.interpro_bp_graft_weight == 0.5
    assert s.interpro_bp_graft_source_version is None


def test_default_embedding_config_id_short_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from protea.config import tuning as tuning_mod

    cid = str(uuid4())
    for key in list(__import__("os").environ):
        if key.startswith("PROTEA_TUNING__") or key == "PROTEA_DEFAULT_EMBEDDING_CONFIG_ID":
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("PROTEA_DEFAULT_EMBEDDING_CONFIG_ID", cid)
    out = tuning_mod._apply_env_overrides({})
    assert out["serve"]["default_embedding_config_id"] == cid


def test_get_tuning_exposes_serve_group() -> None:
    get_tuning.cache_clear()
    assert isinstance(get_tuning().serve, ServeTuning)

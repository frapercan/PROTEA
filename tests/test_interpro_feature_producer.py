"""Unit tests for the S3 InterPro feature producer.

Covers the env-configured loader, the ``(protein, go_id)`` left-join,
and the 11-column unconditional emission contract (contracts 1.1.0).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from protea.core._interpro_features import (
    INTERPRO_GO_PRED_PATH_ENV,
    apply_interpro_features,
    load_interpro_go_pred,
)
from protea.core._leaf_record_builder import _LeafRecordBuilder

_INTERPRO_COLUMNS = (
    "interpro_hit",
    "interpro_score",
    "interpro_n_signatures",
    "interpro_db_pfam",
    "interpro_db_panther",
    "interpro_db_superfamily",
    "interpro_db_smart",
    "interpro_db_cdd",
    "interpro_db_prosite",
    "knn_present",
    "interpro_present",
)


def _write_tsv(path: Path, rows: list[str]) -> Path:
    path.write_text("".join(line + "\n" for line in rows), encoding="utf-8")
    return path


def test_default_fields_emit_all_11_columns() -> None:
    defaults = _LeafRecordBuilder._interpro_default_fields()
    assert set(defaults) == set(_INTERPRO_COLUMNS)
    # knn_present is True for every KNN leaf record; the rest are an
    # absent-source zero-fill.
    assert defaults["knn_present"] is True
    assert defaults["interpro_present"] is False
    assert defaults["interpro_hit"] is False
    assert defaults["interpro_score"] == 0.0
    assert defaults["interpro_n_signatures"] == 0


def test_loader_unset_env_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(INTERPRO_GO_PRED_PATH_ENV, raising=False)
    assert load_interpro_go_pred() == {}


def test_loader_missing_explicit_path_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_interpro_go_pred(tmp_path / "does_not_exist.tsv")


def test_loader_reads_three_column_table(tmp_path: Path) -> None:
    tsv = _write_tsv(
        tmp_path / "pred.tsv",
        ["P12345\tGO:0003824\t1.0000", "P12345\tGO:0000166\t0.5"],
    )
    table = load_interpro_go_pred(tsv)
    assert set(table) == {("P12345", "GO:0003824"), ("P12345", "GO:0000166")}
    match = table[("P12345", "GO:0003824")]
    assert match.score == 1.0
    assert match.n_signatures == 1
    assert match.member_dbs == frozenset()


def test_loader_env_var_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tsv = _write_tsv(tmp_path / "pred.tsv", ["Q9\tGO:0008150\t0.9"])
    monkeypatch.setenv(INTERPRO_GO_PRED_PATH_ENV, str(tsv))
    table = load_interpro_go_pred()
    assert ("Q9", "GO:0008150") in table


def test_loader_parses_member_dbs_and_count(tmp_path: Path) -> None:
    tsv = _write_tsv(
        tmp_path / "pred.tsv",
        ["P1\tGO:1\t0.8\tPfam,SMART\t3"],
    )
    match = load_interpro_go_pred(tsv)[("P1", "GO:1")]
    assert match.member_dbs == frozenset({"pfam", "smart"})
    assert match.n_signatures == 3


def test_apply_matched_and_unmatched() -> None:
    matched = dict(
        _LeafRecordBuilder._interpro_default_fields(),
        protein_accession="P1",
        go_id="GO:1",
    )
    unmatched = dict(
        _LeafRecordBuilder._interpro_default_fields(),
        protein_accession="P2",
        go_id="GO:2",
    )
    table = {
        ("P1", "GO:1"): _make_match(0.75, 2, {"pfam", "cdd"}),
    }
    apply_interpro_features([matched, unmatched], table)

    # Matched record: all signals filled, presence True, only the two
    # hit member-DBs flipped on.
    assert matched["interpro_hit"] is True
    assert matched["interpro_score"] == 0.75
    assert matched["interpro_n_signatures"] == 2
    assert matched["interpro_present"] is True
    assert matched["interpro_db_pfam"] is True
    assert matched["interpro_db_cdd"] is True
    assert matched["interpro_db_panther"] is False
    assert matched["knn_present"] is True

    # Unmatched record: defaults stand, all 11 columns still present.
    assert set(_INTERPRO_COLUMNS).issubset(unmatched)
    assert unmatched["interpro_present"] is False
    assert unmatched["interpro_hit"] is False
    assert unmatched["knn_present"] is True


def test_apply_empty_table_is_noop() -> None:
    rec = dict(
        _LeafRecordBuilder._interpro_default_fields(),
        protein_accession="P1",
        go_id="GO:1",
    )
    apply_interpro_features([rec], {})
    assert rec["interpro_present"] is False
    assert set(_INTERPRO_COLUMNS).issubset(rec)


def _make_match(score: float, n: int, dbs: set[str]):
    from protea.core._interpro_features import _InterProMatch

    return _InterProMatch(score=score, n_signatures=n, member_dbs=frozenset(dbs))

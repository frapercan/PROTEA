"""ADR-D45: producer-seam tests for the research-dataset export.

Three feature families (``classifier`` / ``self_prior`` / ``association``) were
emitted as a constant ``0.0`` by the leaf record builder whether or not a
producer ran, so three declared-absent families shipped semantically null under
an unchanged ``feature_schema_sha``. This module pins the fix:

* :func:`_LeafRecordBuilder._lafa_default_fields` now emits ``NaN`` (a missing
  measurement, the :func:`_reranker_default_fields` yardstick), not ``0.0``.
* the export records per-family production status in the manifest, so a reader
  learns a family's absence from metadata rather than a column of zeros.
* a shard-write degeneracy check fails loudly when a family recorded as
  produced ships a constant column (the exact shape of the original bug),
  while a declared-absent family passes.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd
import pytest
from protea_contracts import ALL_FEATURES

from protea.core.parquet_export import (
    DECLARED_ABSENT,
    PRODUCED,
    FamilyProvenance,
    ParquetExportContext,
    export_reranker_parquets,
)

_LAFA_COLUMNS = (
    "classifier_score",
    "classifier_present",
    "self_prior_score",
    "association_total",
    "association_cross",
    "association_present",
)


def _row(**overrides: object) -> dict[str, object]:
    """A single shard row with reserved cols + every ALL_FEATURES column."""
    row: dict[str, object] = {
        "protein_accession": "P12345",
        "go_id": "GO:0000001",
        "label": 0,
        "aspect": "P",
    }
    for col in ALL_FEATURES:
        if col in row:
            continue
        if col in {"qualifier", "evidence_code", "taxonomic_relation"}:
            row[col] = "x"
        else:
            row[col] = 0.0
    row.update(overrides)
    return row


def _write_shard(path: Path, rows: list[dict[str, object]]) -> Path:
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    df.to_parquet(path, index=False, compression="snappy")
    return path


def _call_export(
    stage_dir: Path,
    train_rows: list[dict[str, object]],
    provenance: tuple[FamilyProvenance, ...],
) -> dict[str, object]:
    train_shard = _write_shard(stage_dir / "_train_nk.parquet", train_rows)
    return export_reranker_parquets(
        ParquetExportContext(
            stage_dir=stage_dir,
            split_files={"nk": [train_shard]},
            valid_split_versions=[(220, 221)],
            test_files={"nk": None},
            test_old_v=221,
            test_new_v=222,
            name="d45-test",
            k=5,
            embedding_config_id="00000000-0000-0000-0000-000000000001",
            ontology_snapshot_id="00000000-0000-0000-0000-000000000002",
            annotation_source="goa",
            store=None,
            producer_version="d45",
            producer_git_sha=None,
            validate_with_contracts=False,
            feature_family_provenance=provenance,
        )
    )


def _read_manifest(stage_dir: Path) -> dict[str, object]:
    return json.loads((stage_dir / "manifest.json").read_text())


class TestDeclaredAbsentPasses:
    def test_absent_family_of_constant_nan_passes_and_is_recorded(self, tmp_path: Path) -> None:
        # All six LAFA columns constant NaN (declared absent) across the shard.
        rows = [
            _row(distance=0.1, **{c: float("nan") for c in _LAFA_COLUMNS}),
            _row(distance=0.2, **{c: float("nan") for c in _LAFA_COLUMNS}),
        ]
        provenance = (
            FamilyProvenance("classifier", DECLARED_ABSENT, None),
            FamilyProvenance("self_prior", DECLARED_ABSENT, None),
            FamilyProvenance("association", DECLARED_ABSENT, None),
        )
        result = _call_export(tmp_path, rows, provenance)
        assert int(result["n_train_rows"]) == 2

        manifest = _read_manifest(tmp_path)
        recorded = {e["family"]: e for e in manifest["feature_family_provenance"]}
        assert recorded["association"]["state"] == DECLARED_ABSENT
        assert recorded["association"]["producer"] is None
        assert set(recorded) == {"classifier", "self_prior", "association"}


class TestProducedFamilyDegeneracy:
    def test_produced_but_constant_family_raises_with_useful_message(self, tmp_path: Path) -> None:
        # classifier is recorded PRODUCED yet every classifier_* column is a
        # constant 0.0 across the shard: the original D45 bug shape.
        rows = [
            _row(distance=0.1, classifier_score=0.0, classifier_present=0.0),
            _row(distance=0.2, classifier_score=0.0, classifier_present=0.0),
        ]
        provenance = (FamilyProvenance("classifier", PRODUCED, "some.wired.producer"),)
        with pytest.raises(ValueError) as exc:
            _call_export(tmp_path, rows, provenance)
        msg = str(exc.value)
        assert "classifier" in msg  # family name
        assert "'train' split" in msg  # shard / split name
        assert "classifier_score" in msg and "0.0" in msg  # constant value

    def test_produced_and_varying_family_passes_and_is_recorded(self, tmp_path: Path) -> None:
        # classifier PRODUCED and varying across the shard -> not degenerate.
        rows = [
            _row(distance=0.1, classifier_score=0.1, classifier_present=1.0),
            _row(distance=0.2, classifier_score=0.9, classifier_present=1.0),
        ]
        provenance = (FamilyProvenance("classifier", PRODUCED, "some.wired.producer"),)
        result = _call_export(tmp_path, rows, provenance)
        assert int(result["n_train_rows"]) == 2
        manifest = _read_manifest(tmp_path)
        recorded = {e["family"]: e for e in manifest["feature_family_provenance"]}
        assert recorded["classifier"]["state"] == PRODUCED
        assert recorded["classifier"]["producer"] == "some.wired.producer"

    def test_no_provenance_disables_the_check(self, tmp_path: Path) -> None:
        # Empty provenance (legacy callers) never triggers the degeneracy check,
        # even with constant LAFA columns, and writes no provenance block.
        rows = [_row(distance=0.1), _row(distance=0.2)]
        result = _call_export(tmp_path, rows, ())
        assert int(result["n_train_rows"]) == 2
        assert "feature_family_provenance" not in _read_manifest(tmp_path)


class TestLafaDefaultsAreMissing:
    def test_lafa_default_fields_emit_nan_not_zero(self) -> None:
        from protea.core._leaf_record_builder import _LeafRecordBuilder

        fields = _LeafRecordBuilder._lafa_default_fields()
        assert set(fields) == set(_LAFA_COLUMNS)
        for col, val in fields.items():
            assert isinstance(val, float) and math.isnan(val), (
                f"{col} must default to NaN (declared absent), got {val!r}"
            )


class TestProvenanceBuilder:
    def test_flags_off_all_declared_absent(self) -> None:
        from protea.core.training_dump._export_features import (
            ExportParityFlags,
            build_lafa_family_provenance,
        )

        prov = {p.family: p for p in build_lafa_family_provenance(ExportParityFlags())}
        assert set(prov) == {"classifier", "self_prior", "association", "protst_text"}
        for entry in prov.values():
            assert entry.state == DECLARED_ABSENT
            assert entry.producer is None

    def test_flags_on_marks_produced_with_producer_identity(self) -> None:
        from protea.core.training_dump._export_features import (
            ExportParityFlags,
            build_lafa_family_provenance,
        )

        prov = {
            p.family: p
            for p in build_lafa_family_provenance(
                ExportParityFlags(
                    self_prior=True, association=True, classifier=True, protst_text=True
                )
            )
        }
        for entry in prov.values():
            assert entry.state == PRODUCED
            assert entry.producer  # a non-empty producer identity
        assert "apply_self_prior" in prov["self_prior"].producer
        assert "apply_association" in prov["association"].producer
        assert "apply_protst_text" in prov["protst_text"].producer

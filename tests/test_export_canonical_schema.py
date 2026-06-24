"""Canonical export parquet schema: mixed-batch write stability.

Reproduces the recurring production failure where the export's
``ParquetWriter`` inferred its schema from the FIRST batch and then rejected
a later batch whose inferred dtype differed. With per-chunk batching a chunk
can be all-KNN (``k_position`` int64) or contain classifier-only rows
(``k_position`` NaN -> float64), and the inferred schemas clash:

    Table schema does not match schema used to create file
    ... k_position: double vs k_position: int64

The fix writes every batch under one explicit canonical schema. These tests
write a KNN-only batch and a classifier-only batch in SEPARATE flushes (as
the live per-chunk flush does), assert the parquet writes successfully with a
single stable schema, and assert the values read back are correct (int-valued
KNN fields became float64 e.g. 3 -> 3.0; classifier-only NaNs preserved).
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from protea.core._export_schema import (
    CANONICAL_COLUMN_ORDER,
    CANONICAL_EXPORT_SCHEMA,
    export_table_from_records,
)

# Columns the live error names plus the audited int-valued KNN fields that go
# NaN for a non-KNN record. All MUST be float64 in the canonical schema.
_FLOAT_INT_DRIFT_COLUMNS = (
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "length_query",
    "length_ref",
    "taxonomic_distance",
    "taxonomic_common_ancestors",
)


def _full_record(overrides: dict[str, Any]) -> dict[str, Any]:
    """Build a record carrying the full canonical column set.

    Defaults are type-correct zeros/blanks per column kind; ``overrides``
    sets the fields under test (the int-vs-NaN drift columns).
    """
    rec: dict[str, Any] = {}
    for name in CANONICAL_COLUMN_ORDER:
        field = CANONICAL_EXPORT_SCHEMA.field(name)
        if pa.types.is_string(field.type):
            rec[name] = ""
        elif pa.types.is_boolean(field.type):
            rec[name] = False
        elif pa.types.is_integer(field.type):
            rec[name] = 0
        else:
            rec[name] = 0.0
    rec.update(overrides)
    return rec


def _knn_record() -> dict[str, Any]:
    """An all-KNN row: int-valued k_position / frequencies (the int64 batch)."""
    return _full_record(
        {
            "protein_accession": "P00001",
            "go_id": "GO:0000001",
            "aspect": "P",
            "label": 1,
            "ref_protein_accession": "Q00001",
            "knn_present": True,
            "k_position": 3,
            "go_term_frequency": 7,
            "ref_annotation_density": 42,
            "length_query": 250,
            "length_ref": 312,
            "taxonomic_distance": 4,
            "taxonomic_common_ancestors": 9,
            "vote_count": 5,
        }
    )


def _classifier_only_record() -> dict[str, Any]:
    """A classifier-only row: the int-valued KNN fields are NaN (float batch)."""
    nan = float("nan")
    return _full_record(
        {
            "protein_accession": "P00002",
            "go_id": "GO:0000002",
            "aspect": "F",
            "label": 0,
            "ref_protein_accession": "classifier",
            "knn_present": False,
            "k_position": nan,
            "go_term_frequency": nan,
            "ref_annotation_density": nan,
            "length_query": nan,
            "length_ref": nan,
            "taxonomic_distance": nan,
            "taxonomic_common_ancestors": nan,
            "vote_count": 0,
            "classifier_score": 0.8,
            "classifier_present": 1.0,
        }
    )


def test_drift_columns_are_float64() -> None:
    """Every int-valued-but-NaN-for-non-KNN column is float64 in the schema."""
    for col in _FLOAT_INT_DRIFT_COLUMNS:
        assert pa.types.is_floating(CANONICAL_EXPORT_SCHEMA.field(col).type), col
    # label stays int64 (always present, never NaN); knn_present stays bool.
    assert pa.types.is_integer(CANONICAL_EXPORT_SCHEMA.field("label").type)
    assert pa.types.is_boolean(CANONICAL_EXPORT_SCHEMA.field("knn_present").type)


def test_mixed_batches_write_with_single_stable_schema(tmp_path: Path) -> None:
    """KNN-only and classifier-only batches written in SEPARATE flushes succeed.

    This is the live per-chunk-flush scenario: without the explicit schema the
    second ``write_table`` raises on k_position double-vs-int64. With it, both
    flushes share one schema and the file is written cleanly.
    """
    out = tmp_path / "train.parquet"
    knn_table = export_table_from_records([_knn_record(), _knn_record()])
    cls_table = export_table_from_records([_classifier_only_record()])

    # Both per-batch tables already carry the identical canonical schema.
    assert knn_table.schema.equals(CANONICAL_EXPORT_SCHEMA)
    assert cls_table.schema.equals(CANONICAL_EXPORT_SCHEMA)

    # Construct the writer with the canonical schema (mirrors _flush) and write
    # the two batches as separate tables: the all-int batch FIRST so a
    # first-batch-inference writer would lock int64 and reject the NaN batch.
    writer = pq.ParquetWriter(str(out), CANONICAL_EXPORT_SCHEMA)
    writer.write_table(knn_table)
    writer.write_table(cls_table)
    writer.close()

    assert out.exists()
    read = pq.read_table(str(out))
    assert read.num_rows == 3
    assert read.schema.equals(CANONICAL_EXPORT_SCHEMA)


def test_values_preserved_ints_become_floats_nans_kept(tmp_path: Path) -> None:
    """Int KNN values cast to float (3 -> 3.0); classifier-only NaNs preserved."""
    out = tmp_path / "train.parquet"
    writer = pq.ParquetWriter(str(out), CANONICAL_EXPORT_SCHEMA)
    writer.write_table(export_table_from_records([_knn_record()]))
    writer.write_table(export_table_from_records([_classifier_only_record()]))
    writer.close()

    df = pq.read_table(str(out)).to_pandas()
    knn_row = df[df["protein_accession"] == "P00001"].iloc[0]
    cls_row = df[df["protein_accession"] == "P00002"].iloc[0]

    # KNN ints became float64, same numeric value.
    assert knn_row["k_position"] == 3.0
    assert knn_row["go_term_frequency"] == 7.0
    assert knn_row["ref_annotation_density"] == 42.0
    assert knn_row["length_query"] == 250.0
    assert knn_row["taxonomic_common_ancestors"] == 9.0
    # label stayed integral and exact.
    assert int(knn_row["label"]) == 1
    assert knn_row["knn_present"] is True or knn_row["knn_present"]

    # Classifier-only NaNs preserved across the round-trip.
    for col in _FLOAT_INT_DRIFT_COLUMNS:
        assert math.isnan(cls_row[col]), col
    assert cls_row["classifier_score"] == 0.8
    assert cls_row["classifier_present"] == 1.0
    assert not cls_row["knn_present"]


def test_schema_column_order_matches_canonical_order() -> None:
    """Schema field order is the #650 canonical builder emit order."""
    assert tuple(CANONICAL_EXPORT_SCHEMA.names) == CANONICAL_COLUMN_ORDER

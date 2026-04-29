"""Round-trip tests for EvaluationData parquet (de)serialization.

``generate_evaluation_set`` persists the full NK/LK/PK/known/pk_known
delta to the artifact store; ``load_evaluation_data_for_set`` reads it
back via ``deserialize_evaluation_data_from_bytes``.  These tests lock
that round-trip contract end to end so the layout stays stable across
refactors — a schema drift here silently breaks every downstream
consumer (lab dump, predict_go_terms, cafaeval).
"""

from __future__ import annotations

from pathlib import Path

from protea.core.evaluation import (
    EvaluationData,
    deserialize_evaluation_data_from_bytes,
    serialize_evaluation_data_to_parquet,
)


def _fixture() -> EvaluationData:
    """Non-trivial ground-truth with every bucket populated."""
    return EvaluationData(
        nk={"P00001": {"GO:0003674"}, "P00002": {"GO:0008150", "GO:0005575"}},
        lk={"P00003": {"GO:0004567"}},
        pk={"P00004": {"GO:0009876", "GO:0001234"}},
        known={"P00004": {"GO:1111111", "GO:2222222"}, "P00005": {"GO:3333333"}},
        pk_known={"P00004": {"GO:1111111"}},
    )


class TestParquetRoundTrip:
    def test_all_five_buckets_preserved(self, tmp_path: Path):
        data = _fixture()
        dest = tmp_path / "gt.parquet"

        serialize_evaluation_data_to_parquet(data, dest)
        blob = dest.read_bytes()
        restored = deserialize_evaluation_data_from_bytes(blob)

        assert restored.nk == data.nk
        assert restored.lk == data.lk
        assert restored.pk == data.pk
        assert restored.known == data.known
        assert restored.pk_known == data.pk_known

    def test_empty_evaluation_data_roundtrips_cleanly(self, tmp_path: Path):
        data = EvaluationData()
        dest = tmp_path / "empty.parquet"

        serialize_evaluation_data_to_parquet(data, dest)
        restored = deserialize_evaluation_data_from_bytes(dest.read_bytes())

        assert restored.nk == {}
        assert restored.lk == {}
        assert restored.pk == {}
        assert restored.known == {}
        assert restored.pk_known == {}

    def test_stats_survive_roundtrip(self, tmp_path: Path):
        data = _fixture()
        dest = tmp_path / "stats.parquet"

        serialize_evaluation_data_to_parquet(data, dest)
        restored = deserialize_evaluation_data_from_bytes(dest.read_bytes())

        assert restored.stats() == data.stats()

    def test_serialize_creates_missing_parent_dirs(self, tmp_path: Path):
        data = _fixture()
        nested = tmp_path / "a" / "b" / "c" / "gt.parquet"
        assert not nested.parent.exists()

        serialize_evaluation_data_to_parquet(data, nested)

        assert nested.exists()
        restored = deserialize_evaluation_data_from_bytes(nested.read_bytes())
        assert restored.nk == data.nk

    def test_returns_dest_for_chaining(self, tmp_path: Path):
        dest = tmp_path / "gt.parquet"
        out = serialize_evaluation_data_to_parquet(_fixture(), dest)
        assert out == dest

    def test_deserialize_on_unknown_bucket_ignores_bad_rows(self, tmp_path: Path):
        """Forward compatibility — extra bucket values should not crash."""
        import pandas as pd

        df = pd.DataFrame(
            [
                ("P00001", "GO:0003674", "nk"),
                ("P00002", "GO:0004567", "lk"),
                ("P00003", "GO:0009876", "pk"),
                ("P00004", "GO:1111111", "known"),
                ("P00004", "GO:1111111", "pk_known"),
            ],
            columns=["protein_accession", "go_id", "bucket"],
        )
        df["bucket"] = df["bucket"].astype("category")
        dest = tmp_path / "mixed.parquet"
        df.to_parquet(dest, index=False, compression="snappy")

        restored = deserialize_evaluation_data_from_bytes(dest.read_bytes())
        assert restored.nk == {"P00001": {"GO:0003674"}}
        assert restored.lk == {"P00002": {"GO:0004567"}}
        assert restored.pk == {"P00003": {"GO:0009876"}}
        assert restored.known == {"P00004": {"GO:1111111"}}
        assert restored.pk_known == {"P00004": {"GO:1111111"}}


class TestParquetFormat:
    def test_parquet_is_snappy_compressed(self, tmp_path: Path):
        """Lock the compression choice — consumers assume snappy."""
        import pyarrow.parquet as pq

        dest = tmp_path / "gt.parquet"
        serialize_evaluation_data_to_parquet(_fixture(), dest)

        meta = pq.read_metadata(dest)
        # Every row group's every column must be snappy
        for rg in range(meta.num_row_groups):
            rgm = meta.row_group(rg)
            for col in range(rgm.num_columns):
                assert rgm.column(col).compression.upper() == "SNAPPY"

    def test_schema_columns_stable(self, tmp_path: Path):
        """The three-column long layout is load-bearing for downstream."""
        import pyarrow.parquet as pq

        dest = tmp_path / "gt.parquet"
        serialize_evaluation_data_to_parquet(_fixture(), dest)
        schema = pq.read_schema(dest)
        assert set(schema.names) == {"protein_accession", "go_id", "bucket"}

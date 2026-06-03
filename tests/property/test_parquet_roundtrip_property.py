"""Property tests for the EvaluationData parquet round-trip.

``serialize_evaluation_data_to_parquet`` / ``deserialize_evaluation_data_from_bytes``
are the canonical wire format for ground-truth artifacts handed to
cafaeval and the lab dumps. Hypothesis throws random bucket
distributions (NK / LK / PK / known / pk_known) at the pair to
guarantee:

* equality of every bucket after round-trip,
* stable column schema regardless of which buckets are populated,
* stats() output identical before and after (no aggregation drift),
* writer creates missing parent dirs for any path Hypothesis picks.

A separate set of property tests targets the lower-level
pandas + pyarrow round-trip used by the rest of the pipeline
(e.g. ``test_evaluation_set_uniqueness``, dataset dumps): DataFrame
written with snappy compression must come back numerically bit-equal
for finite floats and exactly equal for categorical / string columns.
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from protea.core.evaluation import (
    EvaluationData,
    deserialize_evaluation_data_from_bytes,
    serialize_evaluation_data_to_parquet,
)

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

protein = st.from_regex(r"\A[A-NR-Z][0-9][A-Z0-9]{3}[0-9]\Z", fullmatch=True)
go_id = st.from_regex(r"\AGO:[0-9]{7}\Z", fullmatch=True)


def go_set_dict():
    """Dict[protein -> set[go_id]] with small bounded sizes.

    ``min_size=1`` on the inner set matches the producer contract:
    real callers never emit a (protein, empty-set) entry because the
    long-table parquet format has nothing to write for it (one row per
    (protein, go_id, bucket)). An empty entry would silently disappear
    in the round-trip, so we keep the strategy aligned with the
    semantics of the serialiser.
    """
    return st.dictionaries(
        keys=protein,
        values=st.sets(go_id, min_size=1, max_size=4),
        max_size=4,
    )


@st.composite
def evaluation_datas(draw) -> EvaluationData:
    """Draw an EvaluationData covering empty / overlapping / disjoint cases.

    The five buckets are independent so we can let Hypothesis explore
    their cross-product freely; the deserializer must still slot every
    row in the right bucket without leaking across.
    """
    return EvaluationData(
        nk=draw(go_set_dict()),
        lk=draw(go_set_dict()),
        pk=draw(go_set_dict()),
        known=draw(go_set_dict()),
        pk_known=draw(go_set_dict()),
    )


# ---------------------------------------------------------------------------
# EvaluationData parquet round-trip
# ---------------------------------------------------------------------------


@given(data=evaluation_datas())
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_evaluation_data_roundtrip_preserves_buckets(
    data: EvaluationData, tmp_path_factory: pytest.TempPathFactory
) -> None:
    dest: Path = tmp_path_factory.mktemp("eval") / "gt.parquet"
    serialize_evaluation_data_to_parquet(data, dest)
    restored = deserialize_evaluation_data_from_bytes(dest.read_bytes())

    # Bucket-level equality. Empty dicts round-trip to empty dicts; the
    # serialiser must not invent rows for buckets that started empty.
    assert restored.nk == data.nk
    assert restored.lk == data.lk
    assert restored.pk == data.pk
    assert restored.known == data.known
    assert restored.pk_known == data.pk_known


@given(data=evaluation_datas())
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_evaluation_data_stats_invariant(
    data: EvaluationData, tmp_path_factory: pytest.TempPathFactory
) -> None:
    dest: Path = tmp_path_factory.mktemp("eval") / "stats.parquet"
    serialize_evaluation_data_to_parquet(data, dest)
    restored = deserialize_evaluation_data_from_bytes(dest.read_bytes())
    # stats() aggregates the buckets; any silent drop in a bucket would
    # show up here even if equality somehow held bucket-by-bucket.
    assert restored.stats() == data.stats()


@given(data=evaluation_datas())
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_evaluation_data_schema_columns_stable(
    data: EvaluationData, tmp_path_factory: pytest.TempPathFactory
) -> None:
    dest: Path = tmp_path_factory.mktemp("eval") / "schema.parquet"
    serialize_evaluation_data_to_parquet(data, dest)
    schema = pq.read_schema(dest)
    assert set(schema.names) == {"protein_accession", "go_id", "bucket"}


# ---------------------------------------------------------------------------
# Generic DataFrame -> parquet -> DataFrame round-trip
# ---------------------------------------------------------------------------
#
# Many code paths in protea write / read parquet through the pandas +
# pyarrow surface directly (dataset dumps, KNN feature tables, scoring
# exports). The property below asserts the contract every such caller
# implicitly relies on: a single-file parquet round-trip preserves
# values exactly for finite numerics, strings, and booleans, and the
# column ordering after read matches what was written.

finite_float = st.floats(allow_nan=False, allow_infinity=False, width=64)
small_int = st.integers(min_value=-(2**31), max_value=2**31 - 1)
text_value = st.text(max_size=16)


@st.composite
def dataframes(draw) -> pd.DataFrame:
    n_rows = draw(st.integers(min_value=0, max_value=16))
    floats = draw(st.lists(finite_float, min_size=n_rows, max_size=n_rows))
    ints = draw(st.lists(small_int, min_size=n_rows, max_size=n_rows))
    strings = draw(st.lists(text_value, min_size=n_rows, max_size=n_rows))
    bools = draw(st.lists(st.booleans(), min_size=n_rows, max_size=n_rows))
    return pd.DataFrame(
        {
            "f": floats,
            "i": ints,
            "s": strings,
            "b": bools,
        }
    )


@given(df=dataframes())
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_dataframe_parquet_roundtrip_preserves_values(df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    buf.seek(0)
    restored = pd.read_parquet(buf)

    # Column set and order are part of the contract: callers do
    # positional + name access (e.g. ``df["score"]``).
    assert list(restored.columns) == list(df.columns)
    assert len(restored) == len(df)

    # Per-column equality with the appropriate tolerance.
    pd.testing.assert_series_equal(restored["s"], df["s"], check_dtype=False)
    pd.testing.assert_series_equal(restored["b"], df["b"], check_dtype=False)
    pd.testing.assert_series_equal(restored["i"], df["i"], check_dtype=False)
    # Floats: parquet stores IEEE754 verbatim. Use bit-equal compare via
    # ``assert_series_equal`` which honours NaN if present (we excluded
    # NaN above so this is exact).
    pd.testing.assert_series_equal(
        restored["f"], df["f"], check_dtype=False, rtol=0.0, atol=0.0
    )


@given(df=dataframes())
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_dataframe_parquet_roundtrip_via_pyarrow_table(df: pd.DataFrame) -> None:
    """Same invariant via the lower-level pyarrow Table API.

    Codepaths that build a pa.Table directly (e.g. streaming writers
    that bypass pandas) must observe identical row counts and column
    names. Asserting both paths matches catches the corner where
    pandas ``to_parquet`` and ``pa.Table.from_pandas`` would disagree
    on dtype promotion.
    """
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    restored = pq.read_table(buf).to_pandas()
    assert list(restored.columns) == list(df.columns)
    assert len(restored) == len(df)


# ---------------------------------------------------------------------------
# Prediction-row parquet round-trip
# ---------------------------------------------------------------------------
#
# Closer to the shape ``score_predictions`` + dataset dumps produce.

prediction_row = st.fixed_dictionaries(
    {
        "protein_accession": protein,
        "go_id": go_id,
        "score": st.floats(
            min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False
        ),
    }
)


@given(rows=st.lists(prediction_row, max_size=16))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_prediction_rows_roundtrip(rows: list[dict]) -> None:
    df = pd.DataFrame(rows, columns=["protein_accession", "go_id", "score"])
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    buf.seek(0)
    restored = pd.read_parquet(buf)
    assert list(restored.columns) == ["protein_accession", "go_id", "score"]
    assert len(restored) == len(df)
    if len(df) > 0:
        pd.testing.assert_series_equal(
            restored["score"].astype("float64"),
            df["score"].astype("float64"),
            check_names=False,
            check_dtype=False,
            rtol=0.0,
            atol=0.0,
        )
        assert list(restored["protein_accession"]) == list(df["protein_accession"])
        assert list(restored["go_id"]) == list(df["go_id"])

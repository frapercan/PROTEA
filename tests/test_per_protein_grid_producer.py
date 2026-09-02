"""The grid producer, checked through the FILE against the consumer's own reader.

The point of this file is one round trip. ``compare_paired_panels`` defines a
contract, this branch writes the producer for it, and the only thing that
proves they agree is bytes: a file written here, opened by the reader that will
run in production, and accepted. Nothing is imported across the two sides except
the constants, which are declared separately on each side so a typo in one
cannot cancel a typo in the other.

The reader is materialised from its own branch rather than re-implemented. A
re-implementation would be a second opinion about the contract, which is exactly
what the contract exists to remove, and it would pass while the real reader
refused. When the branch is not reachable the tests skip loudly rather than
falling back to a vendored copy that could drift.

The deliberately malformed cases are built by corrupting THIS producer's own
output, one property at a time. A hand-written broken file only proves the
reader rejects a hand-written broken file; corrupting real output proves the
gate stands between this producer and the consumer.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from protea.core.operations._run_cafa_per_protein import (
    GRID_FILENAME,
    GRID_META_PREFIX,
    GridArtifact,
    GridFrame,
    GridProducerError,
    SinkAlignmentError,
    UnstampableFrameError,
    grid_rows_from_sink,
    tau_grid_for,
    write_grid_parquet,
)

REPO = Path(__file__).resolve().parents[1]

#: Where the consumer's reader may be found, in order of preference. The
#: working tree comes first so that the day the consumer merges this stops
#: depending on git at all; ``origin/develop`` next, for the window after the
#: merge and before this branch rebases; the feature branch last, which is the
#: only one that exists today.
#:
#: A list rather than one ref because the single ref was a slow trap. A bare
#: ``actions/checkout`` fetches depth 1 of one branch, so the feature ref is
#: unreachable in CI and the whole round trip skipped: 24 of 35 tests, green
#: check, contract never exercised anywhere but the author's machine. Worse, it
#: was durable, because the branch is deleted when the consumer merges and the
#: skip would then be permanent. The workflow now fetches full history, and
#: :func:`_load_consumer` FAILS rather than skips whenever ``CI`` is set.
CONSUMER_LOCAL = "protea/core/operations/{name}.py"
CONSUMER_REFS = ("origin/develop", "origin/feat/a-difference-carries-its-interval")
CONSUMER_MODULES = ("_paired_panels_bootstrap", "_paired_panels_artifact")

BP = "biological_process"
MF = "molecular_function"


# ---------------------------------------------------------------------------
# The consumer, loaded from its own branch
# ---------------------------------------------------------------------------


def _blob(ref: str, path: str) -> bytes | None:
    proc = subprocess.run(
        ["git", "show", f"{ref}:{path}"], cwd=REPO, capture_output=True, check=False
    )
    return proc.stdout if proc.returncode == 0 else None


def _consumer_source(name: str) -> bytes | None:
    """The reader's bytes, from the working tree if it is here, else from a ref."""
    local = REPO / CONSUMER_LOCAL.format(name=name)
    if local.exists():
        return local.read_bytes()
    for ref in CONSUMER_REFS:
        blob = _blob(ref, f"protea/core/operations/{name}.py")
        if blob is not None:
            return blob
    return None


def _unavailable(name: str) -> str:
    return (
        f"the consumer's {name} is not in this checkout and none of {list(CONSUMER_REFS)} is "
        "reachable, so the round trip cannot run against the real reader. Fetch the branch "
        "(the workflow uses fetch-depth: 0 for exactly this) rather than vendoring the "
        "reader: a vendored copy is a second opinion about the contract and would pass while "
        "the reader refused."
    )


@pytest.fixture(scope="session")
def consumer(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """The consumer's reader module, exactly as it will run in production."""
    root = tmp_path_factory.mktemp("consumer_branch")
    loaded: dict[str, Any] = {}
    for name in CONSUMER_MODULES:
        blob = _consumer_source(name)
        if blob is None:
            # Never a skip under CI. The round trip IS the success criterion of
            # this change, and a criterion that reports green when it did not
            # run is worse than one that fails.
            if os.environ.get("CI"):
                pytest.fail(_unavailable(name))
            pytest.skip(_unavailable(name))
        target = root / f"{name}.py"
        target.write_bytes(blob)
        dotted = f"protea.core.operations.{name}"
        spec = importlib.util.spec_from_file_location(dotted, target)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        # Registered under the dotted name because the reader imports the
        # bootstrap module by that path. Neither module exists on this branch,
        # so nothing real is shadowed.
        sys.modules[dotted] = module
        spec.loader.exec_module(module)
        loaded[name] = module
    yield loaded["_paired_panels_artifact"]
    for name in CONSUMER_MODULES:
        sys.modules.pop(f"protea.core.operations.{name}", None)


# ---------------------------------------------------------------------------
# A sink, shaped the way cafaeval's really is
# ---------------------------------------------------------------------------


class FakeSink:
    """A stand-in holding records of the shape ``PerProteinSink`` stores.

    Built by hand rather than by running cafaeval: the producer's contract is
    with the record dict, and a real evaluation would drag in an OBO, a
    prediction file and a fork pool to assert the same thing.
    """

    def __init__(self, records: list[dict[str, Any]]) -> None:
        self.records = records


def _row(values: list[float], n_tau: int) -> np.ndarray:
    """A non-increasing row of width ``n_tau`` from a short descending shape.

    The mass columns are reverse-cumulative sums along an ascending grid, so
    every row falls. Widening a three-value shape to 99 by repetition keeps
    that property and keeps the pooled curve varying, which is what the
    consumer's broadcast detection looks for.
    """
    idx = (np.arange(n_tau) * len(values)) // n_tau
    return np.asarray(values, dtype=np.float64)[idx]


def _block(rows: list[list[float]], n_tau: int) -> np.ndarray:
    return np.stack([_row(r, n_tau) for r in rows])


def _record(
    ns: str,
    variant: str,
    tp: np.ndarray,
    pred: np.ndarray,
    n_gt: np.ndarray,
    *,
    ids: dict[str, int],
    row_index: np.ndarray,
    protein_rows: np.ndarray | None = None,
) -> dict[str, Any]:
    return {
        "tp_at_tau": np.asarray(tp, dtype=np.float64),
        "pred_at_tau": np.asarray(pred, dtype=np.float64),
        "n_gt": np.asarray(n_gt),
        "protein_rows": protein_rows,
        "ns": ns,
        "variant": variant,
        "row_index": np.asarray(row_index),
        "ids": dict(ids),
    }


IDS = {"P00001": 0, "P00002": 1, "P00003": 2}

#: The two branches, as a real evaluation produces them. Not one rescaled: the
#: per-protein ratios below are 2.7, 2.5 and 1.5, because ``toi_ia`` and
#: ``toi`` are different term sets and a protein's weighted mass is a sum over
#: a different set of terms, not the same sum with weights.
_UNWEIGHTED_TP = [[2.0, 2.0, 1.0], [1.0, 1.0, 1.0], [1.0, 0.0, 0.0]]
_UNWEIGHTED_PRED = [[2.0, 2.0, 1.0], [1.0, 1.0, 1.0], [2.0, 0.0, 0.0]]
_UNWEIGHTED_GT = [2, 1, 1]
_WEIGHTED_TP = [[5.0, 5.0, 3.5], [2.5, 2.5, 2.5], [1.5, 0.0, 0.0]]
_WEIGHTED_PRED = [[5.0, 5.0, 3.5], [2.5, 2.5, 2.5], [4.0, 0.0, 0.0]]
_WEIGHTED_GT = [5.0, 2.5, 1.5]


def _nk_sink(n_tau: int = 3) -> FakeSink:
    """Both variants, full width, two namespaces. The NK/LK shape.

    ``n_gt`` arrives ``int64`` on the unweighted branch and ``float64`` on the
    weighted one, which is what the kernel really hands over, so the producer
    has to cast rather than inherit. The second namespace carries twice the
    weighted mass so a reader that mixed the two up would be caught.
    """
    records: list[dict[str, Any]] = []
    for ns, scale in ((BP, 1.0), (MF, 2.0)):
        records.append(
            _record(
                ns,
                "unweighted",
                _block(_UNWEIGHTED_TP, n_tau),
                _block(_UNWEIGHTED_PRED, n_tau),
                np.array(_UNWEIGHTED_GT, dtype=np.int64),
                ids=IDS,
                row_index=np.arange(3),
            )
        )
        records.append(
            _record(
                ns,
                "weighted",
                _block(_WEIGHTED_TP, n_tau) * scale,
                _block(_WEIGHTED_PRED, n_tau) * scale,
                np.array(_WEIGHTED_GT) * scale,
                ids=IDS,
                row_index=np.arange(3),
            )
        )
    return FakeSink(records)


def _pk_sink(n_tau: int = 3) -> FakeSink:
    """The PK shape, as the installed kernel really emits it.

    Verified against ``compute_confusion_matrix_exclude_sparse``: the PK branch
    does NOT truncate. It hands the kernel every row of ``proteins_has_gt``,
    which is computed BEFORE the exclusion mask, numbers them with
    ``np.flatnonzero(proteins_has_gt)``, and reports eligibility separately in
    ``protein_rows``. So a protein whose whole ground truth turned out to be
    prior knowledge arrives as a full-width row with ``n_gt == 0``, not as an
    absent one. An earlier version of this fixture truncated the arrays, which
    is the NK/LK shape, and in that shape the population defects below cannot
    occur at all.

    P00002 is that protein. Here it predicted nothing inside the terms of
    interest, so its row is identically zero, dropping it changes no pooled sum,
    and the file is faithful. That is the PK case the producer can write.
    """
    unweighted = _record(
        BP,
        "unweighted",
        _block([[2.0, 2.0, 2.0], [0.0, 0.0, 0.0], [1.0, 1.0, 1.0]], n_tau),
        _block([[2.0, 2.0, 2.0], [0.0, 0.0, 0.0], [2.0, 2.0, 1.0]], n_tau),
        np.array([2.0, 0.0, 1.0]),
        ids=IDS,
        row_index=np.arange(3),
        protein_rows=np.array([True, False, True]),
    )
    weighted = _record(
        BP,
        "weighted",
        _block([[5.0, 5.0, 3.5], [0.0, 0.0, 0.0], [1.5, 1.5, 1.5]], n_tau),
        _block([[5.0, 5.0, 3.5], [0.0, 0.0, 0.0], [4.0, 4.0, 2.0]], n_tau),
        np.array([5.0, 0.0, 1.5]),
        ids=IDS,
        row_index=np.arange(3),
        protein_rows=np.array([True, False, True]),
    )
    return FakeSink([unweighted, weighted])


def _pk_sink_with_excluded_mass(n_tau: int = 3) -> FakeSink:
    """The PK case that cannot be written: an ineligible row carrying mass.

    Identical to :func:`_pk_sink` except that P00002 predicted two terms inside
    the terms of interest. cafaeval counted that mass in ``P`` and left the
    protein out of the population it normalised by, so no row set is both the
    scored population and the one whose sums are the published cell.
    """
    unweighted = _record(
        BP,
        "unweighted",
        _block([[2.0, 2.0, 2.0], [0.0, 0.0, 0.0], [1.0, 1.0, 1.0]], n_tau),
        _block([[2.0, 2.0, 2.0], [2.0, 1.0, 0.0], [2.0, 2.0, 1.0]], n_tau),
        np.array([2.0, 0.0, 1.0]),
        ids=IDS,
        row_index=np.arange(3),
        protein_rows=np.array([True, False, True]),
    )
    weighted = _record(
        BP,
        "weighted",
        _block([[5.0, 5.0, 3.5], [0.0, 0.0, 0.0], [1.5, 1.5, 1.5]], n_tau),
        _block([[5.0, 5.0, 3.5], [4.5, 2.0, 0.0], [4.0, 4.0, 2.0]], n_tau),
        np.array([5.0, 0.0, 1.5]),
        ids=IDS,
        row_index=np.arange(3),
        protein_rows=np.array([True, False, True]),
    )
    return FakeSink([unweighted, weighted])


def _pk_sink_split_populations(n_tau: int = 3) -> FakeSink:
    """The two variants scoring different proteins, which one row set cannot serve.

    P00003's surviving ground truth carries zero information accretion, so it is
    in ``toi`` and not in ``toi_ia``: the unweighted branch scores it and the
    weighted branch does not. Both records still reproduce their own pooled
    sums, so only the population comparison catches this.
    """
    unweighted = _record(
        BP,
        "unweighted",
        _block([[2.0, 2.0, 2.0], [0.0, 0.0, 0.0], [1.0, 1.0, 1.0]], n_tau),
        _block([[2.0, 2.0, 2.0], [0.0, 0.0, 0.0], [2.0, 2.0, 1.0]], n_tau),
        np.array([2.0, 0.0, 1.0]),
        ids=IDS,
        row_index=np.arange(3),
        protein_rows=np.array([True, False, True]),
    )
    weighted = _record(
        BP,
        "weighted",
        _block([[5.0, 5.0, 3.5], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0]], n_tau),
        _block([[5.0, 5.0, 3.5], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0]], n_tau),
        np.array([5.0, 0.0, 0.0]),
        ids=IDS,
        row_index=np.arange(3),
        protein_rows=np.array([True, False, False]),
    )
    return FakeSink([unweighted, weighted])


def _frame(setting: str = "NK", th_step: float = 0.25) -> GridFrame:
    return GridFrame(
        setting=setting,
        th_step=th_step,
        max_terms=None,
        normalization="cafa",
        prop="fill",
        no_orphans=True,
        ontology_snapshot_id="11111111-1111-1111-1111-111111111111",
        evaluation_set_id="22222222-2222-2222-2222-222222222222",
        information_accretion_frame="33333333-3333-3333-3333-333333333333",
        producer_git_sha="0" * 40,
    )


def _write(root: Path, sink: FakeSink, *, setting: str = "NK", th_step: float = 0.25) -> Path:
    artifact = grid_rows_from_sink(sink)
    frame = _frame(setting, th_step)
    return write_grid_parquet(
        root / setting / GRID_FILENAME, artifact, frame.stamp(artifact.variants)
    )


def _accept(consumer: Any, path: Path, *, variant: str, setting: str = "NK") -> Any:
    """Put a file through every gate the consumer applies to it."""
    result_id = str(uuid.uuid4())
    resolved = consumer.resolve_setting_file(path.parent.parent, setting, result_id=result_id)
    assert resolved == path
    meta = consumer.read_grid_metadata(path, setting, result_id=result_id)
    consumer.require_variant(meta, variant, result_id=result_id)
    return consumer.load_setting_grid(path, meta, variant=variant, result_id=result_id)


# ---------------------------------------------------------------------------
# The round trip
# ---------------------------------------------------------------------------


def test_round_trip_both_variants(consumer: Any, tmp_path: Path) -> None:
    """A file this producer writes is accepted by the consumer's own reader."""
    path = _write(tmp_path, _nk_sink())
    for variant in ("weighted", "unweighted"):
        grid = _accept(consumer, path, variant=variant)
        assert sorted(grid.panels) == [BP, MF]
        for panel in grid.panels.values():
            assert panel.tp.shape == (3, 3)
            assert panel.accessions == ("P00001", "P00002", "P00003")


def test_round_trip_carries_the_two_term_sets_apart(consumer: Any, tmp_path: Path) -> None:
    """The variants are read as different numbers, not one rescaled.

    The weighted branch scores ``toi_ia`` and the unweighted one scores
    ``toi``. Writing one set of numbers under both names would make every
    weighted-versus-unweighted test vacuous, so the fixture gives the two
    branches different masses per namespace and this asserts the reader sees
    them.
    """
    path = _write(tmp_path, _nk_sink())
    weighted = _accept(consumer, path, variant="weighted")
    unweighted = _accept(consumer, path, variant="unweighted")
    ratios = weighted.panels[BP].tp.sum(axis=1) / unweighted.panels[BP].tp.sum(axis=1)
    assert len({round(float(r), 6) for r in ratios}) > 1
    assert not np.allclose(weighted.panels[BP].n_gt, unweighted.panels[BP].n_gt)
    # The unweighted branch is the same in both namespaces and the weighted one
    # is not, so a reader crossing the columns would show up here.
    assert np.allclose(unweighted.panels[BP].tp, unweighted.panels[MF].tp)
    assert not np.allclose(weighted.panels[BP].tp, weighted.panels[MF].tp)


def test_round_trip_on_the_production_grid(consumer: Any, tmp_path: Path) -> None:
    """99 thresholds, floating-point accumulation and all.

    ``np.arange(0.01, 1, 0.01)`` element 5 is 0.060000000000000005. A producer
    that rounds it still passes its own file's cross-check, whose np.allclose
    runs at numpy's default relative tolerance, and is refused only when
    compared against an unrounded file. So the raw values are asserted here
    rather than left to the file's own gate.
    """
    path = _write(tmp_path, _nk_sink(n_tau=99), th_step=0.01)
    grid = _accept(consumer, path, variant="weighted")
    assert grid.meta.n_tau == 99
    stamped = json.loads(grid.meta.values["tau_grid"])
    assert stamped == tau_grid_for(0.01)
    assert stamped[5] != 0.06
    assert np.allclose(np.asarray(stamped), np.arange(0.01, 1, 0.01), rtol=0.0, atol=0.0)


def test_pk_rows_are_exactly_the_scored_population(consumer: Any, tmp_path: Path) -> None:
    """The PK row set is ``n_gt > 0`` and its sums are still the kernel's sums.

    P00002's whole ground truth is prior knowledge, so cafaeval normalises by a
    population that excludes it. It predicted nothing inside the terms of
    interest, so dropping its row leaves every pooled total exactly where the
    kernel left it, and the file is both the scored population and the
    components of the published cell.
    """
    sink = _pk_sink()
    path = _write(tmp_path, sink, setting="PK")
    for variant in ("unweighted", "weighted"):
        grid = _accept(consumer, path, variant=variant, setting="PK")
        assert grid.panels[BP].accessions == ("P00001", "P00003")
    _assert_pooled(consumer, path, sink, setting="PK")


def test_a_pk_namespace_with_ineligible_mass_is_now_written(tmp_path: Path) -> None:
    """The gate the whole PK path used to turn on, and why it is gone.

    cafaeval used to reduce with ``sum(axis=0)`` over every row it was handed
    while restricting only its coverage column to the eligible ones, so an
    ineligible PK row's predicted mass was inside the published ``P`` and
    outside the published population. Keeping the row reproduced ``f_micro_w``
    and violated the consumer's eligibility rule; dropping it satisfied the rule
    and inflated the metric. The producer wrote neither and refused.

    e937e0e restricts the pooled sums too, so the row set that satisfies the
    eligibility rule is the row set the frame publishes. This asserts the
    namespace is written and, more importantly, that it is written with the
    ineligible row ABSENT: the refusal is gone but the population rule it was
    protecting is not.
    """
    sink = _pk_sink_with_excluded_mass()
    artifact = grid_rows_from_sink(sink)
    assert artifact.dropped == []

    bp = [r for r in artifact.rows if r["namespace"] == BP]
    assert bp, "the namespace that used to be refused is now written"

    record = next(r for r in sink.records if r["variant"] == "weighted")
    n_gt = np.asarray(record["n_gt"], dtype=np.float64)
    assert (n_gt <= 0.0).any(), "the fixture must still carry an ineligible row"
    assert len(bp) == int((n_gt > 0.0).sum()), "an ineligible row reached the file"


def test_refuses_a_namespace_whose_two_variants_scored_different_proteins(
    tmp_path: Path,
) -> None:
    """One row set cannot be the coverage denominator for two populations.

    Zero-filling the smaller variant keeps its ``f_micro_w`` exact, because zero
    adds nothing to ``T``, ``P`` or ``G``, and reports its coverage over the
    other variant's denominator. The estimator-parity guard cannot see that, so
    the producer has to.
    """
    from protea.core.operations import _run_cafa_per_protein as prod

    artifact = grid_rows_from_sink(_pk_sink_split_populations())
    assert artifact.rows == []
    assert [d["code"] for d in artifact.dropped] == [prod.DROP_VARIANT_POPULATIONS_DIFFER]
    assert "coverage denominator" in artifact.dropped[0]["reason"]


def _assert_pooled(consumer: Any, path: Path, sink: FakeSink, *, setting: str = "NK") -> None:
    """Every variant's file columns sum back to the kernel's own totals.

    The sums are taken over the WHOLE record, not over the rows the producer
    kept, because the kernel's ``sum(axis=0)`` runs over every row it was
    handed. That is the difference between checking a producer against itself
    and checking it against the number cafaeval published.
    """
    for record in sink.records:
        grid = _accept(consumer, path, variant=record["variant"], setting=setting)
        panel = grid.panels[record["ns"]]
        assert np.allclose(panel.tp.sum(axis=0), record["tp_at_tau"].sum(axis=0)), record
        assert np.allclose(panel.pred.sum(axis=0), record["pred_at_tau"].sum(axis=0)), record
        assert float(panel.n_gt.sum()) == pytest.approx(float(np.asarray(record["n_gt"]).sum()))


def test_pooled_sums_reproduce_the_sink(consumer: Any, tmp_path: Path) -> None:
    """The file's column sums are the numbers the aggregate was computed from.

    This is the obligation the reader cannot check and the producer owes:
    ``arm_block`` recomposes the published cell from these columns and refuses a
    mismatch. Run on the PK sink as well as the NK one, because on NK every row
    is eligible and the assertion holds whatever the producer does with
    eligibility, so an NK-only version of this test is satisfied by a producer
    that is wrong everywhere it matters.
    """
    for setting, sink in (("NK", _nk_sink()), ("PK", _pk_sink())):
        _assert_pooled(consumer, _write(tmp_path / setting, sink, setting=setting), sink,
                       setting=setting)


def test_every_semantic_key_is_stamped(consumer: Any, tmp_path: Path) -> None:
    """Presence, not just equality. An absent key compares equal to an absent key."""
    path = _write(tmp_path, _nk_sink())
    meta = consumer.read_grid_metadata(path, "NK", result_id="r")
    for key in consumer.SEMANTIC_COMPARABILITY_KEYS:
        assert meta.values.get(key), key
    assert meta.values["version"] == consumer.SCHEMA_VERSION
    assert json.loads(meta.values["variants"]) == ["weighted", "unweighted"]


def test_two_settings_of_one_run_agree(consumer: Any, tmp_path: Path) -> None:
    """One evaluation result speaks with one frame across its own settings."""
    metas = {}
    for setting in ("NK", "LK"):
        path = _write(tmp_path, _nk_sink(), setting=setting)
        metas[setting] = consumer.read_grid_metadata(path, setting, result_id="r")
    consumer.assert_settings_agree("r", metas)
    assert consumer.assert_comparable(metas["NK"], metas["LK"], allow_mismatch=False) == []


# ---------------------------------------------------------------------------
# The refusals, on files derived from this producer's own output
# ---------------------------------------------------------------------------


def _table(path: Path) -> Any:
    import pyarrow.parquet as pq

    return pq.read_table(path)


def _meta_of(table: Any) -> dict[str, str]:
    return {k.decode(): v.decode() for k, v in (table.schema.metadata or {}).items()}


def _rewrite(path: Path, table: Any, meta: dict[str, str]) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        list(table.schema), metadata={k.encode(): v.encode() for k, v in meta.items()}
    )
    pq.write_table(pa.Table.from_arrays(list(table.columns), schema=schema), path)
    return path


def _restamp(path: Path, changes: dict[str, str | None]) -> Path:
    table = _table(path)
    meta = _meta_of(table)
    for key, value in changes.items():
        full = f"{GRID_META_PREFIX}{key}"
        if value is None:
            meta.pop(full, None)
        else:
            meta[full] = value
    return _rewrite(path, table, meta)


def _replace_column(path: Path, name: str, array: Any, field: Any = None) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = _table(path)
    meta = _meta_of(table)
    index = table.schema.get_field_index(name)
    fields = list(table.schema)
    fields[index] = field or fields[index]
    arrays = list(table.columns)
    arrays[index] = array
    schema = pa.schema(fields, metadata={k.encode(): v.encode() for k, v in meta.items()})
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), path)
    return path


def test_refuses_a_file_missing_one_semantic_key(consumer: Any, tmp_path: Path) -> None:
    """Presence is gated before equality, so an omission is a refusal.

    Two files that both omit ``ontology_snapshot_id`` would compare equal, so
    the gate would fire on the producer careful enough to declare its frame and
    never on the one that forgot.
    """
    path = _restamp(_write(tmp_path, _nk_sink()), {"ontology_snapshot_id": None})
    with pytest.raises(consumer.ThresholdGridUnavailableError, match="does not stamp"):
        consumer.read_grid_metadata(path, "NK", result_id="r")


def test_refuses_a_semantic_key_stamped_empty(consumer: Any, tmp_path: Path) -> None:
    """An empty string is an absence, which is why the producer never writes one."""
    path = _restamp(_write(tmp_path, _nk_sink()), {"evaluation_set_id": ""})
    with pytest.raises(consumer.ThresholdGridUnavailableError, match="does not stamp"):
        consumer.read_grid_metadata(path, "NK", result_id="r")


def test_refuses_a_ragged_list_column(consumer: Any, tmp_path: Path) -> None:
    """A short row reshapes without complaint and shifts every row after it."""
    import pyarrow as pa

    path = _write(tmp_path, _nk_sink())
    table = _table(path)
    rows = [list(v) for v in table["tp_w"].to_pylist()]
    rows[1] = rows[1][:-1]
    offsets = np.cumsum([0, *[len(r) for r in rows]]).astype(np.int32)
    flat = pa.array(np.concatenate([np.asarray(r, dtype=np.float32) for r in rows]), pa.float32())
    ragged = pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), flat)
    path = _replace_column(path, "tp_w", ragged, pa.field("tp_w", pa.list_(pa.float32())))
    meta = consumer.read_grid_metadata(path, "NK", result_id="r")
    with pytest.raises(consumer.GridInvariantError, match="holds rows of length"):
        consumer.load_setting_grid(path, meta, variant="weighted", result_id="r")


def test_refuses_a_non_positive_n_gt(consumer: Any, tmp_path: Path) -> None:
    """A row with no ground truth adds predicted mass to P and nothing to G."""
    import pyarrow as pa

    path = _write(tmp_path, _nk_sink())
    values = _table(path)["n_gt"].to_pylist()
    values[2] = 0.0
    path = _replace_column(path, "n_gt", pa.array(values, pa.float64()))
    meta = consumer.read_grid_metadata(path, "NK", result_id="r")
    with pytest.raises(consumer.GridInvariantError, match="carry no ground truth"):
        consumer.load_setting_grid(path, meta, variant="weighted", result_id="r")


def test_refuses_a_grid_that_disagrees_with_its_own_th_step(
    consumer: Any, tmp_path: Path
) -> None:
    """The two facts that should confirm each other must confirm each other."""
    path = _restamp(_write(tmp_path, _nk_sink()), {"th_step": repr(0.2)})
    meta = consumer.read_grid_metadata(path, "NK", result_id="r")
    with pytest.raises(consumer.GridInvariantError, match="th_step"):
        consumer.load_setting_grid(path, meta, variant="weighted", result_id="r")


def test_refuses_a_float32_grid_as_an_incomparable_frame(
    consumer: Any, tmp_path: Path
) -> None:
    """A grid stored narrow passes its own file's gate and fails against a raw one.

    The two checks disagree about what "the same grid" means. Inside one file,
    ``_check_grid_shape`` compares the declared grid against
    ``np.arange(th_step, 1, th_step)`` with ``np.allclose(..., atol=1e-9)`` and
    no ``rtol``, so numpy's default 1e-5 applies and the tolerance at the top of
    the grid is about 1e-5. Between two files, ``_same_value`` passes
    ``rtol=0.0``, so the tolerance is 1e-9 everywhere. A producer that built the
    grid in float32 is off by up to 6e-8 near 0.99: inside the gap between the
    two, so it passes its own file and is refused only when compared.

    Rounding to two decimals is NOT that trap, at this step. The accumulation
    error in ``np.arange(0.01, 1, 0.01)`` is about 1e-16, so a two-decimal grid
    is inside 1e-9 and both gates accept it. The reachable version of the
    mistake is a narrower dtype, which is why the producer emits float64 values
    straight from ``np.arange`` and never widens a float32 grid back.
    """
    raw = _write(tmp_path / "raw", _nk_sink(n_tau=99), th_step=0.01)
    narrow = _restamp(
        _write(tmp_path / "narrow", _nk_sink(n_tau=99), th_step=0.01),
        {"tau_grid": json.dumps([float(np.float32(x)) for x in tau_grid_for(0.01)])},
    )
    meta_narrow = consumer.read_grid_metadata(narrow, "NK", result_id="r")
    # It passes its own file's gate, which is the trap.
    consumer.load_setting_grid(narrow, meta_narrow, variant="weighted", result_id="r")
    meta_raw = consumer.read_grid_metadata(raw, "NK", result_id="r")
    with pytest.raises(consumer.PanelComparabilityError, match="tau_grid"):
        consumer.assert_comparable(meta_raw, meta_narrow, allow_mismatch=True)


def test_two_producers_on_one_grid_compare_equal(consumer: Any, tmp_path: Path) -> None:
    """The same grid written twice is one frame, whatever else the files hold.

    The complement of the test above, and the reason it matters: the cross-arm
    comparison is numeric rather than textual, so two runs of this producer at
    the same step are comparable even though nothing coordinates their footers.
    """
    left = _write(tmp_path / "left", _nk_sink(n_tau=99), th_step=0.01)
    right = _write(tmp_path / "right", _pk_sink(n_tau=99), setting="NK", th_step=0.01)
    metas = [consumer.read_grid_metadata(p, "NK", result_id="r") for p in (left, right)]
    assert consumer.assert_comparable(metas[0], metas[1], allow_mismatch=False) == []


def test_refuses_a_legacy_marker_column(consumer: Any, tmp_path: Path) -> None:
    """A per-row tau in a file with this name is the old table renamed."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = _write(tmp_path, _nk_sink())
    table = _table(path)
    meta = _meta_of(table)
    fields = [*list(table.schema), pa.field("tau", pa.float64())]
    arrays = [*list(table.columns), pa.array([0.31] * table.num_rows, pa.float64())]
    schema = pa.schema(fields, metadata={k.encode(): v.encode() for k, v in meta.items()})
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), path)
    parsed = consumer.read_grid_metadata(path, "NK", result_id="r")
    with pytest.raises(consumer.ThresholdGridUnavailableError, match="single-threshold"):
        consumer.load_setting_grid(path, parsed, variant="weighted", result_id="r")


def test_refuses_a_duplicated_protein(consumer: Any, tmp_path: Path) -> None:
    """A repeated protein carries double weight and the panel still looks plausible."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = _write(tmp_path, _nk_sink())
    table = _table(path)
    doubled = pa.concat_tables([table, table.slice(0, 1)])
    pq.write_table(doubled.replace_schema_metadata(table.schema.metadata), path)
    meta = consumer.read_grid_metadata(path, "NK", result_id="r")
    with pytest.raises(consumer.GridInvariantError, match="not unique"):
        consumer.load_setting_grid(path, meta, variant="weighted", result_id="r")


def test_refuses_one_operating_point_broadcast_across_the_grid(
    consumer: Any, tmp_path: Path
) -> None:
    """The content-side detection of the legacy table under the new name."""
    import pyarrow as pa

    path = _write(tmp_path, _nk_sink())
    n_tau = 3
    for name in ("tp_w", "pred_w"):
        rows = [[float(v[0])] * n_tau for v in _table(path)[name].to_pylist()]
        flat = pa.array(np.concatenate(rows).astype(np.float32), pa.float32())
        path = _replace_column(path, name, pa.FixedSizeListArray.from_arrays(flat, n_tau))
    meta = consumer.read_grid_metadata(path, "NK", result_id="r")
    with pytest.raises(consumer.ThresholdGridUnavailableError, match="one operating point"):
        consumer.load_setting_grid(path, meta, variant="weighted", result_id="r")


def test_refuses_the_legacy_file_standing_alone(consumer: Any, tmp_path: Path) -> None:
    """The producer writes both, so a directory holding only the legacy one is a refusal."""
    import pandas as pd

    setting_dir = tmp_path / "NK"
    setting_dir.mkdir(parents=True)
    pd.DataFrame(
        [{"protein_accession": "P00001", "namespace": BP, "tau": 0.31, "tp_w": 1.0}]
    ).to_parquet(setting_dir / "per_protein.parquet", index=False)
    with pytest.raises(consumer.ThresholdGridUnavailableError, match="per_protein.parquet"):
        consumer.resolve_setting_file(tmp_path, "NK", result_id="r")


def test_the_grid_file_wins_when_both_are_present(consumer: Any, tmp_path: Path) -> None:
    """Both files side by side is the normal, correct state."""
    import pandas as pd

    path = _write(tmp_path, _nk_sink())
    pd.DataFrame(
        [{"protein_accession": "P00001", "namespace": BP, "tau": 0.31, "tp_w": 1.0}]
    ).to_parquet(path.parent / "per_protein.parquet", index=False)
    assert consumer.resolve_setting_file(tmp_path, "NK", result_id="r") == path


def test_refuses_a_variant_the_file_does_not_carry(consumer: Any, tmp_path: Path) -> None:
    """No unweighted number is ever computed and labelled with a weighted name."""
    sink = FakeSink([r for r in _nk_sink().records if r["variant"] == "unweighted"])
    path = _write(tmp_path, sink)
    meta = consumer.read_grid_metadata(path, "NK", result_id="r")
    assert meta.variants == ("unweighted",)
    with pytest.raises(consumer.ThresholdGridUnavailableError, match="ia_weighted"):
        consumer.require_variant(meta, "weighted", result_id="r")
    consumer.require_variant(meta, "unweighted", result_id="r")
    consumer.load_setting_grid(path, meta, variant="unweighted", result_id="r")


def test_refuses_two_settings_written_under_two_frames(consumer: Any, tmp_path: Path) -> None:
    """One evaluation result reports one frame; two frames means two runs mixed."""
    nk = _write(tmp_path, _nk_sink(), setting="NK")
    lk = _restamp(
        _write(tmp_path, _nk_sink(), setting="LK"),
        {"information_accretion_set_id": "44444444-4444-4444-4444-444444444444"},
    )
    metas = {
        "NK": consumer.read_grid_metadata(nk, "NK", result_id="r"),
        "LK": consumer.read_grid_metadata(lk, "LK", result_id="r"),
    }
    with pytest.raises(consumer.PanelComparabilityError, match="disagree on"):
        consumer.assert_settings_agree("r", metas)


# ---------------------------------------------------------------------------
# The producer's own refusals
# ---------------------------------------------------------------------------


def test_refuses_to_write_without_a_frame(tmp_path: Path) -> None:
    """An unstampable file is not written at all, and the message names the keys.

    A plausible substitute would be worse than an absence, and an absence worse
    than a refusal: the consumer's gate is on presence first, so a file missing
    a key compares EQUAL to another file missing it, and two runs under
    different frames would be published as a method difference.
    """
    frame = GridFrame(
        setting="NK",
        th_step=0.25,
        max_terms=None,
        normalization="cafa",
        prop="fill",
        no_orphans=True,
        ontology_snapshot_id=None,
        evaluation_set_id=None,
        information_accretion_frame="null",
    )
    with pytest.raises(UnstampableFrameError) as exc:
        frame.stamp(("weighted", "unweighted"))
    assert "ontology_snapshot_id" in str(exc.value)
    assert "evaluation_set_id" in str(exc.value)
    assert not list(tmp_path.rglob("*.parquet"))


def test_a_run_with_no_ia_stamps_null_rather_than_nothing() -> None:
    """No IA table means no weighted variant, and ``"null"`` is exact, not lossy."""
    frame = GridFrame(
        setting="NK",
        th_step=0.25,
        max_terms=None,
        normalization="cafa",
        prop="fill",
        no_orphans=True,
        ontology_snapshot_id="s",
        evaluation_set_id="e",
        information_accretion_frame="null",
    )
    stamped = frame.stamp(("unweighted",))
    assert stamped["information_accretion_set_id"] == "null"
    assert stamped["max_terms"] == "null"
    assert stamped["no_orphans"] == "true"


def test_refuses_the_unverifiable_row_numbering(tmp_path: Path) -> None:
    """The cafaeval defect: a filtered array numbered 0..n-1 against a full ids map.

    On the NK/LK path cafaeval passes ``np.arange(p.shape[0])`` after it has
    already filtered ``p`` to the ground-truth-bearing proteins, so when any
    protein was dropped every row after the gap wears a neighbour's accession.
    It cannot be detected downstream and it cannot be recovered from the
    record, so the namespace is dropped rather than written under a guess.
    """
    sink = _nk_sink()
    weighted = next(r for r in sink.records if r["ns"] == BP and r["variant"] == "weighted")
    weighted["tp_at_tau"] = weighted["tp_at_tau"][:2]
    weighted["pred_at_tau"] = weighted["pred_at_tau"][:2]
    weighted["n_gt"] = weighted["n_gt"][:2]
    weighted["row_index"] = np.arange(2)
    artifact = grid_rows_from_sink(sink)
    from protea.core.operations import _run_cafa_per_protein as prod

    assert [d["namespace"] for d in artifact.dropped] == [BP]
    assert artifact.dropped[0]["code"] == prod.DROP_UNMAPPABLE_ROWS
    assert "flatnonzero" in artifact.dropped[0]["reason"]
    assert {r["namespace"] for r in artifact.rows} == {MF}


def test_accepts_the_pk_row_numbering(tmp_path: Path) -> None:
    """``np.flatnonzero`` is the real mapping, so a PK subset is written."""
    artifact = grid_rows_from_sink(_pk_sink())
    assert artifact.dropped == []
    assert [r["protein_accession"] for r in artifact.rows] == ["P00001", "P00003"]


def test_refuses_two_records_for_one_key() -> None:
    """One sink is shared across every prediction file in the directory."""
    sink = _nk_sink()
    sink.records.append(sink.records[0])
    with pytest.raises(GridProducerError, match="two unweighted records"):
        grid_rows_from_sink(sink)


def test_refuses_an_empty_file(tmp_path: Path) -> None:
    """A zero-row file is accepted by the consumer and reported as a null panel."""
    with pytest.raises(GridProducerError, match="empty grid file"):
        write_grid_parquet(
            tmp_path / GRID_FILENAME,
            GridArtifact([], ("unweighted",), 3, []),
            _frame().stamp(("unweighted",)),
        )
    assert not list(tmp_path.rglob("*.parquet"))


def test_alignment_gate_is_a_refusal_not_a_guess() -> None:
    """The gate itself, exercised where it lives rather than through a drop.

    ``grid_rows_from_sink`` catches this and records the namespace in
    ``dropped``, which is the right behaviour for a run but hides which check
    fired. This calls the gate directly so the two facts stay separate: a
    full-width record is provably right whichever branch produced it, and a
    narrowed one numbered 0..n-1 is not.
    """
    from protea.core.operations._run_cafa_grid_artifact import _grid_accessions

    full = _record(
        BP, "unweighted", _block(_UNWEIGHTED_TP, 3), _block(_UNWEIGHTED_PRED, 3),
        np.array(_UNWEIGHTED_GT), ids=IDS, row_index=np.arange(3),
    )
    assert _grid_accessions(full) == ["P00001", "P00002", "P00003"]
    narrowed = _record(
        BP, "unweighted", _block(_UNWEIGHTED_TP, 3)[:2], _block(_UNWEIGHTED_PRED, 3)[:2],
        np.array([2, 1]), ids=IDS, row_index=np.arange(2),
    )
    with pytest.raises(SinkAlignmentError, match="flatnonzero"):
        _grid_accessions(narrowed)
    # The PK shape carries the mask, and flatnonzero is the real mapping.
    pk = _record(
        BP, "unweighted", _block(_UNWEIGHTED_TP, 3)[:2], _block(_UNWEIGHTED_PRED, 3)[:2],
        np.array([2, 1]), ids=IDS, row_index=np.array([0, 2]),
        protein_rows=np.array([True, True]),
    )
    assert _grid_accessions(pk) == ["P00001", "P00003"]


# ---------------------------------------------------------------------------
# The legacy artefact is untouched
# ---------------------------------------------------------------------------


def _context(root: Path) -> Any:
    from protea.core.evaluation import EvaluationData
    from protea.core.operations._run_cafa_eval_driver import CafaEvalRunContext

    return CafaEvalRunContext(
        pred_set_id=uuid.uuid4(),
        delta_proteins=set(),
        max_distance=None,
        artifacts_root=root,
        has_rerankers=False,
        reranker_models={},
        scoring_config_snapshot=None,
        data=EvaluationData(),
        obo_path="go.obo",
        nk_path="nk.tsv",
        lk_path="lk.tsv",
        pk_path="pk.tsv",
        pk_known_path="known.tsv",
        ia_path="ia.tsv",
        toi_path="toi.txt",
        shared_pred_dir="predictions",
        ontology_snapshot_id="11111111-1111-1111-1111-111111111111",
        evaluation_set_id="22222222-2222-2222-2222-222222222222",
        information_accretion_frame="33333333-3333-3333-3333-333333333333",
        th_step=0.25,
    )


def _events() -> tuple[list[tuple[str, Any]], Any]:
    seen: list[tuple[str, Any]] = []

    def emit(name: str, _msg: Any, payload: Any, _level: str) -> None:
        seen.append((name, payload))

    return seen, emit


def test_the_legacy_file_is_byte_identical_with_and_without_the_grid(
    tmp_path: Path,
) -> None:
    """Writing the grid does not touch ``per_protein.parquet``.

    ``stratify_evaluation`` reads that file, so a changed schema would break
    it, and an old file and a new file sharing one name would make the
    detection problem permanently unsolvable. A new filename beside it is the
    whole design; this pins that the old bytes do not move.
    """
    from protea.core.operations._run_cafa_eval_driver import (
        _persist_per_protein,
        _persist_per_protein_grid,
    )

    sink = _nk_sink()
    result = {"BPO": {"tau": 0.25}, "MFO": {"tau": 0.25}}
    alone = tmp_path / "alone"
    beside = tmp_path / "beside"
    seen, emit = _events()
    _persist_per_protein(_context(alone), "NK", sink, result, emit)
    _persist_per_protein(_context(beside), "NK", sink, result, emit)
    _persist_per_protein_grid(_context(beside), "NK", sink, emit)

    legacy_alone = (alone / "NK" / "per_protein.parquet").read_bytes()
    legacy_beside = (beside / "NK" / "per_protein.parquet").read_bytes()
    assert legacy_alone == legacy_beside
    assert (beside / "NK" / GRID_FILENAME).exists()
    assert not (alone / "NK" / GRID_FILENAME).exists()
    assert [name for name, _ in seen] == [
        "run_cafa_evaluation.per_protein_written",
        "run_cafa_evaluation.per_protein_written",
        "run_cafa_evaluation.per_protein_grid_written",
    ]


def test_the_legacy_file_still_holds_exactly_what_it_held(tmp_path: Path) -> None:
    """Its content, pinned: weighted masses at one tau, with the derived scores.

    Pinned by value rather than by shape. The grid producer reads the same sink
    records, so a change to the shared helpers underneath both would move these
    numbers without moving any column name.
    """
    import pandas as pd

    from protea.core.operations._run_cafa_eval_driver import _persist_per_protein

    _, emit = _events()
    _persist_per_protein(
        _context(tmp_path), "NK", _nk_sink(), {"BPO": {"tau": 0.25}}, emit
    )
    frame = pd.read_parquet(tmp_path / "NK" / "per_protein.parquet")
    assert list(frame.columns) == [
        "protein_accession", "namespace", "tau",
        "tp_w", "pred_w", "n_gt_w", "precision_w", "recall_w", "f_w",
    ]
    assert frame["protein_accession"].tolist() == ["P00001", "P00002", "P00003"]
    assert frame["tau"].tolist() == [0.25, 0.25, 0.25]
    assert frame["tp_w"].tolist() == [5.0, 2.5, 1.5]
    assert frame["pred_w"].tolist() == [5.0, 2.5, 4.0]
    assert frame["n_gt_w"].tolist() == [5.0, 2.5, 1.5]
    assert frame["f_w"].tolist() == pytest.approx([1.0, 1.0, 6.0 / 11.0])


def test_the_two_files_describe_the_same_run(tmp_path: Path) -> None:
    """The grid at the legacy tau reproduces the legacy file's own columns.

    The legacy file is sliced at the tau ``parse_results`` reports, which is
    the unweighted Fmax optimum. That mismatch is the reason the grid exists;
    it is not a reason for the two files to disagree about the mass at a given
    threshold, and this pins that they do not.
    """
    import pandas as pd

    from protea.core.operations._run_cafa_eval_driver import (
        _persist_per_protein,
        _persist_per_protein_grid,
    )

    sink = _nk_sink()
    _, emit = _events()
    ctx = _context(tmp_path)
    _persist_per_protein(ctx, "NK", sink, {"BPO": {"tau": 0.25}}, emit)
    _persist_per_protein_grid(ctx, "NK", sink, emit)
    legacy = pd.read_parquet(tmp_path / "NK" / "per_protein.parquet")
    grid = pd.read_parquet(tmp_path / "NK" / GRID_FILENAME)
    grid = grid[grid["namespace"] == BP].reset_index(drop=True)
    column = 0  # tau 0.25 is the first threshold of np.arange(0.25, 1, 0.25)
    assert [v[column] for v in grid["tp_w"]] == pytest.approx(legacy["tp_w"].tolist())
    assert [v[column] for v in grid["pred_w"]] == pytest.approx(legacy["pred_w"].tolist())
    assert grid["n_gt_w"].tolist() == pytest.approx(legacy["n_gt_w"].tolist())


def test_a_weighted_only_file_still_carries_n_gt(consumer: Any, tmp_path: Path) -> None:
    """``n_gt`` is the eligibility marker, not a component of the unweighted metric.

    A file may legitimately declare only the weighted variant, and it still has
    to say which proteins were scored. Writing the weighted mass under that
    name instead would make a zero-information-accretion protein look
    ineligible and get the whole file refused, which is the shape of the
    disagreement between the consumer's reader and its own test helper.
    """
    artifact = grid_rows_from_sink(_nk_sink())
    path = write_grid_parquet(
        tmp_path / "NK" / GRID_FILENAME,
        GridArtifact(artifact.rows, ("weighted",), artifact.n_tau, []),
        _frame().stamp(("weighted",)),
    )
    names = set(_table(path).schema.names)
    assert "n_gt" in names and "tp" not in names and "pred" not in names
    grid = _accept(consumer, path, variant="weighted")
    assert grid.panels[BP].accessions == ("P00001", "P00002", "P00003")


def test_the_grid_recomposes_the_published_metric(consumer: Any, tmp_path: Path) -> None:
    """The obligation the reader cannot check and the producer owes.

    ``arm_block`` recomposes the arm's own-population Fmax from these columns
    and refuses a mismatch against the stored ``f_micro_w`` with a message about
    "the components and the published number". There is no evaluation result
    here to compare against, but the other half of that gate can be checked:
    the consumer's vectorised estimator and ``_run_cafa_strata._micro``, the
    function behind every published ``f_micro_w`` cell in this project, must
    read the same number off this producer's own columns.
    """
    boot = sys.modules["protea.core.operations._paired_panels_bootstrap"]
    from protea.core.operations._run_cafa_strata import _micro

    path = _write(tmp_path, _nk_sink(n_tau=99), th_step=0.01)
    grid = _accept(consumer, path, variant="weighted")
    for panel in grid.panels.values():
        curve = boot.panel_curve(panel)
        point = boot.select_operating_point(curve)
        cell = _micro(
            float(panel.tp[:, point.tau_index].sum()),
            float(panel.pred[:, point.tau_index].sum()),
            float(panel.n_gt.sum()),
            panel.n,
        )
        assert point.value == pytest.approx(cell.f_micro_w, abs=1e-12)


# ---------------------------------------------------------------------------
# End to end, through the real cafaeval kernel
# ---------------------------------------------------------------------------
#
# Everything above feeds the producer a sink built by hand, which tests the
# producer against this file's belief about the record shape. These two feed it
# a sink the installed cafaeval filled, which tests the belief.

_OBO_TERMS = (
    ("GO:0000001", None),
    ("GO:0000002", "GO:0000001"),
    ("GO:0000003", "GO:0000001"),
    ("GO:0000004", "GO:0000002"),
)


def _cafaeval_inputs(root: Path, ia: dict[str, float], known: str | None = None) -> dict[str, str]:
    """A four-term ontology, three proteins and one prediction file."""
    root.mkdir(parents=True, exist_ok=True)
    lines = ["format-version: 1.2", ""]
    for gid, parent in _OBO_TERMS:
        lines += ["[Term]", f"id: {gid}", f"name: term {gid[-1]}", "namespace: biological_process"]
        if parent:
            lines.append(f"is_a: {parent} ! parent")
        lines.append("")
    (root / "go.obo").write_text("\n".join(lines))
    pred_dir = root / "predictions"
    pred_dir.mkdir(parents=True, exist_ok=True)
    (pred_dir / "predictions.tsv").write_text(
        "P1\tGO:0000004\t0.9\nP1\tGO:0000002\t0.9\nP1\tGO:0000001\t0.9\n"
        "P2\tGO:0000003\t0.6\nP2\tGO:0000001\t0.6\n"
        "P3\tGO:0000002\t0.3\nP3\tGO:0000001\t0.3\nP3\tGO:0000003\t0.3\n"
    )
    (root / "gt.tsv").write_text("P1\tGO:0000004\nP2\tGO:0000003\nP3\tGO:0000002\n")
    (root / "ia.tsv").write_text("".join(f"{k}\t{v}\n" for k, v in ia.items()))
    (root / "toi.txt").write_text("".join(f"{gid}\n" for gid, _ in _OBO_TERMS))
    out = {
        "obo": str(root / "go.obo"),
        "pred_dir": str(pred_dir),
        "gt": str(root / "gt.tsv"),
        "ia": str(root / "ia.tsv"),
        "toi": str(root / "toi.txt"),
    }
    if known is not None:
        (root / "known.tsv").write_text(known)
        out["known"] = str(root / "known.tsv")
    return out


def _real_run(root: Path, ia: dict[str, float], known: str | None = None) -> tuple[Any, Any]:
    """Run the installed cafaeval; hand back the sink AND the frame it published.

    Both, because a producer checked only against its own sink cannot tell that
    it summed the right rows: the published frame is the second opinion, and it
    is the one every cell in this project is copied from.
    """
    cafaeval = pytest.importorskip("cafaeval.evaluation")
    paths = _cafaeval_inputs(root, ia, known)
    sink = cafaeval.PerProteinSink()
    from protea.core.operations._run_cafa_eval_driver import (
        CAFAEVAL_NO_ORPHANS,
        CAFAEVAL_NORM,
        CAFAEVAL_PROP,
    )

    dfs = cafaeval.cafa_eval(
        paths["obo"], paths["pred_dir"], paths["gt"], ia=paths["ia"],
        exclude=paths.get("known"),
        prop=CAFAEVAL_PROP, norm=CAFAEVAL_NORM, no_orphans=CAFAEVAL_NO_ORPHANS,
        toi_file=paths["toi"], max_terms=None, th_step=0.25, n_cpu=1,
        per_protein_sink=sink,
    )
    return sink, (dfs[0] if isinstance(dfs, tuple) else dfs)


def _real_sink(root: Path, ia: dict[str, float]) -> Any:
    return _real_run(root, ia)[0]


def test_end_to_end_through_the_real_kernel(consumer: Any, tmp_path: Path) -> None:
    """A real cafaeval run, written by the driver, read by the consumer.

    The whole chain with nothing stubbed: cafaeval fills the sink, the driver
    writes both files into the setting directory the uploader walks, and the
    consumer's own reader accepts the grid file. Every term carries positive
    information accretion here, so ``toi_ia`` and ``toi`` keep the same
    proteins and both records arrive full width.
    """
    from protea.core.operations._run_cafa_eval_driver import _persist_per_protein_grid

    sink = _real_sink(tmp_path / "inputs", {g: v for (g, _), v in zip(_OBO_TERMS, [0.5, 1.0, 2.0, 3.5], strict=True)})
    artifacts = tmp_path / "artifacts"
    _, emit = _events()
    ctx = _context(artifacts)
    _persist_per_protein_grid(ctx, "NK", sink, emit)
    path = artifacts / "NK" / GRID_FILENAME
    assert path.exists()
    for variant in ("weighted", "unweighted"):
        grid = _accept(consumer, path, variant=variant, setting="NK")
        panel = grid.panels[BP]
        assert panel.accessions == ("P1", "P2", "P3")
    weighted = _accept(consumer, path, variant="weighted", setting="NK")
    unweighted = _accept(consumer, path, variant="unweighted", setting="NK")
    assert list(weighted.panels[BP].n_gt) == [5.0, 2.5, 1.5]
    assert list(unweighted.panels[BP].n_gt) == [2.0, 1.0, 1.0]


def test_a_zero_ia_term_makes_the_kernel_unmappable(tmp_path: Path) -> None:
    """The blocker, pinned against the installed cafaeval rather than described.

    With a zero-information-accretion term in the closure, ``toi_ia`` drops a
    protein, the weighted kernel filters its rows, and it still numbers them
    0..n-1 against an ``ids`` map that numbers the unfiltered ground truth.
    That is the NK/LK branch of ``compute_metrics``, and it is not recoverable
    downstream, so the producer refuses the namespace and no file is written.

    **This test fails the day the pin is bumped to a cafaeval that passes
    ``np.flatnonzero(proteins_has_gt)``, as its own PK branch already does.**
    That is the intent: the failure is the signal to relax the gate in
    ``_grid_accessions`` and delete this test. Until then it is the receipt
    that the refusal is real and reachable on a four-term ontology.
    """
    from protea.core.operations._run_cafa_eval_driver import _persist_per_protein_grid

    sink = _real_sink(
        tmp_path / "inputs",
        {"GO:0000001": 0.0, "GO:0000002": 0.0, "GO:0000003": 2.0, "GO:0000004": 3.5},
    )
    weighted = next(r for r in sink.records if r["variant"] == "weighted")
    assert weighted["tp_at_tau"].shape[0] < len(weighted["ids"])
    assert list(weighted["row_index"]) == list(range(weighted["tp_at_tau"].shape[0]))

    artifacts = tmp_path / "artifacts"
    seen, emit = _events()
    _persist_per_protein_grid(_context(artifacts), "NK", sink, emit)
    dropped = [p for name, p in seen if name.endswith("namespace_dropped")]
    assert dropped and "flatnonzero" in dropped[0]["reason"]
    failed = [p for name, p in seen if name.endswith("grid_failed")]
    assert failed and "empty grid file" in failed[0]["error"]
    assert not (artifacts / "NK" / GRID_FILENAME).exists()


def _published_curve(df: Any, namespace: str, column: str) -> list[float]:
    """cafaeval's own metric at every threshold, in threshold order."""
    rows = df.reset_index()
    rows = rows[rows["ns"] == namespace].sort_values("tau")
    return [float(v) for v in rows[column]]


def test_the_file_reproduces_the_number_cafaeval_published(
    consumer: Any, tmp_path: Path
) -> None:
    """The round trip that matters: a number out of the FILE against the frame.

    Everything else compares this producer to itself. ``test_pooled_sums...``
    checks the columns against the sink the producer read, and
    ``test_the_grid_recomposes_the_published_metric`` checks two implementations
    of one formula on the same columns; neither can see a wrong POPULATION,
    because both sides of both comparisons use it. Only cafaeval's published
    frame is independent, and it is the number ``arm_block`` refuses a mismatch
    against.

    Run at every threshold rather than at the optimum, because a producer that
    is wrong at one operating point and right at another still cannot carry an
    interval: ``compare_paired_panels`` re-selects the threshold inside every
    resample, so the whole curve is load-bearing.
    """
    boot = sys.modules["protea.core.operations._paired_panels_bootstrap"]
    from protea.core.operations._run_cafa_eval_driver import _persist_per_protein_grid

    ia = {g: v for (g, _), v in zip(_OBO_TERMS, [0.5, 1.0, 2.0, 3.5], strict=True)}
    sink, df = _real_run(tmp_path / "inputs", ia)
    artifacts = tmp_path / "artifacts"
    _, emit = _events()
    _persist_per_protein_grid(_context(artifacts), "NK", sink, emit)
    path = artifacts / "NK" / GRID_FILENAME
    for variant, column in (("weighted", "f_micro_w"), ("unweighted", "f_micro")):
        grid = _accept(consumer, path, variant=variant, setting="NK")
        curve = boot.panel_curve(grid.panels[BP])
        published = _published_curve(df, BP, column)
        assert list(curve) == pytest.approx(published, abs=1e-6), variant


def test_pk_with_an_excluded_protein_that_predicts_is_refused_not_biased(
    tmp_path: Path,
) -> None:
    """The blocker, pinned against the installed kernel rather than argued.

    P3's whole ground truth is prior knowledge, and it predicts inside the terms
    of interest. cafaeval keeps its predicted mass in ``P`` and drops it from the
    population, so recomposing from the eligible rows alone reads 1.0000 where
    the frame publishes 0.8824. That gap is the reason the namespace is refused,
    and it arrives dressed as a better system.

    THE 12 POINTS ARE THIS FIXTURE'S, NOT THE CAMPAIGN'S. Three proteins, one of
    them entirely prior knowledge and predicting inside the terms of interest, is
    a worst case built on purpose to make the defect visible in a unit test. On
    real panels the correction was measured at +0.0002 against a base of 0.15 in
    the cell where it bites. Do not put the fixture number next to campaign
    effects of 0.005 to 0.05 and let a reader conclude the kernel bug was worth
    ten campaigns. What the fix buys is that PK.BPO and PK.MFO can be written at
    all, not a movement in the numbers already published.

    THAT DAY ARRIVED. cafaeval e937e0e restricts ``tp_totals`` and
    ``pred_totals`` to ``eligible_rows`` the way ``metrics[:, 0]`` already did,
    so the published cell is now computed over the population it declares. The
    test that predicted its own obsolescence is rewritten here to assert the
    other side of it: the same three-protein panel that could not be written now
    can, and the number recomposed from the kept rows IS the published one.

    The two facts it still pins are the ones the reconciliation rests on. The
    kernel is still handed the ineligible row, so the raw block still carries
    its predicted mass and a producer that summed every row would still be
    wrong. What changed is which sum the frame publishes.
    """
    ia = {g: v for (g, _), v in zip(_OBO_TERMS, [0.5, 1.0, 2.0, 3.5], strict=True)}
    sink, df = _real_run(
        tmp_path / "inputs", ia, known="P3\tGO:0000002\nP3\tGO:0000001\n"
    )
    weighted = next(r for r in sink.records if r["variant"] == "weighted")
    keep = np.asarray(weighted["n_gt"], dtype=np.float64) > 0.0
    full = weighted["pred_at_tau"].sum(axis=0)
    kept = weighted["pred_at_tau"][keep].sum(axis=0)
    assert weighted["pred_at_tau"].shape[0] == 3, "the PK kernel does not truncate"
    assert not np.allclose(full, kept), "the excluded row carries predicted mass"

    tp = float(weighted["tp_at_tau"][keep][:, 0].sum())
    gt = float(np.asarray(weighted["n_gt"])[keep].sum())
    from_kept_rows = 2 * tp / (float(kept[0]) + gt)
    published = _published_curve(df, BP, "f_micro_w")[0]

    # The reconciliation, stated as the equality it is. Before e937e0e these
    # were 0.882353 and 1.0, a gap of nearly twelve points of Fmax on three
    # proteins, and the namespace was refused rather than written over a
    # population that had not been scored.
    assert published == pytest.approx(from_kept_rows, abs=1e-9)
    assert published == pytest.approx(1.0, abs=1e-9)

    artifact = grid_rows_from_sink(sink)
    assert [r["protein_accession"] for r in artifact.rows if r["namespace"] == BP] != []
    assert artifact.dropped == []


def test_the_writer_refuses_a_footer_it_was_handed(tmp_path: Path) -> None:
    """The gate lives in the writer, not beside it.

    ``GridFrame.stamp`` is a separate object the writer neither calls nor
    requires, so a second caller building a footer by hand gets a file written
    and uploaded, and the consumer then refuses it with a message telling the
    operator to re-run with a producer that stamps the version, which is this
    module, which did write it.
    """
    artifact = grid_rows_from_sink(_nk_sink())
    good = _frame().stamp(artifact.variants)
    out = tmp_path / "NK" / GRID_FILENAME
    with pytest.raises(UnstampableFrameError, match="declares version None"):
        write_grid_parquet(out, artifact, {})
    dropped = {k: v for k, v in good.items() if k != "information_accretion_set_id"}
    with pytest.raises(UnstampableFrameError, match="information_accretion_set_id"):
        write_grid_parquet(out, artifact, dropped)
    with pytest.raises(UnstampableFrameError, match="version"):
        write_grid_parquet(out, artifact, {**good, "version": "0"})
    with pytest.raises(UnstampableFrameError, match="variants"):
        write_grid_parquet(out, artifact, {**good, "variants": json.dumps(["weighted"])})
    assert not out.exists()


def test_the_writer_refuses_a_footer_grid_that_is_not_its_columns(tmp_path: Path) -> None:
    """A footer built from one ``th_step`` and columns written at another.

    The legacy path has this check and the grid path had dropped it. The
    consumer's dedicated width gate is dead (it is called with the declared
    width as the observed width), so without this the only thing left is the
    per-row length check, which reports the disagreement as a ragged column and
    sends whoever debugs it into the mass columns, which are fine.
    """
    artifact = grid_rows_from_sink(_nk_sink(n_tau=99))
    out = tmp_path / "NK" / GRID_FILENAME
    with pytest.raises(GridProducerError, match="thresholds and the rows are"):
        write_grid_parquet(out, artifact, _frame(th_step=0.25).stamp(artifact.variants))
    assert not out.exists()
    write_grid_parquet(out, artifact, _frame(th_step=0.01).stamp(artifact.variants))
    assert out.exists()

"""Turning a finished evaluation into cells.

Reads the per-protein grid `run_cafa_evaluation` writes, slices it at the
threshold the result published, places each protein on the strata axes, and
pools each cell micro. Written as a separate operation so it can stratify
evaluations that already exist without rerunning them.

THE FIXTURE GOES THROUGH THE PRODUCTION WRITER, which is the whole point of
this file's shape. These tests existed and passed while the operation read a
file nothing had written for months: they built `per_protein.parquet`
themselves, so the fixture agreed with the consumer and both disagreed with
the producer. A test that fabricates its own input cannot catch that, however
many assertions it carries. So `_grid` calls `write_grid_parquet` with a
`GridFrame.stamp`, and the filename, the footer and the column layout are the
producer's rather than this file's.
"""

from __future__ import annotations

import uuid

import pandas as pd
import pytest

from protea.core.operations._run_cafa_grid_artifact import (
    GRID_FILENAME,
    GridArtifact,
    GridFrame,
    write_grid_parquet,
)
from protea.core.operations._run_cafa_strata import VARIANT_COLUMNS, project
from protea.core.operations.stratify_evaluation import (
    _ASPECT_FOR_NAMESPACE,
    BEST_F_FILENAME,
    StratifyEvaluationOperation,
    StratifyEvaluationPayload,
    _category_for,
    _strata_for_rows,
)
from protea.core.strata import (
    NEIGHBOURHOOD_AXES,
    SEQUENCE_AXES,
    Aspect,
    Category,
    Neighbourhood,
    Stratum,
    length_band_for,
)

_NB = Neighbourhood(best_identity=45.0, donor_is_experimental=True, taxonomic_relation="close")

#: What the operation crosses by default, donor axis included.
_ALL_AXES = ["category", "aspect", "length", "homology"]


#: The grid this file writes: three thresholds, so the middle one is neither
#: the first nor the last column and a slice at the wrong index shows up as a
#: wrong number rather than as an index error.
_TH_STEP = 0.25
_TAUS = (0.25, 0.5, 0.75)
_REPORTED_TAU = 0.5


def _row(
    acc: str,
    ns: str,
    tp: float = 1.0,
    pred: float = 2.0,
    n_gt: float = 2.0,
    variant: str = "unweighted",
) -> dict:
    """One grid row: the components over the whole sweep, plus scalar n_gt.

    The values at the thresholds either side of the reported one are
    deliberately WRONG -- the ground truth is the only thing that does not
    move with the threshold -- so a slice at the wrong column cannot produce
    the expected cell by coincidence.
    """
    tp_col, pred_col, gt_col = VARIANT_COLUMNS[variant]
    return {
        "protein_accession": acc,
        "namespace": ns,
        tp_col: [tp + 100.0, tp, 0.0],
        pred_col: [pred + 100.0, pred, 0.0],
        gt_col: n_gt,
    }


def _grid(root, rows: list[dict], *, setting: str = "NK", variant: str = "unweighted"):
    """Write one setting through the producer, with the best-F table beside it.

    `write_grid_parquet` decides the filename, the footer and the column
    layout. `information_accretion_set_id` is the string "null" because that is
    what the stored grids on this deployment carry: every evaluation here ran
    without an accretion table, which is why the unweighted variant is the one
    these tests pool.
    """
    frame = GridFrame(
        setting=setting,
        th_step=_TH_STEP,
        max_terms=None,
        normalization="cafa",
        prop="fill",
        no_orphans=True,
        ontology_snapshot_id="11111111-1111-1111-1111-111111111111",
        evaluation_set_id="22222222-2222-2222-2222-222222222222",
        information_accretion_frame="null",
        producer_git_sha="0" * 40,
    )
    artifact = GridArtifact(rows=rows, variants=(variant,), n_tau=len(_TAUS), dropped=[])
    write_grid_parquet(root / setting / GRID_FILENAME, artifact, frame.stamp((variant,)))
    namespaces = sorted({r["namespace"] for r in rows})
    lines = ["filename\tns\ttau\tf"]
    lines += [f"predictions.tsv\t{ns}\t{_REPORTED_TAU}\t0.5" for ns in namespaces]
    (root / setting / BEST_F_FILENAME).write_text("\n".join(lines) + "\n")
    return root


class TestTheSettingNamesTheCategory:
    @pytest.mark.parametrize(
        ("setting", "expected"),
        [("NK", Category.NO_KNOWLEDGE), ("LK", Category.LIMITED_KNOWLEDGE),
         ("PK", Category.PARTIAL_KNOWLEDGE), ("nk_something", Category.NO_KNOWLEDGE)],
    )
    def test_known_settings_resolve(self, setting: str, expected: Category) -> None:
        assert _category_for(setting) is expected

    def test_an_unknown_setting_is_none_rather_than_a_guess(self) -> None:
        """A setting whose category cannot be read is skipped and reported, not
        filed under some default that would move a published number."""
        assert _category_for("scratch") is None


class TestNamespacesMapToAspects:
    def test_all_three_cafaeval_namespaces_are_known(self) -> None:
        assert set(_ASPECT_FOR_NAMESPACE) == {
            "biological_process", "molecular_function", "cellular_component"
        }

    def test_they_map_to_distinct_aspects(self) -> None:
        assert len(set(_ASPECT_FOR_NAMESPACE.values())) == 3


class TestPlacingRows:
    def test_a_complete_row_gets_a_stratum(self) -> None:
        placed = _strata_for_rows(
            [_row("Q1", "molecular_function")],
            category=Category.NO_KNOWLEDGE,
            lengths={"Q1": 300},
            neighbourhoods={"Q1": _NB},
            axes=_ALL_AXES,
        )
        assert placed[0].aspect is Aspect.MOLECULAR_FUNCTION
        assert placed[0].category is Category.NO_KNOWLEDGE

    @pytest.mark.parametrize(
        ("lengths", "neighbourhoods", "namespace"),
        [
            ({}, {"Q1": _NB}, "molecular_function"),               # no length
            ({"Q1": 300}, {}, "molecular_function"),               # no non-self donor
            ({"Q1": 300}, {"Q1": _NB}, "not_a_namespace"),         # unknown aspect
        ],
    )
    def test_rows_that_cannot_be_placed_are_skipped(
        self, lengths: dict, neighbourhoods: dict, namespace: str
    ) -> None:
        """Skipped, never defaulted: every default here would move a number by
        an amount nobody chose.

        Every case here crosses a donor axis, which is what makes the missing
        neighbourhood disqualifying. :class:`TestAxesDecideWhatAPlacementNeeds`
        covers the run that asks for no donor axis and must not skip it.
        """
        assert _strata_for_rows(
            [_row("Q1", namespace)],
            category=Category.NO_KNOWLEDGE, lengths=lengths, neighbourhoods=neighbourhoods,
            axes=_ALL_AXES,
        ) == {}


class TestAxesDecideWhatAPlacementNeeds:
    """A donor neighbourhood is a cost, so only a run that reads one should pay it.

    Placing a query used to require an aligned donor whatever was being
    crossed. Category, aspect and length are three facts about the query's own
    sequence and need no alignment, so that requirement discarded most of the
    campaign from tables that never asked about homology, and reported the
    surviving minority as though it were the population.
    """

    def test_the_sequence_axes_place_a_query_with_no_donor_at_all(self) -> None:
        """Can act: the whole point of the change."""
        placed = _strata_for_rows(
            [_row("Q1", "molecular_function")],
            category=Category.NO_KNOWLEDGE,
            lengths={"Q1": 300},
            neighbourhoods={},
            axes=["category", "aspect", "length"],
        )
        assert list(placed) == [0]
        assert placed[0].length is length_band_for(300)

    @pytest.mark.parametrize("axis", sorted(NEIGHBOURHOOD_AXES))
    def test_any_donor_axis_still_refuses_a_query_with_no_donor(self, axis: str) -> None:
        """Can refuse, once per donor axis: a table that reports homology,
        donor evidence, taxonomy or propagation for a query whose donors were
        never aligned is reporting an empty neighbourhood as a measurement."""
        assert _strata_for_rows(
            [_row("Q1", "molecular_function")],
            category=Category.NO_KNOWLEDGE,
            lengths={"Q1": 300},
            neighbourhoods={},
            axes=["length", axis],
        ) == {}

    def test_dropping_the_donor_does_not_move_the_length_band(self) -> None:
        """The placement path is shared, so this pins that taking it without a
        neighbourhood answers the sequence axes identically. If it did not, the
        change would have bought coverage by moving numbers."""
        args = dict(category=Category.NO_KNOWLEDGE, lengths={"Q1": 300},
                    axes=["category", "aspect", "length"])
        with_donor = _strata_for_rows([_row("Q1", "molecular_function")],
                                      neighbourhoods={"Q1": _NB}, **args)
        without = _strata_for_rows([_row("Q1", "molecular_function")],
                                   neighbourhoods={}, **args)
        assert project(with_donor[0], args["axes"]) == project(without[0], args["axes"])


class TestTheSplitIsCheckedAgainstTheStratum:
    def test_every_stratum_field_is_on_exactly_one_side(self) -> None:
        """A seventh axis added to Stratum without deciding which side it falls
        on would silently join the sequence axes and be reported for queries
        that cannot support it. This makes that a failing test instead."""
        assert NEIGHBOURHOOD_AXES | SEQUENCE_AXES == set(Stratum._fields)
        assert not (NEIGHBOURHOOD_AXES & SEQUENCE_AXES)


class _FakeResult:
    def __init__(self, rows): self._rows = rows
    def mappings(self): return self._rows
    def __iter__(self): return iter(self._rows)


class _FakeSession:
    """Answers the two queries the operation makes, in order."""

    def __init__(self, neighbourhood_rows, length_rows):
        self._answers = [neighbourhood_rows, length_rows]

    def execute(self, *_a, **_k):
        return _FakeResult(self._answers.pop(0))


class TestTheOperationEndToEnd:
    def _artifacts(self, tmp_path, rows):
        return _grid(tmp_path, rows)

    @staticmethod
    def _payload(root, **extra):
        base = {
            "prediction_set_id": str(uuid.uuid4()),
            "artifacts_root": str(root),
            "variant": "unweighted",
        }
        return {**base, **extra}

    def _session(self):
        return _FakeSession(
            [{"acc": "Q1", "best_identity": 45.0, "evidence_code": "IDA",
              "taxonomic_relation": "close", "nearest_any": 0.1,
              "nearest_experimental": 0.1},
             {"acc": "Q2", "best_identity": 20.0, "evidence_code": "IEA",
              "taxonomic_relation": "close", "nearest_any": 0.3,
              "nearest_experimental": None}],
            [("Q1", 300), ("Q2", 300)],
        )

    def test_it_writes_one_row_per_cell(self, tmp_path) -> None:
        root = self._artifacts(
            tmp_path, [_row("Q1", "molecular_function"), _row("Q2", "molecular_function")]
        )
        out = StratifyEvaluationOperation().execute(
            self._session(),
            self._payload(root, axes=["homology"], min_population=1),
            emit=lambda *a, **k: None,
        )
        frame = pd.read_parquet(root / "NK" / "strata.parquet")
        assert out.result["settings"]["NK"]["rows_placed"] == 2
        # two proteins in different homology bands -> two cells
        assert len(frame) == 2
        assert set(frame.homology) == {"30-60", "<=30"}

    def test_axes_are_written_as_their_canonical_values(self, tmp_path) -> None:
        """Category and Aspect are plain Enums, so str() would write
        'Aspect.MOLECULAR_FUNCTION' and the column would stop matching the
        vocabulary every other table uses."""
        root = self._artifacts(tmp_path, [_row("Q1", "molecular_function")])
        StratifyEvaluationOperation().execute(
            self._session(),
            self._payload(root, axes=["category", "aspect"], min_population=1),
            emit=lambda *a, **k: None,
        )
        frame = pd.read_parquet(root / "NK" / "strata.parquet")
        assert frame.category.tolist() == ["NK"]
        assert frame.aspect.tolist() == ["F"]

    def test_thin_cells_are_written_and_flagged_not_dropped(self, tmp_path) -> None:
        """A table that prints only what survived looks identical to a table
        that covered everything."""
        root = self._artifacts(tmp_path, [_row("Q1", "molecular_function")])
        StratifyEvaluationOperation().execute(
            self._session(),
            self._payload(root, axes=["homology"], min_population=99),
            emit=lambda *a, **k: None,
        )
        frame = pd.read_parquet(root / "NK" / "strata.parquet")
        assert len(frame) == 1
        assert frame.reportable.tolist() == [False]

    def test_the_pooled_score_is_micro(self, tmp_path) -> None:
        """Two proteins in one cell: sums then divides, not the mean of F."""
        root = self._artifacts(
            tmp_path,
            [_row("Q1", "molecular_function", tp=1.0, pred=1.0, n_gt=1.0),
             _row("Q2", "molecular_function", tp=1.0, pred=10.0, n_gt=10.0)],
        )
        StratifyEvaluationOperation().execute(
            self._session(),
            self._payload(root, axes=["aspect"], min_population=1),
            emit=lambda *a, **k: None,
        )
        frame = pd.read_parquet(root / "NK" / "strata.parquet")
        assert frame.precision.tolist() == [pytest.approx(2 / 11)]
        assert frame.recall.tolist() == [pytest.approx(2 / 11)]
        # and it says which weighting those two numbers are
        assert frame.variant.tolist() == ["unweighted"]


class TestThePayload:
    def test_the_default_axes_are_the_four_a_first_table_reports(self) -> None:
        p = StratifyEvaluationPayload(
            prediction_set_id="x", artifacts_root="/tmp", variant="unweighted"
        )
        assert p.axes == ["category", "aspect", "length", "homology"]

    def test_the_population_floor_cannot_be_zero(self) -> None:
        with pytest.raises(ValueError):
            StratifyEvaluationPayload(
                prediction_set_id="x",
                artifacts_root="/tmp",
                variant="unweighted",
                min_population=0,
            )

    def test_the_variant_has_no_default(self) -> None:
        """The two weightings are different term sets over different rows, so a
        default would quietly decide which quantity a published cell holds."""
        with pytest.raises(ValueError, match="variant"):
            StratifyEvaluationPayload(prediction_set_id="x", artifacts_root="/tmp")

    def test_an_unknown_weighting_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown variant"):
            StratifyEvaluationPayload(
                prediction_set_id="x", artifacts_root="/tmp", variant="ia"
            )


class TestItIsRegistered:
    def test_the_operation_can_be_dispatched(self) -> None:
        from protea.core.operation_catalog import build_operation_registry

        assert "stratify_evaluation" in build_operation_registry()._ops


class TestReadingFromTheArtifactStore:
    """The normal path: a finished evaluation's temporary directory is gone,
    and its files live in the store under eval_artifacts/<result_id>/."""

    def test_neither_source_is_refused_rather_than_guessed(self) -> None:
        from protea.core.operations.stratify_evaluation import _resolve_source

        p = StratifyEvaluationPayload(prediction_set_id="x", variant="unweighted")
        with pytest.raises(ValueError, match="nothing to read"):
            _resolve_source(p, lambda *a, **k: None)

    def test_a_local_root_needs_no_store(self, tmp_path) -> None:
        from protea.core.operations.stratify_evaluation import _resolve_source

        root, store, tmp = _resolve_source(
            StratifyEvaluationPayload(
                prediction_set_id="x", artifacts_root=str(tmp_path), variant="unweighted"
            ),
            lambda *a, **k: None,
        )
        assert (root, store, tmp) == (tmp_path, None, None)

    def test_only_the_settings_present_in_the_store_are_fetched(self, tmp_path) -> None:
        from protea.core.operations.stratify_evaluation import _settings_from_store

        class _Store:
            def __init__(self): self.asked: list[str] = []
            def exists(self, key: str) -> bool:
                self.asked.append(key)
                return "/NK/" in key
            def get(self, key: str) -> bytes: return b"parquet-bytes"

        store = _Store()
        rid = str(uuid.uuid4())
        found = _settings_from_store(store, rid, tmp_path)

        assert found == ["NK"]
        assert (tmp_path / "NK" / GRID_FILENAME).read_bytes() == b"parquet-bytes"
        # The grid is what it asks for. It used to ask for per_protein.parquet,
        # which the producer stopped writing, and an absent file reads as a
        # setting that was not evaluated: the operation found nothing on every
        # result in the store and reported success.
        assert all(GRID_FILENAME in key or BEST_F_FILENAME in key for key in store.asked)
        assert not any("per_protein.parquet" in key for key in store.asked)
        # three categories probed for the grid, and the best-F table fetched
        # only for the one that had one
        assert len([k for k in store.asked if GRID_FILENAME in k]) == 3
        assert all(rid in key for key in store.asked)


def _is_grid_refusal(error: BaseException, expected: str) -> None:
    """Assert the reader refused, by class NAME and not by class identity.

    ``tests/test_per_protein_grid_producer.py`` loads the consumer's reader a
    second time through ``importlib`` so it can check the producer against the
    version that will run in production. That leaves two
    ``ThresholdGridUnavailableError`` classes alive in one session, so
    ``pytest.raises`` on an imported name passes or fails depending on which
    test file ran first -- which is a fact about module loading and not about
    what the operation refused. The name and the message are what this is
    actually asserting.
    """
    assert type(error).__name__ == "ThresholdGridUnavailableError", type(error).__name__
    assert expected in str(error)


class TestItReadsTheFileTheProducerWrites:
    """The defect this file's shape exists to prevent, asserted directly."""

    def _session(self):
        return _FakeSession(
            [
                {
                    "acc": "Q1",
                    "best_identity": 45.0,
                    "evidence_code": "IDA",
                    "taxonomic_relation": "close",
                    "nearest_any": 0.1,
                    "nearest_experimental": 0.1,
                }
            ],
            [("Q1", 300)],
        )

    def test_the_slice_is_at_the_published_threshold(self, tmp_path) -> None:
        """Not the first column, not the last: the one the best-F table names.

        The row carries tp 101 at tau 0.25, 1 at 0.5 and 0 at 0.75, and the
        table reports 0.5. A cell of 101 or of 0 would mean the index came from
        somewhere other than the artefact's own grid.
        """
        root = _grid(tmp_path, [_row("Q1", "molecular_function", tp=1.0, pred=2.0, n_gt=2.0)])
        StratifyEvaluationOperation().execute(
            self._session(),
            {
                "prediction_set_id": str(uuid.uuid4()),
                "artifacts_root": str(root),
                "variant": "unweighted",
                "axes": ["aspect"],
                "min_population": 1,
            },
            emit=lambda *a, **k: None,
        )
        frame = pd.read_parquet(root / "NK" / "strata.parquet")
        assert frame.precision.tolist() == [pytest.approx(0.5)]
        assert frame.recall.tolist() == [pytest.approx(0.5)]

    def test_a_result_with_no_grid_is_refused_not_reported_as_success(self, tmp_path) -> None:
        """The whole defect in one assertion.

        Six jobs over five stored results each found zero settings and each
        returned SUCCEEDED with an empty summary, so the strata screens had
        nothing to read and no job said why.
        """
        (tmp_path / "NK").mkdir()
        with pytest.raises(ValueError, match="no setting yielded a single stratum"):
            StratifyEvaluationOperation().execute(
                self._session(),
                {
                    "prediction_set_id": str(uuid.uuid4()),
                    "artifacts_root": str(tmp_path),
                    "variant": "unweighted",
                    "axes": ["aspect"],
                    "min_population": 1,
                },
                emit=lambda *a, **k: None,
            )

    def test_the_legacy_file_is_refused_by_name(self, tmp_path) -> None:
        """A legacy table standing where the grid belongs is not an absence.

        It is sliced at one tau and its rows were never filtered on ground
        truth, so pooling it would publish numbers over a population nobody
        chose. The resolver the paired panels already use says so by name.
        """
        (tmp_path / "NK").mkdir()
        pd.DataFrame(
            [{"protein_accession": "Q1", "namespace": "molecular_function", "tau": 0.5}]
        ).to_parquet(tmp_path / "NK" / "per_protein.parquet", index=False)
        with pytest.raises(Exception) as err:
            StratifyEvaluationOperation().execute(
                self._session(),
                {
                    "prediction_set_id": str(uuid.uuid4()),
                    "artifacts_root": str(tmp_path),
                    "variant": "unweighted",
                    "axes": ["aspect"],
                    "min_population": 1,
                },
                emit=lambda *a, **k: None,
            )
        _is_grid_refusal(err.value, GRID_FILENAME)

    def test_a_weighting_the_file_does_not_carry_is_refused(self, tmp_path) -> None:
        """Every evaluation stored on this deployment ran with no accretion
        table, so its grid declares the unweighted variant alone. Pooling it
        under the weighted name is the one thing that must not happen."""
        root = _grid(tmp_path, [_row("Q1", "molecular_function")])
        with pytest.raises(Exception) as err:
            StratifyEvaluationOperation().execute(
                self._session(),
                {
                    "prediction_set_id": str(uuid.uuid4()),
                    "artifacts_root": str(root),
                    "variant": "weighted",
                    "axes": ["aspect"],
                    "min_population": 1,
                },
                emit=lambda *a, **k: None,
            )
        _is_grid_refusal(err.value, "variants")

    def test_a_grid_without_its_best_f_table_is_refused(self, tmp_path) -> None:
        """The grid holds the sweep and nothing in it says which column was
        published. Every column is a defensible number and only one is the one
        already reported."""
        root = _grid(tmp_path, [_row("Q1", "molecular_function")])
        (root / "NK" / BEST_F_FILENAME).unlink()
        with pytest.raises(ValueError, match="nothing says which column"):
            StratifyEvaluationOperation().execute(
                self._session(),
                {
                    "prediction_set_id": str(uuid.uuid4()),
                    "artifacts_root": str(root),
                    "variant": "unweighted",
                    "axes": ["aspect"],
                    "min_population": 1,
                },
                emit=lambda *a, **k: None,
            )

    def test_the_variant_reaches_the_summary_and_the_table(self, tmp_path) -> None:
        """A cell is only readable beside the weighting it was pooled under."""
        root = _grid(tmp_path, [_row("Q1", "molecular_function")])
        out = StratifyEvaluationOperation().execute(
            self._session(),
            {
                "prediction_set_id": str(uuid.uuid4()),
                "artifacts_root": str(root),
                "variant": "unweighted",
                "axes": ["aspect"],
                "min_population": 1,
            },
            emit=lambda *a, **k: None,
        )
        assert out.result["variant"] == "unweighted"
        frame = pd.read_parquet(root / "NK" / "strata.parquet")
        assert frame.variant.tolist() == ["unweighted"]
        assert "precision_w" not in frame.columns

"""A stratified sample of a window's targets: the rule, the operation and its refusals.

Without a database: largest remainder and the allocation of a cell's quota over its
strata (one unit each first, never more than a stratum holds), the draw (the quotas,
every nonempty stratum present when the quota allows it, the same bytes for the same
seed and window and another draw for another window), the payload, the routing, and
the verified door the window's truth is read through.

With ``--with-postgres``: on a placed window whose truth is in a local store, the dry
run returns the allocation the rule gives over the window's benchmark pairs and writes
nothing; a declaration writes the targets with their sequence at the opening cut, the
sorted accession list and the allocation table beside it; a target only ``known`` or
``removed`` is never sampled; and every refusal comes before anything is written.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, get_args

import pytest
import sqlalchemy as sa
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core import queues
from protea.core._artifact_identity import ArtifactMismatch
from protea.core.evaluation import (
    EvaluationData,
    fetch_groundtruth_to_file,
    groundtruth_key_for,
)
from protea.core.operation_catalog import build_operation_registry
from protea.core.operations import declare_window_sample as operation
from protea.core.operations.declare_window_sample import (
    DeclareWindowSampleOperation,
    DeclareWindowSamplePayload,
    WindowSampleRefused,
    window_sample_key_for,
)
from protea.core.query_population import population_of
from protea.core.sampling.window_sample import (
    ASPECT_ORDER,
    CATEGORY_ORDER,
    IDENTITY_BAND_ORDER,
    LENGTH_BAND_ORDER,
    WINDOW_SAMPLE_RULES,
    Unit,
    allocate_cell,
    draw_window_sample,
    largest_remainder,
)
from protea.core.strata import homology_band_for, length_band_for
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.window_placement import WindowPlacement
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.storage.local import LocalFsArtifactStore
from tests.helpers.cut_photos import make_job, make_photo, make_placed_window, member, sequence_ids

NAME = "declare_window_sample"
#: The store the door to a window's truth reads, patched by its dotted name: the module
#: is entered through ``protea.core.evaluation``, which it imports back.
_TRUTH_STORE = "protea.core._evaluation_groundtruth._store"
WINDOW = "4228a366-3dc4-4c30-b4a4-287482a59edf"
RULE = "equal_cells_proportional_strata"


# Largest remainder and a cell's allocation -------------------------------------------


class TestLargestRemainder:
    def test_exact_shares_are_kept(self) -> None:
        assert largest_remainder(10, [5, 3, 2]) == [5, 3, 2]

    def test_the_largest_remainders_take_the_seats_left(self) -> None:
        # Shares 3.75, 2.25, 1.0: the one seat left goes to the 0.75.
        assert largest_remainder(7, [15, 9, 4]) == [4, 2, 1]

    def test_a_tie_goes_to_the_larger_weight_then_the_earlier_position(self) -> None:
        assert largest_remainder(1, [1, 3, 3]) == [0, 1, 0]
        assert largest_remainder(2, [1, 1, 1]) == [1, 1, 0]

    def test_nothing_to_share_over_and_impossible_shares(self) -> None:
        assert largest_remainder(0, [0, 0]) == [0, 0]
        with pytest.raises(ValueError):
            largest_remainder(1, [0, 0])
        with pytest.raises(ValueError):
            largest_remainder(-1, [1])

    @given(
        st.lists(st.integers(min_value=0, max_value=50), min_size=1, max_size=12),
        st.integers(min_value=0, max_value=200),
    )
    def test_every_seat_is_given_and_each_share_is_rounded(
        self, weights: list[int], seats: int
    ) -> None:
        if sum(weights) == 0:
            seats = 0
        given_seats = largest_remainder(seats, weights)
        assert sum(given_seats) == seats
        total = sum(weights) or 1
        for weight, got in zip(weights, given_seats, strict=True):
            assert seats * weight // total <= got <= -(-seats * weight // total)


class TestTheCellAllocation:
    def test_one_unit_each_first_then_in_proportion_to_what_is_left(self) -> None:
        assert allocate_cell(5, [1, 1, 10]) == [1, 1, 3]
        assert allocate_cell(13, [3, 10]) == [3, 10]

    def test_a_small_quota_reaches_as_many_strata_as_it_can_the_largest_first(self) -> None:
        assert allocate_cell(2, [5, 1, 1]) == [1, 1, 0]
        assert allocate_cell(2, [1, 5, 5]) == [0, 1, 1]

    def test_what_it_refuses(self) -> None:
        with pytest.raises(ValueError, match="nonempty"):
            allocate_cell(1, [0, 2])
        with pytest.raises(ValueError, match="does not fit"):
            allocate_cell(4, [1, 2])

    @given(st.lists(st.integers(min_value=1, max_value=40), min_size=1, max_size=20), st.data())
    def test_the_quota_is_spent_with_no_stratum_over_its_size(
        self, sizes: list[int], data: st.DataObject
    ) -> None:
        quota = data.draw(st.integers(min_value=0, max_value=sum(sizes)))
        given_units = allocate_cell(quota, sizes)
        assert sum(given_units) == quota
        assert all(0 <= got <= size for got, size in zip(given_units, sizes, strict=True))
        if quota >= len(sizes):
            assert all(got >= 1 for got in given_units)
        else:
            assert sum(1 for got in given_units if got) == quota


# The draw ----------------------------------------------------------------------------


def _units(count: int, seed: int = 7) -> list[Unit]:
    """A window's benchmark pairs, spread unevenly over the cells and the strata."""
    found = []
    for i in range(count):
        accession = f"P{i:05d}"
        key = int(hashlib.sha256(f"{seed}-{i}".encode()).hexdigest(), 16)
        for position, aspect in enumerate(ASPECT_ORDER):
            if (key >> position) % 3 == 0:
                continue
            category = "NK" if i % 11 == 0 else ("LK" if (key >> 8) % 4 == 0 else "PK")
            found.append(
                Unit(
                    accession,
                    aspect,
                    category,
                    IDENTITY_BAND_ORDER[(key >> 12) % len(IDENTITY_BAND_ORDER)],
                    LENGTH_BAND_ORDER[(key >> 16) % len(LENGTH_BAND_ORDER)],
                )
            )
    return found


class TestTheDraw:
    def test_each_cell_gets_its_quota_or_all_it_has(self) -> None:
        units = _units(600)
        draw = draw_window_sample(units, units_per_cell=40, seed="ladder-toy-1", window_id=WINDOW)
        population = Counter(unit.cell for unit in units)
        assert [(c.category, c.aspect) for c in draw.cells] == [
            (category, aspect) for category in CATEGORY_ORDER for aspect in ASPECT_ORDER
        ]
        for cell in draw.cells:
            assert cell.population == population[(cell.category, cell.aspect)]
            assert cell.quota == cell.selected == min(40, cell.population)
            assert cell.realised >= cell.selected
            assert sum(s.selected for s in cell.strata) == cell.selected

    def test_every_nonempty_stratum_is_present_when_the_quota_allows_it(self) -> None:
        draw = draw_window_sample(_units(900), units_per_cell=60, seed="s", window_id=WINDOW)
        for cell in draw.cells:
            if cell.quota >= len(cell.strata):
                assert all(stratum.selected >= 1 for stratum in cell.strata)

    def test_the_targets_are_the_union_and_keep_every_cell(self) -> None:
        units = _units(300)
        draw = draw_window_sample(units, units_per_cell=10, seed="s", window_id=WINDOW)
        assert draw.targets == tuple(sorted(set(draw.targets)))
        realised = Counter(unit.cell for unit in units if unit.accession in set(draw.targets))
        assert {(c.category, c.aspect): c.realised for c in draw.cells if c.population} == dict(
            realised
        )
        assert len(draw.targets) <= 9 * 10

    def test_the_same_seed_and_window_give_the_same_bytes_whatever_the_order(self) -> None:
        units = _units(400)
        first = draw_window_sample(units, units_per_cell=25, seed="s", window_id=WINDOW)
        again = draw_window_sample(
            list(reversed(units)), units_per_cell=25, seed="s", window_id=WINDOW
        )
        assert again == first
        assert json.dumps(again.allocation()) == json.dumps(first.allocation())

    def test_another_window_or_seed_draws_another_sample(self) -> None:
        units = _units(400)
        first = draw_window_sample(units, units_per_cell=25, seed="s", window_id=WINDOW)
        other_window = draw_window_sample(
            units, units_per_cell=25, seed="s", window_id=str(uuid.uuid4())
        )
        other_seed = draw_window_sample(units, units_per_cell=25, seed="t", window_id=WINDOW)
        assert other_window.targets != first.targets
        assert other_seed.targets != first.targets

    def test_within_a_stratum_the_units_are_taken_in_hash_order(self) -> None:
        units = [Unit(f"Q{i:03d}", "F", "PK", "30-60", "<=512") for i in range(50)]
        draw = draw_window_sample(units, units_per_cell=5, seed="s", window_id=WINDOW)
        keys = sorted(
            (hashlib.sha256(f"s|{WINDOW}|{u.accession}".encode()).hexdigest(), u.accession)
            for u in units
        )
        assert draw.targets == tuple(sorted(accession for _, accession in keys[:5]))

    @pytest.mark.parametrize(
        ("units", "kwargs", "complaint"),
        [
            ([], {}, "no benchmark pair"),
            ([Unit("A", "F", "PK", "none", "<=512")] * 2, {}, "twice"),
            ([Unit("A", "F", "PK", "none", "<=512")], {"units_per_cell": 0}, "at least 1"),
            ([Unit("A", "F", "PK", "none", "<=512")], {"rule": "proportional"}, "unknown rule"),
        ],
    )
    def test_what_it_refuses(
        self, units: list[Unit], kwargs: dict[str, Any], complaint: str
    ) -> None:
        arguments = {"units_per_cell": 3, "seed": "s", "window_id": WINDOW, **kwargs}
        with pytest.raises(ValueError, match=complaint):
            draw_window_sample(units, **arguments)

    def test_a_unit_names_known_bands_and_cells(self) -> None:
        with pytest.raises(ValueError, match="identity_band"):
            Unit("A", "F", "PK", "<=20", "<=512")
        with pytest.raises(ValueError, match="category"):
            Unit("A", "F", "XK", "none", "<=512")


# The payload, the routing and the door to the truth -------------------------------


def _payload(**over: Any) -> dict[str, Any]:
    return {
        "evaluation_set_id": WINDOW,
        "units_per_cell": 300,
        "seed": "ladder-toy-1",
        "rule": RULE,
        "output_name": "ladder-toy-targets-194-211",
        **over,
    }


class TestThePayload:
    def test_the_sketch_of_the_design(self) -> None:
        p = DeclareWindowSamplePayload.model_validate(_payload(dry_run=True))
        assert p.window_id == uuid.UUID(WINDOW) and p.dry_run
        assert get_args(DeclareWindowSamplePayload.model_fields["rule"].annotation) == (
            WINDOW_SAMPLE_RULES
        )

    @pytest.mark.parametrize(
        "over",
        [
            {"rule": "proportional"},
            {"units_per_cell": 0},
            {"units_per_cell": "300"},
            {"seed": ""},
            {"output_name": "a/b"},
            {"evaluation_set_id": "194->211"},
            {"sample": {"size": 3}},
        ],
    )
    def test_what_it_refuses(self, over: dict[str, Any]) -> None:
        with pytest.raises(ValidationError):
            DeclareWindowSamplePayload.model_validate(_payload(**over))

    def test_a_rule_is_named(self) -> None:
        payload = _payload()
        del payload["rule"]
        with pytest.raises(ValidationError):
            DeclareWindowSamplePayload.model_validate(payload)

    def test_the_summary(self) -> None:
        op = DeclareWindowSampleOperation()
        assert (
            op.summarize_payload(_payload(dry_run=True))
            == "sample of window 4228a366, 300 units per cell (dry run)"
        )
        assert op.summarize_payload({}) == ""


def test_it_runs_as_a_job_of_the_general_queue() -> None:
    spec = build_operation_registry().spec(NAME)
    assert (spec.domain, spec.kind, spec.queue, spec.purity) == (
        "universe",
        "job",
        queues.JOBS,
        "database",
    )


def test_the_files_have_one_spelling() -> None:
    window = uuid.uuid4()
    assert window_sample_key_for(window, "d" * 64, "allocation.json") == (
        f"query-sets/window-samples/{window}/{'d' * 64}/allocation.json"
    )


class TestTheVerifiedDoor:
    def _window(self, tmp_path: Path, blob: bytes, **over: Any) -> SimpleNamespace:
        window_id = uuid.uuid4()
        store = LocalFsArtifactStore(tmp_path / "store")
        uri = store.put(groundtruth_key_for(window_id), blob)
        fields = {
            "id": window_id,
            "groundtruth_uri": uri,
            "groundtruth_sha256": hashlib.sha256(blob).hexdigest(),
            **over,
        }
        return SimpleNamespace(**fields)

    @pytest.fixture(autouse=True)
    def _store(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_TRUTH_STORE, lambda: LocalFsArtifactStore(tmp_path / "store"))

    def test_the_bytes_land_when_they_hash_as_recorded(self, tmp_path: Path) -> None:
        window = self._window(tmp_path, b"the truth")
        path = fetch_groundtruth_to_file(window, tmp_path / "truth.parquet")
        assert path.read_bytes() == b"the truth"

    def test_a_row_with_no_sha_is_refused(self, tmp_path: Path) -> None:
        window = self._window(tmp_path, b"the truth", groundtruth_sha256=None)
        with pytest.raises(ArtifactMismatch, match="no groundtruth_sha256"):
            fetch_groundtruth_to_file(window, tmp_path / "truth.parquet")

    def test_other_bytes_or_another_locator_are_refused(self, tmp_path: Path) -> None:
        window = self._window(tmp_path, b"the truth", groundtruth_sha256="0" * 64)
        with pytest.raises(ArtifactMismatch, match="does not match"):
            fetch_groundtruth_to_file(window, tmp_path / "truth.parquet")
        assert not (tmp_path / "truth.parquet").exists()
        moved = self._window(tmp_path, b"the truth", groundtruth_uri="memory://elsewhere")
        with pytest.raises(ArtifactMismatch, match="does not name the key"):
            fetch_groundtruth_to_file(moved, tmp_path / "truth.parquet")


# On Postgres ----------------------------------------------------------------------

ASPECTS = {"GO:0000001": "F", "GO:0000002": "P", "GO:0000003": "C", "GO:0000004": "F"}


def _truth(targets: int) -> tuple[EvaluationData, dict[str, tuple[float | None, int]]]:
    """A window's truth over ``targets`` proteins, and their placement.

    Every tenth protein is no-knowledge in every aspect it gained; the others are
    limited knowledge in function and partial knowledge in process and component. Two
    proteins are only ``known`` or only ``removed``: never benchmark pairs.
    """
    nk: dict[str, set[str]] = {}
    lk: dict[str, set[str]] = {}
    pk: dict[str, set[str]] = {}
    placed: dict[str, tuple[float | None, int]] = {}
    identities = (None, 25.0, 45.0, 75.0, 100.0)
    for i in range(targets):
        accession = f"T{i:04d}"
        if i % 10 == 0:
            nk[accession] = {"GO:0000001", "GO:0000002"}
        else:
            lk[accession] = {"GO:0000001", "GO:0000004"}
            pk[accession] = {"GO:0000002"} if i % 3 else {"GO:0000002", "GO:0000003"}
        placed[accession] = (identities[i % 5], 100 + 700 * (i % 4))
    truth = EvaluationData(
        nk=nk,
        lk=lk,
        pk=pk,
        known={"K0001": {"GO:0000001"}, **{a: {"GO:0000002"} for a in pk}},
        pk_known={a: {"GO:0000002"} for a in pk},
        removed={"R0001": {"GO:0000003"}},
    )
    return truth, placed


def _expected_units(
    truth: EvaluationData, placed: dict[str, tuple[float | None, int]]
) -> list[Unit]:
    units = set()
    for category, bucket in (("NK", truth.nk), ("LK", truth.lk), ("PK", truth.pk)):
        for accession, terms in bucket.items():
            identity, residues = placed[accession]
            for go_id in terms:
                units.add(
                    Unit(
                        accession,
                        ASPECTS[go_id],
                        category,
                        homology_band_for(identity).value,
                        length_band_for(residues).value,
                    )
                )
    return sorted(units, key=lambda u: (u.accession, u.aspect))


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture()
def stored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, bytes]:
    kept: dict[str, bytes] = {}

    def put(key: str, blob: bytes) -> str:
        kept[key] = blob
        return f"memory://{key}"

    monkeypatch.setattr(operation, "publish", put)
    monkeypatch.setattr(operation, "_scratch_root", lambda: tmp_path)
    monkeypatch.setattr(_TRUTH_STORE, lambda: LocalFsArtifactStore(tmp_path / "store"))
    return kept


def _placed_window(
    session: Session, tmp_path: Path, targets: int = 120
) -> tuple[EvaluationSet, CutPhoto, EvaluationData, dict[str, tuple[float | None, int]]]:
    truth, placed = _truth(targets)
    members = [
        member(accession, "ts", residues="MK" + "A" * (i % 7)) for i, accession in enumerate(placed)
    ]
    photo = make_photo(session, [*members, member("K0001", "t"), member("R0001", "s")])
    window = make_placed_window(session, photo, truth, ASPECTS, placed, tmp_path / "store")
    return window, photo, truth, placed


def _run(session: Session, window: EvaluationSet, **over: Any) -> dict[str, Any]:
    payload = {
        **_payload(evaluation_set_id=str(window.id), units_per_cell=6),
        "_job_id": str(make_job(session, NAME)),
        **over,
    }
    result = DeclareWindowSampleOperation().execute(session, payload, emit=lambda *a, **k: None)
    session.flush()
    return result.result


@pytest.mark.integration
def test_a_dry_run_draws_the_rule_over_the_windows_pairs_and_writes_nothing(
    pg: Session, tmp_path: Path, stored: dict[str, bytes]
) -> None:
    window, _, truth, placed = _placed_window(pg, tmp_path)
    result = _run(pg, window, dry_run=True)

    expected = draw_window_sample(
        _expected_units(truth, placed),
        units_per_cell=6,
        seed="ladder-toy-1",
        window_id=str(window.id),
    )
    assert result["allocation"] == expected.allocation()
    assert result["targets"] == len(expected.targets)
    assert result["content_sha256"] == population_of(expected.targets)["sha256"]
    assert result["already_declared"] is None
    assert pg.execute(select(sa.func.count(QuerySet.id))).scalar_one() == 0
    assert stored == {}


@pytest.mark.integration
def test_a_declaration_writes_the_targets_the_list_and_the_table(
    pg: Session, tmp_path: Path, stored: dict[str, bytes]
) -> None:
    window, photo, truth, placed = _placed_window(pg, tmp_path)
    result = _run(pg, window)

    row = pg.get(QuerySet, uuid.UUID(result["query_set_id"]))
    assert row is not None
    assert (row.source_kind, row.evaluation_set_id, row.cut_photo_id) == (
        "window_sample",
        window.id,
        None,
    )
    targets = sorted(
        pg.execute(
            select(QuerySetEntry.accession).where(QuerySetEntry.query_set_id == row.id)
        ).scalars()
    )
    assert len(targets) == result["targets"]
    assert not {"K0001", "R0001"} & set(targets)
    ids = sequence_ids(pg, photo)
    entries = dict(
        pg.execute(
            select(QuerySetEntry.accession, QuerySetEntry.sequence_id).where(
                QuerySetEntry.query_set_id == row.id
            )
        ).all()
    )
    assert entries == {accession: ids[accession] for accession in targets}
    body = stored[window_sample_key_for(window.id, row.content_sha256, "accessions.txt")]
    assert body == "".join(f"{a}\n" for a in targets).encode()
    assert row.content_sha256 == hashlib.sha256(body).hexdigest()
    table = json.loads(
        stored[window_sample_key_for(window.id, row.content_sha256, "allocation.json")]
    )
    assert table == result["allocation"]
    assert table["seed"] == "ladder-toy-1" and table["rule"] == RULE
    assert row.description is not None and row.description.isascii()
    assert "ladder-toy-1" in row.description and RULE in row.description
    assert row.name == f"ladder-toy-targets-194-211-{row.content_sha256[:12]}"


@pytest.mark.integration
def test_every_refusal_comes_before_anything_is_written(
    pg: Session, tmp_path: Path, stored: dict[str, bytes]
) -> None:
    window, photo, truth, placed = _placed_window(pg, tmp_path)
    first = _run(pg, window)
    with pytest.raises(WindowSampleRefused, match="already has these targets"):
        _run(pg, window)
    assert _run(pg, window, dry_run=True)["already_declared"] == first["query_set_id"]
    with pytest.raises(WindowSampleRefused, match="no window"):
        _run(pg, window, evaluation_set_id=str(uuid.uuid4()))
    written = dict(stored)

    pg.execute(sa.delete(WindowPlacement).where(WindowPlacement.evaluation_set_id == window.id))
    with pytest.raises(WindowSampleRefused, match="has no placement"):
        _run(pg, window, units_per_cell=5)
    assert stored == written
    assert pg.execute(select(sa.func.count(QuerySet.id))).scalar_one() == 1


@pytest.mark.integration
def test_a_truth_the_placement_or_the_pivot_does_not_hold_is_refused(
    pg: Session, tmp_path: Path, stored: dict[str, bytes]
) -> None:
    truth, placed = _truth(40)
    members = [member(a, "ts", residues="MK" + "A" * (i % 5)) for i, a in enumerate(placed)]
    photo = make_photo(pg, members)
    unplaced = make_placed_window(
        pg, photo, truth, ASPECTS, dict(list(placed.items())[1:]), tmp_path / "store"
    )
    with pytest.raises(
        WindowSampleRefused, match="1 targets of window .* are not in its placement"
    ):
        _run(pg, unplaced)

    photo_two = make_photo(pg, members, cut=194)
    no_term = make_placed_window(
        pg, photo_two, truth, {**ASPECTS, "GO:0000004": None}, placed, tmp_path / "store"
    )
    with pytest.raises(WindowSampleRefused, match="not in its pivot"):
        _run(pg, no_term)

    photo_three = make_photo(pg, members[1:], cut=177)
    no_sequence = make_placed_window(pg, photo_three, truth, ASPECTS, placed, tmp_path / "store")
    with pytest.raises(WindowSampleRefused, match="no sequence at the window's opening cut"):
        _run(pg, no_sequence, units_per_cell=40)

    photo_four = make_photo(pg, members, cut=160)
    unsealed = make_placed_window(pg, photo_four, truth, ASPECTS, placed, tmp_path / "store")
    unsealed.groundtruth_sha256 = None
    with pytest.raises(ArtifactMismatch, match="no groundtruth_sha256"):
        _run(pg, unsealed)
    assert pg.execute(select(sa.func.count(QuerySet.id))).scalar_one() == 0
    assert stored == {}

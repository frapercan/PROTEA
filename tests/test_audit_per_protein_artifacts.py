"""The calibration helper, and the two different questions it answers.

This project's rule for a new guard is that it is run over everything first,
and a large hit count means the rule is wrong. Applying that to the file census
would be a mistake: nothing has ever written the grid file, so "every result
holds a legacy file" is true before the census runs and says nothing about the
schema. The census is a recompute-cost estimate.

The number that IS a calibration is the row count under ``probe_legacy_rows``:
how many stored per-protein rows carry no ground truth, which is the state the
producer refuses a namespace for. That one is not knowable in advance and is
readable today from artefacts already on the store.

Neither number is invented here. Both are produced once by whoever has the real
store and go in the pull request body; these tests pin the arithmetic and the
access pattern. The store protocol has no ``list``, so the census probes
``exists`` per (result, setting) rather than enumerating a prefix, and it calls
``get`` only on the legacy files and only when asked.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from protea.core.operations._run_cafa_helpers import eval_artifact_key
from protea.core.operations.audit_per_protein_artifacts import (
    GRID_FILENAME,
    LEGACY_FILENAME,
    AuditPerProteinArtifactsOperation,
)

RESULTS = [str(uuid.UUID(int=i)) for i in range(1, 5)]


class ReadOnlyStore:
    """A store that answers ``exists`` and refuses every other verb.

    The refusals are the test. An operation that is described as read-only and
    is not would pass a test that only checked its return value.
    """

    def __init__(self, keys: set[str]) -> None:
        self.keys = keys
        self.probed: list[str] = []

    def exists(self, key: str) -> bool:
        self.probed.append(key)
        return key in self.keys

    def put(self, key: str, src: Any) -> str:
        raise AssertionError(f"the census wrote to {key}")

    def get(self, key: str) -> bytes:
        raise AssertionError(f"the census read {key}")


class RowReadingStore(ReadOnlyStore):
    """As above, but ``get`` returns a legacy table instead of refusing.

    ``put`` and ``delete`` still refuse: reading a row is not a licence to write
    one, and the point of the row census is that it stays safe to run before a
    migration.
    """

    def __init__(self, keys: set[str], tables: dict[str, bytes]) -> None:
        super().__init__(keys)
        self.tables = tables
        self.fetched: list[str] = []

    def get(self, key: str) -> bytes:
        self.fetched.append(key)
        if key not in self.tables:
            raise KeyError(key)
        return self.tables[key]


def _legacy_bytes(n_gt_w: list[float], pred_w: list[float]) -> bytes:
    """A legacy per_protein.parquet holding just the two columns the census reads."""
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"n_gt_w": pa.array(n_gt_w, pa.float64()),
                      "pred_w": pa.array(pred_w, pa.float64())})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()

    def delete(self, key: str) -> bool:
        raise AssertionError(f"the census deleted {key}")

    def url(self, key: str) -> str:
        raise AssertionError(f"the census resolved a url for {key}")


class FakeSession:
    def __init__(self, ids: list[str]) -> None:
        self.ids = ids
        self.statements: list[str] = []

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any:
        self.statements.append(str(statement))
        limit = (params or {}).get("limit", len(self.ids))
        return [(rid,) for rid in self.ids[:limit]]


def _key(result_id: str, setting: str, filename: str) -> str:
    return eval_artifact_key(uuid.UUID(result_id), f"{setting}/{filename}")


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> ReadOnlyStore:
    """A store holding one result of each state the census has to distinguish."""
    keys = set()
    # Legacy only, on all three settings: the population the schema rejects.
    for setting in ("NK", "LK", "PK"):
        keys.add(_key(RESULTS[0], setting, LEGACY_FILENAME))
    # Both files: what a correct producer leaves behind.
    for setting in ("NK", "LK", "PK"):
        keys.add(_key(RESULTS[1], setting, LEGACY_FILENAME))
        keys.add(_key(RESULTS[1], setting, GRID_FILENAME))
    # Partly migrated: one setting recomputed, two not.
    keys.add(_key(RESULTS[2], "NK", GRID_FILENAME))
    keys.add(_key(RESULTS[2], "LK", LEGACY_FILENAME))
    # RESULTS[3] holds nothing: an evaluation that wrote no per-protein file.
    fake = ReadOnlyStore(keys)
    monkeypatch.setattr(
        "protea.infrastructure.storage.factory.get_artifact_store", lambda *a, **k: fake
    )
    return fake


def test_the_census_counts_the_four_states(store: ReadOnlyStore) -> None:
    """Legacy, grid, both and absent, and the fourth is not a rejection.

    A setting holding neither file is a setting the evaluation never scored.
    Counting it as a rejection would inflate the number the pull request turns
    on, which is the one way this helper could do harm.
    """
    session = FakeSession(RESULTS)
    out = AuditPerProteinArtifactsOperation().execute(
        session, {"max_results": 10}, emit=lambda *a, **k: None
    ).result
    assert out["results_probed"] == 4
    assert out["settings"] == {"legacy": 4, "grid": 1, "both": 3, "absent": 4}
    assert out["rejected_results"] == 2
    # RESULTS[2] is half migrated: NK holds a grid file and LK still holds a
    # legacy one, so comparing it on LK still raises. Counting it readable
    # because SOME setting was migrated under-reports rejections the moment a
    # partial migration exists, which is the state a re-run passes through.
    assert out["fully_readable_results"] == 1
    assert out["truncated"] is False


def test_the_census_writes_nothing(store: ReadOnlyStore) -> None:
    """Only ``exists``, and only for the two filenames under the three settings."""
    session = FakeSession(RESULTS)
    AuditPerProteinArtifactsOperation().execute(
        session, {}, emit=lambda *a, **k: None
    )
    assert len(store.probed) == len(RESULTS) * 3 * 2
    assert all(k.startswith("eval_artifacts/") for k in store.probed)
    assert {k.rsplit("/", 1)[-1] for k in store.probed} == {GRID_FILENAME, LEGACY_FILENAME}


def test_a_truncated_census_says_so(store: ReadOnlyStore) -> None:
    """A truncated census that does not say it was truncated reads like a complete one."""
    session = FakeSession(RESULTS)
    out = AuditPerProteinArtifactsOperation().execute(
        session, {"max_results": 2, "max_detail": 1}, emit=lambda *a, **k: None
    ).result
    assert out["results_probed"] == 2
    assert out["truncated"] is True
    assert len(out["detail"]) == 1
    assert out["detail_omitted"] == 1


def test_the_census_is_registered() -> None:
    """A procedure outside the platform is a capability that dies with the disk."""
    from protea.core.operation_catalog import build_operation_registry

    registry = build_operation_registry()
    assert registry.get("audit_per_protein_artifacts") is not None


def test_unknown_settings_are_refused_rather_than_probed() -> None:
    """A free-text setting probes a key nothing wrote and reports it absent.

    The value is interpolated into an object-store key, so a typo does not fail:
    it succeeds, finds nothing, and is counted as a setting the evaluation never
    scored. That is a wrong answer wearing the shape of a right one, on the
    number the pull request turns on.
    """
    from protea.core.operations.audit_per_protein_artifacts import (
        AuditPerProteinArtifactsPayload,
    )

    payload = AuditPerProteinArtifactsPayload(settings=["NK", "nk"])
    with pytest.raises(ValueError, match="unknown settings"):
        payload.validated_settings()
    assert AuditPerProteinArtifactsPayload(settings=["PK", "NK", "PK"]).validated_settings() == [
        "PK",
        "NK",
    ]


def test_the_row_census_counts_the_population_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    """The number that is actually a calibration, read out of stored artefacts.

    ``rows_from_sink`` never filtered on ground truth, so a stored legacy file
    holds one row per kernel array row. The rows with ``n_gt_w <= 0`` are the
    ones cafaeval counted in ``P`` and left out of the population it normalised
    by, and they are exactly what makes the grid producer refuse a namespace. It
    is a lower bound, being one variant at one threshold, and that is why the
    predicted mass is reported beside the count rather than the count alone.
    """
    rid = RESULTS[0]
    keys = {_key(rid, "PK", LEGACY_FILENAME), _key(rid, "NK", LEGACY_FILENAME)}
    tables = {
        _key(rid, "PK", LEGACY_FILENAME): _legacy_bytes([2.0, 0.0, 1.0], [4.0, 6.0, 2.0]),
        _key(rid, "NK", LEGACY_FILENAME): _legacy_bytes([2.0, 1.0], [4.0, 2.0]),
    }
    fake = RowReadingStore(keys, tables)
    monkeypatch.setattr(
        "protea.infrastructure.storage.factory.get_artifact_store", lambda *a, **k: fake
    )
    out = AuditPerProteinArtifactsOperation().execute(
        FakeSession([rid]), {"probe_legacy_rows": True}, emit=lambda *a, **k: None
    ).result
    pk = out["legacy_rows"]["PK"]
    assert pk["files_read"] == 1 and pk["files_with_a_gap"] == 1
    assert pk["rows"] == 3.0 and pk["rows_without_ground_truth"] == 1.0
    assert pk["predicted_mass"] == 12.0
    assert pk["predicted_mass_without_ground_truth"] == 6.0
    # NK cannot have the gap: its kernel is handed only the rows that already
    # carry ground truth in that call's terms of interest. Reported anyway, as
    # the control that says the count above is not an artefact of the reader.
    assert out["legacy_rows"]["NK"]["rows_without_ground_truth"] == 0.0
    assert out["legacy_rows"]["LK"]["files_read"] == 0
    assert all(k.endswith(LEGACY_FILENAME) for k in fake.fetched)


def test_the_row_census_is_off_unless_asked(store: ReadOnlyStore) -> None:
    """The default census never calls ``get``; the fake store raises if it does."""
    out = AuditPerProteinArtifactsOperation().execute(
        FakeSession(RESULTS), {}, emit=lambda *a, **k: None
    ).result
    assert out["legacy_rows"] is None


def test_a_store_pointing_nowhere_is_refused_not_reported_as_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one wrong answer this audit can give is a well-formed zero.

    Every probe is an ``exists()`` against a store resolved from the module's own
    path, so a tree without configuration answers absent to all of them. The
    result is a clean "nothing to migrate", which licenses a merge, and nothing
    in it says the store was never reachable. It happened on the first real run:
    360 absent and 0 legacy, when the true answer was 360 legacy.
    """
    empty = ReadOnlyStore(set())
    monkeypatch.setattr(
        "protea.infrastructure.storage.factory.get_artifact_store", lambda *a, **k: empty
    )
    with pytest.raises(RuntimeError, match="indistinguishable from a store pointing nowhere"):
        AuditPerProteinArtifactsOperation().execute(
            FakeSession(RESULTS), {"max_results": 10}, emit=lambda *a, **k: None
        )

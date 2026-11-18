"""The segment families' loader (ADR-D65): what it verifies, what it writes, what it refuses.

A photo is built and loaded and its lexicon bank decoded and loaded the way
``tests/test_load_lexicon_bank.py`` does it; the families' prepare pass runs on
them, and the search's outputs are written by hand (every donor core its own
family, every target core in the first donor's), so no MMseqs2 binary is needed.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core.file_stage import write_record
from protea.core.operations import load_segment_families as loader
from protea.core.operations.load_segment_families import (
    LoadSegmentFamiliesOperation,
    SegmentFamiliesRefused,
)
from protea.core.segment_families import build
from protea.core.segment_families.build import SCOPES
from protea.infrastructure.orm.models.embedding.segment_families import SegmentFamilies
from tests.test_lexicon_bank_build import fake_decoder  # noqa: F401
from tests.test_load_cut_photo import pg_session  # noqa: F401
from tests.test_load_lexicon_bank import NAME, _loaded_bank, _weights, db, decoder  # noqa: F401
from tests.test_load_lexicon_bank import _run as load_bank


@pytest.fixture()
def fdb(db: Session, decoder: None, monkeypatch: pytest.MonkeyPatch) -> Session:  # noqa: F811
    """The lexicon bank's schema and stand-in decoder, with the families' store kept in memory."""

    def store(root: Path, prefix: str, outputs: list[dict[str, Any]]) -> str:
        for output in outputs:
            output["uri"] = f"memory://{prefix}/{output['name']}"
        return f"memory://{prefix}/segment-families.json"

    monkeypatch.setattr(loader, "store_families", store)
    return db


def _searched(out: Path, flip_to_donor: str | None = None) -> None:
    """The search's records and each scope's families, written by hand over ``segments.parquet``."""
    segments = pq.read_table(out / "segments.parquet")
    if flip_to_donor is not None:
        donor = [
            d or m == flip_to_donor
            for m, d in zip(*(segments[c].to_pylist() for c in ("md5", "donor")), strict=True)
        ]
        segments = segments.set_column(
            segments.schema.get_field_index("donor"), "donor", pa.array(donor)
        )
        pq.write_table(segments, out / "segments.parquet")
    donors = segments["donor"].to_pylist()
    first = donors.index(True)
    family = [i if d else first for i, d in enumerate(donors)]
    n = segments.num_rows
    write_record(
        out / "search.json",
        {
            "mmseqs": {"version": "8cc5ce367b5638c4", "binary_sha256": "e" * 64},
            "commands": [["search"]],
        },
    )
    for scope in SCOPES:
        table = segments.append_column("scope", pa.array([scope] * n))
        for column, values in (("group", family), ("family", family), ("clan", family)):
            table = table.append_column(column, pa.array(values, pa.int64()))
        table = table.append_column("masked", pa.array([False] * n))
        pq.write_table(table, out / f"families-{scope}.parquet")
        write_record(
            out / f"families-{scope}.json",
            {"families": sum(donors), "families_of_two_or_more": 1},
        )


def _families(
    tmp_path: Path, photo_dir: Path, bank: Path, revision: str = "builder", flip: bool = False
) -> tuple[Path, str]:
    """The families of the loaded photo under its loaded bank; their directory and manifest sha256."""
    out = tmp_path / f"families-{revision}"
    build.prepare_pass(photo_dir, bank, NAME, out)
    target = None
    if flip:
        segments = pq.read_table(out / "segments.parquet").to_pydict()
        target = next(m for m, d in zip(segments["md5"], segments["donor"], strict=True) if not d)
    _searched(out, target)
    manifest = build.manifest_pass(out, revision)
    assert manifest["status"] == "complete"
    return out, hashlib.sha256((out / "segment-families.json").read_bytes()).hexdigest()


def _load(session: Session, out: Path, sha: str) -> dict[str, Any]:
    payload = {"families_dir": str(out), "manifest_sha256": sha}
    result = LoadSegmentFamiliesOperation().execute(session, payload, emit=lambda *a, **k: None)
    session.commit()
    return result.result


def _photo_and_bank(session: Session, tmp_path: Path, load: bool = True) -> tuple[Path, Path]:
    bank, sha, lexicon_sha = _loaded_bank(session, tmp_path)
    if load:
        load_bank(session, bank, sha, _weights(session, lexicon_sha))
    return tmp_path / "photo", bank


@pytest.mark.integration
def test_families_load_and_declare_their_outputs(fdb: Session, tmp_path: Path) -> None:
    photo_dir, bank = _photo_and_bank(fdb, tmp_path)
    out, sha = _families(tmp_path, photo_dir, bank)
    result = _load(fdb, out, sha)
    row = fdb.execute(select(SegmentFamilies)).scalar_one()
    assert (
        result["segments"]
        == row.segments
        == pq.ParquetFile(out / "segments.parquet").metadata.num_rows
    )
    assert [o["name"] for o in row.outputs] == list(loader.OUTPUTS)
    assert all(o["uri"].startswith("memory://segment-families/") for o in row.outputs)
    assert row.short_name == "families-lexico-test-211" and row.scopes == list(SCOPES)
    assert row.mmseqs["version"].startswith("8cc5ce3") and row.builder_revision == "builder"
    assert _load(fdb, out, sha)["skipped"] is True
    assert fdb.execute(select(func.count(SegmentFamilies.id))).scalar_one() == 1


@pytest.mark.integration
def test_families_of_a_bank_not_loaded_are_refused(fdb: Session, tmp_path: Path) -> None:
    photo_dir, bank = _photo_and_bank(fdb, tmp_path, load=False)
    out, sha = _families(tmp_path, photo_dir, bank)
    with pytest.raises(SegmentFamiliesRefused, match="load the bank first"):
        _load(fdb, out, sha)


@pytest.mark.integration
def test_a_changed_output_and_a_wrong_digest_are_refused(fdb: Session, tmp_path: Path) -> None:
    photo_dir, bank = _photo_and_bank(fdb, tmp_path)
    out, sha = _families(tmp_path, photo_dir, bank)
    with pytest.raises(SegmentFamiliesRefused, match="hashes to"):
        _load(fdb, out, "0" * 64)
    (out / "families-strict.parquet").write_bytes(b"changed")
    with pytest.raises(SegmentFamiliesRefused, match="does not hash as recorded"):
        _load(fdb, out, sha)


@pytest.mark.integration
def test_a_second_manifest_for_the_same_photo_and_bank_is_refused(
    fdb: Session, tmp_path: Path
) -> None:
    photo_dir, bank = _photo_and_bank(fdb, tmp_path)
    _load(fdb, *_families(tmp_path, photo_dir, bank))
    with pytest.raises(SegmentFamiliesRefused, match="already has families"):
        _load(fdb, *_families(tmp_path, photo_dir, bank, revision="another"))


@pytest.mark.integration
def test_a_target_written_as_a_donor_is_refused(fdb: Session, tmp_path: Path) -> None:
    photo_dir, bank = _photo_and_bank(fdb, tmp_path)
    out, sha = _families(tmp_path, photo_dir, bank, flip=True)
    with pytest.raises(SegmentFamiliesRefused, match="no truth member's"):
        _load(fdb, out, sha)

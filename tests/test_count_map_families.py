"""Segment families as the count map's units (ADR-D65): the reader, the payload and the spec.

A themes run must stay the method it was before families existed (same recipe,
same pins, so the same id); a families run carries its scope and its families'
pin, and reads its units from the verified families file of that scope.
"""

from __future__ import annotations

import hashlib
import io
import uuid
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from protea.core.count_map.inputs import family_units
from protea.core.operations import run_count_map as op
from protea.core.operations.run_count_map import RunCountMapPayload
from protea.infrastructure.storage import LocalFsArtifactStore

ESET = str(uuid.uuid4())
BANK = str(uuid.uuid4())
FAMILIES = str(uuid.uuid4())


def _families_file(store: LocalFsArtifactStore, scope: str) -> list[dict[str, object]]:
    table = pa.table(
        {
            "md5": ["m1", "m1", "m1", "m2", "m3", "m4"],
            "family": pa.array([4, 2, 4, -1, 7, 9], pa.int64()),
            "masked": [False, False, False, False, True, False],
        }
    )
    sink = io.BytesIO()
    pq.write_table(table, sink)
    blob = sink.getvalue()
    name = f"families-{scope}.parquet"
    uri = store.put(f"families/{name}", blob)
    return [{"name": name, "uri": str(uri), "sha256": hashlib.sha256(blob).hexdigest()}]


def test_family_units_are_the_families_of_a_md5s_unmasked_linked_cores(tmp_path: Path) -> None:
    store = LocalFsArtifactStore(tmp_path / "store")
    outputs = _families_file(store, "permissive")
    units = family_units(store, outputs, "permissive", {"m1", "m2", "m3"})
    assert set(units) == {"m1"}
    assert units["m1"].tolist() == [2, 4]
    with pytest.raises(ValueError, match="families-strict"):
        family_units(store, outputs, "strict", {"m1"})


def test_families_need_their_table_and_scope_and_themes_take_neither() -> None:
    themes = RunCountMapPayload(evaluation_set_id=ESET, lexicon_bank_id=BANK)
    assert themes.label == "themes"
    families = RunCountMapPayload(
        evaluation_set_id=ESET,
        lexicon_bank_id=BANK,
        units="families",
        segment_families_id=FAMILIES,
        scope="permissive",
    )
    assert families.label == "families-permissive"
    with pytest.raises(ValidationError, match="need segment_families_id and scope"):
        RunCountMapPayload(evaluation_set_id=ESET, lexicon_bank_id=BANK, units="families")
    with pytest.raises(ValidationError, match="only with families"):
        RunCountMapPayload(evaluation_set_id=ESET, lexicon_bank_id=BANK, scope="strict")


def _sources(families: object | None = None, scope: str | None = None) -> op.Sources:
    lexicon = SimpleNamespace(id=uuid.uuid4(), sha256="a" * 64, name="lexico-test")
    bank = SimpleNamespace(id=uuid.uuid4(), manifest_sha256="c" * 64)
    photo = SimpleNamespace(cut_release=211, ontology_snapshot_id=uuid.uuid4())
    # A reconciled window: the specs these tests pin are the ones such a window has always had.
    window = SimpleNamespace(truth_profile="as_closed")
    return op.Sources(window, photo, bank, lexicon, families, scope)  # type: ignore[arg-type]


WEIGHTS = SimpleNamespace(id=uuid.uuid4(), sha256="d" * 64)
#: The recipe of a themes run as it was before families existed.
THEMES_RECIPE = {"source", "units", "donors", "state", "exclusion", "strength", "reading", "top"}


def test_a_themes_spec_is_the_one_it_was_before_families() -> None:
    p = RunCountMapPayload(evaluation_set_id=ESET, lexicon_bank_id=BANK)
    spec = op.method_spec(_sources(), WEIGHTS, p)  # type: ignore[arg-type]
    assert spec.code == "count-map-themes-lexico-test"
    assert set(spec.recipe) == THEMES_RECIPE and spec.recipe["units"] == "themes"
    assert {pin.role for pin in spec.pins} == {"lexicon", "lexicon-bank", "count-map"}


def test_a_families_spec_carries_its_scope_and_its_families_pin() -> None:
    p = RunCountMapPayload(
        evaluation_set_id=ESET,
        lexicon_bank_id=BANK,
        units="families",
        segment_families_id=FAMILIES,
        scope="strict",
    )
    families = SimpleNamespace(id=uuid.UUID(FAMILIES), manifest_sha256="f" * 64)
    src = _sources(families, "strict")
    spec = op.method_spec(src, WEIGHTS, p)  # type: ignore[arg-type]
    assert spec.code == "count-map-families-strict-lexico-test"
    assert spec.recipe["scope"] == "strict" and set(spec.recipe) == THEMES_RECIPE | {"scope"}
    roles = {pin.role for pin in spec.pins}
    assert roles == {"lexicon", "lexicon-bank", "segment-families", "count-map"}
    assert src.units == "lexico-test's segment families (strict)"
    themes = op.method_spec(
        _sources(), WEIGHTS, RunCountMapPayload(evaluation_set_id=ESET, lexicon_bank_id=BANK)
    )  # type: ignore[arg-type]
    assert spec.method_id != themes.method_id

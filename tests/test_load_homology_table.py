"""The homology table's loader (ADR-D61): what it verifies, what it writes, what it refuses.

A photo is built and loaded the way ``tests/test_load_cut_photo.py`` does it; its
homology table is then built by the real file stage over that photo's directory, with
MMseqs2 replaced by a script that writes hits among the photo's own sequences.
"""

from __future__ import annotations

import json
import stat
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.homology.build import manifest_pass, prepare_pass, search_pass
from protea.core.operations import load_homology_table as loader
from protea.core.operations.load_homology_table import (
    HomologyTableRefused,
    LoadHomologyTableOperation,
    LoadHomologyTablePayload,
    descriptors,
    table_keys_for,
)
from protea.infrastructure.orm.models.annotation.homology_table import HomologyTable
from tests.test_load_cut_photo import _load, build_photo, pg_session  # noqa: F401

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/c7d2e4f9a1b3_the_homology_table_of_a_cut.py"
)


def _fasta_ids(path: Path) -> list[str]:
    return [line[1:] for line in path.read_text(encoding="ascii").splitlines() if line[:1] == ">"]


def _mmseqs(tmp_path: Path, rows: list[str]) -> Path:
    script = tmp_path / "mmseqs"
    body = "\\n".join(rows)
    script.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = version ]; then echo 8cc5ce367b56; exit 0; fi\n'
        f'printf "{body}\\n" > "$4"\n',
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script


def _table(tmp_path: Path, photo_dir: Path, *, sample: int | None = None, strange: bool = False):
    """Build the table of the photo in ``photo_dir``; return its directory and manifest sha256."""
    out = tmp_path / ("table-sampled" if sample else "table")
    prepare_pass(photo_dir, out, sample=sample)
    queries, bank = _fasta_ids(out / "queries.fasta"), _fasta_ids(out / "bank.fasta")
    query = queries[0]
    targets = [md5 for md5 in bank if md5 != query][:2]
    if strange:
        targets = ["f" * 32]
    rows = [f"{query}\t{query}\t1.000\t5\t1.000\t1.000\t1e-30\t50"]
    rows += [
        f"{query}\t{t}\t0.5{i}0\t5\t1.000\t1.000\t1e-05\t{20 - i}" for i, t in enumerate(targets)
    ]
    mmseqs = _mmseqs(tmp_path, rows)
    import hashlib

    digest = hashlib.sha256(mmseqs.read_bytes()).hexdigest()
    search_pass(out, mmseqs, digest, {"threads": 2, "tmp": tmp_path / "tmp"})
    manifest_pass(out, revision="builder-revision")
    sha = hashlib.sha256((out / "homology.json").read_bytes()).hexdigest()
    return out, sha


def _load_table(session: Session, table_dir: Path, sha: str) -> dict[str, Any]:
    payload = {"table_dir": str(table_dir), "manifest_sha256": sha}
    result = LoadHomologyTableOperation().execute(session, payload, emit=lambda *a, **k: None)
    session.commit()
    return result.result


@pytest.fixture()
def stored(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, str]]:
    calls: list[tuple[int, str]] = []

    def _store(_dir: Path, cut: int, sha: str) -> tuple[str, str]:
        calls.append((cut, sha))
        hits_key, manifest_key = table_keys_for(cut, sha)
        return f"memory://{hits_key}", f"memory://{manifest_key}"

    monkeypatch.setattr(loader, "store_table", _store)
    return calls


class TestTheHitsAreReadInBatches:
    def test_the_distinct_md5s_do_not_depend_on_the_batch_size(self, tmp_path: Path) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        from protea.core.operations.load_homology_table import distinct_md5s

        path = tmp_path / "hits.parquet"
        table = pa.table(
            {
                "query_md5": pa.array(["a", "a", "b", "c", "c"], type=pa.large_string()),
                "target_md5": pa.array(["x", "y", "x", "z", "y"], type=pa.large_string()),
            }
        )
        pq.write_table(table, path, row_group_size=2)
        for batch_rows in (1, 2, 10):
            rows, queries, targets = distinct_md5s(path, batch_rows=batch_rows)
            assert (rows, queries, targets) == (5, {"a", "b", "c"}, {"x", "y", "z"})


class TestThePayload:
    def test_a_digest_that_is_not_one_is_refused(self) -> None:
        with pytest.raises(ValueError, match="64 hexadecimal"):
            LoadHomologyTablePayload(table_dir="/x", manifest_sha256="abc")

    def test_the_keys_have_one_spelling(self) -> None:
        assert table_keys_for(211, "a" * 64) == (
            f"homology/211/{'a' * 64}/hits.parquet",
            f"homology/211/{'a' * 64}/homology.json",
        )

    def test_the_descriptors_are_derived(self) -> None:
        manifest = {
            "cut_release": 211,
            "mmseqs": {"version": "8cc5ce367b5638c4"},
            "counts": {
                "prepare": {"queries": 2, "bank": 4},
                "search": {"hits": 2, "queries_with_hits": 1, "self_hits_dropped": 1},
            },
        }
        short, long = descriptors(manifest)
        assert short == "homology-211"
        assert "MMseqs2 8cc5ce367b56" in long and "no threshold applied" in long


class TestTheSchema:
    def test_the_model_and_the_migration_declare_the_same_checks(self) -> None:
        import importlib.util

        from protea.infrastructure.orm.models.annotation import homology_table as model

        spec = importlib.util.spec_from_file_location("homology_migration", _MIGRATION)
        assert spec is not None and spec.loader is not None
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        for name in ("DIGESTS_CHECK", "COUNTS_CHECK", "SHORT_NAME_CHECK", "DESCRIPTIONS_CHECK"):
            assert getattr(migration, name) == getattr(model, name), name
        assert (migration.revision, migration.down_revision) == ("c7d2e4f9a1b3", "e5b1c9d3a7f2")
        source = _MIGRATION.read_text(encoding="utf-8")
        for column in HomologyTable.__table__.columns:
            assert f'"{column.name}"' in source, column.name


@pytest.mark.integration
def test_a_table_loads_and_says_what_it_wrote(
    pg_session: Session,  # noqa: F811
    tmp_path: Path,
    stored: list[tuple[int, str]],
) -> None:
    photo_dir, photo_sha = build_photo(tmp_path)
    _load(pg_session, photo_dir, photo_sha)
    table_dir, sha = _table(tmp_path, photo_dir)
    result = _load_table(pg_session, table_dir, sha)
    row = pg_session.execute(select(HomologyTable)).scalar_one()
    assert result["short_name"] == row.short_name == "homology-211"
    assert (row.hits, row.self_hits_dropped, row.queries_with_hits) == (2, 1, 1)
    assert row.mmseqs_version == "8cc5ce367b56"
    assert row.hits_uri == f"memory://homology/211/{sha}/hits.parquet"
    assert row.builder_revision == "builder-revision"
    assert stored == [(211, sha)]
    # The same manifest again is a no-op.
    assert _load_table(pg_session, table_dir, sha)["skipped"] is True


@pytest.mark.integration
def test_another_table_for_a_cut_with_one_is_refused(
    pg_session: Session,  # noqa: F811
    tmp_path: Path,
    stored: list[tuple[int, str]],
) -> None:
    photo_dir, photo_sha = build_photo(tmp_path)
    _load(pg_session, photo_dir, photo_sha)
    table_dir, sha = _table(tmp_path, photo_dir)
    _load_table(pg_session, table_dir, sha)
    manifest = json.loads((table_dir / "homology.json").read_text())
    manifest["revision"] = "another-builder"
    (table_dir / "homology.json").write_text(json.dumps(manifest))
    import hashlib

    other = hashlib.sha256((table_dir / "homology.json").read_bytes()).hexdigest()
    with pytest.raises(HomologyTableRefused, match="already has a homology table"):
        _load_table(pg_session, table_dir, other)


@pytest.mark.integration
def test_a_measurement_is_never_loaded(
    pg_session: Session,  # noqa: F811
    tmp_path: Path,
    stored: list[tuple[int, str]],
) -> None:
    photo_dir, photo_sha = build_photo(tmp_path)
    _load(pg_session, photo_dir, photo_sha)
    table_dir, sha = _table(tmp_path, photo_dir, sample=1)
    with pytest.raises(HomologyTableRefused, match="'measurement'"):
        _load_table(pg_session, table_dir, sha)
    assert stored == []


@pytest.mark.integration
def test_a_target_that_is_no_member_is_refused(
    pg_session: Session,  # noqa: F811
    tmp_path: Path,
    stored: list[tuple[int, str]],
) -> None:
    photo_dir, photo_sha = build_photo(tmp_path)
    _load(pg_session, photo_dir, photo_sha)
    table_dir, sha = _table(tmp_path, photo_dir, strange=True)
    with pytest.raises(HomologyTableRefused, match="1 targets no member sequence"):
        _load_table(pg_session, table_dir, sha)
    assert stored == []


@pytest.mark.integration
def test_a_table_of_a_photo_that_is_not_loaded_is_refused(
    pg_session: Session,  # noqa: F811
    tmp_path: Path,
    stored: list[tuple[int, str]],
) -> None:
    photo_dir, _ = build_photo(tmp_path)
    table_dir, sha = _table(tmp_path, photo_dir)
    with pytest.raises(HomologyTableRefused, match="load the photo first"):
        _load_table(pg_session, table_dir, sha)

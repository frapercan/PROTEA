"""A bank's population checked against the photos it names (ADR-D62, ADR-D63).

A bank computed away from the server lists its sequences by md5. Before it is
written, its md5s must be unique, each must be a ``sequence`` row, and together
they must be exactly the md5s of the members of the photos it names. The md5s
go into a temporary table that the transaction drops; the shards are read by
their md5 column alone.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto

_GAPS = {
    "md5s with no sequence row": (
        "SELECT count(*) FROM bank_md5 b WHERE NOT EXISTS "
        "(SELECT 1 FROM sequence s WHERE s.sequence_hash = b.md5)"
    ),
    "member md5s the bank lacks": (
        "SELECT count(*) FROM (SELECT DISTINCT m.sequence_md5 AS md5 FROM cut_photo_member m "
        "WHERE m.photo_id = ANY(%(photos)s) AND m.sequence_md5 IS NOT NULL) x "
        "WHERE NOT EXISTS (SELECT 1 FROM bank_md5 b WHERE b.md5 = x.md5)"
    ),
    "bank md5s that are no member's": (
        "SELECT count(*) FROM bank_md5 b WHERE NOT EXISTS (SELECT 1 FROM cut_photo_member m "
        "WHERE m.photo_id = ANY(%(photos)s) AND m.sequence_md5 = b.md5)"
    ),
}


def population_gaps(
    session: Session, md5_files: Iterable[Path], photo_ids: list[Any]
) -> dict[str, int]:
    """How far the md5s of ``md5_files`` are from the photos' members; ``duplicates`` when one repeats."""
    import psycopg

    raw = session.connection().connection.driver_connection
    with raw.cursor() as cur:
        cur.execute("CREATE TEMP TABLE bank_md5 (md5 varchar(32) PRIMARY KEY) ON COMMIT DROP")
        try:
            with cur.copy("COPY bank_md5 (md5) FROM STDIN") as copy:
                for path in md5_files:
                    for md5 in pq.read_table(path, columns=["md5"]).column("md5").to_pylist():
                        copy.write_row((md5,))
        except psycopg.errors.UniqueViolation:
            return {"duplicates": 1}
        gaps = {}
        for name, sql in _GAPS.items():
            cur.execute(sql, {"photos": photo_ids})
            gaps[name] = int(cur.fetchone()[0])
    return {name: count for name, count in gaps.items() if count}


def loaded_photos(
    session: Session, named: list[dict[str, Any]], refused: type[Exception]
) -> list[CutPhoto]:
    """The loaded photos a bank names, each found by its manifest's sha256, of the same cut and kind."""
    photos = []
    for entry in named:
        photo = session.execute(
            select(CutPhoto).where(CutPhoto.manifest_sha256 == entry["manifest_sha256"])
        ).scalar_one_or_none()
        if photo is None:
            raise refused(
                f"no loaded photo has manifest {entry['manifest_sha256']}: load the photo first"
            )
        if photo.cut_release != int(entry["cut_release"]) or photo.kind != entry["kind"]:
            raise refused(
                f"the photo with that manifest is cut {photo.cut_release} ({photo.kind}), not "
                f"cut {entry['cut_release']} ({entry['kind']})"
            )
        photos.append(photo)
    return photos

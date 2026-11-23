"""The rows pass of ADR-D59's photo: every GOA t row of a member, with its channel.

The channel is :func:`protea.core.cut_photo.channels.channel_of` on the row's
reference class, the protein reviewed when it is in the superset. A code or an
electronic reference the channel table does not list is counted, written out
(``unknown_evidence.tsv``, with up to three raw references per key) and refuses
the photo. GOA on stdin, read a second time, after the members pass.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from protea.core.cut_photo.channels import GOREF_LIST_READ_ON, UnknownEvidence, channel_of
from protea.core.cut_photo.formats import (
    EMPTY_GO_REF,
    GAF_ASPECT,
    GAF_ASSIGNED_BY,
    GAF_DATE,
    GAF_EVIDENCE,
    GAF_GO,
    GAF_ID,
    GAF_QUALIFIER,
    GAF_REFERENCE,
    GAF_TAXON,
    GAF_WITH_FROM,
    reference_class,
)
from protea.core.cut_photo.measure import PassSource, _ParquetBuffer, _write_record
from protea.core.cut_photo.photo_io import _gaf_columns, _read_members, channel_table_digest

#: How many distinct raw references ``unknown_evidence.tsv`` shows for each key.
UNKNOWN_EXAMPLES = 3


_ROW_SCHEMA = pa.schema(
    [
        ("accession", pa.string()),
        ("go_id", pa.string()),
        ("qualifier", pa.string()),
        ("code", pa.string()),
        ("reference", pa.string()),
        ("with_from", pa.string()),
        ("aspect", pa.string()),
        ("taxon", pa.string()),
        ("date", pa.string()),
        ("assigned_by", pa.string()),
        ("channel", pa.string()),
    ]
)


class _Rows:
    """The running state of the rows pass: member rows out, unknown keys counted."""

    def __init__(self, members: dict[str, bool], rows_out: _ParquetBuffer):
        self.members = members
        self.rows_out = rows_out
        self.unknown: Counter[tuple[str, str, str, str]] = Counter()
        self.examples: dict[tuple[str, str, str, str], list[str]] = {}
        self.counts: Counter[str] = Counter()

    def feed(self, cols: list[str]) -> None:
        reviewed = self.members.get(cols[GAF_ID])
        if reviewed is None:
            return
        code, reference = cols[GAF_EVIDENCE], cols[GAF_REFERENCE]
        kind = reference_class(reference, cols[GAF_WITH_FROM])
        if reference == EMPTY_GO_REF:
            self.counts[f"empty_go_ref_read_as_{kind}"] += 1
        channel: str
        try:
            channel = channel_of(code, kind, reviewed)
        except UnknownEvidence:
            self._count_unknown(code, kind, reference, cols[GAF_ASSIGNED_BY])
            channel = "unknown"
        self._member_row(cols, channel)

    def _count_unknown(self, code: str, kind: str, reference: str, assigned_by: str) -> None:
        """Count a row the channel table cannot place, keeping a few raw references.

        The key holds the reference CLASS (the whole ``GO_REF`` id, ``publication``
        or ``other``) beside the raw prefix: a prefix alone would turn every unknown
        ``GO_REF`` into the same ``GO_REF`` key, and the examples are what the
        decision to classify them is read from.
        """
        prefix = reference.split(":", 1)[0] if reference else ""
        key = (code, kind, prefix, assigned_by)
        self.unknown[key] += 1
        shown = self.examples.setdefault(key, [])
        if len(shown) < UNKNOWN_EXAMPLES and reference not in shown:
            shown.append(reference)

    def _member_row(self, cols: list[str], channel: str) -> None:
        self.counts["member_rows"] += 1
        self.counts[f"member_rows_{channel}"] += 1
        self.rows_out.append(
            cols[GAF_ID],
            cols[GAF_GO],
            cols[GAF_QUALIFIER],
            cols[GAF_EVIDENCE],
            cols[GAF_REFERENCE],
            cols[GAF_WITH_FROM],
            cols[GAF_ASPECT],
            cols[GAF_TAXON],
            cols[GAF_DATE],
            cols[GAF_ASSIGNED_BY],
            channel,
        )


def rows_pass(
    lines: Iterable[str],
    *,
    members_path: Path,
    source: PassSource,
    source_file: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Write every member's GOA t rows with their channel, and the unknown keys."""
    source.check("GOA")
    members = _read_members(members_path)
    out = source.out
    with pq.ParquetWriter(out / "rows.parquet", _ROW_SCHEMA, compression="zstd") as writer:
        state = _Rows(members, _ParquetBuffer(writer, _ROW_SCHEMA, 1_000_000))
        header: dict[str, str] = {}
        for cols in _gaf_columns(lines, header):
            state.feed(cols)
        state.rows_out.flush()
    _write_unknown(out, state.unknown, state.examples)
    record = {
        **source.head("rows"),
        **dict(state.counts),
        "unknown_evidence_rows": sum(state.unknown.values()),
        "unknown_evidence_keys": len(state.unknown),
        "source_file": dict(source_file or {}),
        "gaf_header": header,
        "channel_table": {
            "goref_list_read_on": GOREF_LIST_READ_ON,
            "sha256": channel_table_digest(),
        },
    }
    _write_record(out / "rows.json", record)
    return record


def _write_unknown(
    out: Path,
    unknown: Counter[tuple[str, str, str, str]],
    examples: dict[tuple[str, str, str, str], list[str]],
) -> None:
    """One line per key, most rows first, with up to ``UNKNOWN_EXAMPLES`` raw references."""
    with open(out / "unknown_evidence.tsv", "w", encoding="utf-8") as handle:
        handle.write("code\treference_class\treference_prefix\tassigned_by\trows\texamples\n")
        for key, n in sorted(unknown.items(), key=lambda kv: (-kv[1], kv[0])):
            code, kind, prefix, by = key
            shown = " ; ".join(examples.get(key, []))
            handle.write(f"{code}\t{kind}\t{prefix}\t{by}\t{n}\t{shown}\n")

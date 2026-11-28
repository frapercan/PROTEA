"""The lexicon cut the segment encoder reads, from a lexicon bank on the cluster (ADR-D63).

A lexicon bank holds, for every sequence of the photos' population (``sequences.parquet``
of the embedding bank, ADR-D62), its cut: the segments of its best path (start, end,
theme), one cut shard per bank shard, in the bank's order. Its manifest
(``lexicon/<name>/lexicon-bank.json``) records every shard's sha256; this module reads
the shards only after checking them against it, and only the segment columns (never
the residues' columns, which the segment encoder does not read).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

from protea.core.file_stage import file_sha256, read_record
from protea.core.own_encoder.model import THEMES, Segments

__all__ = [
    "Corpus",
    "CorpusRefused",
    "LexiconCut",
    "checked_lexicon_cut",
    "read_corpus",
    "shard_segments",
]

#: The segment columns of a cut shard.
SEGMENT_COLUMNS = ("md5", "seg_start", "seg_end", "seg_theme")


class CorpusRefused(RuntimeError):
    """The lexicon bank or the member list cannot give the encoder its input."""


@dataclass(frozen=True)
class LexiconCut:
    """A complete lexicon bank's cut pass: its manifest's sha256 and its shards by number."""

    name: str
    manifest_sha256: str
    lexicon_sha256: str
    folder: Path
    shards: dict[int, dict[str, Any]]

    def path(self, shard: int) -> Path:
        return self.folder / "cut" / f"shard-{shard:05d}.parquet"


def checked_lexicon_cut(bank: Path, name: str) -> LexiconCut:
    """The lexicon bank's cut pass, refused unless its manifest is complete."""
    folder = bank / "lexicon" / name
    path = folder / "lexicon-bank.json"
    if not path.is_file():
        raise CorpusRefused(f"{path} does not exist: decode the lexicon bank first")
    manifest = read_record(path)
    if manifest.get("status") != "complete":
        raise CorpusRefused(f"the lexicon bank {name} is {manifest.get('status')!r}, not complete")
    shards = {int(s["shard"]): s for s in manifest["passes"]["cut"]["shards"]}
    return LexiconCut(name, file_sha256(path), str(manifest["lexicon_sha256"]), folder, shards)


def shard_segments(cut: LexiconCut, shard: int) -> tuple[list[str], np.ndarray, Segments]:
    """One verified cut shard: its md5s, the segment offsets per sequence, and the segments.

    Refused when the shard does not hash to the manifest's record or names a theme
    the encoder has no embedding for.
    """
    record = cut.shards.get(shard)
    path = cut.path(shard)
    if record is None or not path.is_file() or file_sha256(path) != record["sha256"]:
        raise CorpusRefused(
            f"cut shard {shard} of {cut.name} is missing or does not hash as recorded"
        )
    table = pq.read_table(path, columns=list(SEGMENT_COLUMNS))
    counts = pc.list_value_length(table.column("seg_start")).to_numpy(zero_copy_only=False)
    offsets = np.zeros(len(counts) + 1, dtype=np.int64)
    np.cumsum(counts, out=offsets[1:])
    flat = [
        np.asarray(pc.list_flatten(table.column(c)).to_numpy(zero_copy_only=False), np.int64)
        for c in SEGMENT_COLUMNS[1:]
    ]
    if len(flat[2]) and (flat[2].min() < 0 or flat[2].max() >= THEMES):
        raise CorpusRefused(f"cut shard {shard} names a theme outside 0..{THEMES - 1}")
    return table.column("md5").to_pylist(), offsets, (flat[0], flat[1], flat[2])


@dataclass(frozen=True)
class Corpus:
    """Proteins 0..P-1 in the bank's order: md5s, and their segments by offset."""

    md5: list[str]
    offsets: np.ndarray
    start: np.ndarray
    end: np.ndarray
    theme: np.ndarray

    def __len__(self) -> int:
        return len(self.md5)

    def segments(self, protein: int) -> Segments:
        lo, hi = int(self.offsets[protein]), int(self.offsets[protein + 1])
        return self.start[lo:hi], self.end[lo:hi], self.theme[lo:hi]

    def counts(self) -> np.ndarray:
        """The number of segments of every protein."""
        return np.diff(self.offsets)


def _kept_rows(md5s: list[str], wanted: set[str] | None) -> np.ndarray:
    if wanted is None:
        return np.arange(len(md5s))
    return np.flatnonzero(np.fromiter((m in wanted for m in md5s), bool, len(md5s)))


def _ranges(starts: np.ndarray, lengths: np.ndarray) -> np.ndarray:
    """``starts[i] .. starts[i] + lengths[i]`` for every i, concatenated."""
    first = np.zeros(len(lengths), dtype=np.int64)
    np.cumsum(lengths[:-1], out=first[1:])
    return np.repeat(starts - first, lengths) + np.arange(int(lengths.sum()), dtype=np.int64)


def _pieces(
    cut: LexiconCut, shards: list[int], wanted: set[str] | None
) -> Iterator[tuple[list[str], np.ndarray, Segments]]:
    """Each shard's kept rows: md5s, their segment counts and their segments."""
    for shard in shards:
        md5s, offsets, (start, end, theme) = shard_segments(cut, shard)
        rows = _kept_rows(md5s, wanted)
        counts = np.diff(offsets)[rows]
        take = _ranges(offsets[rows], counts)
        yield [md5s[r] for r in rows], counts, (start[take], end[take], theme[take])


def read_corpus(
    cut: LexiconCut,
    shards: list[int],
    wanted: set[str] | None = None,
    say: Callable[[str], None] = lambda _: None,
) -> Corpus:
    """The proteins of ``shards`` (all of them, or the md5s ``wanted``), in the bank's order."""
    md5: list[str] = []
    counts, starts, ends, themes = [], [], [], []
    for shard_md5, shard_counts, (start, end, theme) in _pieces(cut, shards, wanted):
        md5 += shard_md5
        counts.append(shard_counts)
        starts.append(start)
        ends.append(end)
        themes.append(theme)
        if len(counts) % 16 == 0 or len(counts) == len(shards):
            say(f"cut shards read: {len(counts)}/{len(shards)}, proteins {len(md5):,}")
    offsets = np.zeros(len(md5) + 1, dtype=np.int64)
    if counts:
        np.cumsum(np.concatenate(counts), out=offsets[1:])

    def joined(parts: list[np.ndarray]) -> np.ndarray:
        return np.concatenate(parts) if parts else np.zeros(0, np.int64)

    return Corpus(md5, offsets, joined(starts), joined(ends), joined(themes))

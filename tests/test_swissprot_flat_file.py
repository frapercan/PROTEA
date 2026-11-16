"""The UniProt flat-file reader the photo's TrEMBL pass reads a past release with.

UniProt publishes a past release's TrEMBL only as ``uniprot_trembl.dat.gz`` inside
its knowledgebase archive (the 2022_03 archive holds the two ``.dat`` files and the
docs, no FASTA), so the reader has to give, for the same sequence, the md5 the FASTA
reader gives, whatever the chunk boundaries of the stream.
"""

from __future__ import annotations

import io

import pytest

from protea.core import swissprot_table
from protea.core.swissprot_table import iter_flat_file_sequences, iter_swissprot_records

DAT = b"""ID   Q9_HUMAN                Unreviewed;        12 AA.
AC   Q9; Q8;
DE   SubName: Full=A protein; see https://example.org//x
SQ   SEQUENCE   12 AA;  1300 MW;  0 CRC64;
     MQQLKVWTRA YS
//
ID   X7_HUMAN                Unreviewed;         3 AA.
AC   X7;
SQ   SEQUENCE   3 AA;  300 MW;  0 CRC64;
     MAA
//
ID   N1_MOUSE                Unreviewed;        13 AA.
AC   N1;
SQ   SEQUENCE   13 AA;  1400 MW;  0 CRC64;
     MNNPQRSTVW YAC
//"""


@pytest.mark.parametrize("chunk", [5, 64, 1 << 26])
def test_the_wanted_entries_whatever_the_chunks(
    chunk: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(swissprot_table, "_FLAT_FILE_CHUNK", chunk)
    got = list(iter_flat_file_sequences(io.BytesIO(DAT), frozenset({b"Q9", b"N1"})))
    assert [(a, s) for a, _, s in got] == [("Q9", "MQQLKVWTRAYS"), ("N1", "MNNPQRSTVWYAC")]


def test_the_md5_is_the_one_the_fasta_reader_gives() -> None:
    flat = list(iter_flat_file_sequences(io.BytesIO(DAT), frozenset({b"Q9", b"N1"})))
    fasta = [b">tr|Q9|Q9_HUMAN\n", b"MQQLKVWTRA\n", b"YS\n", b">tr|N1|N1_MOUSE\n"]
    fasta += [b"MNNPQRSTVWYAC\n"]
    assert flat == list(iter_swissprot_records(fasta))


def test_an_entry_is_named_by_its_first_accession_only() -> None:
    # Q8 is a secondary accession of Q9's entry: asked for, it is not found.
    assert list(iter_flat_file_sequences(io.BytesIO(DAT), frozenset({b"Q8"}))) == []


def test_a_wanted_entry_with_no_sequence_refuses() -> None:
    broken = b"ID   Z1_HUMAN   Unreviewed;   0 AA.\nAC   Z1;\n//\n"
    with pytest.raises(ValueError, match="Z1 has no sequence"):
        list(iter_flat_file_sequences(io.BytesIO(broken), frozenset({b"Z1"})))

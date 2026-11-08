"""The checks between an exam population's files, each about one disagreement.

WHY THESE EXIST AND THE DIGESTS DO NOT COVER THEM. An exam population is several
files. Each one can be perfectly intact, with a digest the manifest states
correctly, and describe a DIFFERENT population from the one beside it. A digest
is about one file's bytes and has nothing to say about two files.

THE CONVENTION TEST IS THE ONE WITH A HISTORY. The first manifest written for
these populations carried ``positions_are: indices, base 0`` at its root. It was
true of six selenocysteine indices in the laboratory's checks and false of every
label file, which counts residues from 1 the way Swiss-Prot's FT lines do. A
ported exam that believed it would have shifted every site and every domain by
one residue, and no digest, no row count and no test then written would have
noticed. So the convention is verified against the data, not read.
"""

from __future__ import annotations

import hashlib
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from protea.core.operations import _exam_population_checks as C

ACC = ["A0A001", "A0A002", "B0B001"]
MITAD = {"A0A001": "selection", "A0A002": "confirmation", "B0B001": "selection"}
SEQ = {"A0A001": "MKV" * 40, "A0A002": "MQQ" * 30, "B0B001": "MWW" * 50}
MD5 = {a: hashlib.md5(s.encode()).hexdigest() for a, s in SEQ.items()}


def _parquet(**columnas) -> bytes:
    buf = io.BytesIO()
    pq.write_table(pa.table(columnas), buf)
    return buf.getvalue()


def _sites(accs=None, pos=None) -> bytes:
    accs = accs or ["A0A001", "A0A001", "B0B001"]
    pos = pos or [1, 7, 12]
    return _parquet(accession=accs, position=pos, concept=["c"] * len(accs))


def _instances(accs=None, start=None, end=None) -> bytes:
    accs = accs or ["A0A001", "B0B001"]
    start = start or [1, 10]
    end = end or [20, 40]
    return _parquet(accession=accs, start=start, end=end, concept=["d"] * len(accs))


def _proteins(seqs=None) -> bytes:
    seqs = seqs or SEQ
    return _parquet(
        accession=list(seqs), sequence=list(seqs.values()), length=[len(s) for s in seqs.values()]
    )


def _split(halves=None) -> bytes:
    halves = halves or MITAD
    return _parquet(accession=list(halves), half=list(halves.values()))


def _concepts(roles=("selection", "selection", "reserved")) -> bytes:
    return json.dumps(
        {"concepts": {f"c{i}": {"role": r, "proteins": 30} for i, r in enumerate(roles)}}
    ).encode()


# ----------------------------------------------------- la convencion se comprueba


def test_a_position_below_one_is_refused_because_the_convention_says_from_one():
    """A zero means the file is indexed and the manifest says it is numbered."""
    with pytest.raises(ValueError, match="position < 1"):
        C.positions_count_from_one("sitios", {"sites.parquet": _sites(pos=[0, 7, 12])})


def test_a_start_above_its_end_is_refused():
    with pytest.raises(ValueError, match="1 <= start <= end"):
        C.positions_count_from_one(
            "dominios", {"instances.parquet": _instances(start=[30, 10], end=[20, 40])}
        )


def test_an_instance_past_the_exams_own_sequence_is_refused():
    """Against the exam's OWN sequence this cannot be a sequence that moved."""
    with pytest.raises(ValueError, match="past the length of the exam"):
        C.positions_count_from_one(
            "dominios",
            {
                "instances.parquet": _instances(start=[1], end=[9999], accs=["A0A001"]),
                "proteins.parquet": _proteins(),
            },
        )


def test_the_measured_convention_is_reported_and_not_only_checked():
    """The range measured, so a reader can see what the file actually holds."""
    medido = C.positions_count_from_one(
        "sitios", {"sites.parquet": _sites(pos=[1, 7, 12])}
    )
    assert medido["sites.position"] == {"rows": 3, "min": 1, "max": 12}


# -------------------------------------------------- los ficheros tienen que coincidir


def test_a_split_that_disagrees_about_a_half_is_refused():
    """One partition in two files, and nothing says which is the partition."""
    otras = dict(MITAD) | {"B0B001": "confirmation"}
    with pytest.raises(ValueError, match="in one half"):
        C.split_agrees_with_the_population("sitios", ACC, MITAD, _split(otras))


def test_a_split_naming_another_protein_is_refused():
    faltan = {k: v for k, v in MITAD.items() if k != "B0B001"}
    with pytest.raises(ValueError, match="different proteins"):
        C.split_agrees_with_the_population("sitios", ACC, MITAD, _split(faltan))


def test_labels_for_a_protein_outside_the_population_are_refused():
    """Labels for a non-member belong to a different population."""
    with pytest.raises(ValueError, match="not in the population"):
        C.labels_stay_inside_the_population(
            "sitios", ACC, _sites(accs=["A0A001", "ZZZZZZ", "B0B001"]), "sites.parquet"
        )


def test_the_labelled_protein_count_is_returned():
    n = C.labels_stay_inside_the_population("sitios", ACC, _sites(), "sites.parquet")
    assert n == 2


def test_a_sequence_that_does_not_hash_to_its_declared_md5_is_refused():
    """The check that closes the loop on the pairs.

    Without it the tsv could declare any digest and the sequence file beside it
    would not contradict it, with both file digests intact.
    """
    movida = dict(SEQ) | {"A0A002": "MQQ" * 31}
    with pytest.raises(ValueError, match="hash to something other"):
        C.the_sequences_are_the_ones_the_md5_names("dominios", MD5, _proteins(movida))


def test_matching_sequences_pass():
    C.the_sequences_are_the_ones_the_md5_names("dominios", MD5, _proteins())


# ------------------------------------------------- los roles se cuentan, no se copian


def test_the_roles_are_counted_from_the_concepts_file():
    assert C.counted_roles("sitios", _concepts()) == {"selection": 2, "reserved": 1}


def test_the_roles_are_found_under_either_nesting():
    """The two exams nest them differently, which is why it is looked for.

    Sites holds the per-concept mapping under ``concepts``; domains holds an
    aggregate there and the mapping under ``concepts_detail``.
    """
    como_dominios = json.dumps(
        {
            "concepts": 120,
            "roles": {"selection": 71, "reserved": 40},
            "concepts_detail": {"d1": {"role": "selection"}, "d2": {"role": "reserved"}},
        }
    ).encode()
    assert C.counted_roles("dominios", como_dominios) == {"selection": 1, "reserved": 1}


def test_a_concepts_file_with_no_per_concept_role_is_refused():
    """An aggregate alone is a claim, and the row's description shows it."""
    solo_agregado = json.dumps({"concepts": 120, "roles": {"selection": 71}}).encode()
    with pytest.raises(ValueError, match="would have to be believed"):
        C.counted_roles("dominios", solo_agregado)


# --------------------------------------------- la medida contra esta plataforma


def test_labels_past_this_platforms_sequence_are_measured_and_not_refused():
    """What the md5 exclusion protects against, as a figure.

    Measured on the real populations: 38 site rows and 8 domain instances fall
    outside, and every one belongs to a protein whose md5 differs. The converse
    does not hold, which is why this is a measure and the md5 is the rule.
    """
    medida = C.labels_outside_this_platform(
        _sites(accs=["A0A001", "A0A001", "B0B001"], pos=[1, 7, 999]),
        ("position",),
        {"A0A001": 120, "B0B001": 150},
    )
    assert medida == {"rows": 1, "accessions": ["B0B001"]}


def test_an_accession_this_platform_does_not_hold_is_not_counted_as_outside():
    """Absent is not outside, and the two have different causes."""
    medida = C.labels_outside_this_platform(
        _sites(accs=["A0A001"], pos=[999]), ("position",), {}
    )
    assert medida == {"rows": 0, "accessions": []}


# ------------------------------- una clave repetida no se traga en silencio


def test_a_split_naming_one_protein_twice_is_refused_and_not_overwritten():
    """The error this module exists to catch, committed while catching it.

    The first version built the split's halves with ``dict(zip(...))``, which
    keeps the LAST value for a repeated key. A split naming one protein twice,
    in two different halves, would then have produced a lookup that agreed with
    the tsv and a partition nobody could reproduce. ``check_row_alignment``
    refused the expression before any of this ran.
    """
    repetido = _parquet(
        accession=["A0A001", "A0A002", "B0B001", "B0B001"],
        half=["selection", "confirmation", "confirmation", "selection"],
    )
    with pytest.raises(Exception, match="B0B001|unique|duplicate"):
        C.split_agrees_with_the_population("sitios", ACC, MITAD, repetido)


def test_a_proteins_file_naming_one_accession_twice_is_refused():
    """Same hole on the other side: two sequences for one accession."""
    repetido = _parquet(
        accession=["A0A001", "A0A001"],
        sequence=[SEQ["A0A001"], SEQ["A0A001"] + "M"],
        length=[len(SEQ["A0A001"]), len(SEQ["A0A001"]) + 1],
    )
    with pytest.raises(Exception, match="A0A001|unique|duplicate"):
        C.positions_count_from_one(
            "dominios",
            {
                "instances.parquet": _instances(accs=["A0A001"], start=[1], end=[20]),
                "proteins.parquet": repetido,
            },
        )

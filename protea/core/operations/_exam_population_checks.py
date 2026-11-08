"""The checks an exam population must pass between its files, before it is declared.

A digest says the bytes are intact. It says nothing about whether the files
AGREE, and an exam population is several files that can each be perfectly intact
and describe different populations. These are the checks for that, and they are
here rather than in the operation so that each one is about one disagreement.

WHY THE POSITION CONVENTION IS CHECKED AND NOT READ. The first manifest written
for these populations said ``positions_are: indices, base 0`` at its root. That
was true of six selenocysteine indices in the laboratory's own checks and false
of every label file, which counts residues from 1 the way Swiss-Prot's FT lines
do. A ported exam that believed the root key would have shifted every site and
every domain by one residue, and nothing would have failed. So the convention is
now stated per file and per column in the manifest AND verified here against the
data, because a convention written by hand is a claim.

Measured on 2026-11-08, over the delivered populations:

* ``sites.parquet``, 1.986.242 rows: ``position`` from 1 to 7924, zero at 0;
* ``instances.parquet``, 10.070 rows: every one satisfies
  ``1 <= start <= end <= length`` against ``proteins.parquet``.

AND WHY THE UPPER BOUND IS NOT CHECKED AGAINST THIS PLATFORM. It would fail, and
correctly: 38 site rows and 8 domain instances fall outside the sequence stored
here, and every one of them belongs to an accession whose md5 differs. The
converse does not hold, which is the measurement that decides the design: **8 of
the 10 site mismatches and 35 of the 40 domain mismatches have labels that still
FIT**. A range check catches 7 of the 50 proteins whose sequence moved; the md5
catches all 50. So the range against this platform is reported as a measure and
the md5 is what the exam excludes by.
"""

from __future__ import annotations

import io
import json
from typing import Any

import pyarrow.parquet as pq
from protea_contracts import compute_sequence_hash

from protea.core.row_alignment import lookup_by


def _table(blob: bytes) -> dict[str, list[Any]]:
    return pq.read_table(io.BytesIO(blob)).to_pydict()


def positions_count_from_one(exam: str, files: dict[str, bytes]) -> dict[str, Any]:
    """The intrinsic half of the convention: nothing is numbered from zero.

    Intrinsic means it needs no sequence: a position of 0 or a start above its
    end is wrong whatever the protein is. The upper bound is checked only where
    the exam's own sequence is in the package, which is the domain exam.
    """
    medido: dict[str, Any] = {}
    if "sites.parquet" in files:
        pos = _table(files["sites.parquet"])["position"]
        bajo = sum(1 for p in pos if p < 1)
        if bajo:
            raise ValueError(
                f"{exam}: {bajo} rows of sites.parquet have position < 1, and the manifest "
                "declares residue numbers counted from 1. A zero means the file is "
                "indexed and the convention is not"
            )
        medido["sites.position"] = {"rows": len(pos), "min": min(pos), "max": max(pos)}
    if "instances.parquet" in files:
        t = _table(files["instances.parquet"])
        malas = [
            (a, s, e) for a, s, e in zip(t["accession"], t["start"], t["end"], strict=True) if not 1 <= s <= e
        ]
        if malas:
            raise ValueError(
                f"{exam}: {len(malas)} instances do not satisfy 1 <= start <= end, the "
                f"first being {malas[:3]}. The manifest declares inclusive residue "
                "numbers counted from 1"
            )
        medido["instances.start_end"] = {
            "rows": len(t["start"]),
            "min_start": min(t["start"]),
            "max_end": max(t["end"]),
        }
        if "proteins.parquet" in files:
            p = _table(files["proteins.parquet"])
            filas_p = list(zip(p["accession"], p["sequence"], strict=True))
            largo = {
                a: len(s)
                for a, (_, s) in lookup_by(
                    filas_p, lambda r: r[0], context=f"{exam} proteins.parquet"
                ).items()
            }
            fuera = [
                (a, s, e)
                for a, s, e in zip(t["accession"], t["start"], t["end"], strict=True)
                if a in largo and e > largo[a]
            ]
            if fuera:
                raise ValueError(
                    f"{exam}: {len(fuera)} instances end past the length of the exam's own "
                    f"sequence, the first being {fuera[:3]}. Against the exam's own "
                    "sequences this cannot be a sequence that moved"
                )
            medido["instances_within_the_exam_sequence"] = True
    return medido


def split_agrees_with_the_population(
    exam: str, accessions: list[str], half_of: dict[str, str], split: bytes
) -> None:
    """The halves in the tsv and in the split file must be the same halves.

    Two files, one partition. If they disagree, one of them is the partition and
    nothing says which, so both become unusable.
    """
    t = _table(split)
    filas = list(zip(t["accession"], t["half"], strict=True))
    # NOT dict(zip(...)): that keeps the LAST value for a repeated key, so a
    # split naming one protein twice in two halves would pass the disagreement
    # check below. The error this function exists to catch, committed while
    # catching it.
    suyo = {k: v for k, (_, v) in lookup_by(filas, lambda r: r[0], context=f"{exam} split.parquet").items()}
    if set(suyo) != set(accessions):
        solo_split = sorted(set(suyo) - set(accessions))[:5]
        solo_tsv = sorted(set(accessions) - set(suyo))[:5]
        raise ValueError(
            f"{exam}: split.parquet and the population file name different proteins. "
            f"Only in the split: {solo_split}. Only in the tsv: {solo_tsv}"
        )
    distintas = sorted(a for a in accessions if suyo[a] != half_of[a])
    if distintas:
        raise ValueError(
            f"{exam}: {len(distintas)} proteins are in one half in the population file and "
            f"the other in split.parquet, the first being {distintas[:5]}"
        )


def labels_stay_inside_the_population(
    exam: str, accessions: list[str], labels: bytes, name: str
) -> int:
    """Every labelled protein has to be a member, or the labels are of another set."""
    t = _table(labels)
    fuera = sorted(set(t["accession"]) - set(accessions))
    if fuera:
        raise ValueError(
            f"{exam}: {len(fuera)} accessions in {name} are not in the population, the "
            f"first being {fuera[:5]}. Labels for a protein the set does not contain "
            "belong to a different population"
        )
    return len(set(t["accession"]))


def the_sequences_are_the_ones_the_md5_names(
    exam: str, md5_of: dict[str, str], proteins: bytes
) -> None:
    """``md5(proteins.parquet.sequence)`` must be the md5 the tsv declares, row by row.

    The one check that closes the loop on the pairs. Without it the tsv could
    declare any digest and the sequence file sitting beside it would not
    contradict it, with both digests intact.
    """
    t = _table(proteins)
    malas = []
    for acc, seq in zip(t["accession"], t["sequence"], strict=True):
        if acc not in md5_of:
            continue
        # THE PLATFORM'S OWN FUNCTION and not an md5 written again here: the
        # value being compared is what ``sequence.sequence_hash`` holds, so a
        # second implementation of the convention is a second place for it to
        # drift. It is also what keeps bandit quiet about a digest that is an
        # identity and not a security primitive.
        propio = compute_sequence_hash(seq)
        if propio != md5_of[acc]:
            malas.append((acc, md5_of[acc][:12], propio[:12]))
    if malas:
        raise ValueError(
            f"{exam}: {len(malas)} rows of proteins.parquet hash to something other than "
            f"the md5 the population file declares, the first being {malas[:3]} as "
            "(accession, declared, computed)"
        )


def counted_roles(exam: str, concepts: bytes) -> dict[str, int]:
    """The concepts per role, COUNTED from ``concepts.json`` and not copied.

    The row's description carries these numbers and the description is the
    visible identity of the population, so taking them from the manifest would
    put a claim on the surface of the platform. The two exams nest them
    differently, which is why the per-concept mapping is looked for rather than
    assumed: sites holds it under ``concepts``, domains under
    ``concepts_detail``, and ``roles`` in domains is already an aggregate.
    """
    data = json.loads(concepts.decode("utf-8"))
    for campo in ("concepts", "concepts_detail"):
        mapa = data.get(campo)
        if isinstance(mapa, dict) and mapa and all(
            isinstance(v, dict) and "role" in v for v in mapa.values()
        ):
            contado: dict[str, int] = {}
            for v in mapa.values():
                contado[str(v["role"])] = contado.get(str(v["role"]), 0) + 1
            return contado
    raise ValueError(
        f"{exam}: concepts.json has no per-concept mapping carrying a 'role', so the "
        "roles cannot be counted and would have to be believed"
    )


def labels_must_fit_where_the_sequence_agrees(
    exam: str,
    labels: bytes,
    column: tuple[str, ...],
    length_of: dict[str, int],
    agreeing: set[str],
) -> int:
    """Require the upper bound for the proteins the exam will actually read.

    WHERE THE MD5 AGREES, THE PLATFORM'S SEQUENCE **IS** THE EXAM'S. So a label
    past its length is not a sequence that moved: it is a convention written
    wrong or a corrupt file, and the declaration refuses rather than reports it.

    For the rest, the upper bound stays a measure
    (:func:`labels_outside_this_platform`), because a label outside a sequence
    that changed is exactly what the md5 exclusion is for. Measured on the
    delivered populations: every one of the 38 site rows and 8 domain instances
    that fall outside belongs to an accession whose md5 differs, so this
    requirement passes today and closes the hole for the next manifest.
    """
    t = _table(labels)
    fuera = []
    for row in zip(t["accession"], *(t[c] for c in column), strict=True):
        acc, valores = row[0], row[1:]
        n = length_of.get(acc)
        if acc in agreeing and n is not None and max(valores) > n:
            fuera.append((acc, max(valores), n))
    if fuera:
        raise ValueError(
            f"{exam}: {len(fuera)} labels fall past the sequence of a protein whose md5 "
            f"AGREES with this platform, the first being {fuera[:3]} as (accession, "
            "label, length). Where the md5 agrees the stored sequence is the exam's "
            "own, so this is not a sequence that moved: it is the position convention "
            "or the file"
        )
    return len(agreeing)


def labels_outside_this_platform(
    labels: bytes, column: tuple[str, ...], length_of: dict[str, int]
) -> dict[str, Any]:
    """How many labels fall past the sequence THIS platform holds. A measure.

    Not a refusal: every one of them belongs to a protein whose md5 differs, and
    the exam excludes those by the md5 anyway. Reported because it says what the
    exclusion is protecting against, and because the figure is about
    (population, platform) at a date.
    """
    t = _table(labels)
    fuera = []
    for row in zip(t["accession"], *(t[c] for c in column), strict=True):
        acc, valores = row[0], row[1:]
        n = length_of.get(acc)
        if n is not None and max(valores) > n:
            fuera.append(acc)
    return {"rows": len(fuera), "accessions": sorted(set(fuera))}

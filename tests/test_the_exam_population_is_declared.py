"""``declare_exam_population``: what it refuses, which is the whole point of it.

A declared set is declared, described and verifiable, and the third word is the
one that needs code. The package these populations arrive in comes from another
machine, so every number its manifest states is a claim until this operation
recomputes it.

THE CASE THAT MOTIVATES THE ROW COUNTS. On 2026-11-08 a vector file arrived whose
command-line label named one ontology snapshot while its contents were another
one's, and every digest matched, because a digest is of the bytes that were sent
and not of the bytes that were meant. So these tests pin that a manifest which
describes a DIFFERENT file than the one it points at is refused even when every
sha agrees.

AND THE CASE THAT MOTIVATES THE FLUSH TEST. ``ck_query_set_exam_is_declared``
requires the job for this kind, and its sibling constraint caught exactly this
defect on its first real run: the row was built, flushed, and given its
``job_id`` afterwards, so the INSERT went without it while the object looked
correct at the end of the function. A double has no CHECK, so the assertion is
on the value AT the flush.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

import pytest

from protea.core._artifact_identity import ArtifactMismatch
from protea.core.operations import declare_exam_population as D

ACC = ["A0A001", "A0A002", "B0B001", "C0C001"]
MD5 = {a: hashlib.md5(a.encode()).hexdigest() for a in ACC}
MITAD = {"A0A001": "selection", "A0A002": "confirmation", "B0B001": "selection", "C0C001": "confirmation"}


def _tsv(accs: list[str] = ACC, md5: dict[str, str] | None = None) -> bytes:
    md5 = md5 or MD5
    lineas = ["\t".join(D.COLUMNS)]
    lineas += [f"{a}\t{md5[a]}\t{MITAD[a]}" for a in accs]
    return ("\n".join(lineas) + "\n").encode("utf-8")


def _manifiesto(poblacion: bytes, **cambios: Any) -> dict[str, Any]:
    m = {
        "exam": "sitios",
        "lab_commit": "73f8791282a878d13ec48d59f836755c6fef9319",
        "partition": {"selection": 2, "confirmation": 2},
        "concepts": {"selection": 79, "reserved": 42},
        "population_file": "poblacion.tsv",
        "files": {
            "poblacion.tsv": {
                "key": "exam-population/sitios/poblacion-aaaaaaaaaaaa.tsv",
                "sha256": hashlib.sha256(poblacion).hexdigest(),
                "bytes": len(poblacion),
                "rows": len(ACC),
            }
        },
    }
    m.update(cambios)
    return m


class _Store:
    """An artifact store of bytes, with ``url`` and nothing else."""

    def __init__(self, objetos: dict[str, bytes]) -> None:
        self._o = objetos

    def get(self, key: str) -> bytes:
        if key not in self._o:
            raise KeyError(key)
        return self._o[key]

    def url(self, key: str) -> str:
        return f"s3://protea/{key}"


def _armado(poblacion: bytes | None = None, **cambios: Any) -> tuple[_Store, str, str]:
    """A store holding a coherent manifest and its population, and its digest."""
    poblacion = poblacion if poblacion is not None else _tsv()
    man = _manifiesto(poblacion, **cambios)
    crudo = json.dumps(man).encode("utf-8")
    sha = hashlib.sha256(crudo).hexdigest()
    clave = f"exam-population/sitios/manifest-{sha[:12]}.json"
    objetos = {clave: crudo, man["files"]["poblacion.tsv"]["key"]: poblacion}
    return _Store(objetos), clave, sha


def _payload(clave: str, sha: str) -> D.DeclareExamPopulationPayload:
    return D.DeclareExamPopulationPayload(manifest_key=clave, manifest_sha256=sha)


# ------------------------------------------------- el payload es una identidad


def test_a_sha_prefix_is_refused_where_a_digest_is_meant():
    """Eight characters of a sha are a label, and a label agreed twice."""
    with pytest.raises(ValueError, match="64 hexadecimal"):
        D.DeclareExamPopulationPayload(manifest_key="k.json", manifest_sha256="524f1133208d")


def test_a_key_cannot_climb_out_of_the_store():
    """A mistake and an attempt are indistinguishable from here, so both go."""
    with pytest.raises(ValueError, match=r"cannot contain"):
        D.DeclareExamPopulationPayload(manifest_key="../../etc/passwd", manifest_sha256="a" * 64)


# --------------------------------------------- el manifiesto se verifica entero


def test_the_manifest_digest_is_checked_against_the_payload():
    """The bytes under the key have to be the bytes the caller declared.

    It raises ``ArtifactMismatch`` and not a ``ValueError``, which is this
    project's own class and deliberately not catchable as an input error: a
    swapped artefact has to stop the run and not be handled as a bad field.
    """
    store, clave, _ = _armado()
    with pytest.raises(ArtifactMismatch):
        D._manifest(_payload(clave, "b" * 64), store)


def test_a_manifest_that_does_not_list_its_own_population_file_is_refused():
    """Naming a population file and not listing it is a manifest about nothing."""
    store, clave, sha = _armado(population_file="otro.tsv")
    with pytest.raises(ValueError, match="among its files"):
        D._manifest(_payload(clave, sha), store)


@pytest.mark.parametrize("falta", ["exam", "files", "partition", "concepts", "population_file"])
def test_a_manifest_missing_a_required_field_is_refused(falta):
    poblacion = _tsv()
    man = _manifiesto(poblacion)
    del man[falta]
    crudo = json.dumps(man).encode("utf-8")
    sha = hashlib.sha256(crudo).hexdigest()
    store = _Store({"k.json": crudo, man.get("files", {}).get("poblacion.tsv", {}).get("key", "x"): poblacion})
    with pytest.raises(ValueError, match=f"'{falta}'"):
        D._manifest(_payload("k.json", sha), store)


def test_a_file_whose_row_count_is_not_what_the_manifest_claims_is_refused():
    """The case a digest cannot catch, and the one that happened.

    Every sha agrees here: the manifest is intact, the file is intact, and the
    manifest describes a file with 99 rows while the one it points at has 4.
    """
    poblacion = _tsv()
    store, clave, sha = _armado(poblacion)
    man = json.loads(store.get(clave))
    man["files"]["poblacion.tsv"]["rows"] = 99
    crudo = json.dumps(man).encode("utf-8")
    nueva = hashlib.sha256(crudo).hexdigest()
    store._o[f"k-{nueva[:8]}.json"] = crudo
    m = D._manifest(_payload(f"k-{nueva[:8]}.json", nueva), store)
    with pytest.raises(ValueError, match="holds 4 rows and the manifest claims 99"):
        D._verify_every_file(m, store)


def test_a_file_whose_size_is_not_what_the_manifest_claims_is_refused():
    """Two things that cannot both be true, said in those words."""
    poblacion = _tsv()
    store, clave, sha = _armado(poblacion)
    man = json.loads(store.get(clave))
    man["files"]["poblacion.tsv"]["bytes"] = 1
    crudo = json.dumps(man).encode("utf-8")
    nueva = hashlib.sha256(crudo).hexdigest()
    store._o["k2.json"] = crudo
    m = D._manifest(_payload("k2.json", nueva), store)
    with pytest.raises(ValueError, match="cannot both be true"):
        D._verify_every_file(m, store)


# ------------------------------------------------- la poblacion se lee con sus reglas


def test_the_population_is_parsed_with_its_pairs_and_halves():
    pop = D._parse_population(_tsv(), "sitios")
    assert pop.accessions == ACC
    assert pop.md5_of["B0B001"] == MD5["B0B001"]
    assert pop.half_of["B0B001"] == "selection"


def test_columns_in_another_order_are_refused():
    """Same three columns, swapped: md5 would be read as a half and refuse nothing."""
    crudo = b"accession\thalf\tmd5\nA0A001\tselection\tx\n"
    with pytest.raises(ValueError, match="expected"):
        D._parse_population(crudo, "sitios")


def test_an_unsorted_population_is_refused_because_the_digest_is_of_an_order():
    with pytest.raises(ValueError, match="not sorted"):
        D._parse_population(_tsv(["B0B001", "A0A001", "C0C001", "A0A002"]), "sitios")


def test_a_repeated_accession_is_not_a_population():
    crudo = _tsv() + f"C0C001\t{MD5['C0C001']}\tselection\n".encode()
    with pytest.raises(ValueError, match="twice"):
        D._parse_population(crudo, "sitios")


def test_a_half_outside_the_vocabulary_is_refused():
    crudo = b"accession\tmd5\thalf\nA0A001\tdeadbeef\ttraining\n"
    with pytest.raises(ValueError, match="is not one of"):
        D._parse_population(crudo, "sitios")


def test_the_partition_counted_must_be_the_partition_claimed():
    """Two different ways to be wrong, checked apart: the file and the claim."""
    poblacion = _tsv()
    store, clave, sha = _armado(poblacion, partition={"selection": 3, "confirmation": 1})
    m = D._manifest(_payload(clave, sha), store)
    pop = D._parse_population(poblacion, "sitios")
    with pytest.raises(ValueError, match="claims"):
        D._check_partition(m, pop)


# ------------------------------------------------------- la fila y su medida


class _SesionQueRecuerda:
    """A session that snapshots the row's fields AT the flush, not after.

    The defect this guards against left the object correct at the end of the
    function while the INSERT had already gone without the job, so asserting on
    the object afterwards cannot see it.
    """

    def __init__(self) -> None:
        self.en_el_flush: dict[str, Any] = {}
        self.anadidas: list[Any] = []

    def add(self, row: Any) -> None:
        self.anadidas.append(row)

    def add_all(self, rows: Any) -> None:
        self.anadidas.extend(rows)

    def flush(self) -> None:
        row = self.anadidas[0]
        row.id = uuid.uuid4()
        self.en_el_flush = {
            "job_id": row.job_id,
            "source_kind": row.source_kind,
            "content_sha256": row.content_sha256,
            "artifact_uri": row.artifact_uri,
            "description": row.description,
            "evaluation_set_id": getattr(row, "evaluation_set_id", None),
        }


def _m_y_pop():
    poblacion = _tsv()
    store, clave, sha = _armado(poblacion)
    return D._manifest(_payload(clave, sha), store), D._parse_population(poblacion, "sitios")


def test_the_row_reaches_its_flush_already_carrying_its_job():
    """What ``ck_query_set_exam_is_declared`` requires, asserted where it bites."""
    m, pop = _m_y_pop()
    got = D._Resolved(sequence_of={a: i for i, a in enumerate(ACC, 1)}, hash_of=MD5, missing=[])
    job = uuid.uuid4()
    s = _SesionQueRecuerda()

    D._declare(s, m, pop, got, job)

    assert s.en_el_flush["job_id"] == job
    assert s.en_el_flush["source_kind"] == "exam_population"
    assert s.en_el_flush["content_sha256"] == m.digest
    assert s.en_el_flush["artifact_uri"].startswith("s3://")
    assert s.en_el_flush["description"]
    assert s.en_el_flush["evaluation_set_id"] is None


def test_the_description_carries_the_partition_and_the_concepts():
    """The identity of an exam population, readable without a download.

    The same proteins split another way are a different population, and the
    reserved concepts are the ones nobody has looked at.
    """
    m, pop = _m_y_pop()
    texto = D._described(m, pop)
    assert "selection 2" in texto and "confirmation 2" in texto
    assert "reserved 42" in texto
    assert "NO evaluation window" in texto
    assert "73f8791282a8" in texto


def test_the_md5_agreement_is_reported_with_the_accessions_that_differ():
    """A measure of (population, platform) at a date, emitted and never stored."""
    m, pop = _m_y_pop()
    movidas = dict(MD5)
    movidas["C0C001"] = "f" * 32
    got = D._Resolved(
        sequence_of={a: i for i, a in enumerate(ACC, 1)}, hash_of=movidas, missing=[]
    )

    medida = D._agreement(pop, got)

    assert medida == {
        "compared": 4,
        "md5_agrees_with_this_platform": 3,
        "md5_differs": 1,
        "differing_accessions": ["C0C001"],
    }

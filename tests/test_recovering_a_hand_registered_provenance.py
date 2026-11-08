"""Recovering a query set's derivation, by verifying it and never by reading it.

WHAT IS BEING REPAIRED. Four query sets were registered through the FASTA
upload endpoint by a person, with the provenance typed into the description:
the window as an id prefix, the job as a word, the sha as a string nothing
compares against. Migration ``c5e8d1a30b72`` gave the table columns to hold
that and deliberately declared NOTHING.

SO THE SUBJECT OF THIS FILE IS THE DIFFERENCE BETWEEN EVIDENCE AND
TRANSCRIPTION. Reading the window out of the description would be believing a
sentence. Three things have to agree instead: the published target count, the
digest of the bytes, and the accession SET in them. The count alone is a
coincidence waiting to happen, which is why
``test_a_count_that_matches_is_not_enough`` is the test this file exists for.
"""

from __future__ import annotations

import hashlib
import uuid

import pytest

from protea.core.operations.declare_query_set_provenance import (
    DeclareQuerySetProvenanceOperation,
    DeclareQuerySetProvenancePayload,
    _accessions_in,
    _Candidate,
    _collapse,
)
from protea.core.operations.export_evaluation_targets import targets_key_for

VENTANA = "ea0f4334-83fc-4848-93a4-defaa3f7dbbe"
OTRA_VENTANA = "ea94e57e-1111-4848-93a4-defaa3f7dbbe"
FASTA = b">P1\nMKV\n>P2\nMKA\n"
SHA = hashlib.sha256(FASTA).hexdigest()
OTRO_FASTA = b">P3\nMKV\n>P4\nMKA\n"
OTRO_SHA = hashlib.sha256(OTRO_FASTA).hexdigest()


class _Qs:
    def __init__(self, nombre="dianas-valid-211-227", descripcion=None) -> None:
        self.id = uuid.uuid4()
        self.name = nombre
        self.description = descripcion
        self.source_kind = "legacy_undeclared"
        self.evaluation_set_id = None
        self.artifact_uri = None
        self.content_sha256 = None
        self.job_id = None
        self.code_revision = None


class _Session:
    def __init__(self, qs, accesiones, exports) -> None:
        self.qs, self.accesiones, self.exports = qs, accesiones, exports

    def get(self, _model, _ident):
        return self.qs

    def execute(self, statement):
        sql = " ".join(str(statement).split())
        if "export_evaluation_targets" in sql:
            return [_Fila(c) for c in self.exports]
        return _Rows([(a,) for a in self.accesiones])


class _Fila:
    def __init__(self, cand: _Candidate) -> None:
        self._mapping = cand._asdict()


class _Rows:
    def __init__(self, rows) -> None:
        self._rows = rows

    def all(self):
        return self._rows


class _Store:
    """Serves the bytes whose key matches, and records what was asked for."""

    def __init__(self, objetos: dict[str, bytes]) -> None:
        self.objetos = objetos
        self.pedidos: list[str] = []

    def get(self, key: str) -> bytes:
        self.pedidos.append(key)
        if key not in self.objetos:
            raise OSError(f"no such key {key}")
        return self.objetos[key]


def _cand(window=VENTANA, digest=SHA, targets=2, job=None):
    return _Candidate(
        job_id=job or str(uuid.uuid4()),
        window=window,
        output_name="targets",
        digest=digest,
        targets=targets,
        code_revision="beafbd4f2e4141d59fa89e7587ab74355191b955",
    )


def _run(session, store, dry=False):
    import protea.core.operations.declare_query_set_provenance as M

    eventos: list[tuple[str, dict]] = []
    orig_store = M.get_artifact_store
    orig_settings = M.load_settings
    M.get_artifact_store = lambda _s: store
    M.load_settings = lambda _r: object()
    try:
        out = DeclareQuerySetProvenanceOperation().execute(
            session,
            {"query_set_ids": [str(session.qs.id)], "dry_run": dry},
            emit=lambda e, _m, f, _l: eventos.append((e, f)) or f,
        )
    finally:
        M.get_artifact_store = orig_store
        M.load_settings = orig_settings
    return out.result, eventos


# --------------------------------------------------------------------- lo barato


def test_the_headers_are_bare_accessions_and_that_is_what_makes_it_comparable():
    assert _accessions_in(b">P1 some text\nMKV\n>P2\nMKA\n") == {"P1", "P2"}
    assert _accessions_in(b"") == set()


def test_the_same_bytes_exported_twice_are_one_export():
    """Two jobs with the same window and digest ran the same export.

    ``dianas-valid-211-227`` really has two such jobs, which is why this is not
    hypothetical: without collapsing them the set would look ambiguous and be
    left undeclared for no reason.
    """
    a, b = _cand(job="aaa"), _cand(job="bbb")

    assert [c.job_id for c in _collapse([a, b])] == ["aaa"]
    assert len(_collapse([a, _cand(window=OTRA_VENTANA, job="ccc")])) == 2


# ------------------------------------------------------------- lo que de verdad importa


def test_a_count_that_matches_is_not_enough(tmp_path):
    """THE TEST THIS FILE EXISTS FOR.

    An export of two OTHER proteins has the same target count, the same shape
    and a valid digest. Only the accession set tells them apart, and declaring
    on the count would have attributed this population to the wrong window.
    """
    qs = _Qs()
    otro = _cand(window=OTRA_VENTANA, digest=OTRO_SHA)
    store = _Store({targets_key_for(OTRA_VENTANA, "targets", OTRO_SHA): OTRO_FASTA})
    session = _Session(qs, {"P1", "P2"}, [otro])

    result, eventos = _run(session, store)

    assert result["declared"] == 0
    assert result["left_undeclared"] == 1
    assert "no export's bytes hold exactly these accessions" in result["undeclared"][0]["why_not"]
    assert qs.source_kind == "legacy_undeclared"
    # Y se MIRO: el candidato se trajo y se comparo, no se descarto por el nombre.
    assert store.pedidos == [targets_key_for(OTRA_VENTANA, "targets", OTRO_SHA)]


def test_a_verified_set_is_declared_with_what_was_checked(tmp_path):
    qs = _Qs()
    cand = _cand(job="9bebda52-0000-4000-8000-000000000000")
    store = _Store({targets_key_for(VENTANA, "targets", SHA): FASTA})
    session = _Session(qs, {"P1", "P2"}, [cand])

    result, eventos = _run(session, store)

    assert result["declared"] == 1
    assert qs.source_kind == "evaluation_targets"
    assert str(qs.evaluation_set_id) == VENTANA
    assert qs.content_sha256 == SHA
    assert str(qs.job_id) == cand.job_id
    assert qs.code_revision == cand.code_revision
    # La descripcion dice que se COMPROBO, no a que conclusion se llego.
    assert "matched these entries exactly" in qs.description
    assert [e for e, _ in eventos][0] == "declare_query_set_provenance.declared"


def test_two_different_exports_that_both_verify_are_an_ambiguity_it_refuses():
    """Resolving it by picking one would be the defect, not the fix."""
    qs = _Qs()
    a = _cand(job="aaa")
    b = _cand(window=OTRA_VENTANA, digest=SHA, job="bbb")
    store = _Store(
        {
            targets_key_for(VENTANA, "targets", SHA): FASTA,
            targets_key_for(OTRA_VENTANA, "targets", SHA): FASTA,
        }
    )
    session = _Session(qs, {"P1", "P2"}, [a, b])

    result, _ = _run(session, store)

    assert result["declared"] == 0
    assert "more than one" in result["undeclared"][0]["why_not"]
    assert qs.source_kind == "legacy_undeclared"


def test_a_digest_that_does_not_match_the_bytes_is_not_a_match():
    """``fetch_and_verify`` is the door, so a swapped artefact cannot pass."""
    qs = _Qs()
    store = _Store({targets_key_for(VENTANA, "targets", SHA): b">P1\nMKV\n>P2\nMKZ\n"})
    session = _Session(qs, {"P1", "P2"}, [_cand()])

    result, _ = _run(session, store)

    assert result["declared"] == 0
    assert qs.source_kind == "legacy_undeclared"


def test_the_dry_run_writes_nothing_and_still_says_what_it_would_do():
    qs = _Qs()
    store = _Store({targets_key_for(VENTANA, "targets", SHA): FASTA})
    session = _Session(qs, {"P1", "P2"}, [_cand()])

    result, eventos = _run(session, store, dry=True)

    assert result["declared"] == 1
    assert result["dry_run"] is True
    assert qs.source_kind == "legacy_undeclared"
    assert qs.evaluation_set_id is None
    assert [e for e, _ in eventos][0] == "declare_query_set_provenance.would_declare"


def test_a_description_somebody_wrote_is_kept():
    """It may say something the verification cannot.

    Overwriting a human sentence with a generated one loses information to
    gain tidiness.
    """
    qs = _Qs(descripcion="Las 40.531 dianas de la ventana ea0f4334, a mano")
    store = _Store({targets_key_for(VENTANA, "targets", SHA): FASTA})
    session = _Session(qs, {"P1", "P2"}, [_cand()])

    _run(session, store)

    assert qs.description == "Las 40.531 dianas de la ventana ea0f4334, a mano"
    assert qs.source_kind == "evaluation_targets"


def test_there_is_no_all_the_undeclared_ones_mode():
    with pytest.raises(ValueError, match="cannot be empty"):
        DeclareQuerySetProvenancePayload(query_set_ids=[])

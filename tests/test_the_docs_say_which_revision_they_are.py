"""``/health`` says which revision the SERVED documentation was built from.

WHY THIS TEST EXISTS. On 2026-11-10 this instance served code revision
``9f09e2c`` and documentation built on October 7th: five weeks and eight
architecture decision records apart, with nothing anywhere saying so. The API
reported ``revision_matches_declaration: true`` the whole time, truthfully,
because that field is about the code. The gap was invisible because no field
was about the docs.

So the three cases below are the ones that matter: a stamp that agrees, a stamp
that does not, and no stamp at all. The third is every build made before
``scripts/deploy-docs.sh`` existed, and it must report ``None`` rather than
``False``: "nobody wrote it down" is not the same statement as "they differ",
and a health check that conflates them teaches its reader to ignore it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from protea.api.app import _docs_revision

SHA = "9f09e2c396249d31c5dd887433b9d4bf3effbf51"
SELLO = Path("docs") / "build" / "html" / "_static" / "protea-revision.txt"


def _sellar(raiz: Path, contenido: str) -> None:
    destino = raiz / SELLO
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(contenido, encoding="utf-8")


def test_the_stamp_is_read_from_the_served_build(tmp_path: Path) -> None:
    _sellar(tmp_path, SHA + "\n")
    assert _docs_revision(tmp_path) == SHA


def test_a_build_with_no_stamp_is_unknown_and_not_a_mismatch(tmp_path: Path) -> None:
    """The month-old build this was written for. It says nothing, so we say nothing."""
    assert _docs_revision(tmp_path) is None


def test_a_stamp_that_is_not_a_revision_is_refused(tmp_path: Path) -> None:
    """A short sha is a label. Eight characters once agreed and named other bytes."""
    _sellar(tmp_path, "9f09e2c")
    assert _docs_revision(tmp_path) is None


def test_a_dirty_marker_is_refused_because_it_names_no_checkout(tmp_path: Path) -> None:
    _sellar(tmp_path, SHA + "-dirty")
    assert _docs_revision(tmp_path) is None


@pytest.mark.parametrize(
    ("sellado", "espera"),
    [(SHA, True), ("0" * 40, False)],
)
def test_health_compares_the_docs_against_the_code_it_serves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sellado: str, espera: bool
) -> None:
    """The comparison itself, through the endpoint, with the API's own revision faked.

    Patched where ``app`` looks it up rather than at its source, because the
    endpoint reads ``code_revision()`` at request time and the real value here
    is whatever revision the test runner happens to sit on.
    """
    from fastapi.testclient import TestClient

    import protea.api.app as app_mod

    _sellar(tmp_path, sellado)
    monkeypatch.setattr(app_mod, "code_revision", lambda: SHA)

    app = app_mod.FastAPI()
    app_mod._register_health_endpoints(app, None, None, tmp_path)
    cuerpo = TestClient(app).get("/health").json()

    assert cuerpo["docs_revision"] == sellado
    assert cuerpo["docs_match_code"] is espera


def test_health_says_unknown_when_the_docs_carry_no_stamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fastapi.testclient import TestClient

    import protea.api.app as app_mod

    monkeypatch.setattr(app_mod, "code_revision", lambda: SHA)
    app = app_mod.FastAPI()
    app_mod._register_health_endpoints(app, None, None, tmp_path)
    cuerpo = TestClient(app).get("/health").json()

    assert cuerpo["docs_revision"] is None
    assert cuerpo["docs_match_code"] is None
    assert cuerpo["docs_match_code"] is not False

#!/usr/bin/env python3
"""CI guard: every SET in this project is declared, described and verifiable.

THE RULE, from the researcher on 2026-11-08: "cada conjunto que aparece en
nuestro proyecto debe estar declarado, descrito, y con altisima calidad."
Three schema requirements, not documentation habits:

* **DECLARED** -- the row says what it is derived FROM, as foreign keys. Not
  in prose, not in a ``meta`` bag, not by matching a row count. ``job_id``
  does not count: that is provenance, not derivation.
* **DESCRIBED** -- there is a column to describe it in.
* **VERIFIABLE** -- wherever the row points at bytes with a ``*_uri``, it also
  carries the sha256 of those bytes. A locator with no identity cannot tell a
  swapped artefact from the right one, and reading it without checking is a
  silent substitution.
* **PRODUCED BY** -- the job AND the revision of the code that built it. The
  rules a row declares say what it is by DECLARATION; the code says what it is
  by IMPLEMENTATION, and those are different quantities: on 2026-11-07 a fix
  to the synonym map changed the delta of a whole class of windows with every
  identity field untouched.

WHY A GATE AND NOT GOODWILL. The five gaps this found did not arrive through
carelessness; they arrived ONE AT A TIME, each with a good local reason. A
rule that depends on somebody remembering produces the sixth. So this walks
the ORM metadata and reports, in the same shape as the other thirteen lint
gates.

REPORT MODE IS THE DEFAULT, ON PURPOSE. Six set tables fail at least one
criterion today and one of them, ``dataset``, is the only one that passes all
four. Failing CI now would block every unrelated change until a dozen
migrations land, so this prints the gap and exits 0. Pass ``--strict`` to make
it block, which is what the last of those migrations should turn on.

A NEW SET TABLE IS A FAILURE UNTIL IT IS DECLARED HERE. ``EXPECTED`` drives
the audit and the ``_set`` suffix only drives DETECTION of additions, because
the naming is not reliable: **``dataset`` is a set and does not end in
``_set``**. The first draft of this gate detected by suffix alone and silently
left ``dataset`` out of its own report, which is the failure mode it exists to
prevent. Both directions are errors now: a ``*_set`` table missing from
``EXPECTED``, and a name in ``EXPECTED`` that is no longer a table.

Usage:
    poetry run python scripts/check_sets.py [--strict]
"""

from __future__ import annotations

import sys

#: The sets this project has, and what each one IS. Adding a ``*_set`` table
#: without adding it here fails the gate, which is deliberate: the rule is
#: that a set must be described, and the first description is this line.
EXPECTED: dict[str, str] = {
    "annotation_set": "one GOA release's annotations: the corpus everything else hangs off",
    "evaluation_set": "a WINDOW: the truth delta between two corpora, expressed on a pivot",
    "query_set": "a frozen POPULATION of proteins to ask about, as bytes with a sha",
    "prediction_set": "what one method predicted, over one query set, from one reference pool",
    "information_accretion_set": "the IA weights, identified by snapshot x corpus x evidence regime",
    "dataset": "the frozen re-ranker dump the laboratory pulls to train",
}

#: Tables whose name ends in ``_set`` but which are membership or join rows,
#: not sets. Listed rather than pattern-matched: a pattern would quietly
#: swallow a real set whose name happened to look like one of these.
NOT_A_SET: frozenset[str] = frozenset({"query_set_entry"})

#: ``job_id`` is provenance and never derivation, so it cannot satisfy
#: DECLARED on its own.
NOT_DERIVATION: frozenset[str] = frozenset({"job_id"})

DESCRIPTION_COLUMNS: frozenset[str] = frozenset({"description", "notes", "label"})
REVISION_COLUMNS: frozenset[str] = frozenset({"code_revision", "producer_git_sha"})


def _sha_for(uri_column: str, columns: set[str], n_uris: int) -> str | None:
    """The column holding the sha256 of what ``uri_column`` points at.

    Matched BY STEM, so ``obo_uri`` is answered by ``obo_sha256`` and never by
    some other artefact's digest: a table with three uris and one sha would
    otherwise pass while verifying one third of itself. That is the ``dataset``
    case, where ``manifest_uri`` has ``manifest_sha`` and the two parquets have
    nothing.

    THE ONE-ARTEFACT EXCEPTION, because the first draft cried wolf.
    ``information_accretion_set`` carries one ``artifact_uri`` and one
    ``content_sha256``, which IS its sha; stem matching alone reported it as
    unverifiable, and a gate that fails a table doing the right thing gets
    switched off. With exactly one uri, a generic ``content_sha256`` answers
    it. With several, it cannot: nothing would say which one it covers.
    """
    stem = uri_column.removesuffix("_uri").removesuffix("_url")
    for candidate in (f"{stem}_sha256", f"{stem}_sha", f"{stem}_digest"):
        if candidate in columns:
            return candidate
    if n_uris == 1:
        for generico in ("content_sha256", "content_sha", "sha256"):
            if generico in columns:
                return generico
    return None


def _audit(table) -> dict[str, tuple[str, bool]]:
    """The four criteria for one set table: ``{criterio: (texto, cumple)}``.

    The verdict is a BOOLEAN the check decides, never inferred from how the
    text is spelled. The first draft read failure off ``value.isupper()`` and
    a prefix list, so a criterion whose message happened to start with a
    lowercase word passed. A guard that cannot refuse is worse than no guard.
    """
    columns = {c.name for c in table.columns}
    out: dict[str, tuple[str, bool]] = {}

    derivations = sorted(
        fk.parent.name
        for fk in table.foreign_keys
        if fk.parent.name not in NOT_DERIVATION
    )
    out["declarado"] = (
        (", ".join(derivations), True) if derivations else ("SIN NINGUNA FK", False)
    )

    described = sorted(columns & DESCRIPTION_COLUMNS)
    out["descrito"] = (", ".join(described), True) if described else ("NO HAY COLUMNA", False)

    uris = sorted(c for c in columns if c.endswith(("_uri", "_url")))
    faltan = [u for u in uris if _sha_for(u, columns, len(uris)) is None]
    if not uris:
        out["verificable"] = ("sin uri, nada que verificar", True)
    elif faltan:
        out["verificable"] = ("SIN SHA: " + ", ".join(faltan), False)
    else:
        out["verificable"] = (", ".join(uris) + " con su sha", True)

    revisions = sorted(columns & REVISION_COLUMNS)
    tiene_job = "job_id" in columns
    if revisions and tiene_job:
        out["producido_por"] = (f"job_id + {revisions[0]}", True)
    elif tiene_job:
        out["producido_por"] = ("FALTA LA REVISION", False)
    elif revisions:
        out["producido_por"] = ("FALTA EL JOB", False)
    else:
        out["producido_por"] = ("FALTAN LOS DOS", False)

    return out


def _falla(veredicto: dict[str, tuple[str, bool]]) -> bool:
    return not all(cumple for _, cumple in veredicto.values())


def main() -> int:
    estricto = "--strict" in sys.argv
    import protea.infrastructure.orm.models  # noqa: F401 -- register the tables
    from protea.infrastructure.orm.base import Base

    tablas = Base.metadata.tables
    # EXPECTED manda sobre la auditoria; el sufijo solo detecta altas.
    encontradas = {name: tablas[name] for name in EXPECTED if name in tablas}
    por_sufijo = {n for n in tablas if n.endswith("_set") and n not in NOT_A_SET}
    errores: list[str] = []

    sin_declarar = sorted(por_sufijo - set(EXPECTED))
    for name in sin_declarar:
        errores.append(
            f"{name} is a set table and is not in check_sets.EXPECTED. Add it with a "
            "line saying what it IS: the rule is that a set must be described, and "
            "that line is the first description."
        )
    fantasmas = sorted(set(EXPECTED) - set(encontradas))
    for name in fantasmas:
        errores.append(f"{name} is declared in EXPECTED and is not a table any more; drop the line")

    print("CONJUNTOS: declarado, descrito, verificable, producido por")
    print()
    fallan = 0
    for name in sorted(encontradas):
        veredicto = _audit(encontradas[name])
        falla = _falla(veredicto)
        fallan += falla
        print(f"[{'FALLA' if falla else '  ok '}] {name}")
        print(f"          que es ....... {EXPECTED[name]}")
        for criterio, (texto, cumple) in veredicto.items():
            print(f"          {criterio:13} {'' if cumple else '!'} {texto}")
        if falla:
            errores.append(
                f"{name}: "
                + "; ".join(f"{k}={t}" for k, (t, ok) in veredicto.items() if not ok)
            )
        print()

    print(f"{len(encontradas)} conjuntos, {fallan} con al menos un criterio sin cumplir.")
    if not errores:
        print("set guard OK")
        return 0
    # El informe se imprime SIEMPRE: una puerta en modo informe que esconde lo
    # que bloquearia no informa de nada.
    destino = sys.stderr if estricto else sys.stdout
    print("\nlo que bloquearia:" if not estricto else "\nset guard FAILED:", file=destino)
    for err in errores:
        print(f"  - {err}", file=destino)
    if estricto:
        return 1
    print("\nset guard: MODO INFORME, no bloquea. Con --strict devolveria 1.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

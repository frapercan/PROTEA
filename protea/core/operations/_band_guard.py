"""One place that asks whether a declared band may produce this number.

The rule itself lives in :mod:`protea.core.band_registry`; this is the caller
that binds it to a run's two artefacts, the pivot snapshot and the IA table.

It sat as a private staticmethod of ``RunCafaEvaluationOperation`` and had two
callers: the single run, while resolving the IA, and
:mod:`batch_rescore_evaluation`, which reached it through ``self._single``.
A guard addressed through another operation's private attribute is harder to
find than to skip, and it is a guard precisely because skipping it is silent:
the run still produces a number, just one bound to nothing. So it lives here,
beside :mod:`_holdout_guard`, which is the same kind of question asked of the
same operations.

THE IA HALF USED TO RESOLVE BY BASENAME, AND THAT IS WHY A PLATFORM TABLE COULD
NOT PASS. ``band_registry.ia_token`` returned the file name, an
``InformationAccretionSet`` always serialises to ``IA.tsv``, and so a band whose
token set omitted that name rejected every computed table. The note that used
to sit on the guard called that the safe direction, and it was: the registry
could not tell two tables apart, so inferring that one was authoritative would
have been a guess.

That premise went when ``ia_token`` started returning the SET ID for a platform
artifact, the three-axis identity the row is keyed by. A band pinned its own
table by id (``v211`` pinned ``80bbc9e4-...``), and the only thing left to get
right was giving the guard the source reference rather than the tempfile.
Before that, pinning the IA the traceable way (by set id) was the one way a
band-declared cell could NOT pass, which is the opposite of what both guards
are for.
"""

from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from protea.core.band_registry import (
    assert_band_consistency,
    assert_release_not_after_cutoff,
    ia_token,
)
from protea.core.contracts.operation import EmitFn
from protea.core.operations.compute_information_accretion import ia_key_for
from protea.infrastructure.orm.models.annotation.information_accretion_set import (
    InformationAccretionSet,
)
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot


def refuse_if_the_band_is_foreign(
    declared_band: str | None,
    snapshot: OntologySnapshot,
    payload_ia_file: str | None,
    emit: EmitFn,
    *,
    session: Session | None = None,
    ia_set_id: str | None = None,
) -> None:
    """Phantom-gap guard: when the payload declares a band, reject the run
    if the pivot ontology snapshot or the resolved IA come from a foreign
    band, and forbid the uniform IC=1 fallback.

    Binds propagation / term-universe / orphans (the snapshot) AND the IA
    to the declared band, extending the #599 IA-only resolver. The IA is
    checked by its *source*, never by a downloaded tempfile copy: the set
    named by ``ia_set_id``, else the payload ``ia_file``, else the snapshot
    ``ia_url``. No-op when ``declared_band`` is None (ad-hoc episode).

    A SET ID NAMES A ROW OF ONE DATABASE, so a band now pins a computed table
    by its content sha256 instead (ADR-D60). Given ``ia_set_id``, the guard
    reads that digest from the row, and the set id still resolves the rows a
    band pinned before (see :func:`_ia_identity`).
    """
    if not declared_band:
        return
    ia_ref, ia_sha256 = _ia_identity(snapshot, payload_ia_file, session, ia_set_id)
    band = assert_band_consistency(
        declared_band, obo_version=snapshot.obo_version, ia_ref=ia_ref, ia_sha256=ia_sha256
    )
    # No-future-data: even a set-canonical snapshot must not post-date the
    # band's t0 (the set-membership check and the temporal ordering are
    # independent; a band could in principle accept a date-bearing OBO that
    # is still after its cutoff). Raises CutoffViolationError.
    assert_release_not_after_cutoff(
        declared_band, artifact="ontology snapshot", release_ref=snapshot.obo_version
    )
    emit(
        "run_cafa_evaluation.band_verified",
        None,
        {
            "band": band.name,
            "obo_version": snapshot.obo_version,
            "ia_token": ia_token(ia_ref),
            "ia_sha256": ia_sha256,
        },
        "info",
    )


def _ia_identity(
    snapshot: OntologySnapshot,
    payload_ia_file: str | None,
    session: Session | None,
    ia_set_id: str | None,
) -> tuple[str | None, str | None]:
    """The IA reference the band guard checks and, for a computed table, its digest.

    A table pinned by set id is checked by the content sha256 its row records,
    the digest ``resolve_ia_from_set`` verifies the fetched bytes against. Its
    reference is the key ``compute_information_accretion`` wrote it under, the
    same builder, so a row pinned before the digest was the identity still
    resolves by its id. A supplied file is the payload's path, or the
    snapshot's ``ia_url`` when the payload names none, and carries no digest.
    """
    if not ia_set_id:
        return payload_ia_file or snapshot.ia_url, None
    if session is None:
        raise ValueError("information_accretion_set_id requires a database session")
    row = session.get(InformationAccretionSet, uuid.UUID(ia_set_id))
    if row is None:
        raise ValueError(f"InformationAccretionSet {ia_set_id} not found")
    return ia_key_for(ia_set_id), row.content_sha256

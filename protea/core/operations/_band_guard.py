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
"""

from __future__ import annotations

from protea.core.band_registry import (
    assert_band_consistency,
    assert_release_not_after_cutoff,
    ia_token,
)
from protea.core.contracts.operation import EmitFn
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot


def refuse_if_the_band_is_foreign(
    declared_band: str | None,
    snapshot: OntologySnapshot,
    payload_ia_file: str | None,
    emit: EmitFn,
) -> None:
    """Phantom-gap guard: when the payload declares a band, reject the run
    if the pivot ontology snapshot or the resolved IA come from a foreign
    band, and forbid the uniform IC=1 fallback.

    Binds propagation / term-universe / orphans (the snapshot) AND the IA
    to the declared band, extending the #599 IA-only resolver. The caller
    resolves the IA *reference* (payload ``ia_file`` first, else snapshot
    ``ia_url``) so the band is checked against the source artifact, not a
    downloaded tempfile copy. No-op when ``declared_band`` is None (ad-hoc
    episode).

    THE IA HALF USED TO RESOLVE BY BASENAME, AND THAT IS WHY A PLATFORM
    TABLE COULD NOT PASS. ``band_registry.ia_token`` returned the file name,
    an ``InformationAccretionSet`` always serialises to ``IA.tsv``, and so a
    band whose token set omitted that name rejected every computed table.
    The note that used to sit here called that the safe direction, and it
    was: the registry could not tell two tables apart, so inferring that one
    was authoritative would have been a guess.

    That premise is gone. ``ia_token`` now returns the SET ID for a platform
    artifact, which is the three-axis identity the row is keyed by, so the
    registry distinguishes them exactly. A band pins its own table by id --
    ``v211`` pins ``80bbc9e4-...`` -- and the only thing left to get right is
    giving this guard the source reference rather than the tempfile. Before
    that, pinning the IA the traceable way (by set id) was the one way a
    band-declared cell could NOT pass, which is the opposite of what both
    guards are for.
    """
    if not declared_band:
        return
    ia_ref = payload_ia_file or snapshot.ia_url
    band = assert_band_consistency(declared_band, obo_version=snapshot.obo_version, ia_ref=ia_ref)
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
        },
        "info",
    )

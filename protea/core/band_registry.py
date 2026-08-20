"""Per-band canonical (ontology snapshot, IA) registry.

A *band* is a GOA evaluation window (e.g. ``v226`` = the historical
benchmark cut, ``v227`` = the deployed LAFA window). Every band binds a
single canonical pair:

* an ``OntologySnapshot`` (identified by its ``obo_version``) that governs
  True-Path propagation, the term universe / terms-of-interest, and orphan
  handling, and
* an Information Accretion (IA) artifact, identified by a stable file token,
  computed from that same snapshot plus the band's t0 corpus.

These are DERIVED, pinned values: a cell never free-floats its snapshot or
its IA. Mixing a snapshot or IA from one band into a cell declared for
another band inflates a phantom PROTEA-vs-LAFA gap (the train/eval snapshot
drift documented in ``docs/EVAL_LAFA_PARITY.md`` and
``docs/IA_PROVENANCE_v227.md``), so this registry both *resolves* the
canonical pair and *guards* against cross-band contamination at runtime and
in CI.

Resolution is deterministic: the IA comes from the snapshot ``ia_url`` or an
explicit payload ``ia_file``, and the resolver never silently accepts the
uniform IC=1 fallback for a band-declared cell. This complements the #599
``resolve_ia_file`` resolver by binding the propagation / term-universe /
orphan ontology (the snapshot) to the band as well, not just the IA.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date

__all__ = [
    "Band",
    "BANDS",
    "BandMismatchError",
    "CutoffViolationError",
    "assert_band_consistency",
    "assert_release_not_after_cutoff",
    "band_for_ia_token",
    "band_for_obo_version",
    "cutoff_violations_for_cell",
    "ia_token",
    "manifest_cutoff_violations",
    "obo_release_date",
    "resolve_band",
]


class BandMismatchError(ValueError):
    """A cell's snapshot or IA artifact belongs to a band other than the
    one it declares (the phantom-gap guard fired)."""


class CutoffViolationError(ValueError):
    """An artifact references a data release dated *after* the declared
    training cutoff for its cell (the no-future-data rule fired).

    This is the temporal counterpart of :class:`BandMismatchError`: the
    band guard checks set *membership* (is this OBO/IA canonical for the
    band), while the cutoff guard checks the *ordering* (is the release
    date <= the band's t0). The concrete error it exists to catch:
    scoring the v227 band against ontology ``releases/2026-01-23`` when
    v227's congruent (and LAFA's) OBO is ``releases/2025-07-22`` and its
    t0 is ``2025-09-04`` (a release ~4 months in the future of the cut).
    """


@dataclass(frozen=True)
class Band:
    """Canonical (ontology snapshot, IA) binding for one evaluation window.

    ``obo_versions`` is the closed set of ``OntologySnapshot.obo_version``
    values that legitimately back this band (the snapshot governs
    propagation / term universe / orphans). ``ia_tokens`` is the closed set
    of IA-artifact file tokens (basename, case-insensitive) that legitimately
    weight this band. Both are derived constants, not free payload values.
    """

    name: str
    obo_versions: frozenset[str] = field(default_factory=frozenset)
    ia_tokens: frozenset[str] = field(default_factory=frozenset)
    description: str = ""
    #: GOA t0 date for this band: the training cutoff. No artifact bound
    #: to a cell in this band may reference a data release dated after
    #: this (the no-future-data rule). ``None`` means the band does not
    #: yet declare a cutoff and the temporal guard is skipped for it.
    t0_cutoff: date | None = None

    def accepts_obo_version(self, obo_version: str | None) -> bool:
        return obo_version is not None and obo_version in self.obo_versions

    def accepts_ia_token(self, token: str | None) -> bool:
        if token is None:
            return False
        return token.lower() in {t.lower() for t in self.ia_tokens}


# Canonical per-band bindings. The ontology per band is the GO OBO release
# current at that band's GOA t0 date (congruence rule), verified against EBI
# FTP publication dates and the GO release archive:
#   * v226 (t0 = goa 226, 2025-05-03): latest GO release on/before that date is
#     releases/2025-03-16 (April 2025 has no GO release; the next is March). IA
#     = IA_cafa6.tsv (the historical benchmark IA, snapshot 35c3ad67 ia_url).
#   * v227 (t0 = goa 227, 2025-09-04): ontology = releases/2025-07-22, the exact
#     data-version in LAFA's own lafa_t0_Sep_2025/go-basic.obo. IA =
#     lafa_t0_Sep_2025/IA.tsv (39906 terms), the exact table the deployed LAFA
#     endpoint scored against (docs/IA_PROVENANCE_v227.md). The generic
#     IA_cafa6.tsv is a DIFFERENT corpus and is rejected for v227.
#
# PROTEA had only ingested releases/2024-03-28 and releases/2026-01-23, so the
# pre-registry v226/v227/v230 eval_sets all shared releases/2026-01-23 (a
# ~6-months-too-late ontology); that snapshot-sharing IS the phantom-gap bug.
# Each band now pins its own congruent OBO. obo_versions is a closed set (not a
# single value) so an interim ontology refresh inside one GOA window does not
# force a new band. New bands (CAFA7, ...) add a row here; nothing else changes.
BANDS: dict[str, Band] = {
    "v226": Band(
        name="v226",
        obo_versions=frozenset({"releases/2025-03-16"}),
        ia_tokens=frozenset({"IA_cafa6.tsv"}),
        t0_cutoff=date(2025, 5, 3),
        description=(
            "Historical benchmark cut (GOA v226, t0 2025-05-03). Congruent GO "
            "ontology = releases/2025-03-16 (latest release on/before t0). IA = "
            "the generic IA_cafa6.tsv. Superseded by v227 for LAFA-comparable "
            "numbers; kept for history."
        ),
    ),
    "v227": Band(
        name="v227",
        obo_versions=frozenset({"releases/2025-07-22"}),
        ia_tokens=frozenset({"IA.tsv", "IA-swissprot-exp-v227.txt"}),
        t0_cutoff=date(2025, 9, 4),
        description=(
            "Deployed LAFA window (GOA v227 to v230, t0 2025-09-04). Congruent "
            "GO ontology = releases/2025-07-22 (the data-version in LAFA's own "
            "lafa_t0_Sep_2025/go-basic.obo). Canonical IA = "
            "lafa_t0_Sep_2025/IA.tsv (39906 terms, the on-record LAFA table); "
            "the lab recompute IA-swissprot-exp-v227.txt (r=0.98) is accepted "
            "as a faithful reconstruction. The generic IA_cafa6.tsv is rejected."
        ),
    ),
}


def ia_token(ia_ref: str | None) -> str | None:
    """Stable IA-artifact token from a path or URL.

    The token is the basename (``IA.tsv``, ``IA_cafa6.tsv``, ...), which is how
    bands identify their IA without pinning a host-specific absolute path or a
    URL. Query strings and fragments are stripped. ``None`` for a missing ref.
    """
    if not ia_ref:
        return None
    cleaned = re.split(r"[?#]", ia_ref, maxsplit=1)[0].rstrip("/")
    base = os.path.basename(cleaned)
    return base or None


def resolve_band(cutoff: str) -> Band:
    """Resolve a declared band/cutoff string to its canonical ``Band``.

    Accepts the band name directly (``"v226"``) or a ``vNNN``-bearing token
    (``"bench-v1-K5-v226-lineage-prostt5"``) and extracts the band. Raises
    ``BandMismatchError`` for an unknown band so a typo never silently picks a
    wrong (or no) binding.
    """
    if cutoff in BANDS:
        return BANDS[cutoff]
    for token in re.findall(r"v\d+", cutoff or ""):
        if token in BANDS:
            return BANDS[token]
    known = ", ".join(sorted(BANDS))
    raise BandMismatchError(
        f"Unknown band/cutoff {cutoff!r}; registered bands are: {known}. "
        "Add a Band row to protea.core.band_registry.BANDS to register it."
    )


def band_for_obo_version(obo_version: str | None) -> str | None:
    """Reverse lookup: the band name whose canonical snapshot universe
    contains ``obo_version``, or ``None`` if no band claims it."""
    for band in BANDS.values():
        if band.accepts_obo_version(obo_version):
            return band.name
    return None


def band_for_ia_token(token: str | None) -> str | None:
    """Reverse lookup: the band name whose canonical IA tokens contain
    ``token``, or ``None`` if no band claims it."""
    for band in BANDS.values():
        if band.accepts_ia_token(token):
            return band.name
    return None


def assert_band_consistency(
    declared_band: str,
    *,
    obo_version: str | None,
    ia_ref: str | None,
) -> Band:
    """Reject a cell whose snapshot or IA come from a foreign band.

    ``declared_band`` is the band/cutoff the cell declares. ``obo_version`` is
    the resolved pivot ``OntologySnapshot.obo_version`` (governs propagation /
    term universe / orphans). ``ia_ref`` is the resolved IA path or URL.

    Raises ``BandMismatchError`` when:

    * the declared band is unknown,
    * the snapshot ``obo_version`` is not in the declared band's universe,
    * the IA is missing (would fall back to uniform IC=1, never allowed for a
      band-declared cell), or
    * the IA token belongs to a different band.

    Returns the resolved ``Band`` on success. This is the single guard called
    both at runtime (in ``run_cafa_evaluation``) and in CI.
    """
    band = resolve_band(declared_band)

    if not band.accepts_obo_version(obo_version):
        other = band_for_obo_version(obo_version)
        suffix = f" (it belongs to band {other!r})" if other else ""
        raise BandMismatchError(
            f"Phantom-gap guard: cell declares band {band.name!r} but its "
            f"pivot ontology snapshot obo_version {obo_version!r} is not "
            f"canonical for that band{suffix}. The snapshot governs "
            f"propagation, term universe and orphans; a cross-band snapshot "
            f"inflates a fake PROTEA-vs-LAFA gap. Canonical obo_versions for "
            f"{band.name!r}: {sorted(band.obo_versions)}."
        )

    token = ia_token(ia_ref)
    if token is None:
        raise BandMismatchError(
            f"Phantom-gap guard: cell declares band {band.name!r} but no IA "
            f"artifact resolved (snapshot ia_url and payload ia_file are both "
            f"unset). A band-declared cell must never fall back to uniform "
            f"IC=1. Pin the canonical IA for {band.name!r}: "
            f"{sorted(band.ia_tokens)}."
        )
    if not band.accepts_ia_token(token):
        other = band_for_ia_token(token)
        suffix = f" (it belongs to band {other!r})" if other else ""
        raise BandMismatchError(
            f"Phantom-gap guard: cell declares band {band.name!r} but its IA "
            f"artifact {token!r} is not canonical for that band{suffix}. A "
            f"cross-band IA reweights the metric against a foreign corpus and "
            f"inflates a fake PROTEA-vs-LAFA gap. Canonical IA tokens for "
            f"{band.name!r}: {sorted(band.ia_tokens)}."
        )
    return band


# A GO OBO release version is self-dating: ``releases/YYYY-MM-DD`` (the GO
# release archive convention). The date in the token IS the release date, so
# a date-bearing artifact reference can be ordered against a band's t0 cutoff
# with no DB lookup -- which is what makes the cutoff guard a pure CI check.
_OBO_RELEASE_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


def obo_release_date(obo_ref: str | None) -> date | None:
    """Parse the release date embedded in a GO OBO release reference.

    Accepts ``releases/2025-07-22``, a full URL ending in that token, or a
    bare ``2025-07-22``. Returns ``None`` when no ``YYYY-MM-DD`` date is
    present (e.g. a ``go-basic`` snapshot named without a date), in which
    case the temporal guard cannot order it and defers to the band guard.
    """
    if not obo_ref:
        return None
    match = _OBO_RELEASE_DATE_RE.search(obo_ref)
    if match is None:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def assert_release_not_after_cutoff(
    declared_band: str,
    *,
    artifact: str,
    release_ref: str | None,
) -> None:
    """Reject an artifact that references a release dated after the band's t0.

    ``declared_band`` is the band/cutoff the cell declares. ``artifact`` is a
    human label for the offending artifact (``"ontology snapshot"``,
    ``"IA"``, ...). ``release_ref`` is a date-bearing reference (an OBO
    ``releases/YYYY-MM-DD`` token, an IA path/URL carrying a date, ...).

    Raises :class:`CutoffViolationError` when the parsed release date is
    strictly after the band's ``t0_cutoff``. A reference with no parseable
    date, or a band with no declared cutoff, is a no-op here: the band guard
    (:func:`assert_band_consistency`) covers set membership for those.
    """
    band = resolve_band(declared_band)
    if band.t0_cutoff is None:
        return
    released = obo_release_date(release_ref)
    if released is None:
        return
    if released > band.t0_cutoff:
        raise CutoffViolationError(
            f"No-future-data guard: cell declares band {band.name!r} (training "
            f"cutoff t0 = {band.t0_cutoff.isoformat()}) but its {artifact} "
            f"references release {released.isoformat()} ({release_ref!r}), which "
            f"is AFTER the cutoff. An artifact built at the cutoff must not "
            f"reference data from the future of the band. Pin the congruent "
            f"release for {band.name!r}: {sorted(band.obo_versions)}."
        )


def cutoff_violations_for_cell(
    declared_band: str,
    *,
    obo_version: str | None = None,
    ia_ref: str | None = None,
    extra_refs: dict[str, str] | None = None,
) -> list[str]:
    """Collect every no-future-data violation for one cell, as messages.

    Checks the pivot ontology snapshot, the IA artifact, and any
    ``extra_refs`` (``{label: release_ref}``, e.g. a KNN-corpus or
    label-snapshot release token) against the band's t0 cutoff. Returns the
    list of violation messages (empty when the cell is clean). Aggregating
    instead of raising lets the CI guard report all offenders for a cell in
    one pass; runtime callers use :func:`assert_release_not_after_cutoff`.
    """
    checks: list[tuple[str, str | None]] = [
        ("ontology snapshot", obo_version),
        ("IA", ia_ref),
    ]
    if extra_refs:
        checks.extend(extra_refs.items())
    violations: list[str] = []
    for artifact, ref in checks:
        try:
            assert_release_not_after_cutoff(
                declared_band, artifact=artifact, release_ref=ref
            )
        except CutoffViolationError as exc:
            violations.append(str(exc))
    return violations


#: Manifest keys whose values are date-bearing release references the
#: no-future-data rule must order against the band cutoff. Each maps to a
#: human label for the offending artifact. ``scripts/export_lafa_bundle.py``
#: records these on every bundle it emits so the bundle is self-verifying.
_MANIFEST_RELEASE_KEYS: dict[str, str] = {
    "obo_version": "ontology snapshot",
    "ia_ref": "IA",
    "knn_corpus_release": "KNN candidate corpus",
    "label_release": "labels",
}


def manifest_cutoff_violations(manifest: dict[str, object]) -> list[str]:
    """Collect every no-future-data violation declared by a bundle manifest.

    A bundle manifest produced by the single-``cutoff`` inference path
    (``scripts/export_lafa_bundle.py``) records the band it was cut for
    (``cutoff`` or, for legacy bundles, ``cutoff_version``) plus the
    date-bearing release reference of each artifact it froze
    (``obo_version`` and any of :data:`_MANIFEST_RELEASE_KEYS`). This walks
    those references and orders each against the band's ``t0_cutoff`` via
    :func:`assert_release_not_after_cutoff`, so the cutoff CI guard can
    verify an *emitted* bundle, not just the registry.

    Returns the list of violation messages (empty when the bundle is clean
    or declares no recognised band). A manifest with no resolvable band is a
    no-op here: bundles predating the registry are not retro-validated.
    """
    declared = manifest.get("cutoff") or manifest.get("cutoff_version")
    if not isinstance(declared, str):
        return []
    try:
        resolve_band(declared)
    except BandMismatchError:
        # An unknown band (e.g. a legacy free-text cutoff_version) cannot be
        # ordered; the band guard owns set-membership for those.
        return []
    extra_refs: dict[str, str] = {}
    obo_version: str | None = None
    ia_ref: str | None = None
    for key, label in _MANIFEST_RELEASE_KEYS.items():
        value = manifest.get(key)
        if not isinstance(value, str) or not value:
            continue
        if key == "obo_version":
            obo_version = value
        elif key == "ia_ref":
            ia_ref = value
        else:
            extra_refs[label] = value
    return cutoff_violations_for_cell(
        declared,
        obo_version=obo_version,
        ia_ref=ia_ref,
        extra_refs=extra_refs or None,
    )

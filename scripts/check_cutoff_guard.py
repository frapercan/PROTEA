#!/usr/bin/env python3
"""CI guard for the no-future-data (cutoff) rule.

Every evaluation band in ``protea.core.band_registry`` pins a t0 training
cutoff (``Band.t0_cutoff``). The no-future-data rule says: no produced
artifact bound to a cell in a band -- the KNN candidate corpus, the
per-(query, candidate) features, the labels, the IA table, or the ontology
snapshot -- may reference a data release dated AFTER that band's cutoff.

The band guard (``check_band_registry.py``) checks set *membership* (is this
OBO/IA canonical for the band). This guard checks the *ordering* (is the
release date <= the band's t0). The concrete error it exists to catch:
PROTEA scored the v227 band against ontology ``releases/2026-01-23`` when
v227's congruent (and LAFA's) OBO is ``releases/2025-07-22`` and its cutoff
is ``2025-09-04`` (a release ~4 months in the future of the cut).

Three layers run here:

1. Registry self-consistency: every band's own canonical ``obo_versions``
   must NOT post-date its declared cutoff (a band that pins a future OBO is
   itself malformed and would let every cell in it leak).
2. A red-team fixture: the exact ``releases/2026-01-23`` against v227 case
   must be flagged. This pins the guard's behaviour so a future refactor
   that silently weakens it fails CI.
3. Emitted-bundle verification (``--bundle``): each ``manifest.json`` an
   inference bundle declares (the single-``cutoff`` path in
   ``scripts/export_lafa_bundle.py``) is read back and every release-bearing
   ref it froze is ordered against its band's t0. This closes
   F-EVAL-PROTOCOL.c: the cutoff guard runs on the artifacts the path
   PRODUCES, not just on the registry.

Usage:
    poetry run python scripts/check_cutoff_guard.py
    poetry run python scripts/check_cutoff_guard.py --bundle path/to/frozen-dir
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _check_bundles(bundles: list[Path]) -> list[str]:
    """Order every release-bearing ref each bundle manifest froze.

    Each ``bundle`` is a directory holding a ``manifest.json`` (or the
    manifest file itself). Returns the flat list of no-future-data
    violations across all bundles.
    """
    from protea.core.band_registry import manifest_cutoff_violations

    errors: list[str] = []
    for bundle in bundles:
        manifest_path = bundle / "manifest.json" if bundle.is_dir() else bundle
        if not manifest_path.exists():
            errors.append(f"bundle manifest not found at {manifest_path}")
            continue
        try:
            manifest = json.loads(manifest_path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            errors.append(f"bundle manifest {manifest_path} unreadable: {exc}")
            continue
        for violation in manifest_cutoff_violations(manifest):
            errors.append(f"{manifest_path}: {violation}")
    return errors


def _check() -> list[str]:
    from protea.core.band_registry import (
        BANDS,
        CutoffViolationError,
        assert_release_not_after_cutoff,
        cutoff_violations_for_cell,
        obo_release_date,
    )

    errors: list[str] = []
    if not BANDS:
        return ["band registry is empty; at least v226 and v227 must be pinned"]

    # 1. Self-consistency: a band's own canonical OBO release must not
    #    post-date the band's declared cutoff. A band with no cutoff is
    #    skipped (the temporal guard is opt-in per band).
    for name, band in BANDS.items():
        if band.t0_cutoff is None:
            continue
        for obo in band.obo_versions:
            released = obo_release_date(obo)
            if released is not None and released > band.t0_cutoff:
                errors.append(
                    f"band {name!r} pins canonical obo_version {obo!r} "
                    f"(released {released.isoformat()}) AFTER its own cutoff "
                    f"t0 {band.t0_cutoff.isoformat()}; the band is self-leaking"
                )
        # Round-trip: the canonical OBO must pass the cell-level guard.
        try:
            assert_release_not_after_cutoff(
                name, artifact="ontology snapshot", release_ref=next(iter(band.obo_versions))
            )
        except CutoffViolationError as exc:
            errors.append(f"canonical OBO for band {name!r} fails its own cutoff guard: {exc}")

    # 2. Red-team fixture: the exact phantom-gap class of error
    #    (v227 scored against a 2026 ontology release) MUST be flagged.
    if "v227" in BANDS:
        future = "releases/2026-01-23"
        flagged = cutoff_violations_for_cell("v227", obo_version=future)
        if not flagged:
            errors.append(
                f"cutoff guard FAILED to flag {future!r} against band 'v227' "
                f"(t0 {BANDS['v227'].t0_cutoff}); the no-future-data rule is "
                "not being enforced"
            )

    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--bundle",
        action="append",
        type=Path,
        default=[],
        metavar="DIR",
        help=(
            "A frozen-bundle directory (or its manifest.json) to verify "
            "against its declared band cutoff. Repeatable. When omitted only "
            "the registry self-consistency + red-team checks run."
        ),
    )
    args = parser.parse_args(argv)

    errors = _check()
    errors.extend(_check_bundles(args.bundle))
    if errors:
        print("cutoff guard FAILED:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1
    print("cutoff guard OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

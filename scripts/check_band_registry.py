#!/usr/bin/env python3
"""CI guard for the per-band (ontology snapshot, IA) registry.

The registry in ``protea.core.band_registry`` pins, per evaluation band, the
canonical ``OntologySnapshot.obo_version`` set and the IA tables that weight it
(computed tables by content sha256, supplied ones by file name, and the legacy
production rows by set id). The phantom-gap guard
(``assert_band_consistency``) rejects any cell whose snapshot or IA come from a
foreign band. For that guard to be sound, the registry itself must be
deterministically resolvable: no two bands may claim the same ``obo_version``
or the same IA identity (a shared key would make the reverse lookup ambiguous
and let a cross-band artifact slip through), and every band must pin at least
one of each.

This is the CI half of the slice's runtime guard: a malformed registry fails
here before it can mis-bind a cell at runtime.

Usage:
    poetry run python scripts/check_band_registry.py
"""

from __future__ import annotations

import sys
from collections import defaultdict


def _check() -> list[str]:
    from protea.core.band_registry import BANDS

    errors: list[str] = []
    if not BANDS:
        return ["band registry is empty; at least v226 and v227 must be pinned"]

    obo_owners: dict[str, list[str]] = defaultdict(list)
    ia_owners: dict[str, list[str]] = defaultdict(list)
    for name, band in BANDS.items():
        if band.name != name:
            errors.append(f"band {name!r} keyed under a mismatched name {band.name!r}")
        if not band.obo_versions:
            errors.append(f"band {name!r} pins no canonical obo_version")
        if not band.ia_identities():
            errors.append(f"band {name!r} pins no canonical IA table")
        for ver in band.obo_versions:
            obo_owners[ver].append(name)
        for identity in band.ia_identities():
            ia_owners[identity.lower()].append(name)

    for ver, owners in obo_owners.items():
        if len(owners) > 1:
            errors.append(
                f"obo_version {ver!r} is claimed by multiple bands {sorted(owners)}; "
                "reverse lookup is ambiguous, so the phantom-gap guard cannot bind it"
            )
    for identity, owners in ia_owners.items():
        if len(owners) > 1:
            errors.append(
                f"IA identity {identity!r} is claimed by multiple bands {sorted(owners)}; "
                "reverse lookup is ambiguous, so the phantom-gap guard cannot bind it"
            )
    errors.extend(_round_trips())
    return errors


def _round_trips() -> list[str]:
    """Every canonical binding through the public resolvers and the guard.

    A registry that does not validate against its OWN canonical values fails
    CI (catches accidental drift between the data and the lookups). A table
    pinned by content goes through the guard by its digest; one pinned by name
    or by a legacy set id, by that reference.
    """
    from protea.core.band_registry import (
        BANDS,
        assert_band_consistency,
        band_for_ia_sha256,
        band_for_ia_token,
        band_for_obo_version,
    )

    errors: list[str] = []
    for name, band in BANDS.items():
        for ver in band.obo_versions:
            if band_for_obo_version(ver) != name:
                errors.append(
                    f"band_for_obo_version({ver!r}) != {name!r}; registry self-inconsistent"
                )
        for tok in band.ia_tokens | band.legacy_ia_set_ids:
            if band_for_ia_token(tok) != name:
                errors.append(f"band_for_ia_token({tok!r}) != {name!r}; registry self-inconsistent")
        for sha in band.ia_sha256s:
            if band_for_ia_sha256(sha) != name:
                errors.append(f"band_for_ia_sha256({sha!r}) != {name!r}; registry self-inconsistent")
        if not band.obo_versions or not band.ia_identities():
            continue
        ref_obo = next(iter(band.obo_versions))
        ref_sha = next(iter(sorted(band.ia_sha256s)), None)
        ref_ia = None if ref_sha else band.ia_identities()[0]
        try:
            assert_band_consistency(name, obo_version=ref_obo, ia_ref=ref_ia, ia_sha256=ref_sha)
        except Exception as exc:  # noqa: BLE001 - surface any guard failure as a CI error
            errors.append(
                f"canonical binding for band {name!r} fails its own phantom-gap guard: {exc}"
            )
    return errors


def main() -> int:
    errors = _check()
    if errors:
        print("band registry guard FAILED:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1
    print("band registry guard OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

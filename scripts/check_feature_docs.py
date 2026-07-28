#!/usr/bin/env python3
"""CI drift gate: PROTEA's installed feature schema versus its doc registry.

The re-ranker feature reference (``docs/source/reference/feature_reference.rst``)
renders itself from :data:`protea_contracts.feature_docs.FEATURE_DOCS`. That
page can only stay honest if the registry covers exactly the columns the
installed ``protea-contracts`` declares in
:data:`protea_contracts.feature_schema.ALL_FEATURES`. The upstream lint lives
in ``protea-contracts`` and runs there; this gate re-asserts the same invariant
against *PROTEA's* pinned contracts, so a stale or forked pin can never let the
docs ship a column that the schema no longer declares (or drop one it does).

The check function is not exported by the ``protea_contracts`` package (it ships
only in that repo's ``scripts/``), so the small drift logic is mirrored here
rather than imported. It is deliberately identical in spirit to the upstream
check: coverage, no orphans, key integrity, and family agreement.

Checks:
  1. Coverage: every column in ``feature_schema.ALL_FEATURES`` has a doc.
  2. No orphans: every doc names a column declared in ``ALL_FEATURES``.
  3. Key integrity: each doc's mapping key equals ``doc.name``.
  4. Family agreement: each ``doc.family`` is a key of ``FEATURE_FAMILIES``
     whose column list contains ``doc.name``.

Usage:
    poetry run python scripts/check_feature_docs.py
"""

from __future__ import annotations

import sys


def _check() -> list[str]:
    """Return a list of human-readable drift errors (empty when clean)."""
    from protea_contracts.feature_docs import FEATURE_DOCS
    from protea_contracts.feature_schema import ALL_FEATURES, FEATURE_FAMILIES

    declared = set(ALL_FEATURES)
    documented = set(FEATURE_DOCS)
    errors: list[str] = []

    for col in sorted(declared - documented):
        errors.append(f"declared feature has no FeatureDoc: {col!r}")

    for col in sorted(documented - declared):
        errors.append(
            f"FeatureDoc names a column not declared in ALL_FEATURES: {col!r}"
        )

    for key, doc in sorted(FEATURE_DOCS.items()):
        if key != doc.name:
            errors.append(
                f"FeatureDoc mapping key {key!r} does not match doc.name {doc.name!r}"
            )
        family_cols = FEATURE_FAMILIES.get(doc.family)
        if family_cols is None:
            errors.append(
                f"{doc.name!r}: family {doc.family!r} is not a key of FEATURE_FAMILIES"
            )
        elif doc.name not in family_cols:
            errors.append(
                f"{doc.name!r}: family {doc.family!r} does not contain this column "
                "(FEATURE_FAMILIES disagreement)"
            )

    return errors


def main() -> int:
    errors = _check()
    if errors:
        print("feature-docs drift gate FAILED:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        print(
            "\nFix: bump PROTEA's protea-contracts pin (or the registry) so "
            "FEATURE_DOCS covers exactly ALL_FEATURES with agreeing families.",
            file=sys.stderr,
        )
        return 1

    from protea_contracts.feature_docs import FEATURE_DOCS

    print(
        f"feature-docs drift gate OK: {len(FEATURE_DOCS)} features documented, "
        "zero drift against the installed schema."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

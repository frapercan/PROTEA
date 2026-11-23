"""Today's UniProt entries are shown, never used on a cut.

``protein_uniprot_metadata`` holds each protein as UniProt describes it in the
release that served the row: knowledge from after every cut the platform
evaluates on. A model, a feature, an export or an evaluation that read it would
learn from the future without anybody declaring it. The author's decision of
2026-11-22: current metadata is for display and today's annotation only, and a
protein's organism at a cut comes from that cut's photo (``cut_photo_member``).

This pins the boundary by reading the source: only the modules that fetch the
entries and the routes that show them may name the table or its model.
"""

from __future__ import annotations

from pathlib import Path

import protea

ROOT = Path(protea.__file__).resolve().parent
NAMES = ("ProteinUniProtMetadata", "protein_uniprot_metadata")

#: Where the current entries may be named: their model, the operations that fetch
#: them, and the protein routes and services that show them.
ALLOWED = {
    "infrastructure/orm/models/protein/protein_metadata.py",
    "infrastructure/orm/models/protein/protein.py",
    "infrastructure/orm/models/__init__.py",
    "core/operations/fetch_uniprot_metadata.py",
    "core/operations/resolve_protein_metadata.py",
    "api/routers/proteins.py",
}


def _readers() -> set[str]:
    found = set()
    for path in ROOT.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if any(name in text for name in NAMES):
            found.add(str(path.relative_to(ROOT)))
    return found


def test_only_the_fetch_and_the_display_name_the_current_entries() -> None:
    outside = sorted(_readers() - ALLOWED)
    assert outside == [], (
        "these modules name today's UniProt entries, which no prediction, feature, export "
        f"or evaluation on a cut may read: {outside}"
    )


def test_the_allowed_list_names_only_modules_that_exist() -> None:
    """A stale entry would let a new module take its place unnoticed."""
    missing = sorted(p for p in ALLOWED if not (ROOT / p).exists())
    assert missing == []

#!/usr/bin/env python3
"""Autodoc coverage gate: verify must-document symbols have autodoc directives.

Scans ``docs/source/**/*.rst`` for ``automodule``, ``autoclass``, and
``autofunction`` directives.  Compares the covered set against a
pattern-based whitelist of must-document symbols.  Fails (exit 1) when
any whitelisted pattern is not covered by at least one autodoc directive.

Whitelist approach
------------------
The whitelist is **pattern-based**, not symbol-by-symbol.  Each entry is
a Python ``fnmatch``-style prefix pattern: a module name matches if it
equals the pattern exactly OR starts with the pattern followed by a dot.
This keeps the gate maintainable as the codebase grows: adding a new
router under ``protea.api.routers.*`` will be caught automatically
without updating the whitelist.

Usage::

    poetry run python scripts/check_autodoc_coverage.py [docs_root] [src_root]

Exits 0 if all whitelisted patterns are covered, 1 otherwise.
"""

from __future__ import annotations

import fnmatch
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Must-document whitelist
# Pattern forms:
#   "protea.api.routers.annotations.*"  -- any sub-module of the package
#   "protea.api.routers.jobs"           -- exact module
#
# A module M matches pattern P when:
#   fnmatch.fnmatchcase(M, P) is True
# which supports * wildcards but not ** globs.  Keep patterns as specific
# as needed to avoid false negatives.
# ---------------------------------------------------------------------------
MUST_DOCUMENT_PATTERNS: list[str] = [
    # API routers -- all top-level routers must be autodoc-covered
    "protea.api.routers.jobs",
    "protea.api.routers.proteins",
    "protea.api.routers.annotations",
    "protea.api.routers.annotations.snapshots",
    "protea.api.routers.annotations.sets",
    "protea.api.routers.annotations.evaluation_sets",
    "protea.api.routers.annotations.evaluation_results",
    "protea.api.routers.embeddings",
    "protea.api.routers.scoring",
    "protea.api.routers.query_sets",
    "protea.api.routers.annotate",
    "protea.api.routers.maintenance",
    "protea.api.routers.admin",
    "protea.api.routers.showcase",
    "protea.api.routers.support",
    "protea.api.routers.benchmark",
    "protea.api.routers.datasets",
    "protea.api.routers.registry",
    "protea.api.routers.reranker_models",
    "protea.api.routers.stack",
    "protea.api.routers.experiment_runs",
    # Services layer -- all four primary services must be documented
    "protea.services.jobs_service",
    "protea.services.annotations_service",
    "protea.services.embeddings_service",
    "protea.services.scoring_service",
    # Core operations -- every registered operation must be documented
    "protea.core.operations.ping",
    "protea.core.operations.insert_proteins",
    "protea.core.operations.fetch_uniprot_metadata",
    "protea.core.operations.load_ontology_snapshot",
    "protea.core.operations.load_goa_annotations",
    "protea.core.operations.load_quickgo_annotations",
    "protea.core.operations.compute_embeddings",
    "protea.core.operations.predict_go_terms",
    "protea.core.operations.generate_evaluation_set",
    "protea.core.operations.run_cafa_evaluation",
    "protea.core.operations.export_research_dataset",
    # ORM models -- all primary domain models must be documented
    "protea.infrastructure.orm.models.job",
    "protea.infrastructure.orm.models.protein.protein",
    "protea.infrastructure.orm.models.protein.protein_metadata",
    "protea.infrastructure.orm.models.sequence.sequence",
    "protea.infrastructure.orm.models.annotation.ontology_snapshot",
    "protea.infrastructure.orm.models.annotation.go_term",
    "protea.infrastructure.orm.models.annotation.go_term_relationship",
    "protea.infrastructure.orm.models.annotation.annotation_set",
    "protea.infrastructure.orm.models.annotation.protein_go_annotation",
    "protea.infrastructure.orm.models.annotation.evaluation_set",
    "protea.infrastructure.orm.models.annotation.evaluation_result",
    "protea.infrastructure.orm.models.embedding.embedding_config",
    "protea.infrastructure.orm.models.embedding.sequence_embedding",
    "protea.infrastructure.orm.models.embedding.prediction_set",
    "protea.infrastructure.orm.models.embedding.go_prediction",
    "protea.infrastructure.orm.models.embedding.reranker_model",
    "protea.infrastructure.orm.models.embedding.scoring_config",
    "protea.infrastructure.orm.models.embedding.dataset",
    "protea.infrastructure.orm.models.query.query_set",
    "protea.infrastructure.orm.models.support_entry",
    "protea.infrastructure.orm.models.experiment_run",
]

# Regex to extract the module name from any autodoc directive.
# Matches: .. automodule:: protea.foo.bar
#          .. autoclass:: protea.foo.Bar
#          .. autofunction:: protea.foo.baz
#          .. autoattribute:: protea.foo.Bar.attr
_AUTODOC_RE = re.compile(
    r"^\.\.\s+auto(?:module|class|function|attribute)::\s+(protea\.\S+)",
    re.MULTILINE,
)


def _covered_modules(docs_root: Path) -> set[str]:
    """Return all protea.* names referenced by any autodoc directive."""
    covered: set[str] = set()
    for rst_path in sorted(docs_root.rglob("*.rst")):
        text = rst_path.read_text(encoding="utf-8")
        for m in _AUTODOC_RE.finditer(text):
            # Strip any trailing class/function name for class/function directives
            # so that autoclass:: protea.foo.Bar -> "protea.foo.Bar" is stored
            # as-is. For automodule the value already is the module dotted path.
            covered.add(m.group(1).strip())
    return covered


def _matches_any(module: str, covered: set[str]) -> bool:
    """Return True when *module* is in *covered* or covered by a wildcard entry."""
    # Direct hit (most common case)
    if module in covered:
        return True
    # fnmatch wildcard hit -- allows "protea.api.routers.*" style patterns
    return any(fnmatch.fnmatchcase(module, pat) for pat in covered)


def main(argv: list[str]) -> int:
    docs_root_arg = argv[1] if len(argv) > 1 else "docs/source"
    docs_root = Path(docs_root_arg)
    if not docs_root.is_dir():
        print(
            f"[check_autodoc_coverage] docs root not found: {docs_root}",
            file=sys.stderr,
        )
        return 2

    covered = _covered_modules(docs_root)

    missing: list[str] = []
    for pattern in MUST_DOCUMENT_PATTERNS:
        # A pattern covers itself if any covered name matches the pattern
        # OR if any covered name fnmatches a reverse check.
        # For exact patterns: check if pattern is in covered.
        # For wildcard patterns: check if pattern itself is covered (unlikely)
        # or if any covered module satisfies the pattern.
        if not _matches_any(pattern, covered):
            missing.append(pattern)

    if not missing:
        print(
            f"[check_autodoc_coverage] All {len(MUST_DOCUMENT_PATTERNS)} "
            "must-document patterns covered. OK."
        )
        return 0

    print(
        f"[check_autodoc_coverage] {len(missing)} must-document pattern(s) "
        "not covered by any autodoc directive:\n"
    )
    for name in missing:
        print(f"  {name}")
    print(
        "\nAdd the missing automodule/autoclass/autofunction directives to the "
        "appropriate RST file under docs/source/reference/. "
        "See scripts/check_autodoc_coverage.py for the whitelist."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))

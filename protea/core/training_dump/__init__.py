"""Package home for the frozen re-ranker dataset dump helpers.

Split out from ``protea/core/training_dump_helpers.py`` (T2B.6) so each
submodule stays under the §3 file-LOC budget while preserving every
public symbol consumed by ``ExportResearchDatasetOperation`` plus the
private helpers exercised by the unit tests.

Importers should keep using ``protea.core.training_dump_helpers`` —
that module now re-exports the symbols defined here.
"""

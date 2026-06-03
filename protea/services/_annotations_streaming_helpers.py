"""Streaming + per-row TSV/zip helpers extracted from ``annotations_service``.

These pure functions own the streaming bytes/strings that
``StreamingResponse`` consumes (eval-artifacts zip and per-namespace
metrics TSV). They have no domain-exception coupling, so they live
in this sibling module independently of the service layer.
"""

from __future__ import annotations

import io
import uuid
import zipfile
from collections.abc import Iterator
from typing import Any

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult


def iter_evaluation_artifacts_zip(
    keys: list[str],
    result_id: uuid.UUID,
) -> Iterator[bytes]:
    """Stream a deflate-compressed zip of the evaluation-result artifacts.

    The artifact store is resolved internally from settings; the
    caller does not need to thread the store through. Keys are
    rewritten to drop the ``eval_artifacts/<result_id>/`` prefix so
    the zip entries are the relative file names cafaeval emitted.
    """
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    project_root = __import__("pathlib").Path(__file__).resolve().parents[2]
    store = get_artifact_store(load_settings(project_root))
    prefix = f"eval_artifacts/{result_id}/"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for key in sorted(keys):
            rel = key[len(prefix):] if key.startswith(prefix) else key
            zf.writestr(rel, store.get(key))
    yield buf.getvalue()


def render_evaluation_metrics_tsv(
    result: EvaluationResult,
    aspect_codes: tuple[str, ...],
) -> Any:
    """Yield TSV rows for the per-(setting, namespace) metrics summary.

    The caller passes the aspect-codes tuple (``ASPECT_CAFA_CODES``)
    so the service stays free of the domain layer. Returns a
    generator suitable for ``StreamingResponse``.
    """
    yield "setting\tnamespace\tfmax\tprecision\trecall\ttau\tcoverage\tn_proteins\n"
    for setting in ("NK", "LK", "PK"):
        ns_data = result.results.get(setting, {})
        for ns in aspect_codes:
            m = ns_data.get(ns)
            if m is None:
                continue
            yield (
                f"{setting}\t{ns}\t{m.get('fmax', '')}\t{m.get('precision', '')}\t"
                f"{m.get('recall', '')}\t{m.get('tau', '')}\t{m.get('coverage', '')}\t"
                f"{m.get('n_proteins', '')}\n"
            )

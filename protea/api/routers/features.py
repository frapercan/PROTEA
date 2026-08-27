"""Feature-registry endpoint: the machine-readable third renderer.

The reranker feature schema has ONE canonical description, the
``FEATURE_DOCS`` mapping shipped by ``protea-contracts`` (75 entries as
of contracts v1.4.0). The docs render it via Sphinx and the thesis
cites it; this endpoint is the third renderer, serving the same source
as JSON so the Next.js frontend (which cannot import the Python
contract) can render an in-situ glossary for the technician.

The principle is one source, three renderers, never restated: this
router does NOT hand-transcribe any field. It derives every record
directly from ``FEATURE_DOCS`` at request time, so a contract bump
that adds, drops, or re-documents a feature surfaces here on the next
request without touching this file.

The endpoint is read-only, stateless, and needs no database session.
"""

from __future__ import annotations

from collections import Counter

from fastapi import APIRouter
from protea_contracts.feature_docs import FEATURE_DOCS
from pydantic import BaseModel, Field

router = APIRouter(prefix="/features", tags=["features"])


class FeatureDocInfo(BaseModel):
    """One feature's canonical documentation, mirroring
    :class:`protea_contracts.feature_docs.FeatureDoc`.

    Every field is copied verbatim from the contract; ``status`` is
    lowered to its string value so the frontend can key a badge off it
    without importing the Python enum.
    """

    name: str = Field(
        ..., description="Canonical feature/column name (e.g. ``distance``, ``interpro_hit``)."
    )
    family: str = Field(
        ...,
        description="Feature family the column belongs to (e.g. ``knn``, ``interpro``, ``classifier``).",
    )
    status: str = Field(
        ...,
        description=(
            "Production status: ``PRODUCED`` (real values in the default "
            "export), ``DECLARED_ABSENT`` (a producer seam leaves it "
            "unfilled, see ADR-D45), ``POOL_INJECTED`` (stamped by the "
            "lab's pooled loader, not the parquet dump), or ``BROKEN``."
        ),
    )
    summary: str = Field(..., description="One-line plain-language summary for the technician.")
    definition: str = Field(
        ..., description="Precise definition of what the number means and how it is computed."
    )
    producer: str = Field(
        ..., description="Who fills the column (the module, operation, or loader stage)."
    )
    unit: str | None = Field(None, description="Physical/logical unit, or null when dimensionless.")
    value_range: str | None = Field(
        None, description="Expected value range (e.g. ``[0, 1]``), or null."
    )
    notes: str | None = Field(
        None,
        description=(
            "Caveats the operator must know in situ (e.g. why "
            "``interpro_*`` carries no signal when its table is "
            "unpopulated, or the ADR-D45 producer seam)."
        ),
    )


class FeatureRegistryResponse(BaseModel):
    """The whole feature registry, grouped-friendly for the UI."""

    schema_version: str = Field(
        ...,
        description="``protea-contracts`` version the registry was sourced from.",
    )
    total: int = Field(..., description="Number of documented features.")
    families: list[str] = Field(
        ...,
        description="Distinct family names, in first-seen order.",
    )
    status_counts: dict[str, int] = Field(
        ...,
        description="Count of features per status value.",
    )
    features: list[FeatureDocInfo] = Field(
        ...,
        description="Every documented feature, in canonical declaration order.",
    )


def _serialize() -> FeatureRegistryResponse:
    """Build the response FROM ``FEATURE_DOCS`` (never a transcribed
    literal): iterate the contract mapping in declaration order and copy
    each :class:`FeatureDoc` field across, lowering ``status`` to its
    string value.
    """
    from importlib.metadata import PackageNotFoundError, version

    try:
        schema_version = version("protea-contracts")
    except PackageNotFoundError:  # pragma: no cover - always installed in prod
        schema_version = "unknown"

    features = [
        FeatureDocInfo(
            name=doc.name,
            family=doc.family,
            status=doc.status.value,
            summary=doc.summary,
            definition=doc.definition,
            producer=doc.producer,
            unit=doc.unit,
            value_range=doc.value_range,
            notes=doc.notes,
        )
        for doc in FEATURE_DOCS.values()
    ]

    # First-seen family order (dict preserves insertion order).
    families: list[str] = []
    for f in features:
        if f.family not in families:
            families.append(f.family)

    status_counts = dict(Counter(f.status for f in features))

    return FeatureRegistryResponse(
        schema_version=schema_version,
        total=len(features),
        families=families,
        status_counts=status_counts,
        features=features,
    )


@router.get("/registry", response_model=FeatureRegistryResponse)
def get_feature_registry() -> FeatureRegistryResponse:
    """Serve the canonical reranker feature registry as JSON.

    This is the third renderer of the explainability registry (docs via
    Sphinx, thesis via citation, this endpoint for the UI). It reads the
    same ``FEATURE_DOCS`` contract the other two do, so what the operator
    sees in the browser is byte-for-byte the documented truth, including
    which features are ``DECLARED_ABSENT`` (ADR-D45 producer seam) and
    which are ``POOL_INJECTED`` by the lab's pooled loader.
    """
    return _serialize()

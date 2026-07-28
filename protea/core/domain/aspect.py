"""GO aspect (namespace) — the three-way partition of the Gene Ontology.

Two encodings circulate in the codebase, both of them load-bearing:

* **Single-char codes** (``"P"`` / ``"F"`` / ``"C"``) — the wire format
  used by ``GOTerm.aspect`` in PostgreSQL and the ``go-basic.obo`` file
  itself. The ``go_term`` table CHECK constraint is on these codes.

* **Three-char CAFA codes** (``"BPO"`` / ``"MFO"`` / ``"CCO"``) — the
  format expected by ``cafaeval`` (the upstream Fmax / AuPRC evaluator)
  and surfaced in the UI for human readers.

Until this module landed, both encodings appeared as bare string literals
in 30+ places — a textbook Primitive Obsession smell. The enum is the
single source of truth; everything else converts at the boundary.

Typical usage::

    from protea.core.domain.aspect import Aspect

    # Iterate the three aspects in a stable canonical order
    for aspect in Aspect:
        ...

    # Convert from a DB row
    aspect = Aspect.from_code(row.aspect)

    # Hand off to cafaeval
    result_dict[aspect.cafa] = ...
"""

from __future__ import annotations

from enum import Enum


class Aspect(Enum):
    """Gene Ontology aspect / namespace.

    The three GO sub-ontologies. Iteration order is the canonical
    PROTEA order (P → F → C), matching the historical
    ``_ASPECTS = ("P", "F", "C")`` tuples this enum replaces.
    """

    BIOLOGICAL_PROCESS = "P"
    MOLECULAR_FUNCTION = "F"
    CELLULAR_COMPONENT = "C"

    @property
    def code(self) -> str:
        """Single-char code (``"P"`` / ``"F"`` / ``"C"``).

        Wire format in PostgreSQL (``go_term.aspect`` column) and the
        ``go-basic.obo`` file. Use this when reading/writing the DB or
        comparing against the ORM column.
        """
        return self.value

    @property
    def cafa(self) -> str:
        """Three-char CAFA code (``"BPO"`` / ``"MFO"`` / ``"CCO"``).

        Format expected by the upstream ``cafaeval`` package and the
        evaluation results JSON; also the canonical UI label.
        """
        return _CODE_TO_CAFA[self.value]

    @classmethod
    def from_code(cls, code: str) -> Aspect:
        """Build an Aspect from its single-char wire code."""
        return _CODE_TO_ASPECT[code]

    @classmethod
    def from_cafa(cls, cafa: str) -> Aspect:
        """Build an Aspect from its three-char CAFA code."""
        return _CAFA_TO_ASPECT[cafa]


_CODE_TO_CAFA: dict[str, str] = {
    "P": "BPO",
    "F": "MFO",
    "C": "CCO",
}

_CAFA_TO_CODE: dict[str, str] = {v: k for k, v in _CODE_TO_CAFA.items()}

_CODE_TO_ASPECT: dict[str, Aspect] = {a.code: a for a in Aspect}
_CAFA_TO_ASPECT: dict[str, Aspect] = {a.cafa: a for a in Aspect}


#: Integer encoding of the aspect used as an ad-hoc reranker feature
#: (``aspect_code``). This is NOT the enum iteration order (which is
#: ``P -> F -> C``): it reproduces bit-for-bit the mapping the offline
#: ``protea-reranker-lab`` trainer bakes into every per-category booster
#: (``train_serve_reranker.py``:
#: ``df["aspect_code"] = df["aspect"].map({"mfo": 0, "bpo": 1, "cco": 2})``).
#: The lab keys on the lowercase CAFA code; PROTEA prediction rows carry the
#: single-char wire code (``P`` / ``F`` / ``C``), so serve/eval must translate
#: through the enum to land on the SAME integer the booster split on.
_CAFA_TO_RERANKER_CODE: dict[str, int] = {"MFO": 0, "BPO": 1, "CCO": 2}


def reranker_aspect_code(aspect: str | None) -> int | None:
    """Map any aspect encoding to the offline lab's ``aspect_code`` integer.

    Accepts the single-char wire code (``P`` / ``F`` / ``C``, as carried on
    PROTEA prediction rows) or the three-char CAFA code in any case
    (``MFO`` / ``mfo`` / ...). Returns the lab's integer encoding
    (``mfo -> 0``, ``bpo -> 1``, ``cco -> 2``). Empty or unrecognised input
    returns ``None`` so a downstream numeric column routes it through
    LightGBM's native missing-value branch rather than crashing.
    """
    if not aspect:
        return None
    token = aspect.strip().upper()
    if len(token) == 1:
        asp = _CODE_TO_ASPECT.get(token)
    else:
        asp = _CAFA_TO_ASPECT.get(token)
    if asp is None:
        return None
    return _CAFA_TO_RERANKER_CODE[asp.cafa]


# Canonical iteration tuples — kept as module-level constants so existing
# callers that destructure into ``for code in ASPECT_CODES`` keep working
# without importing the enum directly.
ASPECT_CODES: tuple[str, str, str] = tuple(a.code for a in Aspect)  # type: ignore[assignment]
ASPECT_CAFA_CODES: tuple[str, str, str] = tuple(a.cafa for a in Aspect)  # type: ignore[assignment]


__all__ = [
    "ASPECT_CAFA_CODES",
    "ASPECT_CODES",
    "Aspect",
    "reranker_aspect_code",
]

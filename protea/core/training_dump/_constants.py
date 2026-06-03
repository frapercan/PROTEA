"""Shared constants for the dump pipeline submodules.

Kept here so the split helpers, the test-split helpers, and the
runner can share them without forming an import cycle (T2B.6).
"""

from __future__ import annotations

from protea.core.domain.aspect import Aspect

_CATEGORIES = ("nk", "lk", "pk")
# Lower-cased CAFA codes - used only for model-name suffixes ("model-NK-bpo").
# Built from the canonical Aspect enum so the three encodings stay in sync.
_ASPECT_NAMES: dict[str, str] = {a.code: a.cafa.lower() for a in Aspect}

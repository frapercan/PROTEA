"""Unit tests for protea.core.training_dump_helpers.

Covers the module-level helpers (``_load_sequences``,
``_load_taxonomy_ids``) that ``TrainRerankerAutoOperation`` uses to
drive the dataset-export pipeline. Heavy DB / model training is no
longer tested here: LightGBM training lives in protea-reranker-lab.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import numpy as np

from protea.core.training_dump_helpers import (
    _load_sequences,
    _load_taxonomy_ids,
)

# ---------------------------------------------------------------------------
# _load_sequences (used by TrainRerankerAutoOperation in dump_only mode)
# ---------------------------------------------------------------------------


class TestLoadSequences:
    def test_returns_dict(self):
        session = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.all.return_value = [
            ("P1", "MKVLWAGS"),
            ("P2", "ACDEF"),
        ]

        result = _load_sequences(session, {"P1", "P2"})
        assert result == {"P1": "MKVLWAGS", "P2": "ACDEF"}

    def test_empty_accessions(self):
        session = MagicMock()
        result = _load_sequences(session, set())
        assert result == {}


# ---------------------------------------------------------------------------
# _load_taxonomy_ids (used by TrainRerankerAutoOperation in dump_only mode)
# ---------------------------------------------------------------------------


class TestLoadTaxonomyIds:
    def test_returns_dict(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            ("P1", 9606),
            ("P2", 10090),
        ]

        result = _load_taxonomy_ids(session, {"P1", "P2"})
        assert result == {"P1": 9606, "P2": 10090}

    def test_none_taxonomy_id(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            ("P1", None),
        ]

        result = _load_taxonomy_ids(session, {"P1"})
        assert result == {"P1": None}


# Import np to keep the test module's structure consistent with other test
# files even though the explicit numpy assertions used by the previous
# TrainReranker test surface have been removed.
_ = np

"""The audit names the derivation behind each live id, and the omission behind a broken one.

Targets ``scripts/audit_embedding_config_ids.py`` without a database: the rows
below are the live registry's own, as read on 2026-11-04.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

from protea.infrastructure.orm.models.embedding.embedding_config import UNCHUNKED

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "audit_embedding_config_ids.py"
_spec = importlib.util.spec_from_file_location("audit_embedding_config_ids", _SCRIPT)
assert _spec and _spec.loader
audit = importlib.util.module_from_spec(_spec)
sys.modules["audit_embedding_config_ids"] = audit
_spec.loader.exec_module(audit)

_SHAPE: dict[str, Any] = {
    "normalize": False,
    "normalize_residues": False,
    "embedding_scale": 1.0,
    **UNCHUNKED,
}

FULL_LENGTH_ANKH = {
    "id": "f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0",
    "model_name": "ElnaggarLab/ankh-base",
    "model_backend": "ankh",
    "layer_indices": [0],
    "layer_agg": "mean",
    "pooling": "mean_max",
    "max_length": None,
    **_SHAPE,
}
INGESTED_ARM = {
    "id": "db16eaa9-e71a-596b-bf21-a7f62d05f094",
    "model_name": "sfjepa-recipe-ens8-cut230",
    "model_backend": "ingested-method",
    "layer_indices": [0],
    "layer_agg": "none",
    "pooling": "reader",
    "max_length": 1022,
    **_SHAPE,
}
RESIDUE_VOCABULARY = {
    "id": "22166300-a178-599e-835c-afe1c007feb5",
    "model_name": "residue-vocabulary:06fed75ca76d17be:f3a7fe16",
    "model_backend": "residue-vocabulary",
    "layer_indices": [46, 38, 26, 8, 0],
    "layer_agg": "concat",
    "pooling": "per-residue",
    "max_length": 1022,
    **_SHAPE,
}


def test_a_recipe_id_is_reproduced_by_its_recipe() -> None:
    assert audit.classify(FULL_LENGTH_ANKH)[0] == "recipe"


def test_an_ingested_arm_is_reproduced_by_its_name() -> None:
    # db16eaa9 was the one id the omission did not explain: it was never a
    # recipe hash, it is register_ingested_method.config_id_for of its name.
    assert audit.classify(INGESTED_ARM)[0] == "name"


def test_a_broken_id_names_the_omission_that_minted_it() -> None:
    how, derived, omission = audit.classify(RESIDUE_VOCABULARY)
    assert how == "nothing"
    assert omission == ("chunk_overlap", "chunk_size", "embedding_scale")
    # What the row derives as it stands, which is NOT the repair target here:
    # the row's 1022 is wrong too, and the target is derived after fixing it.
    assert str(derived).startswith("ae931b10")


def test_a_name_does_not_vouch_for_a_backend_that_is_not_an_ingested_method() -> None:
    impostor = {
        **RESIDUE_VOCABULARY,
        "model_name": INGESTED_ARM["model_name"],
        "id": INGESTED_ARM["id"],
    }
    assert audit.classify(impostor)[0] == "nothing"


@pytest.mark.parametrize("row", [FULL_LENGTH_ANKH, INGESTED_ARM])
def test_a_reproduced_row_reports_no_omission(row: dict[str, Any]) -> None:
    assert audit.classify(row)[2] is None

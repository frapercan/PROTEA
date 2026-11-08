"""Give an already-written ground truth its digest, and say it is a weaker seal.

WHY THIS IS NOT OPTIONAL. ``evaluation_set.groundtruth_sha256`` arrives in
migration ``b4c17e2a9f08`` and is filled by the generator from the local file
BEFORE the upload. Every window written before that lands has no digest, and
on 2026-11-08 that was 133 windows plus the 63 of the (A) series, which are
the training data. A dataset built from unsealed truth cannot say its truth
was not swapped, and the whole point of the column is to be able to say so.

AND THE SEAL THIS OPERATION TAKES IS WEAKER THAN THE GENERATOR'S, which has
to be written down or the column will be read as stronger than it is. The
generator hashes the bytes it is about to send, so its digest certifies what
the delta computation produced. This one hashes what the store returns NOW, so
it certifies the bytes FROM THE MOMENT OF SEALING ONWARDS. If an object had
already been replaced before the seal, this records the replacement as
authentic. It closes the future, not the past.

IT NEVER DESERIALISES. Only bytes in, digest out. That is a rule and not an
optimisation: ``cb46395a`` is the holdout, its single declared look was spent
in the laboratory on 2026-10-26, and parsing its truth inside the platform
would be a second read of the one artefact the waiver protects. Hashing bytes
is not reading a truth.

THE WINDOWS ARE NAMED, never "all the unsealed ones". A scope that lives in a
default is a scope nobody declared, and the first run of an "all" would have
reached into the holdout without anybody typing its id.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import field_validator
from sqlalchemy.orm import Session

from protea.core._artifact_identity import ArtifactMismatch, sha256_of_bytes
from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class SealEvaluationGroundtruthPayload(ProteaPayload, frozen=True):
    """Which windows to seal. Named one by one, on purpose."""

    evaluation_set_ids: list[str]

    @field_validator("evaluation_set_ids")
    @classmethod
    def _at_least_one(cls, value: list[str]) -> list[str]:
        """Refuse an empty list rather than treat it as "all".

        An "all the unsealed ones" default would have reached the holdout
        without anybody typing its id, and the holdout is the one artefact
        whose single declared look is already spent.
        """
        if not value:
            raise ValueError(
                "evaluation_set_ids cannot be empty. This operation seals the windows "
                "it is given and has no 'all' mode: a scope living in a default is a "
                "scope nobody declared."
            )
        return value


class SealEvaluationGroundtruthOperation(Operation):
    name = "seal_evaluation_groundtruth"
    description = (
        "Record the sha256 of a window's already-uploaded ground-truth parquet on "
        "its row, for windows written before the column existed. Hashes bytes and "
        "never deserialises them. The seal is WEAKER than the generator's, which "
        "hashes before the upload: this one certifies the bytes from the moment of "
        "sealing onwards, not what the delta computation produced."
    )

    def execute(
        self,
        session: Session,
        payload: dict[str, Any],
        *,
        emit: EmitFn,
    ) -> OperationResult:
        p = SealEvaluationGroundtruthPayload.model_validate(contract_payload(payload))
        store = get_artifact_store(load_settings(_project_root()))
        from protea.core.evaluation import groundtruth_key_for

        sellados, ya_estaban, sin_artefacto = 0, 0, 0
        for raw in p.evaluation_set_ids:
            eset = session.get(EvaluationSet, uuid.UUID(raw))
            if eset is None:
                raise ValueError(f"EvaluationSet {raw} does not exist; nothing to seal")
            if not eset.groundtruth_uri:
                sin_artefacto += 1
                emit(
                    "seal_evaluation_groundtruth.no_artifact",
                    None,
                    {"evaluation_set_id": raw},
                    "warning",
                )
                continue
            key = groundtruth_key_for(eset.id)
            digest = sha256_of_bytes(store.get(key))
            if eset.groundtruth_sha256 is None:
                eset.groundtruth_sha256 = digest
                sellados += 1
                emit(
                    "seal_evaluation_groundtruth.sealed",
                    None,
                    {"evaluation_set_id": raw, "sha256": digest, "key": key},
                    "info",
                )
                continue
            if eset.groundtruth_sha256 != digest:
                raise ArtifactMismatch(
                    f"evaluation set {raw} already declares "
                    f"{eset.groundtruth_sha256[:16]}... and the bytes under {key!r} now "
                    f"hash to {digest[:16]}.... The object changed after it was sealed, "
                    "which is a finding and not something to update: overwriting the "
                    "row would erase the evidence."
                )
            ya_estaban += 1

        result = {
            "sealed": sellados,
            "already_sealed": ya_estaban,
            "without_artifact": sin_artefacto,
            "asked": len(p.evaluation_set_ids),
        }
        emit("seal_evaluation_groundtruth.done", None, result, "info")
        return OperationResult(result=result)

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        n = len(payload.get("evaluation_set_ids") or [])
        return f"seal the ground truth of {n} evaluation set(s)"


def _project_root():
    from pathlib import Path

    return Path(__file__).resolve().parents[3]

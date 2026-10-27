"""Say whether a window's truth reproduces an external reference's.

A declared frame is one a reader cannot misread. A FAITHFUL frame is one that
computes what somebody else's implementation computes from the same inputs,
and no amount of declaring gets you the second. This is the operation that
checks it and records the verdict.

WHY THE REFERENCE IS A FILE WITH A DECLARED DIGEST. The reference is somebody
else's acceptance contract, handed over, and the digest is the caller's claim
about which bytes they mean. It is verified before anything is compared, for
the same reason ``load_swissprot_release`` verifies its table: a comparison
against a file nobody pinned proves the two agreed about something, without
saying what.

WHAT IT DOES NOT DO. It does not recompute the window. The counters it
compares are the ones ``generate_evaluation_set`` already wrote to
``stats``, so a claim here is about the record as it stands, and re-running
the window is how you change it. That separation is the point: this operation
cannot make a window agree, only report whether it does.

WHAT A CLAIM IS WORTH IS ITS DENOMINATOR. Measured on the long window, 26 of
the reference's 34 counters are readable on both sides; 7 are quantities a
window does not keep and 1 belongs to the run. All 26 agreed. The row stores
all of that, because "reproduces" with no denominator is the kind of number
this campaign keeps deleting.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from typing import Any

from pydantic import field_validator
from sqlalchemy.orm import Session

from protea.core._reference_counters import NOT_STORED, compare_counters
from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.reference_reproduction import (
    ReferenceReproduction,
)


class VerifyWindowAgainstReferencePayload(ProteaPayload, frozen=True):
    """Which window, which contract, and which bytes of it."""

    evaluation_set_id: str
    #: The reference's acceptance contract, as a local path. Handed over, like
    #: every other artefact from the laboratory.
    contract_path: str
    #: The digest the caller declares for that file. Verified before anything
    #: is read out of it.
    expected_sha256: str

    @field_validator("evaluation_set_id", "contract_path")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must be a non-empty string")
        return value.strip()

    @field_validator("expected_sha256")
    @classmethod
    def _hex64(cls, value: str) -> str:
        text = value.strip().lower()
        if len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
            raise ValueError(f"expected_sha256 must be 64 hex characters, got {value!r}")
        return text


def read_contract(path: Path, expected: str) -> dict[str, Any]:
    """The contract's parsed body, or a refusal naming both digests.

    Refusing rather than warning: the whole value of the claim this operation
    writes is that it names the bytes it was made against, and a claim made
    against bytes that were not the declared ones names the wrong file.
    """
    if not path.is_file():
        raise ValueError(f"no reference contract at {path}")
    raw = path.read_bytes()
    actual = hashlib.sha256(raw).hexdigest()
    if actual != expected:
        raise ValueError(
            f"the contract at {path} hashes to {actual}, and the caller declared "
            f"{expected}. Refusing to compare: a reproduction claim is only worth the "
            "bytes it names, and these are not those bytes."
        )
    return json.loads(raw.decode())


class VerifyWindowAgainstReferenceOperation(Operation):
    """Compare a window's counters against a reference's and record the verdict."""

    name = "verify_window_against_reference"
    description = (
        "Compare one evaluation window's stored counters against an external "
        "reference's acceptance contract, and record what agreed."
    )
    payload_model = VerifyWindowAgainstReferencePayload

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        """One line naming the window and the contract, which is what a reader
        needs to tell two claims apart."""
        window = str(payload.get("evaluation_set_id", "?"))[:8]
        return f"{window} against {Path(str(payload.get('contract_path', '?'))).name}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = VerifyWindowAgainstReferencePayload.model_validate(contract_payload(payload))
        window = session.get(EvaluationSet, uuid.UUID(p.evaluation_set_id))
        if window is None:
            raise ValueError(f"EvaluationSet {p.evaluation_set_id} not found")
        contract = read_contract(Path(p.contract_path), p.expected_sha256)
        counters = contract.get("counts") or {}
        if not counters:
            raise ValueError(
                f"the contract at {p.contract_path} carries no 'counts' block, so there "
                "is nothing to compare. A claim over an empty intersection agrees with "
                "everything."
            )
        emit(
            "verify_window_against_reference.contract",
            None,
            {
                "key": (contract.get("frame") or {}).get("key"),
                "digest": p.expected_sha256,
                "commit": contract.get("lab_commit"),
                "counters": len(counters),
            },
            "info",
        )
        verdict = compare_counters(counters, dict(window.stats or {}))
        row = ReferenceReproduction(
            evaluation_set_id=window.id,
            reference_key=str((contract.get("frame") or {}).get("key") or "unnamed"),
            reference_digest=p.expected_sha256,
            reference_commit=contract.get("lab_commit"),
            compared=verdict.compared,
            agreed=verdict.agreed,
            disagreements=[d._asdict() for d in verdict.disagreements],
            absent={name: NOT_STORED.get(name, "not stored on the window") for name in verdict.absent},
            job_id=job_id_from_payload(payload),
        )
        session.add(row)
        session.flush()
        result = {
            "reference_reproduction_id": str(row.id),
            "reference_key": row.reference_key,
            "compared": verdict.compared,
            "agreed": verdict.agreed,
            "reproduces": verdict.reproduces,
            "disagreements": [d._asdict() for d in verdict.disagreements],
            "absent": list(verdict.absent),
            # Counters the reference publishes that this platform has never
            # been told how to read. Reported separately from ``absent``
            # because that one is a decision and this one is a gap in
            # ``_reference_counters.COUNTER_ALIASES``.
            "unmapped": list(verdict.unknown),
        }
        emit("verify_window_against_reference.done", None, result, "info")
        return OperationResult(result=result)

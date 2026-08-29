"""A key the transport adds is not a key the contract forbids.

``base_worker`` hands every operation ``{**job.payload, "_job_id": ...}`` so
the operation can find its own job row. That key is delivery metadata. No
model declares it, and none can: pydantic treats a leading underscore as a
private attribute, so ``_job_id`` cannot be a field even if we wanted it to
be.

While the base payload ignored undeclared keys this was invisible. It stopped
being invisible the moment the contract began forbidding them, which is the
whole point of forbidding them: every operation that validated its payload
started raising on the one key the worker sends on purpose.

So the strip happens at the validation sites, and only for underscore keys.
A key nobody meant to send still raises, which is the behaviour worth having.
"""

from __future__ import annotations

import ast
import pathlib
import uuid

import pytest

from protea.core.utils import contract_payload

_ROOT = pathlib.Path(__file__).resolve().parents[1] / "protea"


class TestTheStripper:
    def test_it_removes_what_the_worker_injects(self) -> None:
        assert contract_payload({"a": 1, "_job_id": "x"}) == {"a": 1}

    def test_it_keeps_a_key_nobody_declared(self) -> None:
        """That one must still raise downstream. Stripping it would put the
        silence back exactly where it was removed from."""
        assert contract_payload({"a": 1, "invented": 2}) == {"a": 1, "invented": 2}

    def test_it_leaves_a_non_dict_alone(self) -> None:
        assert contract_payload("not a dict") == "not a dict"

    def test_it_removes_every_underscore_key_not_only_the_known_one(self) -> None:
        """The worker is free to add another. The rule is the prefix."""
        assert contract_payload({"a": 1, "_job_id": "x", "_trace": "y"}) == {"a": 1}


class TestTheRouteThatBrokeTheFleet:
    def test_a_retrieval_payload_validates_with_the_injected_key(self) -> None:
        """This is the exact shape base_worker hands the coordinator."""
        from protea_contracts import PredictGOTermsPayload

        u = str(uuid.uuid4())
        delivered = {
            "embedding_config_id": u,
            "annotation_set_id": u,
            "ontology_snapshot_id": u,
            "limit_per_entry": 30,
            "_job_id": u,
        }
        with pytest.raises(Exception, match="_job_id"):
            PredictGOTermsPayload.model_validate(delivered)
        got = PredictGOTermsPayload.model_validate(contract_payload(delivered))
        assert got.limit_per_entry == 30

    def test_an_undeclared_key_still_stops_the_run(self) -> None:
        """The guard has to keep pointing at version skew, which is the thing
        it was added for."""
        from protea_contracts import PredictGOTermsPayload

        u = str(uuid.uuid4())
        with pytest.raises(Exception, match="[Ee]xtra"):
            PredictGOTermsPayload.model_validate(
                contract_payload({
                    "embedding_config_id": u,
                    "annotation_set_id": u,
                    "ontology_snapshot_id": u,
                    "from_a_newer_dispatcher": True,
                })
            )


def test_no_validation_site_takes_the_raw_payload() -> None:
    """Walk the source. One site left behind is one operation that dies on
    its first job, and it would die in production rather than here."""
    raw: list[str] = []
    for path in _ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if getattr(node.func, "attr", None) != "model_validate":
                continue
            if not node.args:
                continue
            arg = node.args[0]
            if isinstance(arg, ast.Name) and arg.id == "payload":
                raw.append(f"{path.relative_to(_ROOT.parent)}:{node.lineno}")
    assert not raw, (
        f"{len(raw)} site(s) validate the raw payload, so the transport's "
        f"_job_id reaches a model that forbids it: {raw}"
    )

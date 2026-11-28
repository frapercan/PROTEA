"""A predict run cannot name what this revision's search does not run.

protea-contracts 1.8.0 lets a predict payload name the space the search runs in
(``embedding_transform``), a declared set of donors (``donor_policy.donor_set_id``),
the evidence channels a donation may come through
(``donor_policy.evidence_channels``) and the query leaving its own
neighbourhood after retrieval (``self_exclusion`` ``"accession_after_retrieval"``).
The payload validates all four, and ``extra="forbid"`` cannot refuse a field the
contract knows. Until the code that runs each one lands, a run that named it
would file its rows under a request they do not answer, so the coordinator
refuses it before reading anything, naming the change that runs it. The donor
set, the channels and the exclusion after retrieval run in the search now
(``tests/test_donor_set_and_channels_in_the_search.py``); the space is still
refused. Everything a stored payload could ask for still passes.

The contract's channel vocabulary is pinned here too. ``EvidenceChannel`` names
PROTEA's partition so a payload can ask for channels without importing the
platform, and the contract returns a policy's channels in that vocabulary's
order: a channel spelt or ordered differently on one side would give a policy a
meaning the other side does not read.
"""

from __future__ import annotations

import uuid
from typing import Any, get_args
from unittest.mock import MagicMock

import pytest
from protea_contracts import payloads as contract_payloads

from protea.core.cut_photo.channels import CHANNELS, Channel
from protea.core.operations.predict_go_terms import PredictGOTermsPayload
from protea.core.operations.predict_go_terms._coordinator import (
    PredictGOTermsOperation,
    _refuse_what_the_search_does_not_run_yet,
)

_SPACE = "feat/embedding-transform-in-the-search"


def _fields(**overrides: Any) -> dict[str, Any]:
    return {
        "embedding_config_id": str(uuid.uuid4()),
        "annotation_set_id": str(uuid.uuid4()),
        "ontology_snapshot_id": str(uuid.uuid4()),
        "query_accessions": ["P00001"],
        **overrides,
    }


def _payload(**overrides: Any) -> PredictGOTermsPayload:
    return PredictGOTermsPayload.model_validate(_fields(**overrides))


def _refusal(p: PredictGOTermsPayload) -> str:
    with pytest.raises(ValueError) as caught:
        _refuse_what_the_search_does_not_run_yet(p)
    return str(caught.value)


@pytest.mark.parametrize("transform", get_args(contract_payloads.EmbeddingTransform))
def test_a_search_space_is_refused_naming_the_search_in_a_declared_space(transform: str) -> None:
    message = _refusal(_payload(embedding_transform=transform))
    assert "embedding_transform" in message
    assert _SPACE in message


def test_the_donor_set_the_channels_and_the_exclusion_after_retrieval_now_pass() -> None:
    """The search runs them, so the guard that stood for them is gone."""
    p = _payload(
        donor_policy={"donor_set_id": str(uuid.uuid4()), "evidence_channels": ["truth"]},
        exclude_self_neighbour=True,
        self_exclusion="accession_after_retrieval",
    )
    _refuse_what_the_search_does_not_run_yet(p)


def test_the_space_beside_the_donor_fields_is_refused_alone() -> None:
    """One refusal, naming the one field still waiting and nothing that runs."""
    p = _payload(
        embedding_transform="whiten",
        donor_policy={"donor_set_id": "set", "evidence_channels": ["truth"]},
        exclude_self_neighbour=True,
        self_exclusion="accession_after_retrieval",
    )
    message = _refusal(p)
    assert "embedding_transform" in message
    assert message.count(_SPACE) == 1
    for runs in ("donor_set_id", "evidence_channels", "accession_after_retrieval"):
        assert runs not in message


@pytest.mark.parametrize(
    ("flag", "rule"),
    [
        (False, None),
        (True, None),
        (False, "none"),
        (True, "sequence"),
        (False, "sequence"),
        (True, "accession"),
        (False, "accession"),
        (True, "accession_after_retrieval"),
    ],
)
def test_every_exclusion_a_stored_payload_could_name_still_passes(
    flag: bool, rule: str | None
) -> None:
    _refuse_what_the_search_does_not_run_yet(
        _payload(exclude_self_neighbour=flag, self_exclusion=rule)
    )


def test_the_older_donor_restrictions_still_pass() -> None:
    p = _payload(
        donor_policy={
            "reviewed_only": True,
            "evidence_codes": ["EXP", "IDA"],
            "exclude_reference_prefixes": ["GO_REF:0000043"],
            "admitted_by_release": 211,
        }
    )
    _refuse_what_the_search_does_not_run_yet(p)


def test_the_refusal_comes_before_the_database_is_read() -> None:
    """``execute`` refuses with the session untouched and nothing emitted."""
    session = MagicMock()
    emit = MagicMock()
    payload = _fields(embedding_transform="center", _job_id=str(uuid.uuid4()))

    with pytest.raises(ValueError, match="embedding_transform"):
        PredictGOTermsOperation().execute(session, payload, emit=emit)

    assert session.mock_calls == []
    assert emit.mock_calls == []


def test_a_payload_that_names_none_of_them_goes_on_to_its_lookups() -> None:
    """The guard stands only where the new fields are: the old path reaches the store."""
    session = MagicMock()
    session.get.return_value = None
    payload = _fields(_job_id=str(uuid.uuid4()))

    with pytest.raises(ValueError, match="EmbeddingConfig .* not found"):
        PredictGOTermsOperation().execute(session, payload, emit=MagicMock())

    session.get.assert_called_once()


def test_the_contract_names_the_platform_channels_in_their_order() -> None:
    assert get_args(contract_payloads.EvidenceChannel) == CHANNELS
    assert get_args(Channel) == CHANNELS


def test_a_policy_keeps_its_channels_in_the_platform_order() -> None:
    """Two orders of one list are one policy, in the order ``CHANNELS`` gives."""
    given = list(reversed(CHANNELS))
    policy = contract_payloads.DonorPolicy(evidence_channels=given)
    assert tuple(policy.evidence_channels or ()) == CHANNELS

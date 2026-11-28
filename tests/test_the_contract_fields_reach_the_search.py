"""Every field protea-contracts 1.8.0 named on a predict payload reaches the search.

The release let a payload name the space the search runs in
(``embedding_transform``), a declared set of donors (``donor_policy.donor_set_id``),
the evidence channels a donation may come through
(``donor_policy.evidence_channels``) and the query leaving its own neighbourhood
after retrieval (``self_exclusion`` ``"accession_after_retrieval"``). The
coordinator refused each one until the search ran it, naming the change that
would; the last of those changes, the search in a declared space, removed the
refusal. What each field does is tested where it runs
(``tests/test_donor_set_and_channels_in_the_search.py``,
``tests/test_the_search_runs_in_a_declared_space.py``); here, that the
coordinator lets all four through to its lookups.

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
from protea.core.operations.predict_go_terms._coordinator import PredictGOTermsOperation


def _fields(**overrides: Any) -> dict[str, Any]:
    return {
        "embedding_config_id": str(uuid.uuid4()),
        "annotation_set_id": str(uuid.uuid4()),
        "ontology_snapshot_id": str(uuid.uuid4()),
        "query_accessions": ["P00001"],
        "_job_id": str(uuid.uuid4()),
        **overrides,
    }


@pytest.mark.parametrize(
    "named",
    [
        {"embedding_transform": "whiten"},
        {"donor_policy": {"donor_set_id": str(uuid.uuid4())}},
        {"donor_policy": {"evidence_channels": ["truth"]}},
        {"exclude_self_neighbour": True, "self_exclusion": "accession_after_retrieval"},
    ],
    ids=["space", "donor set", "channels", "exclusion after retrieval"],
)
def test_no_field_of_the_release_is_refused_before_the_lookups(named: dict[str, Any]) -> None:
    """The coordinator reaches the store for each, as it does for any payload."""
    session = MagicMock()
    session.get.return_value = None

    with pytest.raises(ValueError, match="EmbeddingConfig .* not found"):
        PredictGOTermsOperation().execute(session, _fields(**named), emit=MagicMock())

    session.get.assert_called_once()


def test_the_contract_names_the_platform_channels_in_their_order() -> None:
    assert get_args(contract_payloads.EvidenceChannel) == CHANNELS
    assert get_args(Channel) == CHANNELS


def test_a_policy_keeps_its_channels_in_the_platform_order() -> None:
    """Two orders of one list are one policy, in the order ``CHANNELS`` gives."""
    given = list(reversed(CHANNELS))
    policy = contract_payloads.DonorPolicy(evidence_channels=given)
    assert tuple(policy.evidence_channels or ()) == CHANNELS

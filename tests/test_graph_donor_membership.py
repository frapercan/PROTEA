"""The graph tells a bank restricted to corpus members from the unrestricted one.

``_policy_is_empty`` decides whether a bank ran under no policy at all, and the
graph counts and dates bank levels with it. A membership release is a policy:
a bank built from members at release 211 is a different level from the bank
that admits every protein its annotation set carries.
"""

from __future__ import annotations

import re

from protea.api.routers._graph_dates import _policy_is_empty
from protea.api.routers._graph_nodes import _BANK_FIELDS
from protea.api.routers._graph_reads import _Q_PREDICTION_SETS


def test_a_membership_release_is_a_policy() -> None:
    assert _policy_is_empty({}) is True
    assert _policy_is_empty({"donor_admitted_by_release": "211"}) is False


def test_the_bank_node_names_the_membership() -> None:
    assert "donor_admitted_by_release" in _BANK_FIELDS


def test_the_graph_reads_the_membership_it_names() -> None:
    selected = set(re.findall(r"AS\s+(\w+)", str(_Q_PREDICTION_SETS)))
    assert "donor_admitted_by_release" in selected

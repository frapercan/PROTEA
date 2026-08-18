"""Every knob the batch understands must survive the coordinator.

The coordinator serialises each batch by enumerating field names by hand. A knob
added to both payloads but forgotten in that list is accepted on the coordinator,
validated, persisted into its job payload, and then dropped, so the run reports
the setting it was given and behaves as though it were never set.

That is what happened to ``donor_policy``: a probe asking for an
experimental-evidence donor bank ran against an unfiltered one, and the only
visible symptom was that the filtered run returned MORE distinct donors than the
unfiltered one, which is impossible and was the thread that unravelled it.
``compute_protst`` was dropped the same way and nobody had noticed at all.

This test is deliberately structural rather than a list of known knobs: it
compares the two payload models and the serialiser, so a future field is covered
the day it is added rather than the day someone happens to look.
"""

from __future__ import annotations

import inspect
import re

from protea.core.operations.predict_go_terms import (
    PredictGOTermsBatchPayload,
    PredictGOTermsPayload,
    _coordinator,
)

#: Fields the batch accepts that the coordinator deliberately does not forward,
#: each with the reason. Adding a name here is a decision, which is the point:
#: it has to be argued for once instead of forgotten silently.
DELIBERATELY_NOT_FORWARDED: dict[str, str] = {}


def _forwarded_field_names() -> set[str]:
    """The keys the coordinator writes into a batch dispatch payload."""
    src = inspect.getsource(_coordinator)
    start = src.index('"""Serialise one batch')
    body = src[start : src.index("payload.update(", start)]
    return set(re.findall(r'"([a-z_0-9]+)":', body))


def test_no_shared_knob_is_dropped_between_coordinator_and_batch() -> None:
    shared = set(PredictGOTermsBatchPayload.model_fields) & set(
        PredictGOTermsPayload.model_fields
    )
    dropped = shared - _forwarded_field_names() - set(DELIBERATELY_NOT_FORWARDED)
    assert not dropped, (
        "these fields exist on both payloads but the coordinator never puts them "
        f"in the batch dispatch, so setting them does nothing and says nothing: "
        f"{sorted(dropped)}. Forward them in _batch_dispatch_payload, or add them "
        "to DELIBERATELY_NOT_FORWARDED with the reason."
    )


def test_the_two_fields_that_were_actually_lost_are_forwarded() -> None:
    """A named regression for the pair this test was written after."""
    forwarded = _forwarded_field_names()
    assert "donor_policy" in forwarded
    assert "compute_protst" in forwarded


def test_the_serialiser_is_still_where_this_test_thinks_it_is() -> None:
    """The structural check reads source, so it must fail loudly if that moves.

    Without this, a refactor that renames or relocates the serialiser turns the
    check above into a test that silently passes over nothing, which is the same
    failure mode it exists to catch.
    """
    names = _forwarded_field_names()
    assert "embedding_config_id" in names
    assert len(names) > 20


def test_the_dispatch_payload_survives_json() -> None:
    """Forwarding a field is not enough; it has to reach the other side.

    The batch payload is JSON-serialised onto AMQP. My first version of this fix
    forwarded ``p.donor_policy`` as the pydantic object, which is not JSON
    serialisable, and since the field defaults to a DonorPolicy instance rather
    than to None, EVERY predict_go_terms run failed at dispatch.

    The name-based check above passed that version happily, because the key was
    in the source. This test is the one that would have caught it: it builds the
    real payload and puts it through json.dumps, which is what the dispatcher
    does.
    """
    import json
    import uuid as _uuid

    from protea.core.operations.predict_go_terms import PredictGOTermsPayload
    from protea.core.operations.predict_go_terms._coordinator import (
        PredictGOTermsOperation,
    )

    p = PredictGOTermsPayload.model_validate(
        {
            "embedding_config_id": str(_uuid.uuid4()),
            "annotation_set_id": str(_uuid.uuid4()),
            "ontology_snapshot_id": str(_uuid.uuid4()),
            "query_accessions": ["P00001"],
        }
    )
    assert p.donor_policy is not None, (
        "this test assumes the field defaults to an object rather than to None; "
        "if that changed, the failure mode it guards has changed too"
    )
    msg = PredictGOTermsOperation._build_batch_message(
        p, _uuid.uuid4(), _uuid.uuid4(), ["P00001"], _RerankerDispatchStub()
    )
    json.dumps(msg)


class _RerankerDispatchStub:
    """Minimal stand-in: the serialiser only reads ``single`` and ``per_category``."""

    single = None
    per_category: dict = {}

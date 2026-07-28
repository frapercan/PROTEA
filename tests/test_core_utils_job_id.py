"""Unit tests for ``protea.core.utils.job_id_from_payload``.

The helper threads the worker-injected ``_job_id`` onto persisted result
rows (R0.1 reproducible-frame provenance: no more orphan ``job_id=None``
artifacts).
"""

from __future__ import annotations

import uuid

import pytest

from protea.core.utils import job_id_from_payload


def test_returns_uuid_when_present():
    jid = uuid.uuid4()
    assert job_id_from_payload({"_job_id": str(jid)}) == jid


def test_accepts_uuid_object_value():
    jid = uuid.uuid4()
    assert job_id_from_payload({"_job_id": jid}) == jid


def test_none_when_key_absent():
    assert job_id_from_payload({"evaluation_set_id": "x"}) is None


def test_none_when_value_empty():
    assert job_id_from_payload({"_job_id": ""}) is None
    assert job_id_from_payload({"_job_id": None}) is None


def test_none_when_payload_not_a_dict():
    assert job_id_from_payload(None) is None
    assert job_id_from_payload("not-a-dict") is None


def test_raises_on_malformed_uuid():
    with pytest.raises(ValueError):
        job_id_from_payload({"_job_id": "not-a-uuid"})

"""What a published number was measured with, said in words.

A prediction set has recorded how it was produced since #802, and nothing
could show it: the endpoint that served prediction sets returned UUIDs for
the model and nothing at all for the backend, the metric or which evidence
a donor needed. The data existed and was unreachable.
"""

from __future__ import annotations

from protea.api.routers.receipts import _donors

EXPERIMENTAL = ["EXP", "HDA", "HEP", "HGI", "HMP", "HTP", "IC", "IDA", "IEP", "IGI", "IMP", "IPI", "TAS"]


class TestDonorRegime:
    def test_says_electronic_annotations_were_excluded(self):
        # The difference a reader needs is not the thirteen codes, it is
        # whether IEA was in: that is the difference between two entirely
        # different claims about the method.
        out = _donors({"evidence_codes": EXPERIMENTAL, "reviewed_only": False})
        assert "no electronic" in out["regime"]

    def test_says_electronic_annotations_were_included(self):
        out = _donors({"evidence_codes": [*EXPERIMENTAL, "IEA"]})
        assert "electronic included" in out["regime"]

    def test_no_policy_means_every_annotation_rather_than_none(self):
        # A missing evidence list is not an empty one. It means the bank
        # was not filtered, which is the widest regime rather than the
        # narrowest, and reading it as empty would invert the claim.
        assert _donors(None)["regime"] == "every annotation in the bank"
        assert _donors({})["evidence_codes"] is None

    def test_the_codes_still_travel_for_a_reader_who_wants_them(self):
        assert _donors({"evidence_codes": EXPERIMENTAL})["evidence_codes"] == EXPERIMENTAL

    def test_reviewed_only_is_reported_and_defaults_to_false(self):
        assert _donors({"evidence_codes": []})["reviewed_only"] is False
        assert _donors({"evidence_codes": [], "reviewed_only": True})["reviewed_only"] is True

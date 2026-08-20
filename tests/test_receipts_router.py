"""What a published number was measured with, said in words.

A prediction set has recorded how it was produced since #802, and nothing
could show it: the endpoint that served prediction sets returned UUIDs for
the model and nothing at all for the backend, the metric or which evidence
a donor needed. The data existed and was unreachable.
"""

from __future__ import annotations

from protea.api.routers.receipts import _donors, _finished

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


class TestTheRunSaysWhetherItFinished:
    """A cancelled run leaves its written batches behind.

    The prediction set carries no mark saying it is partial, so a receipt
    read without this describes a half-written run in exactly the words it
    would use for a finished one. 131 of 258 predict jobs in this database
    left such a set, and one of them cost the project a night: a
    measurement built from a 1,024-protein remnant produced a candidate
    pool of 13.4 terms against the real 130, and a ceiling with its sign
    reversed.
    """

    def test_a_completed_job_is_finished(self):
        assert _finished({"status": "SUCCEEDED", "batches_done": 22, "batches_total": 22})

    def test_a_cancelled_job_is_not(self):
        assert not _finished({"status": "CANCELLED", "batches_done": 1, "batches_total": 22})

    def test_succeeded_with_missing_batches_is_not(self):
        # The verdict and the arithmetic are written by different code at
        # different times, so both are checked.
        assert not _finished({"status": "SUCCEEDED", "batches_done": 3, "batches_total": 22})

    def test_two_missing_counts_do_not_count_as_agreeing(self):
        # The defect this replaced elsewhere: comparing two nulls and
        # finding them equal is how a gate passes what it exists to stop.
        assert not _finished({"status": "SUCCEEDED", "batches_done": None, "batches_total": None})

    def test_no_job_is_unattributed_rather_than_unfinished(self):
        # A set from before the receipt existed is not a failed run.
        # Reporting False would accuse it of something the record cannot
        # say, and the panel draws False as a red warning.
        assert _finished(None) is None

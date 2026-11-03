"""A declared frame and a faithful one are different properties.

``declared`` says the record leaves a reader nothing to guess. It says nothing
about whether the numbers are right, and a frame can be perfectly declared and
compute something nobody else computes.

The campaign's headline result is the other property: on the long window, our
truth reproduces the laboratory's counter by counter. Until now the platform
could not state it. The comparison lived in
``tests/test_long_window_acceptance.py``, which is the right place to keep a
contract from rotting and the wrong place to keep a finding: a test says the
claim held on somebody's machine at some commit, and a reader of the platform
saw nothing.

Measured against the real contract on 2026-10-27 (``valid-211-227``, lab
commit ``654f241f090b``, 34 counters): 26 readable on both sides, 26 agreed,
0 disagreed, 8 are quantities a window does not keep. Those are the numbers
these tests are built around.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from protea.core._reference_counters import COUNTER_ALIASES, NOT_STORED, compare_counters
from protea.core.operations.verify_window_against_reference import (
    VerifyWindowAgainstReferencePayload,
    read_contract,
)

#: The long window's own counters, as the record holds them. Trimmed to the
#: pairs under test; the mapping's own completeness is checked separately.
_STATS = {
    "delta_proteins": 40531,
    "nk_proteins": 18268,
    "lk_proteins": 4667,
    "pk_proteins": 19713,
    "nk_annotations": 391694,
    "projected_entries": 2610,
    "projected_pairs": 3821,
    "new_not_unmapped": 19,
}
_REFERENCE = {
    "delta_proteins": 40531,
    "nk_proteins": 18268,
    "lk_proteins": 4667,
    "pk_proteins": 19713,
    "nk_pairs": 391694,
    "new_exp_projected": 2610,
    "new_exp_projected_pairs": 3821,
    "new_not_unmapped": 19,
}


class TestTheNamesAreWrittenOutAndNotGuessed:
    def test_the_aliases_are_not_identity(self) -> None:
        # The interesting half of the mapping: the reference counts pairs
        # where we count annotations, and the two words name one quantity.
        assert COUNTER_ALIASES["nk_pairs"] == "nk_annotations"
        assert COUNTER_ALIASES["known_pairs"] == "known_terms_count"
        assert COUNTER_ALIASES["new_exp_projected"] == "projected_entries"

    def test_a_counter_is_never_matched_by_resemblance(self) -> None:
        # ``nk_pairs`` and ``nk_proteins`` differ by a factor of twenty on the
        # long window. A mapping that paired either with ``nk_annotations`` by
        # substring would report agreement and disagreement with equal
        # confidence, so each pair is written out and this pins that they are
        # two distinct entries pointing at two distinct keys.
        assert COUNTER_ALIASES["nk_pairs"] != COUNTER_ALIASES["nk_proteins"]

    def test_what_we_do_not_store_is_named_with_its_reason(self) -> None:
        # Eight of the reference's counters are quantities a window does not
        # keep. Listed rather than dropped: a comparison that comes out 26 of
        # 26 because it skipped what it could not find is worse than one that
        # says 26 of 34 and names the eight.
        assert len(NOT_STORED) == 8
        for name, reason in NOT_STORED.items():
            assert reason and reason[0].islower(), name
        assert not set(NOT_STORED) & set(COUNTER_ALIASES)


class TestTheVerdictCarriesItsDenominator:
    def test_full_agreement(self) -> None:
        v = compare_counters(_REFERENCE, _STATS)
        assert (v.compared, v.agreed) == (8, 8)
        assert v.disagreements == ()
        assert v.reproduces is True

    def test_one_disagreement_names_both_values(self) -> None:
        # Not a boolean: a reader has to see what the two records said.
        v = compare_counters({**_REFERENCE, "nk_proteins": 18269}, _STATS)
        assert v.reproduces is False
        assert len(v.disagreements) == 1
        d = v.disagreements[0]
        assert (d.counter, d.reference, d.ours) == ("nk_proteins", 18269, 18268)

    def test_an_empty_intersection_does_not_reproduce_anything(self) -> None:
        """The trap. A comparison that read nothing agrees with everything.

        Without the ``compared > 0`` clause this would publish a reproduction
        claim over no counters at all, which is the most confident wrong
        number a surface can print.
        """
        v = compare_counters({"binding_dropped_new": 26056}, _STATS)
        assert v.compared == 0
        assert v.agreed == 0
        assert v.reproduces is False

    def test_a_counter_the_mapping_has_never_been_told_about_is_kept_apart(self) -> None:
        # ``absent`` is a decision about our record; ``unknown`` is a gap in
        # the mapping file. Mixing them would let the second hide inside the
        # first and the agreement count would quietly rise.
        v = compare_counters({**_REFERENCE, "una_metrica_nueva_suya": 7}, _STATS)
        assert v.unknown == ("una_metrica_nueva_suya",)
        assert "una_metrica_nueva_suya" not in v.absent

    def test_a_non_numeric_value_is_a_disagreement_and_not_a_crash(self) -> None:
        # The point of the comparison is to report, not to stop.
        v = compare_counters({**_REFERENCE, "nk_proteins": "dieciocho mil"}, _STATS)
        assert v.reproduces is False
        assert v.disagreements[0].counter == "nk_proteins"


class TestTheContractIsPinnedToItsBytes:
    def _write(self, tmp_path: Path, body: dict[str, Any]) -> tuple[Path, str]:
        path = tmp_path / "acceptance.json"
        path.write_text(json.dumps(body))
        return path, hashlib.sha256(path.read_bytes()).hexdigest()

    def test_the_declared_digest_is_verified_before_anything_is_read(self, tmp_path) -> None:
        path, digest = self._write(tmp_path, {"counts": {"delta_proteins": 1}})
        assert read_contract(path, digest)["counts"]["delta_proteins"] == 1

    def test_a_wrong_digest_refuses_and_names_both(self, tmp_path) -> None:
        # A claim is only worth the bytes it names. Comparing against a file
        # nobody pinned proves the two agreed about something, without saying
        # what.
        path, digest = self._write(tmp_path, {"counts": {"delta_proteins": 1}})
        with pytest.raises(ValueError, match="hashes to"):
            read_contract(path, "0" * 64)

    def test_a_missing_contract_refuses(self, tmp_path) -> None:
        with pytest.raises(ValueError, match="no reference contract"):
            read_contract(tmp_path / "nada.json", "0" * 64)

    def test_the_payload_refuses_a_digest_that_is_not_one(self) -> None:
        for bad in ("", "abc", "z" * 64):
            with pytest.raises(ValueError, match="64 hex"):
                VerifyWindowAgainstReferencePayload(
                    evaluation_set_id="w", contract_path="/x", expected_sha256=bad
                )


class TestTheFrameShowsTheClaim:
    def test_a_window_with_no_claim_shows_an_empty_list(self) -> None:
        # Empty and not absent: the field exists so a reader can tell "nothing
        # has been verified" from "this surface does not report it".
        from protea.api.routers.graph import _reproductions_of

        assert _reproductions_of({"reproductions": []}, {"id": "w"}) == []

    def test_a_claim_travels_with_its_denominator(self) -> None:
        from protea.api.routers.graph import _reproductions_of

        rows = [
            {
                "evaluation_set_id": "w",
                "reference_key": "valid-211-227",
                "reference_digest": "39439e33a423" + "0" * 52,
                "reference_commit": "654f241f090b41cd",
                "compared": 26,
                "agreed": 26,
                "disagreements": 0,
                "verified_on": "2026-10-27",
            }
        ]
        out = _reproductions_of({"reproductions": rows}, {"id": "w"})
        assert out[0]["compared"] == 26
        assert out[0]["agreed"] == 26
        assert out[0]["reference"] == "valid-211-227"
        assert out[0]["digest"] == "39439e33a423"
        assert out[0]["commit"] == "654f241f090b"

    def test_another_window_s_claim_does_not_appear(self) -> None:
        # The frame is about one window. A claim verified on a different one
        # printed here would be the single-field comparison again, this time
        # on the surface that exists to prevent it.
        from protea.api.routers.graph import _reproductions_of

        rows = [
            {
                "evaluation_set_id": "otra",
                "reference_key": "valid-211-227",
                "reference_digest": "x" * 64,
                "reference_commit": None,
                "compared": 26,
                "agreed": 26,
                "disagreements": 0,
                "verified_on": "2026-10-27",
            }
        ]
        assert _reproductions_of({"reproductions": rows}, {"id": "w"}) == []

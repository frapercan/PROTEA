"""Pinning an IA by set id is the traceable route, and it was the one that failed.

``_resolve_and_gate_ia`` resolved the table from
``information_accretion_set_id`` into a local path and then handed the band
guard ``p.ia_file`` -- a different field, empty whenever the set id was used.
So a cell that declared a band and pinned its IA the way the platform records
provenance (by row, with its sha256 verified after download) could never pass,
while one that named a bare path could.

Measured on this server 2026-10-27, scoring the long validation window with
``band: "v211"`` and ``information_accretion_set_id: 80bbc9e4-...``:

    Phantom-gap guard: cell declares band 'v211' but no IA artifact resolved
    (snapshot ia_url and payload ia_file are both unset).

The guard was right about what it could see and wrong about what had happened.
"""

from __future__ import annotations

import pytest

from protea.core.band_registry import (
    BandMismatchError,
    assert_band_consistency,
    ia_token,
    resolve_band,
)
from protea.core.operations.compute_information_accretion import ia_key_for

_IA_211 = "80bbc9e4-c037-4416-a06e-4982eba58008"
_IA_227 = "1aaff913-46b6-4c44-b7f7-13de0e4676c5"


class TestTheCanonicalKeyCarriesTheIdentity:
    def test_ia_key_for_builds_what_ia_token_reads(self) -> None:
        # The same builder compute_information_accretion writes artifacts with,
        # so the reference the guard sees and the object in the store cannot
        # drift apart.
        key = ia_key_for(_IA_211)
        assert key == f"information_accretion/{_IA_211}/IA.tsv"
        assert ia_token(key) == _IA_211

    def test_the_band_accepts_its_own_set_by_that_key(self) -> None:
        assert resolve_band("v211").accepts_ia_token(ia_token(ia_key_for(_IA_211)))

    def test_and_refuses_another_bands_set(self) -> None:
        assert not resolve_band("v211").accepts_ia_token(ia_token(ia_key_for(_IA_227)))

    def test_the_whole_check_passes_with_the_resolved_reference(self) -> None:
        band = assert_band_consistency(
            "v211",
            obo_version="releases/2022-07-01",
            ia_ref=ia_key_for(_IA_211),
        )
        assert band.name == "v211"

    def test_the_tempfile_name_would_NOT_pass(self) -> None:
        # This is the bug in one line: a downloaded copy is always ia.tsv, so
        # handing the guard the path instead of the source reference tokenises
        # to a name no band claims.
        assert ia_token("/tmp/xyz/ia.tsv") == "ia.tsv"
        with pytest.raises(BandMismatchError):
            assert_band_consistency(
                "v211", obo_version="releases/2022-07-01", ia_ref="/tmp/xyz/ia.tsv"
            )

    def test_a_cross_band_set_is_still_refused_end_to_end(self) -> None:
        # The fix must not widen the guard: v227's own table under a v211
        # declaration stays a refusal.
        with pytest.raises(BandMismatchError):
            assert_band_consistency(
                "v211",
                obo_version="releases/2022-07-01",
                ia_ref=ia_key_for(_IA_227),
            )

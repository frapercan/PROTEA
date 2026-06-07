"""Unit tests for the per-band (ontology snapshot, IA) registry.

Covers token extraction, band resolution, reverse lookups, and the
phantom-gap guard (cross-band snapshot / IA rejection + the never-IC=1 rule).
"""

from __future__ import annotations

from datetime import date

import pytest

from protea.core.band_registry import (
    BANDS,
    Band,
    BandMismatchError,
    CutoffViolationError,
    assert_band_consistency,
    assert_release_not_after_cutoff,
    band_for_ia_token,
    band_for_obo_version,
    cutoff_violations_for_cell,
    ia_token,
    manifest_cutoff_violations,
    obo_release_date,
    resolve_band,
)


class TestIaToken:
    def test_basename_of_path(self):
        assert ia_token("/path/to/lafa_t0_Sep_2025/IA.tsv") == "IA.tsv"

    def test_basename_of_url(self):
        assert ia_token("https://example.com/data/IA_cafa6.tsv") == "IA_cafa6.tsv"

    def test_strips_query_and_fragment(self):
        assert ia_token("https://host/IA.tsv?v=2#frag") == "IA.tsv"

    def test_none_for_empty(self):
        assert ia_token(None) is None
        assert ia_token("") is None

    def test_trailing_slash(self):
        assert ia_token("/path/IA.tsv/") == "IA.tsv"


class TestResolveBand:
    def test_direct_name(self):
        assert resolve_band("v226").name == "v226"
        assert resolve_band("v227").name == "v227"

    def test_extract_from_dataset_name(self):
        assert resolve_band("bench-v1-K5-v226-lineage-prostt5").name == "v226"
        assert resolve_band("bench-v1-K3-v227-lineage-esm2").name == "v227"

    def test_unknown_raises(self):
        with pytest.raises(BandMismatchError, match="Unknown band"):
            resolve_band("v999")

    def test_no_band_token_raises(self):
        with pytest.raises(BandMismatchError):
            resolve_band("no-band-here")


class TestReverseLookups:
    def test_obo_version_lookup(self):
        # v226 canonical obo (congruent: latest GO release on/before goa226 t0)
        assert band_for_obo_version("releases/2025-03-16") == "v226"
        # v227 canonical obo (LAFA's own go-basic.obo data-version)
        assert band_for_obo_version("releases/2025-07-22") == "v227"

    def test_unknown_obo_version(self):
        assert band_for_obo_version("releases/1999-01-01") is None
        assert band_for_obo_version(None) is None

    def test_ia_token_lookup(self):
        assert band_for_ia_token("IA_cafa6.tsv") == "v226"
        assert band_for_ia_token("IA.tsv") == "v227"

    def test_ia_token_case_insensitive(self):
        assert band_for_ia_token("ia.tsv") == "v227"

    def test_unknown_ia_token(self):
        assert band_for_ia_token("random.tsv") is None
        assert band_for_ia_token(None) is None


class TestAssertBandConsistency:
    def test_v227_canonical_pair_passes(self):
        band = assert_band_consistency(
            "v227",
            obo_version="releases/2025-07-22",
            ia_ref="/data/lafa_t0_Sep_2025/IA.tsv",
        )
        assert band.name == "v227"

    def test_v226_canonical_pair_passes(self):
        band = assert_band_consistency(
            "v226",
            obo_version="releases/2025-03-16",
            ia_ref="https://host/IA_cafa6.tsv",
        )
        assert band.name == "v226"

    def test_dataset_name_band_passes(self):
        band = assert_band_consistency(
            "bench-v1-K5-v227-lineage-prostt5",
            obo_version="releases/2025-07-22",
            ia_ref="IA.tsv",
        )
        assert band.name == "v227"

    def test_cross_band_snapshot_rejected(self):
        # declares v227 but pivot snapshot is the v226 ontology release
        with pytest.raises(BandMismatchError, match="obo_version"):
            assert_band_consistency(
                "v227",
                obo_version="releases/2025-03-16",
                ia_ref="IA.tsv",
            )

    def test_cross_band_ia_rejected(self):
        # declares v227 but the IA is the v226/generic CAFA6 table
        with pytest.raises(BandMismatchError, match="IA artifact"):
            assert_band_consistency(
                "v227",
                obo_version="releases/2025-07-22",
                ia_ref="/data/IA_cafa6.tsv",
            )

    def test_cross_band_ia_message_names_owner(self):
        with pytest.raises(BandMismatchError, match="belongs to band 'v226'"):
            assert_band_consistency(
                "v227",
                obo_version="releases/2025-07-22",
                ia_ref="IA_cafa6.tsv",
            )

    def test_missing_ia_rejected_never_ic1(self):
        # a band-declared cell must never fall back to uniform IC=1
        with pytest.raises(BandMismatchError, match="IC=1"):
            assert_band_consistency(
                "v226",
                obo_version="releases/2025-03-16",
                ia_ref=None,
            )

    def test_unknown_band_rejected(self):
        with pytest.raises(BandMismatchError, match="Unknown band"):
            assert_band_consistency(
                "v999",
                obo_version="releases/2025-03-16",
                ia_ref="IA_cafa6.tsv",
            )


class TestRegistryWellFormed:
    """The CI-guard invariants, asserted here too so a regression fails the
    fast unit suite, not only the lint job."""

    def test_no_shared_obo_version(self):
        seen: dict[str, str] = {}
        for name, band in BANDS.items():
            for ver in band.obo_versions:
                assert ver not in seen, f"{ver} shared by {seen.get(ver)} and {name}"
                seen[ver] = name

    def test_no_shared_ia_token(self):
        seen: dict[str, str] = {}
        for name, band in BANDS.items():
            for tok in band.ia_tokens:
                key = tok.lower()
                assert key not in seen, f"{tok} shared by {seen.get(key)} and {name}"
                seen[key] = name

    def test_every_band_pins_both(self):
        for name, band in BANDS.items():
            assert band.obo_versions, f"{name} pins no obo_version"
            assert band.ia_tokens, f"{name} pins no IA token"

    def test_band_is_frozen(self):
        band = Band(name="x", obo_versions=frozenset({"a"}), ia_tokens=frozenset({"b"}))
        with pytest.raises((AttributeError, TypeError)):
            band.name = "y"  # type: ignore[misc]


class TestOboReleaseDate:
    def test_releases_token(self):
        assert obo_release_date("releases/2025-07-22") == date(2025, 7, 22)

    def test_bare_date(self):
        assert obo_release_date("2026-01-23") == date(2026, 1, 23)

    def test_url(self):
        assert obo_release_date(
            "http://purl.obolibrary.org/obo/go/releases/2025-03-16/go-basic.obo"
        ) == date(2025, 3, 16)

    def test_no_date_returns_none(self):
        assert obo_release_date("go-basic") is None
        assert obo_release_date(None) is None
        assert obo_release_date("") is None

    def test_invalid_date_returns_none(self):
        assert obo_release_date("releases/2025-13-40") is None


class TestCutoffGuard:
    """No-future-data ordering guard, independent of band set membership."""

    def test_canonical_obo_passes(self):
        # v227's congruent OBO (2025-07-22) precedes its t0 (2025-09-04).
        assert_release_not_after_cutoff(
            "v227", artifact="ontology snapshot", release_ref="releases/2025-07-22"
        )

    def test_future_release_flagged_v227(self):
        # The exact phantom-gap case: v227 scored against a 2026 ontology.
        with pytest.raises(CutoffViolationError, match="No-future-data"):
            assert_release_not_after_cutoff(
                "v227", artifact="ontology snapshot", release_ref="releases/2026-01-23"
            )

    def test_future_release_flagged_v226(self):
        # v226 cutoff is 2025-05-03; a v227-band OBO (2025-07-22) is future.
        with pytest.raises(CutoffViolationError):
            assert_release_not_after_cutoff(
                "v226", artifact="ontology snapshot", release_ref="releases/2025-07-22"
            )

    def test_no_date_ref_is_noop(self):
        # An undated reference cannot be ordered; band guard covers it.
        assert_release_not_after_cutoff(
            "v227", artifact="ontology snapshot", release_ref="go-basic"
        )

    def test_band_without_cutoff_is_noop(self):
        band = Band(
            name="z",
            obo_versions=frozenset({"releases/2099-01-01"}),
            ia_tokens=frozenset({"IA.tsv"}),
            t0_cutoff=None,
        )
        assert band.t0_cutoff is None

    def test_extract_band_from_dataset_name(self):
        with pytest.raises(CutoffViolationError):
            assert_release_not_after_cutoff(
                "bench-v1-K5-v227-lineage-prostt5",
                artifact="ontology snapshot",
                release_ref="releases/2026-01-23",
            )

    def test_collect_violations_aggregates(self):
        viols = cutoff_violations_for_cell(
            "v227",
            obo_version="releases/2026-01-23",
            ia_ref="/data/lafa_t0_Sep_2025/IA.tsv",
            extra_refs={"KNN corpus": "releases/2027-02-02"},
        )
        # OBO and the future corpus violate; the dated IA path (Sep 2025) does
        # not (its embedded date is 2025-09, on the cutoff month, not after).
        assert len(viols) == 2
        assert any("ontology snapshot" in v for v in viols)
        assert any("KNN corpus" in v for v in viols)

    def test_collect_violations_clean_cell(self):
        assert cutoff_violations_for_cell(
            "v227",
            obo_version="releases/2025-07-22",
            ia_ref="/data/lafa_t0_Sep_2025/IA.tsv",
        ) == []

    def test_every_pinned_band_self_consistent(self):
        # Every band's canonical OBO must not post-date its own cutoff.
        for name, band in BANDS.items():
            if band.t0_cutoff is None:
                continue
            for obo in band.obo_versions:
                released = obo_release_date(obo)
                if released is not None:
                    assert released <= band.t0_cutoff, (
                        f"band {name} pins future OBO {obo}"
                    )


class TestManifestCutoffViolations:
    """Bundle-manifest verification for the single-cutoff inference path."""

    def test_clean_bundle_passes(self):
        manifest = {
            "cutoff": "v227",
            "obo_version": "releases/2025-07-22",
        }
        assert manifest_cutoff_violations(manifest) == []

    def test_future_obo_in_bundle_flagged(self):
        # The export-side analogue of the phantom-gap case: a bundle that
        # froze a 2026 ontology for the v227 band.
        manifest = {
            "cutoff": "v227",
            "obo_version": "releases/2026-01-23",
        }
        viols = manifest_cutoff_violations(manifest)
        assert len(viols) == 1
        assert "ontology snapshot" in viols[0]

    def test_legacy_cutoff_version_key_resolved(self):
        # Older bundles record the band under cutoff_version, not cutoff.
        manifest = {
            "cutoff_version": "v226",
            "obo_version": "releases/2025-07-22",
        }
        # v226 cutoff is 2025-05-03; a July OBO is future.
        assert len(manifest_cutoff_violations(manifest)) == 1

    def test_extra_release_refs_checked(self):
        manifest = {
            "cutoff": "v227",
            "obo_version": "releases/2025-07-22",
            "knn_corpus_release": "releases/2027-02-02",
        }
        viols = manifest_cutoff_violations(manifest)
        assert len(viols) == 1
        assert "KNN candidate corpus" in viols[0]

    def test_unknown_band_is_noop(self):
        # A free-text cutoff that resolves to no band cannot be ordered.
        assert manifest_cutoff_violations(
            {"cutoff_version": "freeform-label", "obo_version": "releases/2099-01-01"}
        ) == []

    def test_no_cutoff_key_is_noop(self):
        assert manifest_cutoff_violations({"obo_version": "releases/2099-01-01"}) == []

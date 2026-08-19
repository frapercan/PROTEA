"""The per-aspect disk caches belong to a donor policy, like the pool they index.

The unified reference pool has been keyed by the donor discriminator since the
policy existed. The three per-aspect artefacts derived from it (the index array,
and the annotation CSR it addresses) were not, so a run under a restricted
policy read the aspect index built for the permissive pool.

The two pools are not the same size, so this raised: 556,306 accessions indexed,
86,068 loaded. That is the lucky shape of the bug. Had the sizes agreed, the
index would have addressed the wrong proteins and every donated annotation would
have been silently misattributed, which is the failure these tests exist for.
"""

from __future__ import annotations

import uuid

import numpy as np
import pytest
from protea_contracts.payloads import DonorPolicy

from protea.core import disk_cache as disk_cache_module
from protea.core.disk_cache import (
    _aspect_index_path,
    _aspect_index_pool_path,
    _build_anno_csr,
    _load_anno_csr_from_disk,
    _save_anno_csr_to_disk,
    pool_fingerprint,
)
from protea.core.operations.predict_go_terms._batch_op_reference import (
    _check_index_addresses_pool,
    _find_missing_aspects,
    _PoolRequest,
)

CFG = uuid.uuid4()
ANN = uuid.uuid4()
EXPERIMENTAL = DonorPolicy(evidence_codes=["EXP", "IDA"])


class TestTheCacheIdentityCarriesThePolicy:
    def test_a_restricted_run_does_not_read_the_permissive_index(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        """The regression: the permissive index existed, so the restricted run
        believed its aspect was cached and loaded someone else's index."""
        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        permissive = _aspect_index_path(CFG, ANN, "P")
        permissive.parent.mkdir(parents=True, exist_ok=True)
        np.save(permissive, np.arange(500, dtype=np.int32))

        missing = _find_missing_aspects(_PoolRequest(CFG, ANN, EXPERIMENTAL))

        assert "P" in missing

    def test_the_two_policies_do_not_share_an_index_path(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        permissive = _aspect_index_path(CFG, ANN, "P")
        restricted = _aspect_index_path(CFG, ANN, "P", EXPERIMENTAL.cache_discriminator())
        assert permissive != restricted

    def test_the_annotation_csr_is_separated_too(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        """The CSR is positional against the pool, so it needs the same key.

        Asserted through save/load rather than on the path, because the CSR is
        the artefact whose mismatch would not raise.
        """
        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        from protea.core.disk_cache import _build_anno_csr

        csr = _build_anno_csr(
            ["R1"], {"R1": [{"go_term_id": 1, "qualifier": "", "evidence_code": "IEA"}]}
        )
        _save_anno_csr_to_disk(CFG, ANN, "P", csr)

        assert _load_anno_csr_from_disk(CFG, ANN, "P") is not None
        assert (
            _load_anno_csr_from_disk(CFG, ANN, "P", EXPERIMENTAL.cache_discriminator())
            is None
        )


class TestTheBackstopIdentifiesThePoolRatherThanMeasuringIt:
    """A length check is the original bug promoted to a guard: it only fires
    when the two pools happen to differ in size. These pin the contents."""

    POOL = ["a", "b", "c"]

    def test_the_index_built_against_this_pool_is_accepted(self) -> None:
        _check_index_addresses_pool(
            np.array([0, 2], dtype=np.int32), self.POOL, "P", pool_fingerprint(self.POOL)
        )

    def test_an_index_from_a_different_pool_of_the_SAME_SIZE_raises(self) -> None:
        """The case a size check cannot see, and the one that scores plausibly."""
        other = pool_fingerprint(["x", "y", "z"])
        assert len(other) == len(pool_fingerprint(self.POOL))
        with pytest.raises(ValueError, match="was not built against this pool"):
            _check_index_addresses_pool(np.array([0, 1], dtype=np.int32), self.POOL, "P", other)

    def test_a_donor_built_index_served_permissively_raises(self) -> None:
        """The shape found on the desktop: indices that address a PREFIX of a
        larger pool, so they never overrun and never raised before."""
        donor_pool = self.POOL[:2]
        with pytest.raises(ValueError, match="was not built against this pool"):
            _check_index_addresses_pool(
                np.array([0, 1], dtype=np.int32), self.POOL, "P", pool_fingerprint(donor_pool)
            )

    def test_an_index_that_overruns_still_raises(self) -> None:
        with pytest.raises(ValueError, match="was not built against this pool"):
            _check_index_addresses_pool(
                np.array([0, 7], dtype=np.int32), self.POOL, "P", pool_fingerprint(["a"])
            )

    def test_an_index_with_no_recorded_pool_raises(self) -> None:
        """Written before fingerprints existed: unverifiable, so rebuilt."""
        with pytest.raises(ValueError, match="no fingerprint"):
            _check_index_addresses_pool(np.array([0], dtype=np.int32), self.POOL, "P", None)

    def test_an_empty_index_still_has_to_match_its_pool(self) -> None:
        _check_index_addresses_pool(np.array([], dtype=np.int32), [], "P", pool_fingerprint([]))


class TestTheFingerprintItself:
    def test_it_separates_pools_of_equal_length(self) -> None:
        assert pool_fingerprint(["a", "b"]) != pool_fingerprint(["a", "c"])

    def test_it_does_not_collide_on_concatenation(self) -> None:
        """Without a separator, ['a','b'] and ['ab'] would hash alike."""
        assert pool_fingerprint(["a", "b"]) != pool_fingerprint(["ab"])

    def test_order_matters_because_the_index_is_positional(self) -> None:
        assert pool_fingerprint(["a", "b"]) != pool_fingerprint(["b", "a"])


class TestAnIndexWithoutItsFingerprintIsRebuilt:
    def test_missing_sidecar_makes_the_aspect_missing(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        idx = _aspect_index_path(CFG, ANN, "P")
        idx.parent.mkdir(parents=True, exist_ok=True)
        np.save(idx, np.arange(3, dtype=np.int32))
        assert "P" in _find_missing_aspects(_PoolRequest(CFG, ANN))

    def test_an_index_built_against_another_pool_is_rebuilt_not_raised_on(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        """The pool grows whenever proteins are ingested, so a stale index is
        ordinary. It should cost one query, not a manual deletion before
        anything can run. The guard stays for what this does not catch."""
        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        idx = _aspect_index_path(CFG, ANN, "P")
        idx.parent.mkdir(parents=True, exist_ok=True)
        np.save(idx, np.arange(3, dtype=np.int32))
        _aspect_index_pool_path(CFG, ANN, "P").write_text("stale", encoding="utf-8")

        missing = _find_missing_aspects(_PoolRequest(CFG, ANN), pool_fingerprint(["a", "b", "c"]))

        assert "P" in missing

    def test_without_a_pool_in_hand_the_fingerprint_is_not_compared(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        """A caller with no pool loaded cannot compare, and must not guess.

        The same on-disk state is missing when a pool contradicts it and
        present when no pool is supplied, which is the whole difference.
        """
        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        idx = _aspect_index_path(CFG, ANN, "P")
        idx.parent.mkdir(parents=True, exist_ok=True)
        np.save(idx, np.arange(2, dtype=np.int32))
        _aspect_index_pool_path(CFG, ANN, "P").write_text("whatever", encoding="utf-8")
        csr = _build_anno_csr(
            ["R1"], {"R1": [{"go_term_id": 1, "qualifier": "", "evidence_code": "IEA"}]}
        )
        _save_anno_csr_to_disk(CFG, ANN, "P", csr)

        assert "P" not in _find_missing_aspects(_PoolRequest(CFG, ANN))
        assert "P" in _find_missing_aspects(_PoolRequest(CFG, ANN), pool_fingerprint(["a"]))

    def test_the_sidecar_is_keyed_by_donor_policy_too(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        assert _aspect_index_pool_path(CFG, ANN, "P") != _aspect_index_pool_path(
            CFG, ANN, "P", EXPERIMENTAL.cache_discriminator()
        )

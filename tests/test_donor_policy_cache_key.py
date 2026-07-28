"""The donor policy must reach the reference-cache key.

The reference pool is cached on disk and keyed by configuration and annotation
set. Anything that changes what a cached pool CONTAINS has to change its key,
or one pool is served under another's name with nothing to notice. This cache
has already been measured mispairing silently once, so these tests pin the
property rather than trusting the call sites.
"""

from __future__ import annotations

import uuid

import pytest
from protea_contracts.payloads import DonorPolicy

from protea.core.disk_cache import (
    _anno_disk_cache_paths,
    _aspect_index_path,
    _cache_key,
    _disk_cache_paths,
)

CFG = uuid.UUID("11111111-1111-1111-1111-111111111111")
ANN = uuid.UUID("22222222-2222-2222-2222-222222222222")


def _disc(**kwargs: object) -> str:
    return DonorPolicy(**kwargs).cache_discriminator()  # type: ignore[arg-type]


class TestThePermissivePolicyChangesNothing:
    def test_it_keeps_the_historical_key(self) -> None:
        """Caches built before policies existed stay valid."""
        assert _cache_key(CFG, ANN, _disc()) == f"{CFG}__{ANN}"

    def test_it_keeps_the_historical_paths(self) -> None:
        with_policy = _disk_cache_paths(CFG, ANN, _disc())
        without = _disk_cache_paths(CFG, ANN)
        assert with_policy == without


class TestARestrictedPolicyChangesTheKey:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"reviewed_only": True},
            {"evidence_codes": ["EXP"]},
            {"exclude_reference_prefixes": ["GO_REF:0000002"]},
        ],
    )
    def test_every_restriction_moves_the_key(self, kwargs: dict) -> None:
        assert _cache_key(CFG, ANN, _disc(**kwargs)) != _cache_key(CFG, ANN)

    def test_different_policies_never_collide(self) -> None:
        keys = {
            _cache_key(CFG, ANN, _disc()),
            _cache_key(CFG, ANN, _disc(reviewed_only=True)),
            _cache_key(CFG, ANN, _disc(evidence_codes=["EXP"])),
            _cache_key(CFG, ANN, _disc(evidence_codes=["IEA"])),
            _cache_key(CFG, ANN, _disc(reviewed_only=True, evidence_codes=["EXP"])),
        }
        assert len(keys) == 5


class TestEveryPathBuilderHonoursThePolicy:
    """The key used to be spelled out per builder, so this checks all of them.

    A builder that ignored the policy would write one policy's arrays into
    another's slot, which is the exact shape of the mispairing already
    observed on this cache.
    """

    def test_unified_pool_paths(self) -> None:
        a = _disk_cache_paths(CFG, ANN, _disc(reviewed_only=True))
        b = _disk_cache_paths(CFG, ANN)
        assert a != b

    def test_aspect_index_path(self) -> None:
        a = _aspect_index_path(CFG, ANN, "P", _disc(reviewed_only=True))
        b = _aspect_index_path(CFG, ANN, "P")
        assert a != b

    def test_annotation_csr_paths(self) -> None:
        a = _anno_disk_cache_paths(CFG, ANN, "P", _disc(reviewed_only=True))
        b = _anno_disk_cache_paths(CFG, ANN, "P")
        assert a != b

    def test_the_aspect_still_separates_within_one_policy(self) -> None:
        d = _disc(reviewed_only=True)
        assert _aspect_index_path(CFG, ANN, "P", d) != _aspect_index_path(CFG, ANN, "F", d)


class TestFreshnessLooksAtTheKeyItWillRead:
    def test_a_missing_policy_specific_cache_is_not_fresh(self, tmp_path, monkeypatch) -> None:
        """Freshness must not report a permissive cache as covering a restricted one."""
        import protea.core.disk_cache as dc

        monkeypatch.setattr(dc, "_DISK_CACHE_DIR", tmp_path)
        emb, acc = dc._disk_cache_paths(CFG, ANN)
        emb.write_bytes(b"x")
        acc.write_bytes(b"x")
        assert dc._is_cache_fresh(CFG, ANN, 3600) is True
        assert dc._is_cache_fresh(CFG, ANN, 3600, _disc(reviewed_only=True)) is False


class TestReadAndWriteAgreeOnTheKey:
    """The asymmetry that would make this cache worse than absent.

    If the write ignored the policy while the read honoured it, a filtered
    pool would be stored under the unfiltered name: every later read of the
    unfiltered pool would silently receive someone else's filtered one, and
    every read of the filtered pool would miss forever.
    """

    def test_a_restricted_pool_round_trips_under_its_own_key(
        self, tmp_path, monkeypatch
    ) -> None:
        import numpy as np

        import protea.core.disk_cache as dc

        monkeypatch.setattr(dc, "_DISK_CACHE_DIR", tmp_path)
        disc = _disc(reviewed_only=True)

        dc._save_to_disk_cache(CFG, ANN, ["P00001"], np.zeros((1, 4), dtype=np.float16), disc)

        restricted = dc._load_from_disk_cache(CFG, ANN, donor_discriminator=disc)
        assert restricted is not None
        assert restricted["accessions"] == ["P00001"]

        # And it must NOT be visible as the unfiltered pool.
        permissive = dc._load_from_disk_cache(CFG, ANN)
        assert permissive is None

    def test_two_policies_do_not_overwrite_each_other(self, tmp_path, monkeypatch) -> None:
        import numpy as np

        import protea.core.disk_cache as dc

        monkeypatch.setattr(dc, "_DISK_CACHE_DIR", tmp_path)
        dc._save_to_disk_cache(
            CFG, ANN, ["REVIEWED"], np.zeros((1, 4), dtype=np.float16), _disc(reviewed_only=True)
        )
        dc._save_to_disk_cache(
            CFG, ANN, ["ANY"], np.zeros((1, 4), dtype=np.float16), _disc()
        )
        a = dc._load_from_disk_cache(CFG, ANN, donor_discriminator=_disc(reviewed_only=True))
        b = dc._load_from_disk_cache(CFG, ANN)
        assert a is not None and b is not None
        assert a["accessions"] == ["REVIEWED"]
        assert b["accessions"] == ["ANY"]

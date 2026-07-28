"""Anything that changes what a cached pool contains must change its key.

The reference pool is cached on disk under a name built from the embedding
configuration and the annotation set. That name is the cache's whole notion of
identity, so a pool built under a restriction that the name does not mention
would be served to a reader that asked for the unrestricted one, with nothing
to notice. This cache has already been measured mispairing silently once.

The restriction itself arrives as an opaque discriminator string, so these
tests use plain strings and stay independent of whichever payload happens to
produce them. What is pinned here is the cache's behaviour: that the
discriminator reaches the key, that it reaches every path built from that key,
and that reads and writes agree on it.
"""

from __future__ import annotations

import uuid

import numpy as np
import pytest

from protea.core.disk_cache import (
    RefPoolKey,
    _anno_disk_cache_paths,
    _aspect_index_path,
    _cache_key,
    _disk_cache_paths,
)

CFG = uuid.UUID("11111111-1111-1111-1111-111111111111")
ANN = uuid.UUID("22222222-2222-2222-2222-222222222222")

# Representative of what a donor policy serialises to: several fields, with
# separators that are not safe to drop into a filename unhashed.
REVIEWED = "reviewed_only=True|evidence_codes=EXP,IDA|exclude=GO_REF"
EVIDENCE = "reviewed_only=False|evidence_codes=EXP|exclude="


class TestTheEmptyDiscriminatorChangesNothing:
    """Pools cached before restrictions existed have to stay valid."""

    def test_it_keeps_the_historical_key(self) -> None:
        assert _cache_key(CFG, ANN, "") == f"{CFG}__{ANN}"

    def test_it_keeps_the_historical_paths(self) -> None:
        assert _disk_cache_paths(CFG, ANN, "") == _disk_cache_paths(CFG, ANN)


class TestARestrictionMovesTheKey:
    @pytest.mark.parametrize("disc", [REVIEWED, EVIDENCE])
    def test_the_key_is_not_the_unrestricted_one(self, disc: str) -> None:
        assert _cache_key(CFG, ANN, disc) != _cache_key(CFG, ANN, "")

    def test_two_restrictions_never_collide(self) -> None:
        assert _cache_key(CFG, ANN, REVIEWED) != _cache_key(CFG, ANN, EVIDENCE)

    def test_the_key_stays_filename_safe(self) -> None:
        """The discriminator carries separators; the key must not."""
        key = _cache_key(CFG, ANN, REVIEWED)
        assert not set(key) & set("|=,/\\ ")

    def test_the_key_stays_bounded_however_long_the_discriminator_grows(self) -> None:
        short = _cache_key(CFG, ANN, "a=1")
        long = _cache_key(CFG, ANN, "a=1|" + "b=2|" * 500)
        assert len(short) == len(long)


class TestEveryPathBuilderHonoursTheDiscriminator:
    """One forgotten builder is one silently mispaired file."""

    def test_unified_pool_paths(self) -> None:
        assert _disk_cache_paths(CFG, ANN, REVIEWED) != _disk_cache_paths(CFG, ANN)

    def test_aspect_index_path(self) -> None:
        assert _aspect_index_path(CFG, ANN, "P", REVIEWED) != _aspect_index_path(CFG, ANN, "P")

    def test_annotation_csr_paths(self) -> None:
        assert _anno_disk_cache_paths(CFG, ANN, "P", REVIEWED) != _anno_disk_cache_paths(
            CFG, ANN, "P"
        )

    def test_the_aspect_still_separates_within_one_restriction(self) -> None:
        assert _aspect_index_path(CFG, ANN, "P", REVIEWED) != _aspect_index_path(
            CFG, ANN, "F", REVIEWED
        )


class TestReadAndWriteAgreeOnTheKey:
    """The asymmetry that would make this cache worse than absent.

    If the write ignored the discriminator while the read honoured it, a
    restricted pool would be stored under the unrestricted name: every later
    read of the unrestricted pool would silently receive someone else's
    restricted one, and every read of the restricted pool would miss forever.
    """

    def test_a_restricted_pool_round_trips_under_its_own_key(self, tmp_path, monkeypatch) -> None:
        import protea.core.disk_cache as dc

        monkeypatch.setattr(dc, "_DISK_CACHE_DIR", tmp_path)
        dc._save_to_disk_cache(CFG, ANN, ["P00001"], np.zeros((1, 4), dtype=np.float16), REVIEWED)

        restricted = dc._load_from_disk_cache(CFG, ANN, donor_discriminator=REVIEWED)
        assert restricted is not None
        assert restricted["accessions"] == ["P00001"]

        # And it must NOT be visible as the unrestricted pool.
        assert dc._load_from_disk_cache(CFG, ANN) is None

    def test_two_restrictions_do_not_overwrite_each_other(self, tmp_path, monkeypatch) -> None:
        import protea.core.disk_cache as dc

        monkeypatch.setattr(dc, "_DISK_CACHE_DIR", tmp_path)
        dc._save_to_disk_cache(
            CFG, ANN, ["RESTRICTED"], np.zeros((1, 4), dtype=np.float16), REVIEWED
        )
        dc._save_to_disk_cache(CFG, ANN, ["ANY"], np.zeros((1, 4), dtype=np.float16), "")

        a = dc._load_from_disk_cache(CFG, ANN, donor_discriminator=REVIEWED)
        b = dc._load_from_disk_cache(CFG, ANN)
        assert a is not None and b is not None
        assert a["accessions"] == ["RESTRICTED"]
        assert b["accessions"] == ["ANY"]


class TestFreshnessLooksAtTheKeyItWillRead:
    """A freshness probe on the wrong file skips validation on the wrong pool."""

    def test_a_missing_restricted_cache_is_not_fresh(self, tmp_path, monkeypatch) -> None:
        import protea.core.disk_cache as dc

        monkeypatch.setattr(dc, "_DISK_CACHE_DIR", tmp_path)
        dc._save_to_disk_cache(CFG, ANN, ["ANY"], np.zeros((1, 4), dtype=np.float16), "")

        assert dc._is_cache_fresh(CFG, ANN, 3600)
        assert not dc._is_cache_fresh(CFG, ANN, 3600, REVIEWED)


class TestTheLoaderCarriesTheKeyEndToEnd:
    """The property the key type exists to make unbreakable.

    The classes above exercise the read and the write helpers directly. This
    one drives the loader that orchestrates them, because the defect being
    guarded against is not a wrong helper but a call site that threads the
    discriminator into some of them and not the rest. Passing the identity as
    one value is what makes that impossible to express.
    """

    def test_a_restricted_pool_is_written_and_reread_under_its_own_key(
        self, tmp_path, monkeypatch
    ) -> None:
        import protea.core.disk_cache as dc

        monkeypatch.setattr(dc, "_DISK_CACHE_DIR", tmp_path)
        key = RefPoolKey(CFG, ANN, REVIEWED)
        calls: list[str] = []

        def db_loader() -> tuple[list[str], np.ndarray]:
            calls.append("db")
            return ["P00001"], np.zeros((1, 4), dtype=np.float16)

        accs, _ = dc._load_reference_pool_cached(key, db_loader, expected_count=1)
        assert accs == ["P00001"]
        assert calls == ["db"]  # miss on the first pass

        emb_path, _acc_path = dc._disk_cache_paths(*key)
        assert emb_path.exists(), "the pool was not written under its own key"

        accs2, _ = dc._load_reference_pool_cached(key, db_loader, expected_count=1)
        assert accs2 == ["P00001"]
        assert calls == ["db"], "the second pass should have hit the restricted cache"

    def test_the_unrestricted_pool_never_sees_the_restricted_one(
        self, tmp_path, monkeypatch
    ) -> None:
        import protea.core.disk_cache as dc

        monkeypatch.setattr(dc, "_DISK_CACHE_DIR", tmp_path)
        calls: list[str] = []

        def restricted_loader() -> tuple[list[str], np.ndarray]:
            calls.append("restricted")
            return ["RESTRICTED"], np.zeros((1, 4), dtype=np.float16)

        def unrestricted_loader() -> tuple[list[str], np.ndarray]:
            calls.append("unrestricted")
            return ["ANY", "OTHER"], np.zeros((2, 4), dtype=np.float16)

        dc._load_reference_pool_cached(
            RefPoolKey(CFG, ANN, REVIEWED), restricted_loader, expected_count=1
        )
        accs, _ = dc._load_reference_pool_cached(
            RefPoolKey(CFG, ANN), unrestricted_loader, expected_count=2
        )

        assert calls == ["restricted", "unrestricted"], (
            "the unrestricted read hit the restricted pool instead of missing"
        )
        assert accs == ["ANY", "OTHER"]

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
    _load_anno_csr_from_disk,
    _save_anno_csr_to_disk,
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


class TestTheBackstopWhenTheKeyIsNotEnough:
    def test_an_index_that_overruns_the_pool_raises(self) -> None:
        with pytest.raises(ValueError, match="does not address this pool"):
            _check_index_addresses_pool(np.array([0, 7], dtype=np.int32), ["a", "b"], "P", "")

    def test_an_index_that_fits_is_accepted(self) -> None:
        _check_index_addresses_pool(np.array([0, 1], dtype=np.int32), ["a", "b"], "P", "")

    def test_an_empty_index_is_accepted(self) -> None:
        """An aspect with no annotated references is legitimate, not a mismatch."""
        _check_index_addresses_pool(np.array([], dtype=np.int32), [], "P", "")

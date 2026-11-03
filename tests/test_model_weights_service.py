"""The model weights registry without a database (ADR-D54).

What these pin, because each is where an upload goes wrong without anyone noticing:

* a name becomes part of a store key, so it must be a plain stem;
* the key an upload gets is content-addressed in the form the operations' resolver
  verifies (``-<hex>`` before the extension);
* every part but the last has exactly the part size, so the joined file is the file;
* a part missing, a size that does not add up or a hash that differs refuses the
  upload, and the upload folder is removed either way;
* a registration by reference hashes the object in the store, in chunks.

The rows themselves are pinned against Postgres in ``test_model_weights_pg.py``.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from protea.config.tuning import get_tuning
from protea.core.operations._encoder_artifact import content_digest_in
from protea.infrastructure.storage.local import LocalFsArtifactStore
from protea.services import model_weights_service as svc


@pytest.fixture()
def uploads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Uploads in a temporary folder, with 1 KiB parts so a small file has several."""
    root = tmp_path / "uploads"
    monkeypatch.setenv("PROTEA_TUNING__api__weights_upload_dir", str(root))
    monkeypatch.setenv("PROTEA_TUNING__api__weights_part_bytes", "1024")
    get_tuning.cache_clear()
    yield root
    get_tuning.cache_clear()


def _session_without_rows() -> MagicMock:
    session = MagicMock()
    session.scalars.return_value.first.return_value = None
    return session


@pytest.mark.parametrize("name", ["vocabulary-v1", "reader_211.v2", "a"])
def test_a_plain_stem_is_a_name(name: str) -> None:
    assert svc.validate_name(name) == name


@pytest.mark.parametrize(
    "name", ["", "../etc/passwd", "a/b", "-starts-with-dash", "x" * 121, "spa ce"]
)
def test_anything_that_could_move_the_key_is_refused(name: str) -> None:
    with pytest.raises(svc.WeightsError):
        svc.validate_name(name)


def test_an_unknown_kind_is_refused() -> None:
    with pytest.raises(svc.WeightsError, match="unknown kind"):
        svc.validate_kind("booster")


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("vocabulary-v1.npz", ("npz", ".npz")),
        ("model.safetensors", ("safetensors", ".safetensors")),
        ("bundle.tar.gz", ("tar.gz", ".tar.gz")),
        ("ENCODER.PT", ("pt", ".pt")),
        ("weights", ("bin", "")),
    ],
)
def test_the_extension_gives_the_format(filename: str, expected: tuple[str, str]) -> None:
    assert svc.format_of(filename) == expected


def test_an_uploaded_key_is_one_the_resolver_verifies() -> None:
    digest = hashlib.sha256(b"weights").hexdigest()
    key = svc.content_key("residue_vocabulary", "vocabulary-v1", digest, ".npz")
    assert key == f"model-weights/residue_vocabulary/vocabulary-v1-{digest[:16]}.npz"
    assert content_digest_in(key) == digest[:16]


def _send(upload: dict, data: bytes) -> None:
    size = upload["part_bytes"]
    for n in range(upload["parts"]):
        svc.put_part(upload["upload_id"], n, data[n * size : (n + 1) * size])


def test_an_upload_in_parts_stores_the_file_under_its_content_key(
    uploads: Path, tmp_path: Path
) -> None:
    data = bytes(range(256)) * 13  # 3,328 bytes: three full parts and a remainder
    digest = hashlib.sha256(data).hexdigest()
    store = LocalFsArtifactStore(tmp_path / "store")
    upload = svc.create_upload(
        svc.WeightsInfo("residue_vocabulary", "vocabulary-test"), "v.npz", len(data), digest
    )
    assert upload["parts"] == 4
    _send(upload, data)
    row = svc.complete_upload(_session_without_rows(), store, upload["upload_id"])
    assert row.sha256 == digest
    assert row.size_bytes == len(data)
    assert row.format == "npz"
    assert row.uri == f"model-weights/residue_vocabulary/vocabulary-test-{digest[:16]}.npz"
    assert store.get(row.uri) == data
    assert not (uploads / upload["upload_id"]).exists(), "the upload folder is removed"


def test_a_part_of_the_wrong_size_is_refused(uploads: Path) -> None:
    upload = svc.create_upload(svc.WeightsInfo("reader", "r"), "r.pt", 3000)
    with pytest.raises(svc.WeightsError, match="must have 1024"):
        svc.put_part(upload["upload_id"], 0, b"x" * 1000)
    with pytest.raises(svc.WeightsError, match="must have 952"):
        svc.put_part(upload["upload_id"], 2, b"x" * 1024)
    with pytest.raises(svc.WeightsError, match="outside"):
        svc.put_part(upload["upload_id"], 3, b"x")


def test_a_missing_part_refuses_the_upload_and_removes_it(uploads: Path, tmp_path: Path) -> None:
    upload = svc.create_upload(svc.WeightsInfo("reader", "r"), "r.pt", 3000)
    svc.put_part(upload["upload_id"], 0, b"x" * 1024)
    with pytest.raises(svc.WeightsError, match="parts missing"):
        svc.complete_upload(
            _session_without_rows(), LocalFsArtifactStore(tmp_path / "s"), upload["upload_id"]
        )
    assert not (uploads / upload["upload_id"]).exists()


def test_a_file_that_does_not_hash_as_announced_is_refused(uploads: Path, tmp_path: Path) -> None:
    data = b"y" * 1500
    upload = svc.create_upload(svc.WeightsInfo("reader", "r"), "r.pt", len(data), "0" * 64)
    _send(upload, data)
    store = LocalFsArtifactStore(tmp_path / "s")
    with pytest.raises(svc.WeightsError, match="not to the sha256 announced"):
        svc.complete_upload(_session_without_rows(), store, upload["upload_id"])
    assert not any((tmp_path / "s").rglob("*.pt")), "nothing reaches the store"


def test_an_upload_larger_than_the_limit_is_refused_before_any_byte(uploads: Path) -> None:
    too_big = get_tuning().api.max_weights_bytes + 1
    with pytest.raises(svc.WeightsError, match="size must be"):
        svc.create_upload(svc.WeightsInfo("reader", "r"), "r.pt", too_big)


def test_an_upload_the_disk_cannot_hold_is_refused_before_any_byte(
    uploads: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On the API host the system temporary folder is a tmpfs: RAM. Whatever the folder,
    an upload that would leave less than a gigabyte free is refused up front."""
    import shutil
    from collections import namedtuple

    usage = namedtuple("usage", "total used free")
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: usage(16 << 30, 15 << 30, 1 << 30))
    with pytest.raises(svc.WeightsError, match="bytes free"):
        svc.create_upload(svc.WeightsInfo("residue_vocabulary", "v"), "v.npz", 260_233_730)


def test_the_default_upload_folder_is_not_the_system_temporary_folder() -> None:
    import tempfile

    assert not str(svc.DEFAULT_UPLOAD_ROOT).startswith(tempfile.gettempdir())


def test_the_purge_only_removes_old_uploads(uploads: Path) -> None:
    import os
    import time

    uploads.mkdir(parents=True, exist_ok=True)
    old = time.time() - 2 * svc.STALE_UPLOAD_SECONDS
    keep = uploads / "someone-elses-folder"
    keep.mkdir()
    os.utime(keep, (old, old))
    stale = svc.create_upload(svc.WeightsInfo("reader", "r"), "r.pt", 10)
    os.utime(uploads / stale["upload_id"], (old, old))
    svc.create_upload(svc.WeightsInfo("reader", "r"), "r.pt", 10)
    assert keep.exists(), "a folder the module did not make is never removed"
    assert not (uploads / stale["upload_id"]).exists(), "an abandoned upload older than a day is"


def test_joining_drops_each_part_once_copied(uploads: Path) -> None:
    data = b"q" * 3000
    upload = svc.create_upload(svc.WeightsInfo("reader", "r"), "r.pt", len(data))
    _send(upload, data)
    folder = uploads / upload["upload_id"]
    joined, digest, size = svc._join(folder, upload["parts"])
    assert (size, digest) == (len(data), hashlib.sha256(data).hexdigest())
    assert joined.read_bytes() == data
    assert not list(folder.glob("part-*")), "the folder holds the file, not the file twice"


def test_an_unknown_or_malformed_upload_id_is_refused(uploads: Path) -> None:
    with pytest.raises(svc.WeightsError):
        svc.put_part("../../etc", 0, b"x")
    with pytest.raises(svc.WeightsNotFound):
        svc.put_part("6b1f0c9e-1d3a-4c55-9a0e-6e0c5d7f2a11", 0, b"x")


def test_a_reference_is_hashed_in_the_store_and_must_match(tmp_path: Path) -> None:
    store = LocalFsArtifactStore(tmp_path / "s")
    data = b"vocabulary bytes" * 1000
    store.put("residue-vocabulary/vocabulary-v1-0123456789ab.npz", data)
    digest = hashlib.sha256(data).hexdigest()
    row = svc.register_by_reference(
        _session_without_rows(),
        store,
        svc.WeightsInfo("residue_vocabulary", "vocabulary-v1"),
        "residue-vocabulary/vocabulary-v1-0123456789ab.npz",
        digest,
    )
    assert (row.sha256, row.size_bytes, row.format) == (digest, len(data), "npz")
    with pytest.raises(svc.WeightsError, match="hashes to"):
        svc.register_by_reference(
            _session_without_rows(),
            store,
            svc.WeightsInfo("residue_vocabulary", "vocabulary-v1"),
            "residue-vocabulary/vocabulary-v1-0123456789ab.npz",
            "1" * 64,
        )


def test_a_reference_to_nothing_or_to_a_path_is_refused(tmp_path: Path) -> None:
    store = LocalFsArtifactStore(tmp_path / "s")
    with pytest.raises(svc.WeightsNotFound):
        svc.register_by_reference(
            _session_without_rows(),
            store,
            svc.WeightsInfo("reader", "r"),
            "readers/none.pt",
            "a" * 64,
        )
    with pytest.raises(svc.WeightsError, match="not a path"):
        svc.register_by_reference(
            _session_without_rows(), store, svc.WeightsInfo("reader", "r"), "/etc/passwd", "a" * 64
        )


def test_the_local_store_streams_what_it_holds(tmp_path: Path) -> None:
    store = LocalFsArtifactStore(tmp_path / "s")
    data = b"z" * (3 * 1024 + 5)
    store.put("k/f.bin", data)
    assert b"".join(store.iter_chunks("k/f.bin", 1024)) == data

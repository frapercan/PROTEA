"""``put`` returned a URI and ``get`` took a key, so the obvious call failed.

The protocol's own docstring says URIs from ``put`` "should be persisted as-is
in the database so consumers can resolve them without knowing the backend".
Until 2026-10-28 that was a sentence and not a fact: handed back exactly what
``put`` returned, MinIO's ``get`` used ``s3://bucket/key`` as the object name
and answered ``XMinioInvalidObjectName`` on ``/bucket/s3://bucket/key`` -- the
scheme embedded twice.

Measured the hard way: a dispatched ``apply_learned_encoder`` job failed in
0,0 min because its payload carried what ``put`` had just returned. The field
is even named ``encoder_artifact_uri``.

Both backends had the asymmetry: ``put`` and ``url`` return ``self.url(key)``
and every reader took a key.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from protea.infrastructure.storage.local import LocalFsArtifactStore


@pytest.fixture
def store(tmp_path: Path) -> LocalFsArtifactStore:
    return LocalFsArtifactStore(tmp_path / "root")


class TestTheRoundTripThatFailed:
    def test_get_accepts_the_uri_put_returned(self, store: LocalFsArtifactStore):
        """The whole defect, in one call."""
        uri = store.put("encoders/head-abc123.pt", b"weights")
        assert "://" in uri, "put must still return a URI; consumers persist it"
        assert store.get(uri) == b"weights"

    def test_get_still_accepts_a_plain_key(self, store: LocalFsArtifactStore):
        """Every existing caller passes a key and must keep working."""
        store.put("a/b.bin", b"x")
        assert store.get("a/b.bin") == b"x"

    def test_exists_and_delete_take_either_form(self, store: LocalFsArtifactStore):
        uri = store.put("k/one.bin", b"x")
        assert store.exists(uri) and store.exists("k/one.bin")
        assert store.delete(uri) is True
        assert not store.exists("k/one.bin")

    def test_the_uri_from_url_resolves_too(self, store: LocalFsArtifactStore):
        """``url`` is the no-I/O half of ``put`` and must address the same object."""
        store.put("k/two.bin", b"y")
        assert store.get(store.url("k/two.bin")) == b"y"


class TestWhatIsRefusedRatherThanGuessed:
    def test_another_backends_scheme_is_refused(self, store: LocalFsArtifactStore):
        """A key that happens to begin ``s3://`` would otherwise become a silent
        miss on an object nobody can find."""
        with pytest.raises(ValueError, match="another backend"):
            store.get("s3://protea/encoders/head.pt")

    def test_a_file_uri_outside_the_root_is_refused(self, store: LocalFsArtifactStore):
        """It names another deployment's root, or a path that was never in the
        store. Resolving it against this root would answer with a different
        object, which is worse than refusing."""
        with pytest.raises(ValueError, match="outside this store's root"):
            store.get("file:///somewhere/else/head.pt")

    def test_the_refusal_names_the_root_so_it_can_be_acted_on(
        self, store: LocalFsArtifactStore
    ):
        with pytest.raises(ValueError) as exc:
            store.get("file:///nope/x.bin")
        assert str(store.root) in str(exc.value)


class TestTheMinioHalf:
    """The backend the failure happened on. Tested through ``_key`` alone,
    because the rest of it needs a server and the normalisation is the part
    that was wrong."""

    @staticmethod
    def _key(bucket: str, address: str) -> str:
        from protea.infrastructure.storage.minio_store import MinioArtifactStore

        fake = object.__new__(MinioArtifactStore)
        fake.bucket = bucket
        return MinioArtifactStore._key(fake, address)

    def test_the_uri_form_becomes_its_key(self):
        assert (
            self._key("protea", "s3://protea/encoders/head-abc123.pt")
            == "encoders/head-abc123.pt"
        )

    def test_a_plain_key_is_returned_unchanged(self):
        assert self._key("protea", "encoders/head.pt") == "encoders/head.pt"

    def test_a_key_with_slashes_survives_the_split(self):
        """``partition`` on the first slash, so a deep key keeps its depth."""
        assert self._key("protea", "s3://protea/a/b/c/d.pt") == "a/b/c/d.pt"

    def test_another_bucket_is_refused(self):
        """Fetching it from the wrong bucket would answer with a different
        object or with nothing, and both are worse than refusing."""
        with pytest.raises(ValueError, match="names bucket"):
            self._key("protea", "s3://otro/encoders/head.pt")

    def test_another_scheme_is_refused(self):
        with pytest.raises(ValueError, match="another backend"):
            self._key("protea", "file:///var/lib/head.pt")

    def test_a_bucket_with_no_object_is_refused(self):
        with pytest.raises(ValueError, match="no object"):
            self._key("protea", "s3://protea/")


def test_the_protocol_says_what_is_now_true():
    """The sentence that was false. Pinned so a future edit that narrows ``get``
    back to keys has to delete the promise as well."""
    import inspect

    from protea.infrastructure.storage import ArtifactStore

    doc = inspect.getdoc(ArtifactStore.get) or ""
    assert "OR a URI" in doc
    assert "refused" in doc

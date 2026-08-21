"""The artifact cache, and why a content-addressed key makes staleness unwritable.

The first version cached every key under a filename derived from the key. Re-uploading a
corrected artifact under the same name left every host that had already fetched it serving
the old bytes, with nothing to notice. It stalled a corpus run, and the host that found it
could not clear the cache on the host that was wrong.
"""

from __future__ import annotations

import hashlib

import pytest

from protea.core.operations._encoder_artifact import (
    content_digest_in,
    resolve_encoder_artifact,
)

PAYLOAD = b"a frozen encoder, or near enough"
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()


class _Store:
    def __init__(self, payload=PAYLOAD):
        self.payload = payload
        self.fetches = 0

    def get(self, _key):
        self.fetches += 1
        return self.payload


@pytest.fixture
def store(monkeypatch, tmp_path):
    s = _Store()
    import protea.infrastructure.storage as storage_mod

    monkeypatch.setattr(storage_mod, "get_artifact_store", lambda _s: s)
    monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))
    return s


def test_a_key_ending_in_a_digest_names_its_own_content():
    assert content_digest_in("encoders/e-ac763f848c52.npz") == "ac763f848c52"


def test_a_plain_key_names_nothing():
    assert content_digest_in("encoders/exp220-residue-k4-d2048-s128.npz") is None


def test_recipe_fields_are_not_mistaken_for_a_digest():
    """`-d2048-` and `-s128` are hex-ish and far too short to be one."""
    assert content_digest_in("encoders/thing-d2048-s128.npz") is None


def test_a_local_path_is_returned_untouched(store):
    assert resolve_encoder_artifact("/tmp/e.npz", None) == "/tmp/e.npz"
    assert store.fetches == 0


def test_neither_address_is_refused(store):
    with pytest.raises(ValueError, match="neither"):
        resolve_encoder_artifact(None, None)


def test_a_content_addressed_key_is_fetched_once(store):
    uri = f"encoders/e-{DIGEST[:12]}.npz"

    first = resolve_encoder_artifact(None, uri)
    second = resolve_encoder_artifact(None, uri)

    assert first == second
    assert store.fetches == 1, "different bytes would be a different key, so caching is safe"


def test_a_plain_key_is_never_cached(store):
    """The cost of a fetch per call, in exchange for a cache that cannot be wrong."""
    resolve_encoder_artifact(None, "encoders/plain.npz")
    resolve_encoder_artifact(None, "encoders/plain.npz")

    assert store.fetches == 2


def test_a_plain_key_picks_up_a_re_upload(store):
    """The exact failure this replaces: a corrected artifact under the same name."""
    first = resolve_encoder_artifact(None, "encoders/plain.npz")
    assert open(first, "rb").read() == PAYLOAD

    store.payload = b"the corrected artifact"
    second = resolve_encoder_artifact(None, "encoders/plain.npz")

    assert open(second, "rb").read() == b"the corrected artifact"


def test_a_key_whose_content_does_not_match_is_refused(store):
    """The key is checked rather than trusted, so a swapped object never loads."""
    store.payload = b"something else entirely"

    with pytest.raises(ValueError, match="is not the one the key describes"):
        resolve_encoder_artifact(None, f"encoders/e-{DIGEST[:12]}.npz")


def test_a_refused_object_is_not_left_in_the_cache(store, tmp_path):
    store.payload = b"something else entirely"
    uri = f"encoders/e-{DIGEST[:12]}.npz"

    with pytest.raises(ValueError):
        resolve_encoder_artifact(None, uri)

    cached = tmp_path / "protea-encoder-artifacts" / uri.replace("/", "_")
    assert not cached.exists(), "a rejected download must not become tomorrow's cache hit"

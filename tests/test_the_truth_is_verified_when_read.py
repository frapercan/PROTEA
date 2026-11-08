"""The ground truth is checked against its row, and reading it has one door.

A `*_uri` is a LOCATOR and a sha256 is an IDENTITY. ``evaluation_set`` carried
the first without the second, so ``load_evaluation_data_for_set`` fetched the
parquet and verified nothing -- and the crucial part is WHY: not because it
verified badly, but because there was nothing to verify against. The read was
indistinguishable from a verified one, which is why nobody noticed for 133
windows.

So these assert two different things. The rule (does a mismatch refuse? does a
missing digest report?) and the SEAM (does the only reader actually go through
the checked path, and does the writer hash the bytes it sends rather than the
bytes that come back?). The rule without the seam is what the project already
had: a verification nothing called.
"""

from __future__ import annotations

import hashlib
import inspect
import warnings

import pytest

from protea.core._artifact_identity import (
    ArtifactMismatch,
    fetch_and_verify,
    sha256_of_bytes,
    sha256_of_file,
)

BLOB = b"PAR1-fake-parquet-bytes"
SHA = hashlib.sha256(BLOB).hexdigest()


class _Store:
    """Returns the blob it was built with, and says it was asked."""

    def __init__(self, blob: bytes = BLOB) -> None:
        self.blob = blob
        self.asked: list[str] = []

    def get(self, key: str) -> bytes:
        self.asked.append(key)
        return self.blob


def _code_of(obj: object) -> str:
    """Source with the docstring removed: the prose NAMES the call it replaced."""
    source = inspect.getsource(obj)
    doc = inspect.getdoc(obj)
    for line in (doc or "").splitlines():
        source = source.replace(line, "")
    return source


class TestTheRule:
    def test_matching_bytes_come_back(self) -> None:
        store = _Store()
        assert fetch_and_verify(store, "k", SHA, "la verdad") == BLOB
        assert store.asked == ["k"]

    def test_a_mismatch_refuses_and_names_both_digests(self) -> None:
        # The caller's reaction is not the caller's choice: an evaluation
        # scored against a swapped truth produced a number about something
        # else, so this stops the run.
        otro = hashlib.sha256(b"otros bytes").hexdigest()
        with pytest.raises(ArtifactMismatch) as err:
            fetch_and_verify(_Store(), "k", otro, "la verdad de X")
        texto = str(err.value)
        assert otro[:16] in texto and SHA[:16] in texto
        assert "la verdad de X" in texto

    def test_a_missing_digest_does_not_raise(self) -> None:
        # 133 windows predate the column. Refusing would break reading every
        # one of them, so NULL cannot be an error.
        assert fetch_and_verify(_Store(), "k", None, "legado") == BLOB

    def test_a_missing_digest_is_reported_and_not_passed_in_silence(self) -> None:
        dicho: list[str] = []
        fetch_and_verify(_Store(), "k", None, "legado", on_unverified=dicho.append)
        assert len(dicho) == 1
        assert "WITHOUT verification" in dicho[0]

    def test_the_two_digests_agree_on_the_same_content(self, tmp_path) -> None:
        # sha256_of_file reads in blocks; a difference between the two would
        # make a row written from a file unverifiable from its bytes.
        p = tmp_path / "a.parquet"
        p.write_bytes(BLOB * 4096)
        assert sha256_of_file(p) == sha256_of_bytes(BLOB * 4096)


class TestTheSeam:
    def test_the_api_reader_comes_through_the_same_door(self) -> None:
        # THE TEST BELOW WAS CALLED "the only reader" AND ONLY LOOKED AT ONE.
        # ``api/routers/graph.py`` split the SAVED uri and fetched it with
        # store.get, verifying nothing, while the core derived the key from
        # the id: one object, two locators, nothing comparing them. Found by
        # the desktop session reading the diff, not by this file.
        from protea.api.routers import graph

        code = _code_of(graph._counted_units)
        assert "read_groundtruth" in code
        assert "store.get(" not in code, "la API volvio a leer sin verificar"

    def test_the_locator_in_the_row_is_checked_against_the_derived_key(self) -> None:
        # Refused rather than resolved by preferring either: the derived key
        # would hide a row pointing elsewhere, the stored uri would hide a
        # renamed object.
        from protea.core.evaluation import read_groundtruth

        code = _code_of(read_groundtruth)
        assert "groundtruth_key_for" in code and "endswith" in code
        assert "ArtifactMismatch" in code

    def test_the_core_reader_goes_through_the_checked_path(self) -> None:
        # Imported from ``protea.core.evaluation`` and NOT from the module it
        # lives in. That is not style: ``_evaluation_groundtruth`` imports
        # ``evaluation`` for ``EvaluationData`` and ``evaluation`` imports it
        # back at the end of its own file, so entering by the module raises
        # ImportError on a partially initialised cycle.
        from protea.core.evaluation import load_evaluation_data_for_set

        code = _code_of(load_evaluation_data_for_set)
        assert "read_groundtruth" in code
        assert "store.get(" not in code, "vuelve a leer sin verificar"

    def test_it_passes_the_row_s_digest_and_not_something_else(self) -> None:
        from protea.core.evaluation import read_groundtruth

        assert "groundtruth_sha256" in _code_of(read_groundtruth)

    def test_a_row_one_migration_behind_still_reads(self) -> None:
        # The column arrives in b4c17e2a9f08. A dump run against a database
        # that has not migrated must report, not crash on the attribute.
        from protea.core.evaluation import read_groundtruth

        assert "getattr(eval_set" in _code_of(read_groundtruth)

    def test_the_writer_hashes_what_it_sends_not_what_comes_back(self) -> None:
        # A digest taken after the round trip would agree with whatever the
        # store returned, including the wrong object. So the hash has to be
        # computed BEFORE the put.
        from protea.core.operations.generate_evaluation_set import (
            GenerateEvaluationSetOperation,
        )

        code = _code_of(GenerateEvaluationSetOperation._persist_groundtruth)
        assert code.index("sha256_of_file") < code.index("store.put")
        assert "groundtruth_sha256 = digest" in code

    def test_the_set_guard_now_sees_the_pairing(self) -> None:
        # What enforces that a uri has a digest is the gate, not a mixin.
        # Before this change it listed evaluation_set under "SIN SHA".
        import importlib.util
        import pathlib

        ruta = pathlib.Path(__file__).resolve().parents[1] / "scripts/check_sets.py"
        spec = importlib.util.spec_from_file_location("_cs", ruta)
        assert spec and spec.loader
        guard = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(guard)

        import protea.infrastructure.orm.models  # noqa: F401
        from protea.infrastructure.orm.base import Base

        texto, cumple = guard._audit(Base.metadata.tables["evaluation_set"])["verificable"]
        assert cumple, texto


class TestItWarnsRatherThanWhispers:
    def test_the_reader_raises_a_warning_for_an_unverified_row(self) -> None:
        # Asserted because the callback is the whole mechanism: wire it to
        # nothing and the silence comes back.
        from protea.core.evaluation import read_groundtruth

        code = _code_of(read_groundtruth)
        assert "on_unverified" in code and "warnings.warn" in code

    def test_a_plain_warning_does_not_stop_a_read(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            with pytest.raises(RuntimeWarning):
                fetch_and_verify(
                    _Store(),
                    "k",
                    None,
                    "legado",
                    on_unverified=lambda m: warnings.warn(m, RuntimeWarning, stacklevel=2),
                )

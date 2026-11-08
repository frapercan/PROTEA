"""``residue_refs``: where each sequence's per-residue codes live, named.

NOT ONE BYTE OF CODE IS COPIED. The 90,4 GB of shards stay where they are and
the reader fetches the one it needs, verifying it by the sha this table
carries.

IT COVERS THE WHOLE BANK, which is what lets it be written before the windows
are chosen: a superset covers any selection a bundle could make, and the
loader reads only the ``sequence_id`` values of its own batch.

AND THE CONFIG THAT KEYS THESE ROWS IS NOT THE ONE THEY ARE NAMED AFTER, which
is the subject of this file. Measured 2026-11-08: all 683.887 rows carry
``43193409-de70-526b-ae8d-43892a744049``, whose model name is
``residue-vocabulary:06fed75ca76d17be:f3a7fe16``, while the ankh config
``f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0`` that the vocabulary was derived FROM
has **zero** rows. A reader asking for "the f3a7fe16 bank" gets nothing, and an
empty ``residue_refs`` is indistinguishable from a bank with no codes at all.
So ``test_a_config_with_no_rows_is_refused`` is the test this file exists for.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from protea.core.operations.export_transitions_dataset import (
    ExportTransitionsDatasetPayload,
)
from protea.core.transitions_export import _residue_refs as R

VOCAB = uuid.UUID("43193409-de70-526b-ae8d-43892a744049")
ANKH = uuid.UUID("f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0")


class _Config:
    def __init__(self, nombre) -> None:
        self.model_name = nombre


class _Session:
    def __init__(self, config, filas) -> None:
        self.config, self.filas = config, filas

    def get(self, _model, _ident):
        return self.config

    def execute(self, _statement):
        return _Rows(self.filas)


class _Rows:
    def __init__(self, rows) -> None:
        self._rows = rows

    def all(self):
        return self._rows


FILAS = [
    (22, "residue-codes/s-0001.npz", "b" * 64, 7),
    (11, "residue-codes/s-0000.npz", "a" * 64, 3),
    (33, "residue-codes/s-0001.npz", "b" * 64, 0),
]


def test_a_config_with_no_rows_is_refused():
    """THE TEST THIS FILE EXISTS FOR.

    The ankh config has zero ``residue_code`` rows while the vocabulary config
    derived from it has 683.887. Naming the wrong one is a plausible mistake,
    and without this it would publish a table that joins to nothing and says
    so to nobody.
    """
    session = _Session(_Config("ElnaggarLab/ankh-base"), [])

    with pytest.raises(ValueError, match="indistinguishable from a bank with no"):
        R.refs_of(session, ANKH)


def test_the_refusal_says_which_config_keys_the_rows():
    session = _Session(_Config("ElnaggarLab/ankh-base"), [])

    with pytest.raises(ValueError) as exc:
        R.refs_of(session, ANKH)

    texto = str(exc.value)
    assert "VOCABULARY config" in texto
    assert "ankh-base" in texto


def test_a_config_that_does_not_exist_is_refused():
    session = _Session(None, FILAS)

    with pytest.raises(ValueError, match="does not exist"):
        R.refs_of(session, VOCAB)


def test_it_reports_the_model_name_so_a_wrong_id_is_visible():
    """The id alone does not say which bank it is; the name does."""
    session = _Session(_Config("residue-vocabulary:06fed75ca76d17be:f3a7fe16"), FILAS)

    refs = R.refs_of(session, VOCAB)

    assert refs.model_name == "residue-vocabulary:06fed75ca76d17be:f3a7fe16"
    assert refs.embedding_config_id == str(VOCAB)
    assert len(refs.rows) == 3
    assert refs.shards == 2


def test_the_rows_are_sorted_by_sequence_and_the_bytes_are_stable(tmp_path: Path):
    session = _Session(_Config("v"), FILAS)
    refs = R.refs_of(session, VOCAB)
    al_reves = R.Refs(
        embedding_config_id=refs.embedding_config_id,
        model_name=refs.model_name,
        rows=list(reversed(refs.rows)),
        shards=refs.shards,
    )

    uno, otro = tmp_path / "a.parquet", tmp_path / "b.parquet"
    assert R.write_table(refs, str(uno)) == 3
    assert R.write_table(al_reves, str(otro)) == 3

    assert hashlib.sha256(uno.read_bytes()).hexdigest() == (
        hashlib.sha256(otro.read_bytes()).hexdigest()
    )
    tabla = pq.read_table(uno).to_pydict()
    assert tabla["sequence_id"] == [11, 22, 33]
    assert tabla["position"] == [3, 7, 0]
    assert set(tabla["embedding_config_id"]) == {str(VOCAB)}


def test_the_five_columns_are_the_loader_s():
    assert R.SCHEMA.names == [
        "embedding_config_id",
        "sequence_id",
        "shard_key",
        "shard_sha256",
        "position",
    ]


def test_it_copies_no_codes_only_references():
    """The shards stay where they are, which is the whole point."""
    import inspect

    fuente = inspect.getsource(R)

    assert "shard_key" in fuente and "shard_sha256" in fuente
    # Nothing here reads a shard: no store, no fetch, no bytes.
    assert "get_artifact_store" not in fuente
    assert "iter_chunks" not in fuente


def test_the_field_is_not_called_embedding_config_id():
    """Two configs, and neither derives from the other.

    The codes config indexes the reference tables; the kNN config gives the
    neighbourhood. One field named for both invites putting one where the
    other goes, which is the ``cut`` and ``cut_version`` mistake again. The
    laboratory asked for the two to be separate in the manifest before either
    was written, which is the cheap moment.

    The COLUMN inside the table stays ``embedding_config_id``: that is the
    loader's contract, and inside one table there is only one config to mean.
    """
    campos = set(ExportTransitionsDatasetPayload.model_fields)

    assert "codes_config_id" in campos
    assert "embedding_config_id" not in campos
    assert R.SCHEMA.names[0] == "embedding_config_id"

    # Y el nombre viejo se RECHAZA, no se ignora: el payload prohibe campos
    # extra, asi que quien lo pase recibe un error y no silencio.
    with pytest.raises(ValueError, match="embedding_config_id"):
        ExportTransitionsDatasetPayload(
            windows=[],
            cut="c",
            cut_version="211",
            tables=["residue_refs"],
            embedding_config_id=str(VOCAB),
        )


def test_the_payload_needs_the_config_named():
    with pytest.raises(ValueError, match="codes_config_id"):
        ExportTransitionsDatasetPayload(
            windows=[], cut="c", cut_version="211", tables=["residue_refs"]
        )


def test_the_payload_needs_no_windows_for_it():
    p = ExportTransitionsDatasetPayload(
        windows=[],
        cut="c",
        cut_version="211",
        tables=["residue_refs"],
        codes_config_id=str(VOCAB),
    )

    assert p.windows == []
    assert p.codes_config_id == str(VOCAB)

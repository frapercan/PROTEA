"""On Postgres: the reference pool crosses the driver in binary, bit for bit.

The unit tests beside this one mock the driver, which is the single layer under
suspicion: a ``MagicMock`` cursor cannot say whether psycopg binds the
parameters of the statement SQLAlchemy compiled, and that is exactly where an
expanded ``IN`` or a cast would fail. Such a failure is invisible in production
today -- the route falls back, writes one warning, and the bank is correct and
slow -- so only a test against a real Postgres turns it into a failure.

Two questions, and the second is the one a mock cannot reach:

- does the binary route actually answer, and are its values the text route's
  values bit for bit and in the same order;
- does it survive a pool query carrying an expanded ``IN`` (a donor policy's
  evidence codes) and ``admitted_by_release``, which is what
  ``render_postcompile`` with ``compiled.params`` exists for.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest
from protea_contracts.payloads import DonorPolicy
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.operations.predict_go_terms import _batch_op_reference as refmod
from protea.core.operations.predict_go_terms._batch_op import PredictGOTermsBatchOperation
from protea.core.operations.predict_go_terms._batch_op_reference import _PoolRequest
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.session import build_session_factory
from tests.helpers.prediction_sets import make_parents

DIM = 4

#: Accession, its vector, the evidence code of its annotation, and the release
#: that admitted it. The vectors are deliberately awkward: values that do not
#: round-trip through a short decimal, so a text form that lost a digit would
#: show up as a difference rather than as luck.
ROWS: tuple[tuple[str, list[float], str, int], ...] = (
    ("P00001", [0.1, 0.2, 0.3, 0.4], "IDA", 200),
    ("P00002", [-0.7001, 1.3331, 0.0, 65504.0], "IDA", 200),
    ("P00003", [1e-5, -1e-5, 0.33333, -0.66667], "IEA", 200),
    ("P00004", [2.0, 4.0, 8.0, 16.0], "IDA", 230),
    ("P00005", [0.123456, 0.654321, -0.000123, 9.87654], "IDA", 200),
)


def _schema(url: str) -> None:
    engine = create_engine(url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    engine.dispose()


@pytest.fixture()
def pg(postgres_url: str) -> Any:
    """A session built the way the fleet builds one, adapters and all.

    NOT ``create_engine`` as the other Postgres tests here do, and the
    difference is the whole point. ``build_session_factory`` goes through
    ``build_engine``, which attaches ``_register_vector_adapters`` on connect;
    a bare engine has no such listener, pgvector's binary loaders are absent,
    and a binary ``halfvec`` comes back as raw ``bytes``. Written the usual way
    this test exercised a connection the fleet never has, which is the opposite
    of what an integration test is for.
    """
    _schema(postgres_url)
    factory = build_session_factory(postgres_url)
    with factory() as s:
        yield s


@pytest.fixture()
def pg_without_adapters(postgres_url: str) -> Any:
    """A session on a connection that never had pgvector's adapters.

    Which is not a contrivance: ``_register_vector_adapters`` is deliberately
    non-fatal, because an engine that refuses to build takes the API and the
    workers with it, so a driver without the adapters is a state the fleet can
    really reach.
    """
    _schema(postgres_url)
    engine = create_engine(postgres_url, future=True)
    with Session(engine) as s:
        yield s
    engine.dispose()


@pytest.fixture()
def pool(pg: Session) -> dict[str, Any]:
    """A bank of five proteins, each with an annotation, built through the ORM."""
    return _fill(pg)


@pytest.fixture()
def pool_without_adapters(pg_without_adapters: Session) -> dict[str, Any]:
    return _fill(pg_without_adapters)


def _fill(pg: Session) -> dict[str, Any]:
    parents = make_parents(pg)
    term = GOTerm(
        go_id="GO:0003674",
        name="molecular_function",
        aspect="F",
        ontology_snapshot_id=parents["snapshot"],
    )
    pg.add(term)
    pg.flush()
    for i, (acc, vector, code, release) in enumerate(ROWS):
        seq = Sequence(sequence="MKVLWAGS" + "A" * i, sequence_hash=f"hash-{i}")
        pg.add(seq)
        pg.flush()
        pg.add(
            Protein(
                accession=acc,
                canonical_accession=acc,
                is_canonical=True,
                sequence_id=seq.id,
                first_admitted_release=release,
            )
        )
        pg.add(
            SequenceEmbedding(
                sequence_id=seq.id,
                embedding_config_id=parents["embedding"],
                embedding=vector,
                embedding_dim=DIM,
            )
        )
        pg.add(
            ProteinGOAnnotation(
                annotation_set_id=parents["annotation"],
                protein_accession=acc,
                go_term_id=term.id,
                evidence_code=code,
            )
        )
    pg.flush()
    return parents


def _both_routes(
    pg: Session,
    op: PredictGOTermsBatchOperation,
    request: _PoolRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[tuple[list[str], np.ndarray, str], tuple[list[str], np.ndarray, str]]:
    """Read the same pool twice: as the code chooses, and with text forced."""
    chosen = op._stream_reference_pool(op._reference_pool_query(pg, request))
    monkeypatch.setattr(refmod, "fill_from_binary", lambda *a, **k: None)
    forced = op._stream_reference_pool(op._reference_pool_query(pg, request))
    return chosen, forced


def test_the_pool_crosses_in_binary_and_matches_text_bit_for_bit(
    pg: Session, pool: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    op = PredictGOTermsBatchOperation()
    request = _PoolRequest(pool["embedding"], pool["annotation"], None)

    (accs_b, emb_b, route_b), (accs_t, emb_t, route_t) = _both_routes(pg, op, request, monkeypatch)

    assert route_b == "binary", "the real driver must give a binary cursor"
    assert route_t == "text", "forcing the fallback must say so"
    assert accs_b == accs_t == [acc for acc, _, _, _ in ROWS]
    assert np.array_equal(emb_b.view(np.uint16), emb_t.view(np.uint16)), (
        "the two routes must agree bit for bit, not to a tolerance"
    )


def test_a_policy_with_an_expanded_in_and_a_release_still_goes_binary(
    pg: Session, pool: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The case no mock can reach: parameters psycopg has to bind itself.

    ``evidence_codes`` compiles to an expanded ``IN`` and
    ``admitted_by_release`` to a bound integer. If either reached the driver
    unbound, the binary route would raise and fall back in silence.
    """
    op = PredictGOTermsBatchOperation()
    policy = DonorPolicy(evidence_codes=["IDA", "TAS"], admitted_by_release=200)
    request = _PoolRequest(pool["embedding"], pool["annotation"], policy)

    (accs_b, emb_b, route_b), (accs_t, emb_t, route_t) = _both_routes(pg, op, request, monkeypatch)

    assert route_b == "binary", "an expanded IN must not push the pool back to text"
    # The policy must actually bite, or the test would pass on an empty filter:
    # P00003 is IEA and P00004 was admitted at 230.
    assert accs_b == ["P00001", "P00002", "P00005"]
    assert accs_b == accs_t
    assert np.array_equal(emb_b.view(np.uint16), emb_t.view(np.uint16))


def test_without_the_adapters_it_falls_back_and_the_bank_is_still_right(
    pg_without_adapters: Session, pool_without_adapters: dict[str, Any]
) -> None:
    """A connection with no binary loaders gives ``bytes``, and text answers.

    This is the savepoint earning its place against a real database rather than
    a mock: the binary read raises part way through, inside a transaction, and
    the text retry has to work afterwards. Without the savepoint the retry
    would fail on a poisoned transaction and the operation would die on a
    performance detail.
    """
    op = PredictGOTermsBatchOperation()
    request = _PoolRequest(
        pool_without_adapters["embedding"], pool_without_adapters["annotation"], None
    )

    accs, embeddings, route = op._stream_reference_pool(
        op._reference_pool_query(pg_without_adapters, request)
    )

    assert route == "text"
    assert accs == [acc for acc, _, _, _ in ROWS]
    assert embeddings.shape == (len(ROWS), DIM)
    assert embeddings[3].tolist() == [2.0, 4.0, 8.0, 16.0]

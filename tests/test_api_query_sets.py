"""Unit tests for the /query-sets router.

Database and filesystem are fully mocked — no real infrastructure required.
"""

from __future__ import annotations

from contextlib import contextmanager
from io import BytesIO
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.query_sets import (
    _parse_fasta,
    extract_uniprot_header_metadata,
    router,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_qs(qs_id=None, name="Test", description=None):
    qs = MagicMock()
    qs.id = qs_id or uuid4()
    qs.name = name
    qs.description = description
    qs.created_at = MagicMock()
    qs.created_at.isoformat.return_value = "2024-01-01T00:00:00"
    return qs


def _make_app(session_factory):
    app = FastAPI()
    app.state.session_factory = session_factory
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


@pytest.fixture()
def session():
    return MagicMock()


@pytest.fixture()
def factory(session):
    f = MagicMock()
    return f


@pytest.fixture()
def client(session, factory):
    app = _make_app(factory)
    with patch(
        "protea.api.routers.query_sets.session_scope", side_effect=lambda _: _mock_scope(session)
    ):
        with TestClient(app) as c:
            yield c, session


# ---------------------------------------------------------------------------
# POST /query-sets
# ---------------------------------------------------------------------------

FASTA_CONTENT = b">P12345\nACDEFGHIK\n>Q67890\nLMNPQRST\n"


class TestCreateQuerySet:
    def _post(self, client_pair, content=FASTA_CONTENT, name="MySet"):
        client, session = client_pair
        return client.post(
            "/query-sets",
            data={"name": name},
            files={"file": ("proteins.fasta", BytesIO(content), "text/plain")},
        )

    def test_creates_query_set_returns_201(self, client) -> None:
        client_obj, session = client
        qs = _make_qs(name="MySet")

        # Sequence dedup: no existing hashes
        session.query.return_value.filter.return_value.all.return_value = []

        # Flush assigns IDs
        def flush_side():
            pass

        session.flush.side_effect = flush_side

        # Intercept QuerySet add to set its id
        added_qs = None

        def add_side(obj):
            nonlocal added_qs
            from protea.infrastructure.orm.models.query.query_set import QuerySet

            if isinstance(obj, QuerySet):
                obj.id = qs.id
                obj.created_at = qs.created_at

        session.add.side_effect = add_side
        session.add_all.return_value = None

        with patch(
            "protea.api.routers.query_sets.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            resp = client_obj.post(
                "/query-sets",
                data={"name": "MySet"},
                files={"file": ("proteins.fasta", BytesIO(FASTA_CONTENT), "text/plain")},
            )

        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "MySet"
        assert "id" in data

    def test_empty_fasta_returns_422(self, client) -> None:
        client_obj, session = client
        with patch(
            "protea.api.routers.query_sets.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            resp = client_obj.post(
                "/query-sets",
                data={"name": "empty"},
                files={"file": ("empty.fasta", BytesIO(b""), "text/plain")},
            )
        assert resp.status_code == 422

    def test_non_utf8_file_returns_422(self, client) -> None:
        client_obj, session = client
        with patch(
            "protea.api.routers.query_sets.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            resp = client_obj.post(
                "/query-sets",
                data={"name": "binary"},
                files={"file": ("binary.fasta", BytesIO(b"\xff\xfe binary"), "text/plain")},
            )
        assert resp.status_code == 422

    def test_duplicate_accession_in_upload_returns_422(self, client) -> None:
        client_obj, session = client
        dup_fasta = b">P12345\nACDEF\n>P12345\nGHIKL\n"
        with patch(
            "protea.api.routers.query_sets.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            resp = client_obj.post(
                "/query-sets",
                data={"name": "dup"},
                files={"file": ("dup.fasta", BytesIO(dup_fasta), "text/plain")},
            )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /query-sets
# ---------------------------------------------------------------------------


class TestListQuerySets:
    def test_returns_list(self, client) -> None:
        client_obj, session = client
        qs = _make_qs(name="Set1")

        session.query.return_value.order_by.return_value.all.return_value = [qs]
        session.query.return_value.group_by.return_value.all.return_value = [(qs.id, 5)]

        with patch(
            "protea.api.routers.query_sets.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            resp = client_obj.get("/query-sets")

        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_empty_list(self, client) -> None:
        client_obj, session = client

        session.query.return_value.order_by.return_value.all.return_value = []
        session.query.return_value.group_by.return_value.all.return_value = []

        with patch(
            "protea.api.routers.query_sets.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            resp = client_obj.get("/query-sets")

        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# GET /query-sets/{id}
# ---------------------------------------------------------------------------


class TestGetQuerySet:
    def test_returns_query_set(self, client) -> None:
        client_obj, session = client
        qs = _make_qs(name="Specific")
        qs_id = qs.id

        session.get.return_value = qs
        session.query.return_value.filter.return_value.scalar.return_value = 3
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
            ("P12345", 1, 9606, "Homo sapiens"),
            ("Q67890", 2, None, None),
        ]

        with patch(
            "protea.api.routers.query_sets.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            resp = client_obj.get(f"/query-sets/{qs_id}")

        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "Specific"
        assert "entries" in data

    def test_not_found_returns_404(self, client) -> None:
        client_obj, session = client
        session.get.return_value = None

        with patch(
            "protea.api.routers.query_sets.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            resp = client_obj.get(f"/query-sets/{uuid4()}")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /query-sets/{id}
# ---------------------------------------------------------------------------


class TestDeleteQuerySet:
    def test_deletes_and_returns_id(self, client) -> None:
        client_obj, session = client
        qs = _make_qs()
        qs_id = qs.id
        session.get.return_value = qs

        with patch(
            "protea.api.routers.query_sets.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            resp = client_obj.delete(f"/query-sets/{qs_id}")

        assert resp.status_code == 200
        assert resp.json()["deleted"] == str(qs_id)
        session.delete.assert_called_once_with(qs)

    def test_not_found_returns_404(self, client) -> None:
        client_obj, session = client
        session.get.return_value = None

        with patch(
            "protea.api.routers.query_sets.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            resp = client_obj.delete(f"/query-sets/{uuid4()}")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# FASTA parser unit tests
# ---------------------------------------------------------------------------


class TestParseFasta:
    def test_parses_two_records(self) -> None:
        fasta = ">P12345\nACDEF\n>Q67890\nGHIKL\n"
        records = _parse_fasta(fasta)
        assert len(records) == 2
        assert records[0] == ("P12345", "ACDEF", "P12345")
        assert records[1] == ("Q67890", "GHIKL", "Q67890")

    def test_multiline_sequence(self) -> None:
        fasta = ">P12345\nACD\nEFG\n"
        records = _parse_fasta(fasta)
        assert records[0][1] == "ACDEFG"

    def test_skips_empty_sequences(self) -> None:
        fasta = ">EMPTY\n\n>P12345\nACDEF\n"
        records = _parse_fasta(fasta)
        assert len(records) == 1
        assert records[0][0] == "P12345"

    def test_uses_first_token_as_accession(self) -> None:
        fasta = ">P12345 some description here\nACDEF\n"
        records = _parse_fasta(fasta)
        assert records[0][0] == "P12345"

    def test_preserves_full_description(self) -> None:
        fasta = ">sp|P12345|FOO_HUMAN Foo OS=Homo sapiens OX=9606 GN=FOO PE=1 SV=1\nACDEF\n"
        records = _parse_fasta(fasta)
        # Accession is the unwrapped UniProt id (sp|P12345|FOO_HUMAN → P12345);
        # the full header is kept in records[0][2] so downstream code can mine
        # OX= / OS= / GN= metadata.
        assert records[0][0] == "P12345"
        assert "sp|P12345|FOO_HUMAN" in records[0][2]
        assert "OX=9606" in records[0][2]
        assert "OS=Homo sapiens" in records[0][2]

    def test_uppercase_conversion(self) -> None:
        fasta = ">P12345\nacdef\n"
        records = _parse_fasta(fasta)
        assert records[0][1] == "ACDEF"

    def test_empty_fasta_returns_empty(self) -> None:
        assert _parse_fasta("") == []

    def test_comment_only_returns_empty(self) -> None:
        assert _parse_fasta(">NOSEQ\n") == []


# ---------------------------------------------------------------------------
# UniProt header metadata extractor
# ---------------------------------------------------------------------------


class TestExtractUniProtHeaderMetadata:
    def test_full_swissprot_header(self) -> None:
        header = "sp|P12345|FOO_HUMAN Foo OS=Homo sapiens OX=9606 GN=FOO PE=1 SV=1"
        meta = extract_uniprot_header_metadata(header)
        assert meta["taxonomy_id"] == 9606
        assert meta["species"] == "Homo sapiens"

    def test_header_without_ox_returns_none(self) -> None:
        header = "sp|P12345|FOO_HUMAN Foo OS=Homo sapiens GN=FOO"
        meta = extract_uniprot_header_metadata(header)
        assert meta["taxonomy_id"] is None
        assert meta["species"] == "Homo sapiens"

    def test_plain_accession_header(self) -> None:
        meta = extract_uniprot_header_metadata("P12345")
        assert meta == {"taxonomy_id": None, "species": None}

    def test_empty_header(self) -> None:
        assert extract_uniprot_header_metadata("") == {"taxonomy_id": None, "species": None}

    def test_multi_word_species(self) -> None:
        header = "sp|Q9ZZZ9|BAR_ECOLI Bar OS=Escherichia coli (strain K12) OX=83333 GN=BAR PE=1 SV=1"
        meta = extract_uniprot_header_metadata(header)
        assert meta["taxonomy_id"] == 83333
        assert meta["species"] == "Escherichia coli (strain K12)"

    def test_ox_only_no_os(self) -> None:
        header = "custom|X1|X OX=42"
        meta = extract_uniprot_header_metadata(header)
        assert meta["taxonomy_id"] == 42
        assert meta["species"] is None

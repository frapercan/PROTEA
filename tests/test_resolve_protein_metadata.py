"""Today's UniProt entry of every protein, to show it, stamped with the release that served it.

What an entry says today was not known at an earlier cut, so the operation that
fetches it writes ``protein_uniprot_metadata`` and nothing else: never the
``protein`` row, whose organism and taxonomy feature code reads. These tests fake
the plugin and nothing else; the transport is tested in ``protea_sources``.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.operations import resolve_protein_metadata as module
from protea.core.operations.resolve_protein_metadata import ResolveProteinMetadataOperation
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.protein.protein_metadata import ProteinUniProtMetadata
from tests.test_run_count_map import pg  # noqa: F401

HEADER = (
    "Entry\tReviewed\tEntry Name\tProtein names\tGene Names\tOrganism\tLength\t"
    "EC number\tFunction [CC]\tOrganism (ID)"
)
HAEMOGLOBIN = (
    "P69905\treviewed\tHBA_HUMAN\tHemoglobin subunit alpha\tHBA1 HBA2\tHomo sapiens (Human)\t142\t"
    "\tInvolved in oxygen transport.\t9606"
)


def _op(*answers: SimpleNamespace) -> ResolveProteinMetadataOperation:
    """The operation with its plugin replaced by one that gives ``answers`` in turn."""
    op = ResolveProteinMetadataOperation()
    op._uniprot = MagicMock()
    op._uniprot.fetch_accessions.side_effect = list(answers)
    return op


def _answer(*rows: str, release: str | None = "2026_04") -> SimpleNamespace:
    return SimpleNamespace(
        tsv="\n".join((HEADER, *rows)) + "\n",
        release=release,
        release_date="04-November-2026" if release else None,
    )


def _protein(session: Session, accession: str, **fields: object) -> None:
    session.add(Protein(accession=accession, canonical_accession=accession.split("-")[0], **fields))
    session.flush()


def _emit(*_args: object) -> None:
    return None


class TestThePopulationIsAQuery:
    """Which proteins are asked about is read from the tables, never from the payload."""

    def test_it_asks_about_rows_with_no_release_in_order(self) -> None:
        src = inspect.getsource(ResolveProteinMetadataOperation._population)
        assert "uniprot_release.is_not(None)" in src
        assert "order_by(Protein.accession)" in src
        assert "Protein.is_canonical.is_(True)" in src

    def test_the_accession_grammar_is_a_barrier_before_the_batch(self) -> None:
        session = MagicMock()
        session.scalars.return_value.all.return_value = ["P69905", "NOEXISTE1", "Q8CF25"]
        assert ResolveProteinMetadataOperation._population(session, None) == ["P69905", "Q8CF25"]


class TestTheTransportBelongsToThePlugin:
    def test_it_imports_no_networking_library(self) -> None:
        tree = ast.parse(Path(inspect.getsourcefile(module) or "").read_text())
        imported = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in (
                node.names if isinstance(node, ast.Import) else [ast.alias(node.module or "")]
            )
        }
        assert not imported & {"requests", "httpx", "urllib", "urllib3", "aiohttp", "http"}


def test_every_row_names_the_release_that_served_it(pg: Session) -> None:  # noqa: F811
    _protein(pg, "P69905")
    _op(_answer(HAEMOGLOBIN)).execute(pg, {}, emit=_emit)

    entry = pg.get(ProteinUniProtMetadata, "P69905")
    assert entry is not None
    assert (entry.uniprot_release, entry.uniprot_release_date) == ("2026_04", "04-November-2026")
    assert (entry.entry_name, entry.reviewed, entry.organism_id, entry.length) == (
        "HBA_HUMAN",
        True,
        9606,
        142,
    )
    assert entry.function_cc == "Involved in oxygen transport."
    assert entry.organism_name == "Homo sapiens (Human)"


def test_the_protein_row_is_never_written(pg: Session) -> None:  # noqa: F811
    """Feature code reads the protein row's organism and taxonomy; today's entry must not reach it."""
    _protein(pg, "P69905")
    _op(_answer(HAEMOGLOBIN)).execute(pg, {}, emit=_emit)

    protein = pg.get(Protein, "P69905")
    assert protein is not None
    assert (protein.taxonomy_id, protein.organism, protein.gene_name, protein.entry_name) == (
        None,
        None,
        None,
        None,
    )


def test_an_answer_that_names_no_release_is_refused_and_writes_nothing(pg: Session) -> None:  # noqa: F811
    _protein(pg, "P69905")
    with pytest.raises(RuntimeError, match="named no release"):
        _op(_answer(HAEMOGLOBIN, release=None)).execute(pg, {}, emit=_emit)
    assert pg.scalars(select(ProteinUniProtMetadata)).all() == []


def test_what_uniprot_no_longer_serves_is_counted_absent(pg: Session) -> None:  # noqa: F811
    _protein(pg, "P69905")
    _protein(pg, "Q00000")
    result = _op(_answer(HAEMOGLOBIN)).execute(pg, {}, emit=_emit).result

    assert (result["candidates"], result["written"], result["absent"]) == (2, 1, 1)
    assert result["releases"] == ["2026_04"]
    assert pg.get(ProteinUniProtMetadata, "Q00000") is None


def test_a_second_run_asks_only_about_what_has_no_release(pg: Session) -> None:  # noqa: F811
    _protein(pg, "P69905")
    _protein(pg, "P69905-2", is_canonical=False)
    _protein(pg, "P68871")
    pg.add(ProteinUniProtMetadata(canonical_accession="P68871", uniprot_release="2026_04"))
    pg.flush()

    assert ResolveProteinMetadataOperation._population(pg, None) == ["P69905"]


def test_a_dry_run_writes_nothing(pg: Session) -> None:  # noqa: F811
    _protein(pg, "P69905")
    op = _op()
    result = op.execute(pg, {"dry_run": True}, emit=_emit).result

    assert (result["candidates"], result["written"], result["absent"]) == (1, 0, None)
    assert not op._uniprot.fetch_accessions.called

"""The shared store: one copy of the MD5 dedup and of the protein upsert.

These tests lived in ``tests/test_insert_proteins.py`` when the code was
``InsertProteinsOperation._store_records``, a private method that
``ensure_goa_universe`` reached into from outside. They moved with the code, which
is where they belong: what they cover is now used by TWO operations,
``insert_proteins`` and ``resolve_protein_sequences``, so a failure here breaks
both rather than one.

Mocked session and HTTP: no database, no network.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401
from protea.core.operations._protein_store import StoreCounts, store_records


def _noop_emit(event, message, fields, level):
    pass


def _make_mock_session():
    """Session mock that returns empty results for all DB queries."""
    session = MagicMock(spec=Session)
    session.query.return_value.filter.return_value.all.return_value = []
    session.query.return_value.filter.return_value.first.return_value = None
    return session


def _make_record(
    accession: str = "P12345",
    sequence: str = "MKTAYIAK",
    is_canonical: bool = True,
    isoform_index: int | None = None,
    canonical_accession: str | None = None,
):
    """Build a UniProtProteinRecord for store-records testing."""
    from protea_contracts import UniProtProteinRecord, compute_sequence_hash

    return UniProtProteinRecord(
        accession=accession,
        entry_name="TEST_HUMAN",
        canonical_accession=canonical_accession or accession,
        is_canonical=is_canonical,
        isoform_index=isoform_index,
        organism="Homo sapiens",
        taxonomy_id="9606",
        gene_name="TEST",
        reviewed=True,
        sequence=sequence,
        length=len(sequence),
        sequence_hash=compute_sequence_hash(sequence),
    )


class TestStoreRecords:
    def test_empty_records_returns_zeros(self):
        """With no records nothing is queried: the early return."""
        session = _make_mock_session()
        result = store_records(session, [], _noop_emit)
        assert result == StoreCounts(0, 0, 0, 0)
        session.query.assert_not_called()

    def test_updates_existing_protein(self):
        """An existing row is patched, never clobbered.

        This is what makes the two universe operations composable:
        ``extract_goa_universe`` writes a row with everything NULL and this
        function fills it in later without touching anything that already had a
        value.
        """
        record = _make_record()
        seq_hash = record.sequence_hash

        # Existing protein with missing fields (triggers updates)
        existing_prot = MagicMock()
        existing_prot.accession = "P12345"
        existing_prot.sequence_id = None  # will be updated
        existing_prot.entry_name = None  # will be updated
        existing_prot.canonical_accession = "OLD_ACC"  # will be updated
        existing_prot.is_canonical = False  # will be updated
        existing_prot.isoform_index = 2  # will be updated
        existing_prot.reviewed = None  # will be updated
        existing_prot.taxonomy_id = None  # will be updated
        existing_prot.organism = None  # will be updated
        existing_prot.gene_name = None  # will be updated
        existing_prot.length = None  # will be updated

        session = MagicMock(spec=Session)

        # _load_existing_sequences returns the hash → id map
        seq_query = MagicMock()
        seq_query.filter.return_value.all.return_value = [(seq_hash, 42)]

        # _load_existing_proteins returns the existing protein
        prot_query = MagicMock()
        prot_query.filter.return_value.all.return_value = [existing_prot]

        call_idx = {"n": 0}

        def query_side_effect(*args):
            call_idx["n"] += 1
            if call_idx["n"] == 1:
                return seq_query
            return prot_query

        session.query.side_effect = query_side_effect

        c = store_records(session, [record], _noop_emit)

        assert c.proteins_inserted == 0
        assert c.proteins_updated == 1  # existing protein was updated
        assert c.sequences_reused == 1  # sequence was reused from DB
        assert c.sequences_inserted == 0
        # Verify fields were updated
        assert existing_prot.sequence_id == 42
        assert existing_prot.entry_name == "TEST_HUMAN"
        assert existing_prot.canonical_accession == "P12345"
        assert existing_prot.is_canonical is True
        assert existing_prot.isoform_index is None
        assert existing_prot.reviewed is True

    def test_inserts_new_sequence_when_missing(self):
        """A hash the database does not hold becomes a new ``sequence`` row."""
        record = _make_record()

        session = MagicMock(spec=Session)

        # No existing sequences
        seq_query = MagicMock()
        seq_query.filter.return_value.all.return_value = []

        # No existing proteins
        prot_query = MagicMock()
        prot_query.filter.return_value.all.return_value = []

        call_idx = {"n": 0}

        def query_side_effect(*args):
            call_idx["n"] += 1
            if call_idx["n"] == 1:
                return seq_query
            return prot_query

        session.query.side_effect = query_side_effect

        c = store_records(session, [record], _noop_emit)

        assert c.proteins_inserted == 1
        assert c.proteins_updated == 0
        assert c.sequences_inserted == 1
        assert c.sequences_reused == 0
        # add_all called twice: once for sequences, once for proteins
        assert session.add_all.call_count == 2




class TestTheSequencelessRowIsCompletedLater:
    """The invariant that makes a two-step universe possible.

    ``extract_goa_universe`` writes a row with an accession, its canonical and
    nothing else; ``resolve_protein_sequences`` finds it later and fills it in. If
    the patching clobbered values, the ORDER of the two operations would matter,
    and a later ``insert_proteins`` pass over the same accession would undo one of
    them.
    """

    def _extracted_row(self, accession="P12345"):
        """Exactly what ``ExtractGoaUniverseOperation._accession_row`` writes."""
        from protea.infrastructure.orm.models.protein.protein import Protein

        canonical, is_canonical, isoform = Protein.parse_isoform(accession)
        return Protein(
            accession=accession,
            canonical_accession=canonical,
            is_canonical=is_canonical,
            isoform_index=isoform,
            first_admitted_release=156,
        )

    def test_fills_everything_that_was_null(self):
        from protea.core.operations._protein_store import apply_record_updates

        row = self._extracted_row()
        assert apply_record_updates(row, _make_record(), seq_id=42) is True
        assert row.sequence_id == 42
        assert row.entry_name == "TEST_HUMAN"
        assert row.organism == "Homo sapiens"
        assert row.taxonomy_id == "9606"
        assert row.gene_name == "TEST"
        assert row.reviewed is True
        assert row.length == 8

    def test_does_not_touch_the_release_that_admitted_it(self):
        """``first_admitted_release`` is the one thing the GAF knows and UniProt
        does not. A patch that clobbered it would erase the only observation of it
        the series makes."""
        from protea.core.operations._protein_store import apply_record_updates

        row = self._extracted_row()
        apply_record_updates(row, _make_record(), seq_id=42)
        assert row.first_admitted_release == 156

    def test_a_second_patch_changes_nothing(self):
        from protea.core.operations._protein_store import apply_record_updates

        row = self._extracted_row()
        apply_record_updates(row, _make_record(), seq_id=42)
        assert apply_record_updates(row, _make_record(), seq_id=99) is False
        assert row.sequence_id == 42, "the sequence was already there and is not overwritten"

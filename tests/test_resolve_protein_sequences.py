"""The UniProt pass: the universe gets its sequences once, not seventy-five times.

``extract_goa_universe`` writes accessions and nothing else. This operation is
what turns them into proteins with a chain to embed, and it runs ONE time over the
union of the series instead of once per release -- which is where the 34% of
repeated requests measured across the ten passes of ``ensure_goa_universe`` went.

These tests fake the plugin and NOTHING else. The transport -- retries, backoff,
``Retry-After``, which statuses are transient -- lives in ``protea_sources`` and is
tested there; a second copy of it lived in ``protea/core/operations/_universe_http.py``
until this split deleted it, which is why the retry tests that used to be in this
file are gone rather than rewritten. What is pinned here is everything the
operation decides: which proteins it asks about, what it does with a merge, what
it records about an accession that no longer exists, and that the audit dates
reach rows this run never fetched.
"""

import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from protea.core.operations._protein_store import StoreCounts
from protea.core.operations._universe_sources import _FetchOutcome
from protea.core.operations.resolve_protein_sequences import (
    ResolveProteinSequencesOperation,
    ResolveProteinSequencesPayload,
)


def _op():
    """The operation with its plugin reference replaced by a mock.

    ``__init__`` imports the real plugin, which is what production does; the tests
    swap the attribute afterwards so the import path itself stays exercised.
    """
    op = ResolveProteinSequencesOperation()
    op._uniprot = MagicMock()
    return op


class TestThePopulationIsAQuery:
    """WHICH proteins get asked about is NOT a payload field.

    It is "every ``protein`` row with no ``sequence_id``", read from the table at
    the moment the job runs. A list of accessions in the payload would be a
    hypothesis about the state of the database frozen at submission time, and this
    campaign has already been bitten by one of those: ``search_criteria:
    reviewed:true``, on 2026-09-15, fixed the scope of a whole campaign without
    anybody declaring it.
    """

    def test_the_query_filters_on_a_null_sequence(self):
        src = inspect.getsource(ResolveProteinSequencesOperation._without_sequence)
        assert "Protein.sequence_id.is_(None)" in src

    def test_it_is_ordered_so_a_cap_is_reproducible(self):
        """Without ``order_by`` two capped runs ask for different sets, and the
        second does not continue where the first stopped."""
        src = inspect.getsource(ResolveProteinSequencesOperation._without_sequence)
        assert "order_by(Protein.accession)" in src

    def test_the_accession_grammar_is_a_barrier_before_the_batch(self):
        """One malformed member makes UniProt answer 400 for the WHOLE batch.
        Measured: 'Accession NOEXISTE1 has invalid format'. That is a thousand
        proteins for one stray identifier, and this table is written by more than
        one operation."""
        op = _op()
        session = MagicMock()
        session.scalars.return_value.all.return_value = ["P12345", "NOEXISTE1", "Q8CF25"]
        assert op._without_sequence(session, None) == ["P12345", "Q8CF25"]


class TestTheTransportBelongsToThePlugin:
    """What this operation no longer contains.

    ``_universe_http.py`` was 115 lines duplicating the plugin's retry logic: the
    same ``{429, 500, 502, 503, 504}`` set, the same ``Retry-After``, the same
    backoff. Two copies of that diverge without anybody noticing, and the copy here
    was the one with no tests of its own.
    """

    def test_it_imports_no_networking_library(self):
        """Read from the AST and not from the text: the module's prose TALKS about
        retries and ``Retry-After`` to explain why it does not implement them, and a
        grep over the file cannot tell an explanation from an implementation.
        """
        import ast

        import protea.core.operations.resolve_protein_sequences as mod

        arbol = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
        importados = set()
        for nodo in ast.walk(arbol):
            if isinstance(nodo, ast.Import):
                importados.update(a.name.split(".")[0] for a in nodo.names)
            elif isinstance(nodo, ast.ImportFrom) and nodo.module:
                importados.add(nodo.module.split(".")[0])
        assert not importados & {"requests", "urllib", "http", "httpx", "socket"}, importados

    def test_everything_remote_goes_through_the_plugin(self):
        """The plugin's two methods and no others. A third way out would be another
        place where the retries can be missing."""
        import ast

        import protea.core.operations.resolve_protein_sequences as mod

        arbol = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
        llamadas = {
            n.func.attr
            for n in ast.walk(arbol)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and isinstance(n.func.value, ast.Attribute)
            and n.func.value.attr == "_uniprot"
        }
        assert llamadas == {"fetch_accessions_tsv", "search_secondary_accessions"}, llamadas

    def test_the_duplicated_module_no_longer_exists(self):
        with pytest.raises(ModuleNotFoundError):
            __import__("protea.core.operations._universe_http")

    def test_the_batch_sizes_are_the_plugins_measured_constants(self):
        """1001 answers "Only '1000' accessions are allowed in each request", and
        101 OR conditions answer "Maximum allowed is 100". These are facts about
        UniProt, not parameters: a caller cannot choose them, so they are not
        payload fields."""
        from protea_sources.uniprot import MAX_ACCESSIONS_PER_REQUEST, MAX_OR_CONDITIONS

        assert MAX_ACCESSIONS_PER_REQUEST == 1000
        assert MAX_OR_CONDITIONS == 100

    def test_the_payload_timeout_reaches_the_knobs(self):
        knobs = ResolveProteinSequencesOperation._knobs(
            ResolveProteinSequencesPayload(timeout_seconds=7)
        )
        assert knobs.timeout_seconds == 7
        assert knobs.max_retries == 6, "everything else keeps the measured values"


class TestSecondaryAccessions:
    """``/uniprotkb/accessions`` matches PRIMARY accessions only: a merged accession
    is not returned, and UniProt counts it in X-Total-Results regardless, so the
    response says "7 results" with an empty body. On GOA 156 that is 10,791
    accessions, of which 18.2% are recoverable merges."""

    _ENTRY = {
        "primaryAccession": "P04439",
        "secondaryAccessions": ["P30456", "P01892"],
        "entryType": "UniProtKB reviewed (Swiss-Prot)",
        "sequence": {"value": "MAVMAPRTLLLLLSG"},
        "organism": {"scientificName": "Homo sapiens", "taxonId": 9606},
        "genes": [{"geneName": {"value": "HLA-A"}}],
    }

    def _resolve(self, requested):
        op = _op()
        op._uniprot.search_secondary_accessions.return_value = {"results": [self._ENTRY]}
        stored = []

        def fake_store(_session, records, _emit):
            stored.extend(records)
            return StoreCounts(proteins_inserted=len(records), sequences_inserted=1)

        outcome = _FetchOutcome()
        with (
            patch(
                "protea.core.operations.resolve_protein_sequences.store_records",
                side_effect=fake_store,
            ),
            patch.object(
                ResolveProteinSequencesOperation,
                "_still_without_sequence",
                return_value=list(requested),
            ),
        ):
            op._resolve_merges(
                MagicMock(), list(requested), ResolveProteinSequencesPayload(),
                MagicMock(), outcome,
            )
        return outcome, stored

    def test_resolves_the_secondary_to_its_primary(self):
        outcome, _ = self._resolve(["P30456"])
        assert outcome.alias == {"P30456": "P04439"}

    def test_stores_the_primary_AND_the_alias(self):
        """Both rows are needed: the primary is the protein, and the alias is what
        satisfies the foreign key when phase 2 loads the 2016 annotation, which
        arrives under the old accession."""
        _, stored = self._resolve(["P30456"])
        by_acc = {r.accession: r for r in stored}
        assert set(by_acc) == {"P04439", "P30456"}
        assert by_acc["P04439"].is_canonical is True
        assert by_acc["P30456"].is_canonical is False
        assert by_acc["P30456"].canonical_accession == "P04439"

    def test_the_alias_is_not_an_isoform(self):
        """``isoform_index`` is what separates the two cases: an integer for an
        isoform, None for a merge alias. Without it, any count of isoforms by
        ``NOT is_canonical`` would count merges."""
        _, stored = self._resolve(["P30456"])
        assert all(r.isoform_index is None for r in stored)

    def test_the_two_rows_share_the_sequence(self):
        """Same hash, so ``store_records`` inserts ONE sequence row. And embeddings
        are keyed on Sequence rather than Protein, so this adds no duplicate
        neighbour to the KNN bank."""
        _, stored = self._resolve(["P30456"])
        assert len({r.sequence_hash for r in stored}) == 1
        assert len({r.sequence for r in stored}) == 1

    def test_only_the_requested_secondaries_produce_an_alias(self):
        """The entry carries two secondaries and only one was asked for. Creating
        the other would invent a protein no GAF annotated."""
        outcome, stored = self._resolve(["P30456"])
        assert "P01892" not in outcome.alias
        assert "P01892" not in {r.accession for r in stored}

    def test_what_it_cannot_resolve_is_left_named(self):
        """And the question goes to the DATABASE, not to the arithmetic:
        ``candidates`` minus ``fetched`` counts right but does not say WHO is
        missing, and who is missing is what has to be recorded."""
        outcome, _ = self._resolve(["P30456", "Q11111"])
        assert outcome.unresolved == ["Q11111"]


class TestWhatCannotBeResolvedKeepsItsName:
    """``not_retrievable: 10,791`` was a number with no names: proteins carrying
    curated experimental evidence that do not enter the corpus and could not be
    cited. A number cannot be audited; a list can."""

    def _store(self, alias, unresolved, job_id="11111111-2222-3333-4444-555555555555"):
        op = _op()
        put = {}

        class _Store:
            def put(self, key, path):
                put[key] = open(path, encoding="utf-8").read()
                return f"s3://artifacts/{key}"

        outcome = _FetchOutcome(alias=dict(alias), unresolved=list(unresolved))
        with (
            patch("protea.infrastructure.storage.get_artifact_store", return_value=_Store()),
            patch("protea.infrastructure.settings.load_settings", return_value=MagicMock()),
        ):
            out = op._store_artifacts(job_id, outcome)
        return out, put

    def test_the_list_of_unresolved_is_persisted(self):
        out, put = self._store({}, ["Q11111", "Q22222"])
        assert out["sin_resolver"]["filas"] == 2
        key = next(k for k in put if k.endswith("sin_resolver.txt"))
        assert put[key].split() == ["Q11111", "Q22222"]

    def test_the_merge_map_is_persisted_with_both_columns(self):
        """Without the map there is no canonicalising afterwards, and canonicalising
        is what joins the history of a protein that changed accession mid-series."""
        out, put = self._store({"P30456": "P04439"}, [])
        assert out["fusiones"]["filas"] == 1
        key = next(k for k in put if k.endswith("fusiones.tsv"))
        rows = [ln.split("\t") for ln in put[key].strip().split("\n")]
        assert rows[0] == ["accesion_gaf", "accesion_primaria"]
        assert rows[1] == ["P30456", "P04439"]

    def test_the_key_names_this_operation(self):
        """The prefix moved from ``goa_universe/`` to ``protein_resolution/``: the
        artifacts of the ten old passes stay where they were and do not mix with
        those of an operation that no longer does the same thing."""
        _, put = self._store({}, ["Q11111"])
        assert all(k.startswith("protein_resolution/") for k in put), put

    def test_with_no_job_id_it_writes_nothing(self):
        """A dry run and the tests call it with no job: there is nowhere to hang the
        artifact, and that is not an error."""
        out, put = self._store({"A": "B"}, ["C"], job_id=None)
        assert out == {}
        assert put == {}


class TestADemergeHasNoIdentity:
    """`C8VQ65` brought down the release-156 pass on 2026-10-05 after 317 correct
    merges: it is `DEMERGED` with `mergeDemergeTo: [P9WEV8, P9WEV9]`, so it appears
    in TWO entries. That produced two alias rows with the same accession in one
    batch, and the store, which separates inserts from updates by looking at the
    database and does not deduplicate its own input, sent them as two INSERTs:
    duplicate key.

    But the key clash is the symptom. The substance is that an accession split
    across two entries DOES NOT POINT AT ONE PROTEIN, and the sequence it would
    inherit would be one of two different ones, chosen by the order of the response.
    """

    def _entry(self, primary, secondaries, seq="MAVM"):
        return {
            "primaryAccession": primary,
            "secondaryAccessions": list(secondaries),
            "entryType": "UniProtKB reviewed (Swiss-Prot)",
            "sequence": {"value": seq},
            "organism": {"scientificName": "Mycobacterium tuberculosis", "taxonId": 83332},
            "genes": [],
        }

    def _candidates(self, entries, requested):
        from protea.core.operations._universe_sources import records_for_merge

        out = {}
        for e in entries:
            done = records_for_merge(e, set(requested))
            if done is None:
                continue
            primary, rows, found = done
            for sec in found:
                out.setdefault(sec, []).append((primary, rows))
        return out

    def test_a_demerge_produces_no_alias(self):
        from protea.core.operations._universe_sources import classify

        cand = self._candidates(
            [self._entry("P9WEV8", ["C8VQ65"], "AAAA"), self._entry("P9WEV9", ["C8VQ65"], "BBBB")],
            ["C8VQ65"],
        )
        alias, demerges = {}, {}
        records = classify(cand, alias, demerges)
        assert alias == {}, "no primary can be assigned to it"
        assert demerges == {"C8VQ65": ["P9WEV8", "P9WEV9"]}, "recorded with its destinations"
        assert "C8VQ65" not in {r.accession for r in records}

    def test_a_real_merge_does_produce_an_alias(self):
        from protea.core.operations._universe_sources import classify

        cand = self._candidates([self._entry("P04439", ["P30456"])], ["P30456"])
        alias, demerges = {}, {}
        records = classify(cand, alias, demerges)
        assert alias == {"P30456": "P04439"}
        assert demerges == {}
        assert {r.accession for r in records} == {"P04439", "P30456"}

    def test_no_accession_repeats_across_the_rows(self):
        """The immediate cause of the duplicate key. Two different secondaries
        landing on the same primary produce that primary twice."""
        from protea.core.operations._universe_sources import classify

        cand = self._candidates([self._entry("P04439", ["P30456", "P01892"])], ["P30456", "P01892"])
        alias, demerges = {}, {}
        records = classify(cand, alias, demerges)
        accs = [r.accession for r in records]
        assert len(accs) == len(set(accs)), f"repeated accession: {accs}"
        assert set(accs) == {"P04439", "P30456", "P01892"}
        assert alias == {"P30456": "P04439", "P01892": "P04439"}

    def test_the_demerge_does_not_contaminate_the_merges_of_its_batch(self):
        from protea.core.operations._universe_sources import classify

        cand = self._candidates(
            [
                self._entry("P9WEV8", ["C8VQ65"], "AAAA"),
                self._entry("P9WEV9", ["C8VQ65"], "BBBB"),
                self._entry("P04439", ["P30456"], "CCCC"),
            ],
            ["C8VQ65", "P30456"],
        )
        alias, demerges = {}, {}
        records = classify(cand, alias, demerges)
        assert alias == {"P30456": "P04439"}
        assert list(demerges) == ["C8VQ65"]
        accs = [r.accession for r in records]
        assert len(accs) == len(set(accs))


class TestTheAuditDates:
    """``date_created`` is what separates knowledge gain from entry creation, and
    ``date_sequence_modified`` is what turns the sequence leak into a named
    subset. Both arrive in the same request as the sequence, and both must be
    written by BOTH fetch paths -- the batch TSV one and the secondary JSON one.
    If only one wrote them, half the corpus would carry NULL temporal columns and
    any date filter would exclude those proteins without saying so.
    """

    _TSV = (
        "Entry\tEntry Name\tReviewed\tOrganism\tOrganism (ID)\tGene Names\t"
        "Length\tSequence\tSequence version\tDate of creation\t"
        "Date of last sequence modification\n"
        "P04439\tHLA_A\treviewed\tHomo sapiens\t9606\tHLA-A\t"
        "4\tMAVM\t2\t1987-08-13\t2003-08-22\n"
    )

    def test_the_tsv_path_carries_them(self):
        from protea.core.operations._universe_sources import _parse_tsv

        parsed = _parse_tsv(self._TSV)
        assert len(parsed) == 1
        record, dates = parsed[0]
        assert record.accession == "P04439"
        assert record.reviewed is True
        assert record.sequence == "MAVM"
        assert dates.date_created == "1987-08-13"
        assert dates.date_sequence_modified == "2003-08-22"
        assert dates.sequence_version == 2

    def test_a_reordered_response_fails_loudly(self):
        """A silently reordered response would write dates into the wrong column,
        and nothing downstream would notice a plausible date in a plausible
        field."""
        from protea.core.operations._universe_sources import _parse_tsv

        with pytest.raises(RuntimeError, match="columns"):
            _parse_tsv("Entry\tSequence\nP04439\tMAVM\n")

    def test_the_json_path_carries_the_same_three(self):
        from protea.core.operations._universe_sources import _audit_dates_of

        dates = _audit_dates_of(
            {
                "entryAudit": {
                    "firstPublicDate": "1987-08-13",
                    "lastSequenceUpdateDate": "2003-08-22",
                    "sequenceVersion": 2,
                }
            }
        )
        assert dates.date_created == "1987-08-13"
        assert dates.date_sequence_modified == "2003-08-22"
        assert dates.sequence_version == 2

    def test_an_entry_without_audit_gives_nulls_not_a_crash(self):
        from protea.core.operations._universe_sources import _audit_dates_of

        dates = _audit_dates_of({})
        assert dates == (None, None, None)

    def test_the_two_paths_agree_on_the_field_names(self):
        """The TSV and the JSON reach the same NamedTuple. A field renamed on one
        side only would leave the other writing to a column that no longer
        exists."""
        from protea.core.operations._universe_sources import _audit_dates_of, _parse_tsv

        _, from_tsv = _parse_tsv(self._TSV)[0]
        from_json = _audit_dates_of(
            {"entryAudit": {"firstPublicDate": "1987-08-13",
                            "lastSequenceUpdateDate": "2003-08-22",
                            "sequenceVersion": 2}}
        )
        assert from_tsv._fields == from_json._fields
        assert from_tsv == from_json


class TestTheDatesReachEveryUniverseMember:
    """The defect four independent reviewers caught on 2026-10-05, before it shipped.

    The sequence pass only ever sees rows with no sequence, so only those would
    carry audit dates. Everything ``insert_proteins`` loaded -- roughly 575,000 of
    some 680,000 rows -- has a sequence and NULL dates, and a date filter would
    silently exclude 85% of the corpus while appearing to work.

    What makes it dangerous is that nothing fails: the columns exist, the pass
    succeeds, and the numbers it reports are all correct. Only a query that filters
    on a date would reveal it, by returning far too little.
    """

    def test_a_dates_only_response_parses(self):
        from protea.core.operations._universe_sources import _parse_dates_tsv

        rows = _parse_dates_tsv(
            "Entry\tSequence version\tDate of creation\t"
            "Date of last sequence modification\n"
            "P04439\t2\t1987-08-13\t2003-08-22\n"
            "Q9NTW7\t3\t2002-03-27\t2003-04-30\n"
        )
        assert [acc for acc, _ in rows] == ["P04439", "Q9NTW7"]
        assert rows[0][1].date_created == "1987-08-13"
        assert rows[0][1].sequence_version == 2

    def test_a_reordered_dates_response_fails_loudly(self):
        """A creation date and a sequence version are both plausible in either
        column, so a silent reorder would be unnoticeable in the data."""
        from protea.core.operations._universe_sources import _parse_dates_tsv

        with pytest.raises(RuntimeError, match="date columns"):
            _parse_dates_tsv("Entry\tDate of creation\nP04439\t1987-08-13\n")

    def test_the_backfill_has_its_own_predicate(self):
        """``date_created IS NULL``, not "what this run fetched". They are
        different populations and that difference IS the defect."""
        src = inspect.getsource(ResolveProteinSequencesOperation._backfill_dates)
        assert "Protein.date_created.is_(None)" in src
        assert "sequence_id" not in src, "la poblacion de las fechas no es la de las secuencias"

    def test_the_backfill_runs_after_the_fetch(self):
        """Order matters: the backfill reads what is in the table, so it has to
        run after the fetch inserted this run's new proteins."""
        src = inspect.getsource(ResolveProteinSequencesOperation.execute)
        assert src.index("_fetch_sequences") < src.index("_backfill_dates")

    def test_the_result_reports_how_many_were_backfilled(self):
        """A run that silently filled nothing and a run that had nothing to fill
        look identical without this number."""
        op = _op()
        with patch.object(
            ResolveProteinSequencesOperation, "_without_sequence", return_value=["P12345"]
        ):
            out = op.execute(MagicMock(), {"dry_run": True}, emit=MagicMock())
        assert "dates_backfilled" in out.result
        assert out.result["candidates"] == 1


class TestACappedRunIsNotReadAsComplete:
    """``max_accessions`` exists for a smoke run before the real one.

    A silent cap is worse than no cap: the report of a capped run and that of a
    complete one would be identical, and the second is the one that says the corpus
    is whole.
    """

    def test_the_cap_travels_into_the_report(self):
        op = _op()
        with patch.object(
            ResolveProteinSequencesOperation, "_without_sequence", return_value=["P12345"]
        ):
            out = op.execute(
                MagicMock(), {"dry_run": True, "max_accessions": 500}, emit=MagicMock()
            )
        assert out.result["limit"] == 500

    def test_with_no_cap_the_report_says_so_too(self):
        op = _op()
        with patch.object(
            ResolveProteinSequencesOperation, "_without_sequence", return_value=[]
        ):
            out = op.execute(MagicMock(), {"dry_run": True}, emit=MagicMock())
        assert out.result["limit"] is None

    def test_a_cap_of_zero_is_an_error_not_an_empty_run(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ResolveProteinSequencesPayload(max_accessions=0)

    def test_execute_accepts_the_shape_base_worker_delivers(self):
        """``base_worker`` hands over ``{**job.payload, "_job_id": ...}`` and
        ``ProteaPayload`` forbids undeclared keys, so ``execute`` has to strip the
        transport key with ``contract_payload``. Written without it, the operation
        would have failed on its first real job."""
        import uuid

        op = _op()
        with patch.object(
            ResolveProteinSequencesOperation, "_without_sequence", return_value=[]
        ):
            out = op.execute(
                MagicMock(),
                {"dry_run": True, "_job_id": str(uuid.uuid4())},
                emit=MagicMock(),
            )
        assert out.result["dry_run"] is True

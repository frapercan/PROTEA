"""Writing the target file that makes the grid and the submission one measurement.

The defect this closes is not a crash. ``protea_predict`` requires
``--query_file``; the internal payload leaves the query population optional and
selects the whole corpus when it is omitted. A run that forgets to name its
targets therefore produces a complete, plausible, differently-populated result.

So the tests that matter here are about population and about bytes: that the
targets are the proteins which GAINED annotation and not the ones that lost it,
that the same evaluation set always produces the same file, and that a target
whose sequence is absent is reported rather than quietly dropped behind an
authoritative-looking sha256.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from protea.core.operations.export_evaluation_targets import (
    SCORED_CATEGORIES,
    WRAP,
    ExportEvaluationTargetsOperation,
    ExportEvaluationTargetsPayload,
    delta_accessions,
    format_fasta,
)


@dataclass
class _Data:
    """Stands in for EvaluationData; only the category maps are read."""

    nk: dict[str, set[str]] = field(default_factory=dict)
    lk: dict[str, set[str]] = field(default_factory=dict)
    pk: dict[str, set[str]] = field(default_factory=dict)
    removed: dict[str, set[str]] = field(default_factory=dict)


# --------------------------------------------------------------------------- population


def test_the_targets_are_the_union_of_the_scored_categories():
    data = _Data(nk={"P1": {"GO:1"}}, lk={"P2": {"GO:2"}}, pk={"P3": {"GO:3"}})

    assert delta_accessions(data, list(SCORED_CATEGORIES)) == ["P1", "P2", "P3"]


def test_a_protein_in_two_categories_appears_once():
    data = _Data(nk={"P1": {"GO:1"}}, lk={"P1": {"GO:2"}})

    assert delta_accessions(data, ["nk", "lk"]) == ["P1"]


def test_proteins_that_only_lost_annotation_are_never_targets():
    """The regression. ``removed`` is reported by the evaluation and never scored,
    so a target there is a query no metric can reward."""
    data = _Data(nk={"P1": {"GO:1"}}, removed={"P9": {"GO:9"}})

    assert delta_accessions(data, list(SCORED_CATEGORIES)) == ["P1"]


def test_removed_cannot_be_requested_as_a_category():
    """It would be a plausible request that silently changes the population."""
    with pytest.raises(ValueError, match="removed|unknown categories"):
        ExportEvaluationTargetsPayload(evaluation_set_id="e", categories=["removed"])


def test_an_empty_category_list_is_refused():
    with pytest.raises(ValueError, match="cannot be empty"):
        ExportEvaluationTargetsPayload(evaluation_set_id="e", categories=[])


def test_the_default_is_all_three_scored_categories():
    p = ExportEvaluationTargetsPayload(evaluation_set_id="e")

    assert p.categories == list(SCORED_CATEGORIES)


def test_a_single_category_can_be_taken_alone():
    """Useful for a no-knowledge-only run, which is a real thing we measure."""
    data = _Data(nk={"P1": set()}, lk={"P2": set()})

    assert delta_accessions(data, ["nk"]) == ["P1"]


# --------------------------------------------------------------------------- bytes


def test_the_order_is_sorted_rather_than_map_order():
    """A dict's order reflects how the delta happened to be built, so the same
    evaluation set would otherwise produce different bytes on a rebuild."""
    data = _Data(nk={"P9": set(), "P1": set(), "P5": set()})

    assert delta_accessions(data, ["nk"]) == ["P1", "P5", "P9"]


def test_the_header_is_a_bare_accession():
    """LAFA's parser takes the token before the first whitespace, and the field
    between the first two pipes only if they are there. A bare accession
    round-trips, so no convention has to be agreed twice."""
    got = format_fasta([("P12345", "MKV")]).decode()

    assert got.startswith(">P12345\n")


def test_sequences_are_wrapped_at_a_fixed_width():
    got = format_fasta([("P1", "A" * (WRAP + 5))]).decode().splitlines()

    assert got[1] == "A" * WRAP
    assert got[2] == "A" * 5


def test_a_sequence_shorter_than_the_wrap_is_one_line():
    got = format_fasta([("P1", "MKV")]).decode().splitlines()

    assert got == [">P1", "MKV"]


def test_the_same_records_always_produce_the_same_bytes():
    """The sha256 is the proof that two consumers scored one population, so the
    bytes have to be a function of the records and nothing else."""
    records = [("P1", "MKV"), ("P2", "AAA")]

    assert format_fasta(records) == format_fasta(list(records))


def test_no_records_produce_no_bytes_rather_than_a_stray_newline():
    assert format_fasta([]) == b""


def test_the_file_ends_with_a_newline():
    """Concatenation and line-counting downstream both assume it."""
    assert format_fasta([("P1", "MKV")]).endswith(b"\n")


# --------------------------------------------------------------------------- the operation


def test_the_operation_declares_what_it_writes():
    """It used to say "writes no rows", and that was the problem.

    The operation wrote the FASTA and a PERSON registered those bytes as a
    QuerySet through the upload endpoint, typing the provenance into the free
    text: the window as an id prefix, the job as a word, the sha as a string
    nothing compares against. So this assertion is inverted on purpose, and
    this comment is why: a reader who finds "writes no rows" in a later
    version is looking at a regression, not at a simplification.
    """
    op = ExportEvaluationTargetsOperation()

    assert op.name == "export_evaluation_targets"
    assert "writes no rows" not in op.description.lower()
    # The three things that make the set attributable are named in the
    # description, because the description is what a dispatcher reads.
    assert "QuerySet" in op.description
    assert "sha256" in op.description
    assert "window" in op.description


def test_the_description_says_which_proteins_are_excluded():
    """A reader must not have to guess whether losses are targets."""
    op = ExportEvaluationTargetsOperation()

    assert "lost" in op.description


def test_summarize_names_the_evaluation_set():
    op = ExportEvaluationTargetsOperation()

    assert "eval-1" in op.summarize_payload({"evaluation_set_id": "eval-1"})


def test_it_is_registered_so_it_can_be_dispatched():
    from protea.core.operation_catalog import build_operation_registry

    registry = build_operation_registry()

    assert registry.get("export_evaluation_targets").name == "export_evaluation_targets"


# ------------------------------------------------------- the round trip through LAFA


def _lafa_parser():
    """LAFA's own parser, imported from the image source rather than reimplemented.

    A copy here would keep agreeing with itself after the real one changed, which
    is the exact failure this section exists to catch.
    """
    import sys
    from pathlib import Path

    runtime = Path(__file__).resolve().parents[1] / "apps" / "method_runtime"
    if str(runtime) not in sys.path:
        sys.path.insert(0, str(runtime))
    prott5_encoder = pytest.importorskip("prott5_encoder")
    return prott5_encoder.parse_fasta, prott5_encoder.fasta_accessions


def _round_trip(tmp_path, records):
    parse_fasta, fasta_accessions = _lafa_parser()
    path = tmp_path / "targets.fasta"
    path.write_bytes(format_fasta(records))
    return parse_fasta(str(path)), fasta_accessions(str(path))


def test_the_published_file_round_trips_through_lafas_own_parser(tmp_path):
    """The claim the whole operation rests on, checked against the real reader."""
    records = [("P12345", "MKVLAA"), ("Q9Y6K9", "A" * (WRAP + 17))]

    parsed, _ = _round_trip(tmp_path, records)

    assert parsed == dict(records)


def test_an_isoform_accession_survives_the_pipe_rule(tmp_path):
    """LAFA takes the field between the first two pipes when they exist. A bare
    accession has none, so a dash-bearing isoform id must come back whole."""
    parsed, _ = _round_trip(tmp_path, [("P12345-2", "MKV")])

    assert list(parsed) == ["P12345-2"]


def test_file_order_is_what_lafa_reads_as_output_order(tmp_path):
    records = [("P1", "MK"), ("P2", "VA"), ("P3", "LL")]

    _, order = _round_trip(tmp_path, records)

    assert order == ["P1", "P2", "P3"]


def test_a_lower_case_sequence_is_published_in_the_form_that_gets_embedded(tmp_path):
    """The regression. LAFA upper-cases every residue line, so publishing lower
    case would mean the sha256 describes a file that is not what the model reads."""
    parsed, _ = _round_trip(tmp_path, [("P1", "mkvlaa")])

    assert parsed["P1"] == "MKVLAA"
    assert format_fasta([("P1", "mkvlaa")]) == format_fasta([("P1", "MKVLAA")])


def test_a_gap_character_is_removed_before_the_hash_rather_than_after(tmp_path):
    """Same argument: LAFA strips dashes, so a gap left in the file makes the
    checksum authoritative about the wrong object."""
    parsed, _ = _round_trip(tmp_path, [("P1", "MKV-LAA")])

    assert parsed["P1"] == "MKVLAA"
    assert format_fasta([("P1", "MKV-LAA")]) == format_fasta([("P1", "MKVLAA")])


def test_canonicalisation_is_idempotent():
    from protea.core.operations.export_evaluation_targets import canonical_residues

    once = canonical_residues("mkv-laa")

    assert canonical_residues(once) == once


def test_wrapping_counts_canonical_residues_not_raw_ones():
    """A gap inside the first line would otherwise push a residue onto the next
    line and change the bytes without changing the sequence."""
    with_gap = format_fasta([("P1", "A" * 30 + "-" + "A" * 30)])
    without = format_fasta([("P1", "A" * 60)])

    assert with_gap == without


# ------------------------------------------------- la declaracion del conjunto


class _FakeQuerySet:
    def __init__(self, qid, name) -> None:
        self.id, self.name = qid, name


class _DeclareSession:
    """Answers the two statements ``_declare`` issues, and records the adds."""

    def __init__(self, existing=None, ids=None) -> None:
        self.existing = existing
        self.ids = ids or {}
        self.added: list = []
        self.flushes = 0

    def execute(self, statement):
        sql = " ".join(str(statement).split())
        if "FROM query_set" in sql:
            return _Scalar(self.existing)
        # _sequence_ids
        return _Rows([(a, i) for a, i in self.ids.items()])

    def add(self, obj):
        import uuid as _uuid

        obj.id = _uuid.uuid4()
        self.added.append(obj)

    def add_all(self, objs):
        self.added.extend(objs)

    def flush(self):
        self.flushes += 1


class _Scalar:
    def __init__(self, value) -> None:
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _Rows:
    def __init__(self, rows) -> None:
        self._rows = rows

    def all(self):
        return self._rows


def _pub(digest="a" * 64):
    from protea.core.operations.export_evaluation_targets import _Published

    return _Published(
        uri=f"s3://protea/eval_targets/w/targets-{digest[:12]}.fasta",
        key="eval_targets/w/targets.fasta",
        digest=digest,
        records=[("P1", "MKV"), ("P2", "MKA")],
        missing=0,
        altered=0,
    )


def _dec(window):
    from protea.core.operations.export_evaluation_targets import _Declaration

    return _Declaration(
        window=window, name="targets-w-aaaa", description="the delta of w", job_id=None
    )


def test_declaring_the_same_population_twice_finds_the_first_row():
    """One window plus one set of bytes is ONE population.

    Without this a re-export makes a second row for the same delta, which is
    two names for one thing: exactly what ``dianas-valid-211-227`` and its
    titular already were.
    """
    import uuid

    from protea.core.operations.export_evaluation_targets import _declare

    ya = _FakeQuerySet(uuid.uuid4(), "el-que-ya-estaba")
    session = _DeclareSession(existing=ya)
    eventos: list[tuple[str, dict]] = []

    devuelto = _declare(
        session, _dec(uuid.uuid4()), _pub(), lambda e, _m, f, _l: eventos.append((e, f)) or f
    )

    assert devuelto == ya.id
    assert session.added == []
    assert [e for e, _ in eventos] == ["targets.query_set_reused"]


def test_the_entries_point_at_the_sequence_the_protein_already_has():
    """NOT at a re-hash of the published bytes, and this is the trap it disarms.

    The file carries ``canonical_residues``, so re-hashing those bytes hashes
    the CANONICALISED sequence; where that differs from the stored one the
    upload endpoint inserts a SECOND ``sequence`` row and the entries point at
    sequences with no embedding. Measured on 2026-11-08: 0 of 138.297 entries
    had sprung it, because the canonicalisation was a no-op for this
    population. A trap that has not sprung is still armed.
    """
    import uuid

    from protea.core.operations.export_evaluation_targets import _declare

    window = uuid.uuid4()
    session = _DeclareSession(existing=None, ids={"P1": 11, "P2": 22})
    eventos: list[tuple[str, dict]] = []

    _declare(session, _dec(window), _pub(), lambda e, _m, f, _l: eventos.append((e, f)) or f)

    entradas = [o for o in session.added if hasattr(o, "accession")]
    assert {(e.accession, e.sequence_id) for e in entradas} == {("P1", 11), ("P2", 22)}
    declarado = next(o for o in session.added if hasattr(o, "source_kind"))
    assert declarado.source_kind == "evaluation_targets"
    assert declarado.evaluation_set_id == window
    assert declarado.content_sha256 == "a" * 64
    assert [e for e, _ in eventos] == ["targets.query_set_declared"]


def test_a_target_with_no_sequence_row_is_left_out_and_not_guessed():
    """The entry needs a ``sequence_id``, and inventing one would be a lie.

    ``_report_missing`` already emits these as a warning, so they are visible;
    what must not happen is an entry pointing at some other protein's
    sequence.
    """
    import uuid

    from protea.core.operations.export_evaluation_targets import _declare

    session = _DeclareSession(existing=None, ids={"P1": 11})
    _declare(session, _dec(uuid.uuid4()), _pub(), lambda *_a: None)

    entradas = [o for o in session.added if hasattr(o, "accession")]
    assert [e.accession for e in entradas] == ["P1"]

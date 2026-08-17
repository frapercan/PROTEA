"""Unit tests for the protea-sparse-knn LAFA container driver.

Every test here runs without torch, without the backbone and without the
frozen bank: the pieces exercised are the ones a wrong answer would ship
silently, namely the ontology walk, the vote, the propagation and the
output contract.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import numpy as np
import pytest

from apps.lafa_sparse_knn import sparse_driver as driver

# A three-level toy ontology. GO:0000003 is a molecular function whose
# parent chain reaches the root, so a vote on the leaf must surface every
# ancestor under the true path rule.
OBO = """format-version: 1.2
data-version: releases/2025-07-22

[Term]
id: GO:0000001
name: root function
namespace: molecular_function

[Term]
id: GO:0000002
name: middle function
namespace: molecular_function
is_a: GO:0000001 ! root function

[Term]
id: GO:0000003
name: leaf function
namespace: molecular_function
alt_id: GO:0000099
is_a: GO:0000002 ! middle function

[Term]
id: GO:0000010
name: a component
namespace: cellular_component
relationship: part_of GO:0000011 ! bigger component

[Term]
id: GO:0000011
name: bigger component
namespace: cellular_component

[Term]
id: GO:0000020
name: dead term
namespace: biological_process
is_obsolete: true
"""


@pytest.fixture
def ontology(tmp_path: Path) -> Path:
    path = tmp_path / "go-basic.obo"
    path.write_text(OBO)
    return path


def test_parse_obo_reads_parents_aspects_and_alt_ids(ontology: Path) -> None:
    parents, aspect, alt = driver.parse_obo(ontology)

    assert parents["GO:0000003"] == ["GO:0000002"]
    assert aspect["GO:0000003"] == "F"
    assert aspect["GO:0000010"] == "C"
    assert alt["GO:0000099"] == "GO:0000003"


def test_parse_obo_drops_obsolete_terms(ontology: Path) -> None:
    parents, _, _ = driver.parse_obo(ontology)

    assert "GO:0000020" not in parents


def test_part_of_counts_as_a_parent(ontology: Path) -> None:
    """A component that is part_of another must propagate into it.

    Dropping ``part_of`` and keeping only ``is_a`` is the classic way to
    under-propagate the cellular component aspect, which then reads as a
    weak method rather than a parsing bug.
    """
    parents, _, _ = driver.parse_obo(ontology)

    assert driver.ancestors_of("GO:0000010", parents, {}) == {"GO:0000010", "GO:0000011"}


def test_ancestors_include_the_term_and_the_whole_chain(ontology: Path) -> None:
    parents, _, _ = driver.parse_obo(ontology)

    assert driver.ancestors_of("GO:0000003", parents, {}) == {
        "GO:0000003",
        "GO:0000002",
        "GO:0000001",
    }


def test_read_fasta_keeps_file_order_and_takes_the_first_token(tmp_path: Path) -> None:
    path = tmp_path / "q.fasta"
    path.write_text(">Q1 some description here\nMKV\nLLA\n>Q2\nGGG\n")

    assert driver.read_fasta(path) == {"Q1": "MKVLLA", "Q2": "GGG"}


def test_transfer_scores_by_similarity_weighted_agreement(ontology: Path) -> None:
    """Under pure agreement: a term every neighbour carries reaches 1.0, and
    a term one of two carries at equal similarity reaches 0.5.

    Stated against ``vote`` explicitly, since the container's default is the
    blend and this test is about what the agreement channel means.
    """
    parents, _, alt = driver.parse_obo(ontology)
    donors = {"D1": ["GO:0000003"], "D2": ["GO:0000002"]}
    neighbours = [[("D1", 1.0), ("D2", 1.0)]]

    scored = driver.transfer(["Q1"], neighbours, donors, parents, alt, scheme="vote")

    # Both donors imply the root and the middle term, only D1 the leaf.
    assert scored["Q1"]["GO:0000001"] == pytest.approx(1.0)
    assert scored["Q1"]["GO:0000002"] == pytest.approx(1.0)
    assert scored["Q1"]["GO:0000003"] == pytest.approx(0.5)


def test_transfer_resolves_an_alternate_identifier(ontology: Path) -> None:
    """A donor annotated with a secondary id must still vote for the term.

    The bank carries whatever the annotation release used; if the
    container did not map alt ids it would drop those votes silently.
    """
    parents, _, alt = driver.parse_obo(ontology)
    scored = driver.transfer(["Q1"], [[("D1", 1.0)]], {"D1": ["GO:0000099"]}, parents, alt)

    assert scored["Q1"]["GO:0000003"] == pytest.approx(1.0)


def test_transfer_weights_a_closer_neighbour_more(ontology: Path) -> None:
    parents, _, alt = driver.parse_obo(ontology)
    donors = {"D1": ["GO:0000003"], "D2": ["GO:0000002"]}

    scored = driver.transfer(["Q1"], [[("D1", 0.9), ("D2", 0.1)]], donors, parents, alt)

    assert scored["Q1"]["GO:0000003"] == pytest.approx(0.9)


def test_an_unannotated_neighbour_does_not_dilute_the_score(ontology: Path) -> None:
    """A neighbour with no GO terms must not lower the other neighbours' scores.

    About 3 percent of the bank has an embedding and no annotation. If
    those entered the denominator, a query whose retrieval happened to
    return three of them would see every score fall by a tenth, which is
    a property of the reference set and not of the evidence for the call.
    """
    parents, _, alt = driver.parse_obo(ontology)
    donors = {"D1": ["GO:0000003"], "D2": []}

    with_silent = driver.transfer(["Q1"], [[("D1", 1.0), ("D2", 1.0)]], donors, parents, alt)
    without = driver.transfer(["Q1"], [[("D1", 1.0)]], donors, parents, alt)

    assert with_silent["Q1"]["GO:0000003"] == pytest.approx(1.0)
    assert with_silent["Q1"] == without["Q1"]


def test_a_neighbour_absent_from_the_donor_table_is_also_silent(ontology: Path) -> None:
    """Absent from the donor file and present but empty must behave alike."""
    parents, _, alt = driver.parse_obo(ontology)

    scored = driver.transfer(
        ["Q1"], [[("D1", 1.0), ("UNKNOWN", 1.0)]], {"D1": ["GO:0000003"]}, parents, alt
    )

    assert scored["Q1"]["GO:0000003"] == pytest.approx(1.0)


def test_transfer_yields_nothing_when_no_neighbour_can_vote(ontology: Path) -> None:
    """Every neighbour unannotated is not the same as a weak call: it is no
    call at all, so the query gets an empty prediction rather than zeros."""
    parents, _, alt = driver.parse_obo(ontology)

    scored = driver.transfer(["Q1"], [[("D1", 0.9), ("D2", 0.8)]], {}, parents, alt)

    assert scored["Q1"] == {}


def test_transfer_yields_nothing_when_every_neighbour_is_orthogonal(ontology: Path) -> None:
    """Non-positive similarity carries no evidence, so the query gets no
    calls rather than a uniform prior."""
    parents, _, alt = driver.parse_obo(ontology)

    scored = driver.transfer(["Q1"], [[("D1", 0.0)]], {"D1": ["GO:0000003"]}, parents, alt)

    assert scored["Q1"] == {}


def test_scores_never_leave_the_unit_interval(ontology: Path) -> None:
    parents, _, alt = driver.parse_obo(ontology)
    donors = {f"D{i}": ["GO:0000003"] for i in range(10)}
    neighbours = [[(f"D{i}", 1.0) for i in range(10)]]

    scored = driver.transfer(["Q1"], neighbours, donors, parents, alt)

    assert all(0.0 <= v <= 1.0 for v in scored["Q1"].values())


def test_write_predictions_caps_each_aspect_independently(ontology: Path, tmp_path: Path) -> None:
    """The cap is per aspect on purpose.

    A global cap lets a large biological process closure crowd out the
    molecular function calls, which shows up as a collapsed MF score and
    not as a truncation.
    """
    _, aspect, _ = driver.parse_obo(ontology)
    scored = {
        "Q1": {
            "GO:0000001": 0.9,
            "GO:0000002": 0.8,
            "GO:0000003": 0.7,
            "GO:0000010": 0.6,
        }
    }
    out = tmp_path / "predictions.tsv"

    written = driver.write_predictions(scored, aspect, ["Q1"], out, {"P": 2, "F": 2, "C": 2})

    lines = out.read_text().strip().split("\n")
    assert written == 3
    functions = [line for line in lines if line.split("\t")[1] in ("GO:0000001", "GO:0000002")]
    components = [line for line in lines if line.split("\t")[1] == "GO:0000010"]
    assert len(functions) == 2
    assert len(components) == 1


def test_output_is_the_three_column_contract(ontology: Path, tmp_path: Path) -> None:
    _, aspect, _ = driver.parse_obo(ontology)
    out = tmp_path / "predictions.tsv"

    driver.write_predictions(
        {"Q1": {"GO:0000001": 0.5}}, aspect, ["Q1"], out, {"P": 500, "F": 500, "C": 500}
    )

    assert out.read_text() == "Q1\tGO:0000001\t0.500\n"


def test_load_bank_rejects_a_misaligned_bank(tmp_path: Path) -> None:
    """The accession list and the code rows index each other positionally.

    If they ever disagree, every prediction is attributed to the wrong
    protein and nothing downstream would notice, so this must fail loudly.
    """
    (tmp_path / "BANK.json").write_text(json.dumps({"dict_dim": 16}))
    np.save(tmp_path / "codes_idx.npy", np.zeros((3, 2), dtype=np.uint16))
    np.save(tmp_path / "codes_val.npy", np.ones((3, 2), dtype=np.float16))
    np.save(tmp_path / "accessions.npy", np.array(["A", "B"]))

    with pytest.raises(ValueError, match="misaligned"):
        driver.load_bank(tmp_path)


def test_search_returns_the_nearest_first(tmp_path: Path) -> None:
    """End to end over the sparse retrieval, with a bank built by hand."""
    from scipy import sparse

    bank = sparse.csr_matrix(
        np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.7, 0.7, 0.0]], dtype=np.float32)
    )
    queries = sparse.csr_matrix(np.array([[1.0, 0.0, 0.0]], dtype=np.float32))

    hits = driver.search(queries, bank, ["A", "B", "C"], k=3)

    assert [accession for accession, _ in hits[0]] == ["A", "C", "B"]


def test_load_donors_groups_by_accession(tmp_path: Path) -> None:
    with gzip.open(tmp_path / "donors.tsv.gz", "wt") as handle:
        handle.write("A\tGO:0000001\nA\tGO:0000002\nB\tGO:0000003\n")

    donors = driver.load_donors(tmp_path)

    assert donors["A"] == ["GO:0000001", "GO:0000002"]
    assert donors["B"] == ["GO:0000003"]


def test_a_query_is_never_evicted_from_its_own_neighbour_list() -> None:
    """A protein tied with many identical twins must still be its own donor.

    The acyl carrier protein of E. coli has 41 canonical accessions
    carrying its exact sequence. Every one ties at cosine 1.0, so with
    k=30 the query itself can be dropped by the sort's tie-break and
    stops donating what the reference set already knows about it.
    """
    from scipy import sparse

    vector = np.array([[1.0, 0.0]], dtype=np.float32)
    twins = np.repeat(vector, 5, axis=0)
    bank = sparse.csr_matrix(twins)
    names = ["TWIN1", "TWIN2", "SELF", "TWIN3", "TWIN4"]

    hits = driver.search(sparse.csr_matrix(vector), bank, names, k=2, query_accessions=["SELF"])

    assert "SELF" in [accession for accession, _ in hits[0]]


def test_search_without_query_accessions_is_unchanged() -> None:
    """The guarantee is opt-in, so the plain call keeps its old behaviour."""
    from scipy import sparse

    bank = sparse.csr_matrix(np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32))
    hits = driver.search(
        sparse.csr_matrix(np.array([[1.0, 0.0]], dtype=np.float32)), bank, ["A", "B"], k=1
    )

    assert [a for a, _ in hits[0]] == ["A"]


class _FakeTorch:
    """Minimal stand-in exposing the two torch attributes _embed_block uses."""

    class OutOfMemoryError(Exception):
        pass

    class cuda:  # noqa: N801
        @staticmethod
        def empty_cache() -> None:
            return None


class _OomBackend:
    """Backend that refuses blocks larger than ``limit`` with an OOM."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.calls: list[int] = []

    def embed_chunks(self, model, tokenizer, seqs, config, dev):  # noqa: ANN001
        self.calls.append(len(seqs))
        if len(seqs) > self.limit:
            raise _FakeTorch.OutOfMemoryError("out of memory")
        return [[type("C", (), {"vector": np.ones(4, dtype=np.float32)})()] for _ in seqs]


def test_a_block_that_does_not_fit_is_halved_until_it_does() -> None:
    """A fixed batch size dies on the worst block; splitting survives it.

    Attention is quadratic in length, so memory depends on the block's
    contents and on whatever else is resident on the card. A run died at
    1,248 of 7,401 queries because another process held 4.4 GB.
    """
    backend = _OomBackend(limit=2)
    seqs = {f"P{i}": "MKV" for i in range(8)}

    out = driver._embed_block(backend, None, None, seqs, list(seqs), None, "cuda", _FakeTorch)

    assert [a for a, _ in out] == list(seqs)
    assert max(backend.calls) == 8
    assert min(backend.calls) <= 2


def test_a_single_sequence_that_never_fits_is_skipped_not_fatal() -> None:
    """Losing one protein beats losing the run."""
    backend = _OomBackend(limit=0)

    out = driver._embed_block(backend, None, None, {"P1": "MKV"}, ["P1"], None, "cuda", _FakeTorch)

    assert out == []


def test_the_default_score_is_the_platform_blend(ontology: Path) -> None:
    """The container's default must be the blend at PROTEA's own ratio.

    Pure agreement won none of the nine cells of a release window; the
    blend at 0.70 won four, and 0.67 is that ratio as the platform's
    composite configuration already stated it.
    """
    parents, _, alt = driver.parse_obo(ontology)
    donors = {"D1": ["GO:0000003"], "D2": ["GO:0000002"]}
    hits = [[("D1", 0.9), ("D2", 0.3)]]

    default = driver.transfer(["Q1"], hits, donors, parents, alt)
    explicit = driver.transfer(["Q1"], hits, donors, parents, alt, scheme="blend", blend=0.67)

    assert driver.DEFAULT_BLEND == 0.67
    assert default == explicit


def test_the_blend_ends_are_the_two_pure_schemes(ontology: Path) -> None:
    """w=0 must reproduce vote exactly and w=1 must reproduce maxsim."""
    parents, _, alt = driver.parse_obo(ontology)
    donors = {"D1": ["GO:0000003"], "D2": ["GO:0000002"]}
    hits = [[("D1", 0.9), ("D2", 0.3)]]

    for w, scheme in ((0.0, "vote"), (1.0, "maxsim")):
        blended = driver.transfer(["Q1"], hits, donors, parents, alt, scheme="blend", blend=w)
        pure = driver.transfer(["Q1"], hits, donors, parents, alt, scheme=scheme)
        for term, value in pure["Q1"].items():
            assert blended["Q1"][term] == pytest.approx(value), f"{scheme} at w={w}, {term}"


def test_a_handful_of_misses_is_reported_but_does_not_fail_the_check() -> None:
    """Drift is systemic or it is not drift.

    Measured on 256 real benchmark targets, 254 rank first and two do not,
    because the benchmark's FASTA carries a different sequence for those two
    accessions than the release the bank was built from. The first version of
    this check demanded all of them and reported FAILURE on that run, which is
    the same mistake as the cosine threshold it replaced: it sends a correctly
    configured evaluator away to debug a working setup.
    """
    from apps.lafa_sparse_knn.sparse_driver import report_self_check

    hits = [[(f"P{i}", 0.99)] for i in range(10)]
    hits[3] = [("OTHER", 0.99), ("P3", 0.40)]
    accs = [f"P{i}" for i in range(10)]
    out = report_self_check(accs, hits, set(accs) | {"OTHER"})
    assert out["rank_one"] == 9
    assert out["exceptions"] == ["P3"], "the miss is named, not swallowed"


def test_a_systemic_miss_still_fails_the_check() -> None:
    """The broken case measured here was 0 of 25 at a negative self-cosine."""
    from apps.lafa_sparse_knn.sparse_driver import report_self_check

    accs = [f"P{i}" for i in range(10)]
    hits = [[("OTHER", 0.9), (a, -0.03)] for a in accs]
    out = report_self_check(accs, hits, set(accs) | {"OTHER"})
    assert out["rank_one"] == 0
    assert len(out["exceptions"]) == 10


def test_the_self_check_passes_when_every_query_is_its_own_neighbour() -> None:
    """The parity report the method card tells a recipient to run.

    It has to exist for that instruction to be honest: before this, the sheet
    prescribed a check the delivered container could not perform, because the
    driver never emitted a neighbour identity or a cosine.
    """
    from apps.lafa_sparse_knn.sparse_driver import report_self_check

    hits = [
        [("P1", 1.0), ("P2", 0.8)],
        [("P2", 0.97), ("P1", 0.5)],
    ]
    out = report_self_check(["P1", "P2"], hits, {"P1", "P2", "P3"})
    assert out["checked"] == 2
    assert out["rank_one"] == 2
    assert out["missing"] == 0


def test_an_exact_tie_ahead_of_a_query_still_counts_as_rank_one() -> None:
    """Accessions sharing a sequence share a code exactly, so a query is often
    preceded by its own twins at an identical cosine. That is not a failure."""
    from apps.lafa_sparse_knn.sparse_driver import report_self_check

    hits = [[("TWIN", 1.0), ("P1", 1.0), ("OTHER", 0.4)]]
    out = report_self_check(["P1"], hits, {"P1", "TWIN", "OTHER"})
    assert out["rank_one"] == 1, "an equal cosine ahead of it is a tie, not a miss"


def test_a_query_outranked_by_a_strictly_closer_donor_fails_the_check() -> None:
    """This is the drift the check exists to catch: something genuinely nearer
    than the protein's own bank row means the two are in different geometries."""
    from apps.lafa_sparse_knn.sparse_driver import report_self_check

    hits = [[("OTHER", 0.99), ("P1", 0.60)]]
    out = report_self_check(["P1"], hits, {"P1", "OTHER"})
    assert out["rank_one"] == 0
    assert out["lowest_cosine"] == 0.60


def test_a_query_absent_from_its_own_neighbours_is_counted_as_missing() -> None:
    from apps.lafa_sparse_knn.sparse_driver import report_self_check

    out = report_self_check(["P1"], [[("A", 0.9), ("B", 0.8)]], {"P1", "A", "B"})
    assert out["missing"] == 1
    assert out["rank_one"] == 0


def test_a_query_not_in_the_bank_is_not_checked() -> None:
    """Only proteins the bank knows can be checked, and most queries are not."""
    from apps.lafa_sparse_knn.sparse_driver import report_self_check

    assert report_self_check(["NEW"], [[("A", 0.7)]], {"A", "B"}) == {}


def test_a_uniprot_header_yields_the_accession_not_the_whole_field() -> None:
    """The benchmark ships `>sp|ACC|NAME`, and the first token is not the ID.

    Taking the first token gives `sp|Q6GZX4|001R_FRG3G`, which matches nothing
    in a ground truth keyed by accession, so every row in the output file is
    silently unscorable. Confirmed against the published image before the fix.
    """
    from apps.lafa_sparse_knn.sparse_driver import accession_of

    assert accession_of("sp|Q6GZX4|001R_FRG3G Putative transcription factor") == "Q6GZX4"
    assert accession_of("tr|A0A075B6T8|A0A075B6T8_HUMAN Something") == "A0A075B6T8"


def test_a_bare_header_is_unchanged() -> None:
    """The form the locally derived target files use must keep working."""
    from apps.lafa_sparse_knn.sparse_driver import accession_of

    assert accession_of("A0A075B6T8") == "A0A075B6T8"
    assert accession_of("A0A075B6T8 some description here") == "A0A075B6T8"


def test_an_identifier_containing_a_pipe_is_left_alone() -> None:
    """Anchored on sp and tr, so a pipe in someone else's identifier survives."""
    from apps.lafa_sparse_knn.sparse_driver import accession_of

    assert accession_of("weird|id|here description") == "weird|id|here"
    assert accession_of("gnl|MYDB|thing") == "gnl|MYDB|thing"


def test_read_fasta_keys_a_uniprot_file_by_accession(tmp_path: Path) -> None:
    from apps.lafa_sparse_knn.sparse_driver import read_fasta

    p = tmp_path / "q.fasta"
    p.write_text(">sp|Q6GZX4|001R_FRG3G desc\nMAFS\n>sp|Q6GZX3|002L_FRG3G desc\nMSII\n")
    assert read_fasta(p) == {"Q6GZX4": "MAFS", "Q6GZX3": "MSII"}

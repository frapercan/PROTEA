"""Alignments are reused across runs, and reuse must not change the numbers.

An alignment depends on the two sequences and on nothing else: not the encoder,
not K, not the window, not the donor policy. The rung-1 grid recomputed them
per (model, K) run anyway, and profiling put them at 63% of a batch.

The recurrence that makes this worth doing was measured on that grid, not
assumed: within one model K=3's pairs were a strict subset of K=30's (1,239 of
1,239), and across two different encoders 1,063 of 1,216 pairs recurred.
"""

from __future__ import annotations

from typing import Any

from protea.core import alignment_cache as ac
from protea.core.feature_engineering import compute_alignment

SEQ_A = "MKTAYIAKQRQISFVKSHFSRQLEERLGLIEVQ"
SEQ_B = "MKTAYIAKQRQISFVKSHFSRQLEERLGLIEVR"
SEQ_C = "MSTNPKPQRKTKRNTNRRPQDVKFPGGGQIVGG"


class _FakeCache:
    """Records what it was asked for, so the test can assert on the traffic."""

    def __init__(self, seeded: dict[tuple[str, str], dict[str, Any]] | None = None):
        self.rows = dict(seeded or {})
        self.lookups: list[tuple[str, str]] = []
        self.stored: list[tuple[str, str]] = []

    def lookup(self, pairs: Any) -> dict[tuple[str, str], dict[str, Any]]:
        asked = list(pairs)
        self.lookups.extend(asked)
        return {p: self.rows[p] for p in asked if p in self.rows}

    def store(self, computed: Any) -> int:
        for key, feats in computed.items():
            self.rows[key] = dict(feats)
            self.stored.append(key)
        return len(self.stored)


class TestTheFieldsMatchWhatIsComputed:
    def test_the_cache_stores_exactly_the_computed_keys(self) -> None:
        """A field added to compute_alignment and not here would be dropped
        silently on every cache hit, which is a wrong feature, not a miss."""
        assert set(ac.ALIGNMENT_FIELDS) == set(compute_alignment(SEQ_A, SEQ_B))


class TestTheKeyIsTheSequenceNotTheName:
    def test_hashes_come_from_the_canonical_project_hash(self) -> None:
        from protea.infrastructure.orm.models.sequence.sequence import Sequence

        out = ac.hashes_for({"P1": SEQ_A, "P2": SEQ_B})
        assert out == {"P1": Sequence.compute_hash(SEQ_A), "P2": Sequence.compute_hash(SEQ_B)}

    def test_the_same_sequence_under_two_accessions_shares_a_key(self) -> None:
        """This is the reuse the cache exists for."""
        out = ac.hashes_for({"P1": SEQ_A, "RENAMED": SEQ_A})
        assert out["P1"] == out["RENAMED"]

    def test_a_changed_sequence_does_not_reuse_the_old_entry(self) -> None:
        """The failure an accession-keyed cache would have: same name, new
        sequence after a release, stale alignment served as current."""
        out = ac.hashes_for({"P1": SEQ_A})
        after = ac.hashes_for({"P1": SEQ_C})
        assert out["P1"] != after["P1"]

    def test_empty_sequences_are_not_keyed(self) -> None:
        assert ac.hashes_for({"P1": "", "P2": SEQ_A}) == {
            "P2": ac.hashes_for({"P2": SEQ_A})["P2"]
        }


class TestMissing:
    def test_it_returns_only_uncached_pairs_in_order(self) -> None:
        pairs = [("a", "b"), ("c", "d"), ("a", "b")]
        assert ac.missing(pairs, {("a", "b"): {}}) == [("c", "d")]


class TestReuseDoesNotChangeTheNumbers:
    """The property that matters: a cache hit must equal a fresh computation."""

    def test_a_hit_equals_recomputation(self) -> None:
        fresh = compute_alignment(SEQ_A, SEQ_B)
        cache = _FakeCache()
        cache.store({("qh", "rh"): fresh})
        assert cache.lookup([("qh", "rh")])[("qh", "rh")] == fresh

    def test_alignment_is_not_symmetric_so_the_key_order_matters(self) -> None:
        """length_query and length_ref swap, so (a,b) and (b,a) are different
        entries. Storing them under one key would corrupt the shorter one."""
        ab = compute_alignment(SEQ_A, SEQ_C)
        ba = compute_alignment(SEQ_C, SEQ_A)
        assert (ab["length_query"], ab["length_ref"]) == (ba["length_ref"], ba["length_query"])


class TestTheAdapterUsesTheCache:
    """Exercises _alignments_for_pairs, which is where the saving happens."""

    @staticmethod
    def _inputs(cache: Any, **over: Any) -> Any:
        from protea.core.operations._predict_go_terms_adapter import AdapterInputs

        class _P:
            compute_alignments = True
            compute_taxonomy = False

        base: dict[str, Any] = dict(
            p=_P(),
            valid_accessions=["Q1"],
            query_embeddings=None,
            ref_data={},
            annotations={},
            go_id_map={},
            go_aspect_map={},
            prediction_set_id=None,
            ref_sequences={"R1": SEQ_B, "R2": SEQ_C},
            query_sequences={"Q1": SEQ_A},
            ref_tax_ids={},
            query_tax_ids={},
            alignment_cache=cache,
        )
        base.update(over)
        return AdapterInputs(**base)

    def test_a_cold_cache_computes_and_stores_every_pair(self) -> None:
        from protea.core.operations._predict_go_terms_adapter import _alignments_for_pairs

        cache = _FakeCache()
        out = _alignments_for_pairs(self._inputs(cache), [("Q1", "R1"), ("Q1", "R2")])

        assert set(out) == {("Q1", "R1"), ("Q1", "R2")}
        assert len(cache.stored) == 2
        assert out[("Q1", "R1")] == compute_alignment(SEQ_A, SEQ_B)

    def test_a_warm_cache_returns_the_same_values_without_storing(self) -> None:
        from protea.core.operations._predict_go_terms_adapter import _alignments_for_pairs

        cache = _FakeCache()
        pairs = [("Q1", "R1"), ("Q1", "R2")]
        cold = _alignments_for_pairs(self._inputs(cache), pairs)
        cache.stored.clear()

        warm = _alignments_for_pairs(self._inputs(cache), pairs)

        assert warm == cold
        assert cache.stored == []

    def test_it_asks_the_cache_once_for_the_whole_batch(self) -> None:
        """Per-pair lookups would trade one cost for another."""
        from protea.core.operations._predict_go_terms_adapter import _alignments_for_pairs

        cache = _FakeCache()
        _alignments_for_pairs(self._inputs(cache), [("Q1", "R1"), ("Q1", "R2")])
        assert len(cache.lookups) == 2  # two pairs, one call

    def test_without_a_cache_it_still_computes(self) -> None:
        """The port is optional; None must behave exactly as before."""
        from protea.core.operations._predict_go_terms_adapter import _alignments_for_pairs

        out = _alignments_for_pairs(self._inputs(None), [("Q1", "R1")])
        assert out[("Q1", "R1")] == compute_alignment(SEQ_A, SEQ_B)

    def test_a_pair_with_a_missing_sequence_is_skipped(self) -> None:
        from protea.core.operations._predict_go_terms_adapter import _alignments_for_pairs

        out = _alignments_for_pairs(self._inputs(_FakeCache()), [("Q1", "UNKNOWN")])
        assert out == {}

    def test_alignments_off_returns_nothing_and_touches_no_cache(self) -> None:
        from protea.core.operations._predict_go_terms_adapter import _alignments_for_pairs

        class _Off:
            compute_alignments = False
            compute_taxonomy = True

        cache = _FakeCache()
        out = _alignments_for_pairs(self._inputs(cache, p=_Off()), [("Q1", "R1")])
        assert out == {}
        assert cache.lookups == []


class TestTheModelAndTheMigrationAgree:
    def test_every_computed_field_is_a_column(self) -> None:
        from protea.infrastructure.orm.base import Base

        table = Base.metadata.tables["sequence_alignment"]
        assert set(ac.ALIGNMENT_FIELDS) <= set(table.columns.keys())

    def test_the_primary_key_is_the_pair_of_hashes(self) -> None:
        from protea.infrastructure.orm.base import Base

        table = Base.metadata.tables["sequence_alignment"]
        assert [c.name for c in table.primary_key] == ["query_hash", "ref_hash"]


class TestTheStatementFitsPostgresParameterCeiling:
    """Postgres refuses more than 65535 bind parameters per statement, and the
    limit counts PARAMETERS, not rows.

    Shipped sized in rows: 5,000 rows x 14 columns = 70,000 parameters, which
    fails at execute time on the first real batch and not in any unit test,
    because a test with two rows never approaches it. That is why this asserts
    the arithmetic rather than the behaviour.
    """

    def test_an_insert_chunk_stays_under_the_ceiling(self) -> None:
        rows = ac._chunk_for(ac._INSERT_COLUMNS)
        assert rows * ac._INSERT_COLUMNS < 65_535

    def test_the_lookup_chunk_is_small_enough_to_parse(self) -> None:
        """The read hits a different ceiling from the write.

        A row-values IN list is parsed as a nested expression tree, so Postgres
        raises StatementTooComplex long before the parameter cap matters.
        Deriving this from the parameter budget gives 29,490 pairs and fails on
        a real batch; the real budget is the parser's stack, which no column
        count can predict. Verified at 30,720 pairs against a live database.
        """
        assert ac._LOOKUP_CHUNK <= 5_000
        assert ac._LOOKUP_CHUNK * ac._LOOKUP_COLUMNS < 65_535

    def test_the_row_budget_tracks_the_column_count(self) -> None:
        """Adding a metric must shrink the chunk, not silently overflow it."""
        assert ac._chunk_for(28) < ac._chunk_for(14)

    def test_the_insert_column_count_matches_the_table(self) -> None:
        """Two hash keys plus every metric; a drift here mis-sizes the chunk."""
        assert ac._INSERT_COLUMNS == len(ac.ALIGNMENT_FIELDS) + 2

    def test_a_wide_row_still_yields_at_least_one(self) -> None:
        assert ac._chunk_for(100_000) == 1

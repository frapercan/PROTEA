"""Which proteins may donate, decided in the query rather than after it.

The corpus for this campaign is a reviewed spine plus unreviewed material at
scale, so the donor pool has to be restrictable. The restriction is applied in
the query because the alternative, filtering the materialised pool, means
streaming the largest thing this operation touches out of the database in order
to throw part of it away.

These tests compile the query and read the predicates it carries. They need no
database, which is the point: the question is what the query asks for, not what
some fixture happens to contain.
"""

from __future__ import annotations

import uuid

import pytest
from protea_contracts.payloads import DonorPolicy
from sqlalchemy.orm import Session

from protea.core.disk_cache import RefPoolKey
from protea.core.operations.predict_go_terms._batch_op_reference import (
    _PoolRequest,
    _ReferenceMixin,
    _restrict_annotations,
)
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation

CFG = uuid.UUID("11111111-1111-1111-1111-111111111111")
ANN = uuid.UUID("22222222-2222-2222-2222-222222222222")


def _sql(request: _PoolRequest) -> str:
    """Compile the pool query for this request and return its SQL text."""
    query = _ReferenceMixin()._reference_pool_query(Session(), request)
    return str(query.statement.compile(compile_kwargs={"literal_binds": True}))


class TestThePermissivePoolIsTheHistoricalOne:
    """An unrestricted run must resolve exactly the query it always did."""

    def test_no_policy_adds_no_predicate(self) -> None:
        sql = _sql(_PoolRequest(CFG, ANN))
        assert "reviewed" not in sql.lower()
        assert "evidence_code" not in sql.lower()

    def test_the_permissive_policy_adds_no_predicate_either(self) -> None:
        sql = _sql(_PoolRequest(CFG, ANN, DonorPolicy()))
        assert "reviewed" not in sql.lower()
        assert "evidence_code" not in sql.lower()

    def test_it_still_joins_embeddings_and_annotations(self) -> None:
        sql = _sql(_PoolRequest(CFG, ANN)).lower()
        assert "join" in sql and "sequence_embedding" in sql


class TestReviewedOnlyExcludesTheUnknowns:
    def test_the_predicate_reaches_the_query(self) -> None:
        sql = _sql(_PoolRequest(CFG, ANN, DonorPolicy(reviewed_only=True))).lower()
        assert "reviewed" in sql

    def test_an_unknown_review_status_is_not_reviewed(self) -> None:
        """The column is nullable, and a null is not a yes.

        A reviewed spine that quietly admitted proteins of unknown provenance
        would not be a reviewed spine, so the comparison is to true rather than
        to not-false.
        """
        sql = _sql(_PoolRequest(CFG, ANN, DonorPolicy(reviewed_only=True))).lower()
        assert "is true" in sql or "= true" in sql
        assert "is not false" not in sql


class TestEvidenceCodesRestrictTheAnnotation:
    def test_the_codes_reach_the_query(self) -> None:
        sql = _sql(_PoolRequest(CFG, ANN, DonorPolicy(evidence_codes=["EXP", "IDA"])))
        assert "EXP" in sql and "IDA" in sql

    def test_an_empty_list_restricts_nothing(self) -> None:
        """Empty means unset, not exclude everything."""
        sql = _sql(_PoolRequest(CFG, ANN, DonorPolicy(evidence_codes=[]))).lower()
        assert "evidence_code" not in sql


class TestExcludedReferencesKeepTheNulls:
    def _sql_excluding(self, *prefixes: str) -> str:
        policy = DonorPolicy(exclude_reference_prefixes=list(prefixes))
        return _sql(_PoolRequest(CFG, ANN, policy))

    def test_the_prefix_reaches_the_query(self) -> None:
        assert "GO_REF" in self._sql_excluding("GO_REF")

    def test_a_row_with_no_reference_survives(self) -> None:
        """Left to SQL alone, NOT LIKE against a null drops the row silently.

        A row carrying no reference is not evidence of the excluded kind, so
        dropping it would quietly shrink the pool for a reason nobody asked for.
        """
        sql = self._sql_excluding("GO_REF").lower()
        assert "is null" in sql

    def test_several_prefixes_all_apply(self) -> None:
        sql = self._sql_excluding("GO_REF", "PMID")
        assert "GO_REF" in sql and "PMID" in sql


class TestTheRestrictionHelperOnItsOwn:
    def _base(self):
        return Session().query(ProteinGOAnnotation.protein_accession)

    def test_none_is_a_passthrough(self) -> None:
        base = self._base()
        assert _restrict_annotations(base, None) is base

    def test_the_permissive_policy_is_a_passthrough_too(self) -> None:
        base = self._base()
        before = str(base.statement.compile())
        after = str(_restrict_annotations(base, DonorPolicy()).statement.compile())
        assert before == after


class TestTheRequestCarriesOneIdentity:
    """The policy decides both what the query admits and where it is cached."""

    def test_the_permissive_request_keeps_the_historical_key(self) -> None:
        assert _PoolRequest(CFG, ANN, DonorPolicy()).cache_key == RefPoolKey(CFG, ANN, "")

    def test_no_policy_keeps_it_too(self) -> None:
        assert _PoolRequest(CFG, ANN).cache_key == RefPoolKey(CFG, ANN, "")

    def test_a_restriction_moves_the_key(self) -> None:
        restricted = _PoolRequest(CFG, ANN, DonorPolicy(reviewed_only=True))
        assert restricted.cache_key != _PoolRequest(CFG, ANN).cache_key

    @pytest.mark.parametrize(
        "policy",
        [
            DonorPolicy(reviewed_only=True),
            DonorPolicy(evidence_codes=["EXP"]),
            DonorPolicy(exclude_reference_prefixes=["GO_REF"]),
        ],
    )
    def test_every_restriction_that_changes_the_query_changes_the_key(self, policy) -> None:
        """The property that stops a filtered pool being served as unfiltered."""
        assert _PoolRequest(CFG, ANN, policy).discriminator != ""
        assert _sql(_PoolRequest(CFG, ANN, policy)) != _sql(_PoolRequest(CFG, ANN))

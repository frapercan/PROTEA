"""Tests for protea.core.domain.aspect.

Exhaustive: only three values and a handful of conversions, but they
underpin a lot of pipeline code so we check every direction explicitly.
"""

from __future__ import annotations

import pytest

from protea.core.domain.aspect import (
    ASPECT_CAFA_CODES,
    ASPECT_CODES,
    Aspect,
)


class TestAspectCodes:
    def test_three_aspects_only(self):
        assert len(list(Aspect)) == 3

    def test_iteration_order_is_canonical(self):
        # P → F → C, matches the historical _ASPECTS = ("P", "F", "C") order.
        assert [a.code for a in Aspect] == ["P", "F", "C"]

    def test_cafa_iteration_matches(self):
        assert [a.cafa for a in Aspect] == ["BPO", "MFO", "CCO"]

    def test_module_constants_match_iteration(self):
        assert ASPECT_CODES == ("P", "F", "C")
        assert ASPECT_CAFA_CODES == ("BPO", "MFO", "CCO")


class TestAspectAccessors:
    @pytest.mark.parametrize(
        "aspect,code,cafa",
        [
            (Aspect.BIOLOGICAL_PROCESS, "P", "BPO"),
            (Aspect.MOLECULAR_FUNCTION, "F", "MFO"),
            (Aspect.CELLULAR_COMPONENT, "C", "CCO"),
        ],
    )
    def test_code_and_cafa_properties(self, aspect: Aspect, code: str, cafa: str):
        assert aspect.code == code
        assert aspect.cafa == cafa


class TestAspectFromString:
    @pytest.mark.parametrize(
        "code,expected",
        [
            ("P", Aspect.BIOLOGICAL_PROCESS),
            ("F", Aspect.MOLECULAR_FUNCTION),
            ("C", Aspect.CELLULAR_COMPONENT),
        ],
    )
    def test_from_code(self, code: str, expected: Aspect):
        assert Aspect.from_code(code) is expected

    @pytest.mark.parametrize(
        "cafa,expected",
        [
            ("BPO", Aspect.BIOLOGICAL_PROCESS),
            ("MFO", Aspect.MOLECULAR_FUNCTION),
            ("CCO", Aspect.CELLULAR_COMPONENT),
        ],
    )
    def test_from_cafa(self, cafa: str, expected: Aspect):
        assert Aspect.from_cafa(cafa) is expected

    def test_from_code_invalid_raises(self):
        with pytest.raises(KeyError):
            Aspect.from_code("X")

    def test_from_cafa_invalid_raises(self):
        with pytest.raises(KeyError):
            Aspect.from_cafa("XYZ")


class TestRoundtrips:
    @pytest.mark.parametrize("aspect", list(Aspect))
    def test_code_roundtrip(self, aspect: Aspect):
        assert Aspect.from_code(aspect.code) is aspect

    @pytest.mark.parametrize("aspect", list(Aspect))
    def test_cafa_roundtrip(self, aspect: Aspect):
        assert Aspect.from_cafa(aspect.cafa) is aspect

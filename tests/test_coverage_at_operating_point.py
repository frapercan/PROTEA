"""Coverage and n_proteins have to describe the same threshold.

cafaeval's best-frame row carries both ``cov``, the coverage at the tau
that maximised Fmax, and ``cov_max``, the maximum of coverage over every
tau. We stored only cov_max, under the name ``coverage``, beside ``n``
read from the best-tau row.

Those describe different points, and read as a pair they say something
false. In rung 1 a single 0.98 -> 0.99 step in the optimal threshold moved
n by 17 per cent while coverage sat at 1.0, which reads as the scored
population having shrunk. It had not: the ground-truth restriction was a
no-op in all 32 runs, 3031 -> 3031 NK proteins in every one of them. What
moved was how many proteins had any prediction clearing that threshold.
"""

from __future__ import annotations

import pandas as pd

from protea.core.operations._run_cafa_artifacts import parse_results


def _best_frame(*, cov: float, cov_max: float, n: int) -> dict:
    return {
        "f": pd.DataFrame(
            [
                {
                    "ns": "molecular_function",
                    "f": 0.4047,
                    "pr": 0.42,
                    "rc": 0.39,
                    "tau": 0.99,
                    "cov": cov,
                    "cov_max": cov_max,
                    "n": n,
                }
            ]
        )
    }


class TestTheTwoCoveragesAreBothPublished:
    def test_coverage_at_tau_is_the_one_that_pairs_with_n(self):
        out = parse_results(_best_frame(cov=0.83, cov_max=1.0, n=973))
        assert out["MFO"]["coverage_at_tau"] == 0.83
        assert out["MFO"]["n_proteins"] == 973

    def test_coverage_keeps_meaning_the_maximum_over_thresholds(self):
        # Unchanged on purpose. Rows written before this existed hold
        # cov_max under this name, and quietly redefining it would make
        # old and new results incomparable while looking identical.
        out = parse_results(_best_frame(cov=0.83, cov_max=1.0, n=973))
        assert out["MFO"]["coverage"] == 1.0

    def test_the_pair_reconciles_where_it_used_to_contradict(self):
        # The rung-1 shape: coverage 1.0 beside n=973 looked like a
        # vanished population. With cov at tau published, 0.83 x the
        # cohort is simply how many cleared a 0.99 threshold.
        mfo = parse_results(_best_frame(cov=0.83, cov_max=1.0, n=973))["MFO"]
        assert mfo["coverage"] > mfo["coverage_at_tau"]


class TestItDoesNotInventANumber:
    def test_absent_cov_gives_none_rather_than_zero(self):
        # A frame from an older cafaeval without the column. Zero would
        # read as "nothing was predicted", which is a claim, not a gap.
        frame = _best_frame(cov=0.83, cov_max=1.0, n=973)
        frame["f"] = frame["f"].drop(columns=["cov"])
        assert parse_results(frame)["MFO"]["coverage_at_tau"] is None

    def test_a_genuine_zero_survives(self):
        out = parse_results(_best_frame(cov=0.0, cov_max=1.0, n=0))
        assert out["MFO"]["coverage_at_tau"] == 0.0

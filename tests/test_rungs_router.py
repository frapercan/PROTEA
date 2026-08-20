"""The campaign as a line, tested where the decisions are.

Three of them are easy to get wrong in ways that look right: counting
jobs where a reader counts arms, reading a headline off the cell that
flatters it, and letting the question a rung asks drift from what it
actually varies.
"""

from __future__ import annotations

from protea.api.routers.rungs import _arm_counts, _arm_key, _best, _question


def _arm(model: str, k: int, status: str, scorer: str | None = None) -> dict:
    return {"model": model, "k": k, "status": status, "scorer": scorer}


class TestArmCounts:
    def test_counts_the_grid_not_the_jobs(self):
        # A retried arm is one arm. Counting jobs made a 2-arm rung report
        # 3 successes, which reads as a grid that grew rather than as a
        # retry that worked.
        arms = [
            _arm("ankh", 3, "FAILED"),
            _arm("ankh", 3, "SUCCEEDED"),
            _arm("esm", 3, "SUCCEEDED"),
        ]
        assert _arm_counts(arms) == {
            "arms": 2,
            "succeeded": 2,
            "running": 0,
            "failed": 0,
        }

    def test_an_arm_is_running_until_something_succeeds(self):
        arms = [_arm("ankh", 1, "FAILED"), _arm("ankh", 1, "RUNNING")]
        counts = _arm_counts(arms)
        assert counts == {"arms": 1, "succeeded": 0, "running": 1, "failed": 0}

    def test_an_arm_all_of_whose_attempts_failed_is_failed(self):
        arms = [_arm("ankh", 1, "FAILED"), _arm("ankh", 1, "FAILED")]
        assert _arm_counts(arms)["failed"] == 1

    def test_no_arm_is_counted_twice(self):
        arms = [_arm("a", 1, "SUCCEEDED"), _arm("a", 1, "RUNNING")]
        counts = _arm_counts(arms)
        assert counts["succeeded"] + counts["running"] + counts["failed"] == counts["arms"]


class TestArmKey:
    def test_two_scorers_over_one_run_are_two_arms(self):
        # Rung 1's third axis re-scores prediction sets that already exist.
        # Keyed on (model, K) alone, eight scorers collapse into one cell and
        # the rung reports a grid an eighth of its real size.
        a = _arm("ankh", 3, "SUCCEEDED", scorer="composite")
        b = _arm("ankh", 3, "SUCCEEDED", scorer="vote_fraction")
        assert _arm_key(a) != _arm_key(b)

    def test_the_same_run_under_the_same_scorer_is_one_arm(self):
        a = _arm("ankh", 3, "FAILED", scorer="composite")
        b = _arm("ankh", 3, "SUCCEEDED", scorer="composite")
        assert _arm_key(a) == _arm_key(b)
        assert _arm_counts([a, b]) == {"arms": 1, "succeeded": 1, "running": 0, "failed": 0}

    def test_no_scorer_is_its_own_arm_not_a_missing_one(self):
        # The None fallback is a scorer: arithmetically embedding_only. An
        # arm that ran under it is a real arm and must not merge with one
        # that named a config.
        assert _arm_key(_arm("ankh", 3, "SUCCEEDED")) != _arm_key(
            _arm("ankh", 3, "SUCCEEDED", scorer="embedding_only")
        )


class TestQuestion:
    def test_names_every_axis_that_varies(self):
        q = _question({"a", "b"}, {1, 3, 30}, {"x", "y"})
        assert "2 representations" in q
        assert "1 to 30" in q
        assert "2 score weightings" in q

    def test_names_only_the_axis_that_varies(self):
        assert "representations" not in _question({"a"}, {1, 3}, set())
        assert "neighbours" not in _question({"a", "b"}, {3}, set())
        assert "weightings" not in _question({"a", "b"}, {1, 3}, {"x"})

    def test_the_scoring_axis_is_named_even_when_it_is_the_only_one(self):
        # Rung 1's third axis re-scores one model at one K. The rung is
        # still asking something and must say what.
        assert "8 score weightings" in _question({"a"}, {3}, set("abcdefgh"))

    def test_says_so_when_nothing_varies(self):
        # "which of 1 representation" is not a question.
        assert _question({"a"}, {3}, set()) == "a single configuration, measured"


class TestBest:
    @staticmethod
    def _row(model, k, values):
        return {
            "model": model,
            "k": k,
            "evaluation_result_id": f"{model}-{k}",
            "results": {"NK": {a: {"f_micro_w": v} for a, v in values.items()}},
        }

    def test_averages_the_grid_rather_than_reading_one_cell(self):
        # A rung's headline must not be the cell that happened to flatter
        # it: one strong aspect can hide two weak ones.
        rows = [
            self._row("spiky", 3, {"MFO": 0.9, "BPO": 0.1, "CCO": 0.1}),
            self._row("even", 3, {"MFO": 0.4, "BPO": 0.4, "CCO": 0.4}),
        ]
        assert _best(rows, "f_micro_w")["model"] == "even"

    def test_reports_how_many_cells_it_averaged(self):
        rows = [self._row("a", 3, {"MFO": 0.4, "BPO": 0.2})]
        assert _best(rows, "f_micro_w")["cells"] == 2

    def test_returns_none_when_nothing_carries_the_metric(self):
        # Not zero, and not an arbitrary arm: a rung with no scored cell
        # has no best, and saying otherwise would invent one.
        rows = [{"model": "a", "k": 3, "evaluation_result_id": "x", "results": {}}]
        assert _best(rows, "f_micro_w") is None

    def test_skips_arms_missing_the_metric_rather_than_scoring_them_zero(self):
        rows = [
            self._row("scored", 3, {"MFO": 0.3}),
            {"model": "unscored", "k": 3, "evaluation_result_id": "y", "results": {}},
        ]
        assert _best(rows, "f_micro_w")["model"] == "scored"

    def test_ignores_a_non_numeric_cell(self):
        rows = [
            {
                "model": "a",
                "k": 3,
                "evaluation_result_id": "z",
                "results": {"NK": {"MFO": {"f_micro_w": None}, "BPO": {"f_micro_w": 0.5}}},
            }
        ]
        best = _best(rows, "f_micro_w")
        assert best["cells"] == 1
        assert best["value"] == 0.5


class TestAnArmIsAFinishedRun:
    """A cancelled job leaves its written batches behind.

    The prediction set carries no mark saying it is half written: the
    completion state lives on the job. Four sets match (rung 1, ankh-base,
    K=30) in the live database and three came from FAILED or CANCELLED
    jobs, one of them holding 1,024 proteins from a single batch written
    before the cancel. Selecting sets and inspecting them cannot tell those
    apart, however carefully, because the distinguishing fact is not in the
    object being held.
    """

    def test_the_query_requires_a_succeeded_job(self):
        from protea.api.routers.rungs import _ARMS

        sql = str(_ARMS)
        assert "j.status::text = 'SUCCEEDED'" in sql

    def test_the_query_requires_the_progress_to_agree(self):
        # SUCCEEDED is the job's verdict; the progress counts are its
        # arithmetic. A gate wants both, because they are recorded by
        # different code at different times.
        from protea.api.routers.rungs import _ARMS

        sql = str(_ARMS)
        assert "progress_current = j.progress_total" in sql

    def test_the_gate_cannot_pass_on_missing_data(self):
        # The first version compared two meta keys with IS NOT DISTINCT
        # FROM, and one of the 258 predict jobs carries neither. Two nulls
        # are NOT DISTINCT, so that job passed a gate meant to stop it.
        # A gate whose failure mode is silent success is not a gate.
        from protea.api.routers.rungs import _ARMS

        sql = str(_ARMS)
        assert "progress_total IS NOT NULL" in sql
        # The defect, not the phrase: the phrase survives in the comment
        # that explains why it was removed, which is where it belongs.
        executable = "\n".join(
            line for line in sql.splitlines() if not line.strip().startswith("--")
        )
        assert "IS NOT DISTINCT FROM" not in executable
        assert "batches_completed" not in executable

    def test_the_gate_sits_on_the_join_not_the_where(self):
        # It has to be a join condition. Moved into WHERE it would drop the
        # whole arm rather than only its prediction set, and an arm whose
        # job is still running would vanish from the rung instead of
        # showing as running.
        from protea.api.routers.rungs import _ARMS

        sql = str(_ARMS)
        head = sql[: sql.index("UNION ALL")]
        join_at = head.index("LEFT JOIN prediction_set")
        where_at = head.index("WHERE j.operation")
        assert join_at < head.index("SUCCEEDED") < where_at

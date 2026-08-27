"""The retired ladder endpoint, tested where the retirement can fail.

The failure modes here are not arithmetic, because there is no longer any
arithmetic. They are the three ways a retirement breaks something that was
working: it stops answering, it answers in a shape its client rejects, or
it goes quiet without saying it is retired, so an empty response reads as
an empty database.

There is a fourth, which is the one that caused the retirement. The old
implementation derived its answer by joining the job table to
``evaluation_result``. Those jobs are still there and those results are
not, and a LEFT JOIN reports a full grid of successes from that pairing.
A test that the SQL is gone is a test that the claim cannot come back.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from protea.api.routers import rungs as rungs_module
from protea.api.routers.rungs import list_rungs, router


class TestItStillAnswers:
    """``scripts/deploy-check.sh`` fails the deploy on anything but 200.

    The smoke loop there curls this path and treats a non-200 as a broken
    deploy. Retirement is a fact about meaning, and expressing it as a
    status code would turn a check that is right to be strict red on every
    deploy from now on.
    """

    def test_the_route_is_registered_at_the_prefix_deploy_check_walks(self):
        # The other check in deploy-check.sh reads each router module's
        # declared prefix and requires it in the served OpenAPI document.
        # Hiding the route from the schema fails that check.
        assert router.prefix == "/rungs"
        assert [r for r in router.routes if getattr(r, "include_in_schema", False)]

    def test_it_is_marked_deprecated_so_the_schema_says_so(self):
        # The body carries the explanation; the flag is what a generated
        # client and the docs page read.
        route = next(r for r in router.routes if getattr(r, "path", None) == "/rungs")
        assert route.deprecated is True

    def test_it_answers_two_hundred(self):
        client = TestClient(_app())
        assert client.get("/rungs").status_code == 200


class TestTheShapeItsClientAccepts:
    """``getRungs`` rejects any 200 whose body has no array under ``rungs``.

    It was written that way after a mock replied 200 with ``[]`` to every
    unknown route, the cast asserted a shape that was not there, and the
    caller threw outside the promise chain where its own catch could not
    see it. Dropping the key would put the retirement into that same error
    path instead of through the empty branch every consumer already has.
    """

    def test_the_body_carries_an_empty_rungs_array(self):
        body = list_rungs()
        assert body["rungs"] == []
        assert isinstance(body["rungs"], list)

    def test_the_metric_parameter_is_echoed_rather_than_dropped(self):
        # An existing caller's URL still parses. It selects nothing.
        assert list_rungs(metric="s_min")["metric"] == "s_min"

    def test_a_caller_passing_unknown_params_still_gets_two_hundred(self):
        # deploy-check.sh appends ``?limit=3`` to every endpoint it smokes.
        client = TestClient(_app())
        assert client.get("/rungs?limit=3").status_code == 200


class TestEmptyDoesNotReadAsUnpopulated:
    def test_it_declares_itself_retired(self):
        assert list_rungs()["retired"]["retired"] is True

    def test_it_names_its_replacement(self):
        assert list_rungs()["retired"]["superseded_by"] == "/v1/graph"

    def test_it_says_which_kind_of_empty_this_is(self):
        # Without this a reader cannot tell a retired surface from a
        # database that happens to hold nothing, and those call for
        # opposite responses.
        assert "retired, not unpopulated" in list_rungs()["retired"]["empty_means"]


class TestTheCountingCannotComeBack:
    """The deleted derivation, asserted deleted.

    Left in the module and merely unreferenced, it would be one call away
    from being restored by somebody who saw an empty response and took it
    for a bug. The job rows it counted are still in the database; only the
    results it claimed to summarise are gone.
    """

    def test_no_query_survives_in_the_module(self):
        assert not hasattr(rungs_module, "_ARMS")
        assert not hasattr(rungs_module, "_WINDOW_DATES")

    def test_no_counting_helper_survives_in_the_module(self):
        for gone in ("_arm_counts", "_arm_key", "_best", "_question"):
            assert not hasattr(rungs_module, gone)

    def test_the_endpoint_needs_no_database_at_all(self):
        # Callable with no session factory and no dependency override. A
        # signature that still wanted one would mean something in here is
        # still prepared to read.
        assert list_rungs()["rungs"] == []


def _app():
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(router)
    return app

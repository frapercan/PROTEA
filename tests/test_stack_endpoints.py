"""Tests for the cross-repo stack endpoints.

``GET /stack`` reads ``docs/source/_data/stack.yaml`` and returns the
eight-repo registry. ``GET /stack/pulls`` queries GitHub once per repo
and aggregates open pull requests; the GitHub call is replaced here
with a fake transport so the test never touches the real API.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers import stack as stack_router


@pytest.fixture(autouse=True)
def reset_pulls_cache() -> None:
    stack_router._pulls_cache["fetched_at"] = 0.0
    stack_router._pulls_cache["payload"] = None


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(stack_router.router)
    return TestClient(app)


class TestStackEndpoint:
    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/stack")
        assert resp.status_code == 200

    def test_returns_eight_repos(self, client: TestClient) -> None:
        body = client.get("/stack").json()
        assert len(body["repos"]) == 8

    def test_includes_protea_core(self, client: TestClient) -> None:
        body = client.get("/stack").json()
        slugs = {r["slug"] for r in body["repos"]}
        assert "protea-core" in slugs
        assert "protea-contracts" in slugs

    def test_each_repo_has_required_fields(self, client: TestClient) -> None:
        body = client.get("/stack").json()
        required = {"name", "slug", "role", "role_label", "status", "summary", "github_url"}
        for repo in body["repos"]:
            assert required <= set(repo.keys())
            assert repo["github_url"].startswith("https://github.com/")

    def test_each_repo_carries_spanish_summary(self, client: TestClient) -> None:
        # summary_es backs the /es locale; the frontend falls back to the
        # English summary when it is missing, but the registry ships one
        # per repo so the Spanish chrome and the table stay consistent.
        body = client.get("/stack").json()
        for repo in body["repos"]:
            assert "summary_es" in repo
            assert repo["summary_es"]
            assert repo["summary_es"] != repo["summary"]


def _make_fake_client_builder(
    fake_get: Any, captured_headers: dict[str, str] | None = None
) -> Any:
    """Build a replacement for ``stack._build_github_client`` so tests
    only intercept calls to GitHub, not the TestClient -> ASGI traffic
    (FastAPI's TestClient also runs over httpx).
    """

    class _FakeClient:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
            token = stack_router._github_token()
            if token:
                self.headers["Authorization"] = f"Bearer {token}"
            if captured_headers is not None:
                captured_headers.update(self.headers)

        def __enter__(self) -> _FakeClient:
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

        def get(self, url: str, **kwargs: Any) -> httpx.Response:
            return fake_get(url, **kwargs)

    return _FakeClient


class TestStackPullsEndpoint:
    def test_aggregates_pulls_from_every_repo(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen_urls: list[str] = []

        def fake_get(url: str, **kwargs: Any) -> httpx.Response:
            seen_urls.append(url)
            payload = [
                {
                    "number": 42,
                    "title": "fix: leakage in anc2vec_query_known_count",
                    "html_url": f"{url}/42",
                    "state": "open",
                    "draft": False,
                    "user": {"login": "frapercan"},
                    "created_at": "2026-05-07T10:00:00Z",
                    "updated_at": "2026-05-07T11:00:00Z",
                    "labels": [{"name": "bug"}],
                }
            ]
            return httpx.Response(200, json=payload, headers={"X-RateLimit-Remaining": "59"})

        monkeypatch.setattr(stack_router, "_build_github_client", _make_fake_client_builder(fake_get))

        resp = client.get("/stack/pulls")
        assert resp.status_code == 200
        body = resp.json()
        assert body["repos_queried"] == 8
        assert len(body["pulls"]) == 8
        assert len(seen_urls) == 8
        assert body["rate_limit_remaining"] == 59
        assert body["errors"] == {}

    def test_records_errors_per_repo_without_failing(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_get(url: str, **kwargs: Any) -> httpx.Response:
            return httpx.Response(403, text="API rate limit exceeded")

        monkeypatch.setattr(stack_router, "_build_github_client", _make_fake_client_builder(fake_get))

        body = client.get("/stack/pulls").json()
        assert body["pulls"] == []
        assert len(body["errors"]) == 8

    def test_uses_cache_within_ttl(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = {"n": 0}

        def fake_get(url: str, **kwargs: Any) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(200, json=[], headers={})

        monkeypatch.setattr(stack_router, "_build_github_client", _make_fake_client_builder(fake_get))

        client.get("/stack/pulls")
        client.get("/stack/pulls")
        assert calls["n"] == 8

    def test_passes_token_when_set(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured_headers: dict[str, str] = {}

        def fake_get(url: str, **kwargs: Any) -> httpx.Response:
            return httpx.Response(200, json=[], headers={})

        monkeypatch.setenv("PROTEA_GITHUB_TOKEN", "ghp_test_token")
        monkeypatch.setattr(
            stack_router,
            "_build_github_client",
            _make_fake_client_builder(fake_get, captured_headers),
        )
        client.get("/stack/pulls")
        assert captured_headers.get("Authorization") == "Bearer ghp_test_token"


class TestStackYamlFile:
    def test_yaml_file_exists_and_parses(self) -> None:
        assert stack_router._STACK_YAML.exists()
        repos = stack_router._load_repos()
        assert len(repos) >= 1


class TestDocsUrls:
    def test_docs_url_points_at_public_readthedocs(self, client: TestClient) -> None:
        body = client.get("/stack").json()
        for repo in body["repos"]:
            assert repo["docs_url"] is not None
            assert repo["docs_url"].startswith("https://")
            assert ".readthedocs.io/" in repo["docs_url"]

    def test_protea_core_docs_url(self, client: TestClient) -> None:
        body = client.get("/stack").json()
        protea = next(r for r in body["repos"] if r["slug"] == "protea-core")
        assert protea["docs_url"] == "https://protea.readthedocs.io/"

    def test_local_docs_path_field_removed(self, client: TestClient) -> None:
        body = client.get("/stack").json()
        assert all("local_docs_path" not in r for r in body["repos"])


class TestLocalArtefacts:
    def test_thesis_pdf_url_reflects_filesystem(
        self, client: TestClient, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pdf = tmp_path / "thesis.pdf"
        monkeypatch.setattr(stack_router, "_THESIS_PDF", pdf)

        body = client.get("/stack").json()
        assert body["thesis_pdf_url"] is None

        pdf.write_bytes(b"%PDF-1.7 fake")
        body = client.get("/stack").json()
        assert body["thesis_pdf_url"] == "/thesis.pdf"

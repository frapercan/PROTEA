"""PROTEA stack metadata + cross-repo PR listing.

Two read-only endpoints intended to power the ``/stack`` page in the UI:

  * ``GET /stack`` returns the eight-repo registry from
    ``docs/source/_data/stack.yaml``.
  * ``GET /stack/pulls`` proxies GitHub's ``/repos/{owner}/{repo}/pulls``
    endpoint for every repo and aggregates the open PRs into a single
    list. Useful when bouncing between repositories during review.

The PR listing is cached in-process for ``_PULLS_TTL_SECONDS`` to keep
the unauthenticated GitHub rate limit (60 req/h) from being a problem.
Set ``PROTEA_GITHUB_TOKEN`` (or any token in ``GITHUB_TOKEN`` /
``GH_TOKEN``) to lift the limit to 5000 req/h.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import httpx
import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(tags=["stack"])

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_STACK_YAML = _PROJECT_ROOT / "docs" / "source" / "_data" / "stack.yaml"
_DOCS_BUILD_ROOT = _PROJECT_ROOT / "docs" / "build"
_THESIS_PDF = _PROJECT_ROOT / "static" / "thesis.pdf"
_PULLS_TTL_SECONDS = 300
_GITHUB_API = "https://api.github.com"


class RepoEntry(BaseModel):
    name: str
    slug: str
    role: str
    role_label: str
    status: str
    summary: str
    github_url: str
    docs_url: str | None = None
    package_url: str | None = None
    local_docs_path: str | None = None


class StackResponse(BaseModel):
    repos: list[RepoEntry]
    thesis_pdf_url: str | None = None


class PullRequest(BaseModel):
    repo: str
    number: int
    title: str
    url: str
    state: str
    draft: bool
    author: str | None
    created_at: str
    updated_at: str
    labels: list[str]


class PullsResponse(BaseModel):
    fetched_at: float
    cached: bool
    repos_queried: int
    pulls: list[PullRequest]
    rate_limit_remaining: int | None = None
    errors: dict[str, str] = {}


_pulls_cache: dict[str, Any] = {"fetched_at": 0.0, "payload": None}


def _build_github_client() -> httpx.Client:
    """Construct the httpx client used to talk to GitHub. Indirected
    so tests can patch this single function instead of monkey-patching
    ``httpx.Client.get`` globally (which would also intercept the
    TestClient's calls back into the ASGI app).
    """
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    token = _github_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(timeout=10.0, headers=headers)


def _local_docs_path(slug: str) -> str | None:
    """Return ``/docs/<slug>/`` when a built HTML tree exists for the
    given slug, otherwise ``None`` so the UI falls back to ``docs_url``.
    """
    if (_DOCS_BUILD_ROOT / slug / "html" / "index.html").exists():
        return f"/docs/{slug}/"
    return None


def _thesis_pdf_url() -> str | None:
    return "/static/thesis.pdf" if _THESIS_PDF.exists() else None


def _load_repos() -> list[RepoEntry]:
    if not _STACK_YAML.exists():
        raise HTTPException(status_code=500, detail=f"stack.yaml missing at {_STACK_YAML}")
    data = yaml.safe_load(_STACK_YAML.read_text(encoding="utf-8"))
    repos: list[RepoEntry] = []
    for r in data["repos"]:
        repos.append(RepoEntry(local_docs_path=_local_docs_path(r["slug"]), **r))
    return repos


def _github_token() -> str | None:
    return (
        os.environ.get("PROTEA_GITHUB_TOKEN")
        or os.environ.get("GITHUB_TOKEN")
        or os.environ.get("GH_TOKEN")
    )


def _owner_repo(github_url: str) -> tuple[str, str]:
    parts = github_url.rstrip("/").split("/")
    return parts[-2], parts[-1]


@router.get("/stack", response_model=StackResponse)
def get_stack() -> StackResponse:
    """Return the eight-repo PROTEA stack registry.

    Single source of truth: ``docs/source/_data/stack.yaml`` in this
    repo. Edit that file (and run ``scripts/sync_stack.py``) to refresh
    the README block and the Sphinx page in the same commit.

    Per-repo ``local_docs_path`` and the top-level ``thesis_pdf_url``
    are computed from the filesystem at request time: the field is
    populated whenever the corresponding artefact has been built into
    ``docs/build/<slug>/html/`` or ``static/thesis.pdf`` respectively,
    and is ``None`` otherwise.
    """
    return StackResponse(repos=_load_repos(), thesis_pdf_url=_thesis_pdf_url())


@router.get("/stack/pulls", response_model=PullsResponse)
def list_open_pulls() -> PullsResponse:
    """Aggregate open pull requests across every repo in the stack.

    Cached in-process for five minutes. Pass an optional
    ``PROTEA_GITHUB_TOKEN`` env var to use authenticated requests
    (rate limit 5000/h instead of 60/h).
    """
    now = time.time()
    cached = _pulls_cache["payload"]
    if cached is not None and (now - _pulls_cache["fetched_at"]) < _PULLS_TTL_SECONDS:
        return PullsResponse.model_validate(json.loads(cached))

    repos = _load_repos()
    pulls: list[PullRequest] = []
    errors: dict[str, str] = {}
    rate_remaining: int | None = None

    with _build_github_client() as client:
        for repo in repos:
            owner, name = _owner_repo(repo.github_url)
            url = f"{_GITHUB_API}/repos/{owner}/{name}/pulls"
            try:
                resp = client.get(url, params={"state": "open", "per_page": 100})
                rate_remaining = (
                    int(resp.headers["X-RateLimit-Remaining"])
                    if "X-RateLimit-Remaining" in resp.headers
                    else rate_remaining
                )
                if resp.status_code != 200:
                    errors[repo.name] = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    continue
                for item in resp.json():
                    pulls.append(
                        PullRequest(
                            repo=repo.name,
                            number=item["number"],
                            title=item["title"],
                            url=item["html_url"],
                            state=item["state"],
                            draft=bool(item.get("draft", False)),
                            author=(item.get("user") or {}).get("login"),
                            created_at=item["created_at"],
                            updated_at=item["updated_at"],
                            labels=[lbl["name"] for lbl in item.get("labels", [])],
                        )
                    )
            except httpx.HTTPError as exc:
                errors[repo.name] = str(exc)

    pulls.sort(key=lambda p: p.updated_at, reverse=True)
    payload = PullsResponse(
        fetched_at=now,
        cached=False,
        repos_queried=len(repos),
        pulls=pulls,
        rate_limit_remaining=rate_remaining,
        errors=errors,
    )
    _pulls_cache["fetched_at"] = now
    _pulls_cache["payload"] = payload.model_dump_json()
    return payload

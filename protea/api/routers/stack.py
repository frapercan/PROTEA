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
from pydantic import BaseModel, Field

router = APIRouter(tags=["stack"])

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_STACK_YAML = _PROJECT_ROOT / "docs" / "source" / "_data" / "stack.yaml"
_DOCS_BUILD_ROOT = _PROJECT_ROOT / "docs" / "build"
_THESIS_PDF = _PROJECT_ROOT / "apps" / "web" / "public" / "thesis.pdf"
_PULLS_TTL_SECONDS = 300
_GITHUB_API = "https://api.github.com"


class RepoEntry(BaseModel):
    """One repository row in the multi-repo stack landing page.

    Each entry represents a sibling git repo (PROTEA itself or one of
    the plugin/lab packages). ``role`` is the architectural slot
    (``core``, ``contracts``, ``plugin``, ``lab``); ``status`` is a
    coarse health signal sourced from the repo's CI / release state.
    """

    name: str = Field(..., description="Display name (human-readable, e.g. ``PROTEA``).")
    slug: str = Field(
        ...,
        description="URL-safe slug (lowercase, hyphenated; e.g. ``protea-method``).",
    )
    role: str = Field(
        ...,
        description=(
            "Architectural slot key: ``core``, ``contracts``, "
            "``plugin``, ``lab``."
        ),
    )
    role_label: str = Field(
        ...,
        description="Human-readable label for ``role`` shown in the UI.",
    )
    status: str = Field(
        ...,
        description=(
            "Coarse health signal sourced from the repo's CI / release "
            "state (``green``, ``warn``, ``red``)."
        ),
    )
    summary: str = Field(..., description="One-line description shown on the stack landing page.")
    github_url: str = Field(..., description="Canonical ``https://github.com/...`` URL.")
    docs_url: str | None = Field(
        default=None,
        description="Sphinx docs URL when published; ``None`` for sibling repos without docs.",
    )
    package_url: str | None = Field(
        default=None,
        description="PyPI / package registry URL; ``None`` if not published.",
    )
    local_docs_path: str | None = Field(
        default=None,
        description=(
            "Path under the local docs portal where this repo's "
            "Sphinx build is served (when present in the worktree)."
        ),
    )


class StackResponse(BaseModel):
    """Top-level payload for ``GET /stack``.

    Lists every repository in the PROTEA family plus the link to the
    canonical thesis PDF. Consumed by the frontend's stack overview
    page and the docs portal sidebar.
    """

    repos: list[RepoEntry] = Field(
        ...,
        description="One ``RepoEntry`` per repository in the PROTEA family.",
    )
    thesis_pdf_url: str | None = Field(
        default=None,
        description="Public URL of the canonical thesis PDF; ``None`` if not yet hosted.",
    )


class PullRequest(BaseModel):
    """One open PR in the stack as reported by GitHub's REST API.

    Used by the stack landing page's PR widget; the payload is the
    intersection of fields the UI actually renders, not a full echo of
    GitHub's response.
    """

    repo: str = Field(..., description="Slug of the repository the PR belongs to.")
    number: int = Field(..., description="GitHub's PR number (per-repo, integer).")
    title: str = Field(..., description="PR title as set on GitHub.")
    url: str = Field(..., description="Public ``https://github.com/.../pull/N`` URL.")
    state: str = Field(..., description="GitHub state (``open``, ``closed``).")
    draft: bool = Field(
        ...,
        description="True when the PR is in draft state (not ready for review).",
    )
    author: str | None = Field(
        ...,
        description="GitHub login of the PR author; ``None`` for ghost users.",
    )
    created_at: str = Field(..., description="ISO 8601 timestamp of PR creation.")
    updated_at: str = Field(..., description="ISO 8601 timestamp of the last PR update.")
    labels: list[str] = Field(..., description="Names of labels attached to the PR.")


class PullsResponse(BaseModel):
    """Aggregated open-PR snapshot across all stack repos.

    The handler caches the GitHub query for a few minutes; ``cached``
    flips to ``True`` when a response is served from the in-process
    cache, ``fetched_at`` records the original wall-clock time, and
    ``errors`` carries per-repo failures (e.g. rate-limited, 404).
    """

    fetched_at: float = Field(
        ...,
        description="Unix timestamp when the snapshot was originally built.",
    )
    cached: bool = Field(
        ...,
        description=(
            "True when the response is served from the in-process cache "
            "(GitHub was not contacted for this request)."
        ),
    )
    repos_queried: int = Field(
        ...,
        description="Number of repos GitHub was queried for in the snapshot.",
    )
    pulls: list[PullRequest] = Field(
        ...,
        description="Aggregated open PRs across all queried repos.",
    )
    rate_limit_remaining: int | None = Field(
        default=None,
        description=(
            "Value of GitHub's ``X-RateLimit-Remaining`` header from "
            "the underlying call; ``None`` if served from cache."
        ),
    )
    errors: dict[str, str] = Field(
        default_factory=dict,
        description="Per-repo error map (slug → message) for repos GitHub failed on.",
    )


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
    # The PDF lives in apps/web/public/, which Next serves at / under the
    # frontend host. No FastAPI proxy hop, no /static mount dependency.
    return "/thesis.pdf" if _THESIS_PDF.exists() else None


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
    ``docs/build/<slug>/html/`` or ``apps/web/public/thesis.pdf``
    respectively, and is ``None`` otherwise.
    """
    return StackResponse(repos=_load_repos(), thesis_pdf_url=_thesis_pdf_url())


def _fetch_repo_pulls(
    client: httpx.Client,
    repo: RepoEntry,
) -> tuple[list[PullRequest], int | None, str | None]:
    """Fetch open PRs for one repo. Returns (pulls, rate_remaining, error)."""
    owner, name = _owner_repo(repo.github_url)
    url = f"{_GITHUB_API}/repos/{owner}/{name}/pulls"
    try:
        resp = client.get(url, params={"state": "open", "per_page": 100})
    except httpx.HTTPError as exc:
        return [], None, str(exc)

    rate_remaining = (
        int(resp.headers["X-RateLimit-Remaining"])
        if "X-RateLimit-Remaining" in resp.headers
        else None
    )
    if resp.status_code != 200:
        return [], rate_remaining, f"HTTP {resp.status_code}: {resp.text[:200]}"

    pulls = [
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
        for item in resp.json()
    ]
    return pulls, rate_remaining, None


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
            repo_pulls, repo_rate, repo_err = _fetch_repo_pulls(client, repo)
            pulls.extend(repo_pulls)
            if repo_rate is not None:
                rate_remaining = repo_rate
            if repo_err is not None:
                errors[repo.name] = repo_err

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

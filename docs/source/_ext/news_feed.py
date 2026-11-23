"""Sphinx directives that render the news and the project status from one file.

Registers two directives that read ``_data/news.json``:

``.. news-feed::``
    The announcements, newest first, one card each. ``:limit: N`` keeps the N most
    recent, which is how the front page shows the latest few and the news page shows
    all of them from the same entries.

``.. project-status::``
    Where the project stands: its stage, the current champion with the number it was
    measured at and where, the nightly publication and the next milestones.

The file is the single source. An announcement is a commit to it, so every change to
what visitors are told has an author, a date and a review. Its text is plain prose (no
markup), so the instrument's web front can show the same entries without parsing
reStructuredText; a link names a documentation page and is rendered here as a
cross-reference.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from docutils import nodes
from docutils.parsers.rst import Directive, directives
from docutils.statemachine import StringList

if TYPE_CHECKING:
    from sphinx.application import Sphinx

NEWS = Path(__file__).resolve().parents[1] / "_data" / "news.json"


def _load() -> dict:
    return json.loads(NEWS.read_text())


def _plain(text: str) -> str:
    """Escape what reStructuredText would read as markup, so prose stays prose."""
    for char in ("\\", "*", "`", "|", "_"):
        text = text.replace(char, "\\" + char)
    return text


class _Rendered(Directive):
    """A directive whose body is reStructuredText built from the news file."""

    def lines(self) -> list[str]:
        raise NotImplementedError

    def run(self):
        env = self.state.document.settings.env
        env.note_dependency(str(NEWS))
        node = nodes.container()
        self.state.nested_parse(
            StringList(self.lines(), source=env.docname), self.content_offset, node
        )
        return node.children


class NewsFeed(_Rendered):
    """The announcements, newest first, as cards."""

    option_spec = {"limit": directives.nonnegative_int}

    def lines(self) -> list[str]:
        entries = sorted(_load()["announcements"], key=lambda e: e["date"], reverse=True)
        limit = self.options.get("limit")
        if limit:
            entries = entries[:limit]
        out: list[str] = []
        for entry in entries:
            out += [f".. card:: {_plain(entry['title'])}", "   :class-card: sd-mb-3"]
            if entry.get("link"):
                out += [f"   :link: {entry['link']}", "   :link-type: doc"]
            out += ["", f"   :bdg-secondary:`{entry['date']}` {_plain(entry['body'])}", ""]
        return out


class ProjectStatus(_Rendered):
    """Where the project stands, as a field list."""

    def lines(self) -> list[str]:
        status = _load()["status"]
        champion = status["champion"]
        out = [
            f":As of: {status['as_of']}",
            f":Stage: {_plain(status['stage'])}",
            f":Champion: {_plain(champion['name'])}: **{champion['measure']}** "
            f"({_plain(champion['reading'])}; fitted on {_plain(champion['fitted_on'])}). "
            f"{_plain(champion['rule'])}",
            f":Nightly publication: {_plain(status['nightly'])}",
            ":Next:",
            "",
        ]
        out += [f"   {i}. {_plain(step)}" for i, step in enumerate(status["next"], start=1)]
        return out + [""]


def setup(app: Sphinx) -> dict:
    app.add_directive("news-feed", NewsFeed)
    app.add_directive("project-status", ProjectStatus)
    return {"version": "1.0", "parallel_read_safe": True, "parallel_write_safe": True}

"""Read the GO release a GAF file declares, and refuse a mismatched snapshot.

Every GAF carries a ``!go-version:`` header naming the ontology build it was
generated against.  That is a fact stated by the file, not an inference from
publication dates, and it is the only honest way to decide which
``OntologySnapshot`` an ``AnnotationSet`` may be bound to.

The declared builds are ``go-plus`` versions and GO does not archive all of
them as public releases, so the rule cannot be equality.  It is *the most
recent published release at or before the declared build*: annotations were
made against a graph no newer than the one the file names.  Binding a GAF to a
later release silently backdates the ontology onto annotations that could not
have used it, which is how GOA 220 ended up eleven months in its own future.

Parsing is separated from HTTP so the rule can be tested without a server.
"""

from __future__ import annotations

import re
import zlib
from datetime import date

import requests

#: ``releases/YYYY-MM-DD`` appears inside the OWL URL the header carries.
_RELEASE_RE = re.compile(r"releases/(\d{4}-\d{2}-\d{2})")

#: A GAF header is a few hundred bytes; 64 KiB of gzip is a wide margin.
_HEADER_BYTES = 65536


def release_date(obo_version: str) -> date | None:
    """Return the ``YYYY-MM-DD`` embedded in an OBO version string.

    Accepts both the stored ``releases/2024-03-28`` form and the full OWL URL
    a GAF header carries.  Returns ``None`` when no release date is present.
    """
    match = _RELEASE_RE.search(obo_version or "")
    return date.fromisoformat(match.group(1)) if match else None


def declared_release(header_text: str) -> date | None:
    """Return the release date the GAF's ``!go-version`` header declares.

    A ranged read can stop mid-line, so an unterminated tail is dropped rather
    than parsed: half of ``releases/2024-04-13`` must read as "could not
    verify", never as a different date.
    """
    lines = header_text.split("\n")
    if lines and not header_text.endswith("\n"):
        lines.pop()
    for line in lines:
        if not line.startswith("!"):
            break
        if line.lower().startswith("!go-version:"):
            return release_date(line)
    return None


def fetch_header(gaf_url: str, timeout_seconds: int) -> str:
    """Download and decode just the leading header block of a GAF.

    Uses a ranged request so a 19 GB file costs a few kilobytes.  A truncated
    gzip stream is expected and not an error: ``decompressobj`` returns what it
    could inflate and the tail is discarded.  Returns ``""`` when the header
    cannot be read, which the caller treats as unverified rather than valid.
    """
    try:
        resp = requests.get(
            gaf_url,
            headers={"Range": f"bytes=0-{_HEADER_BYTES - 1}"},
            timeout=timeout_seconds,
        )
        resp.raise_for_status()
        raw = resp.content
        if gaf_url.endswith(".gz"):
            raw = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(raw)
        return raw.decode("utf-8", errors="replace")
    except (requests.RequestException, zlib.error, ValueError):
        return ""


def assert_not_newer_than_declared(
    *, gaf_url: str, obo_version: str, declared: date | None, allow_unverified: bool
) -> dict[str, str | None]:
    """Raise unless ``obo_version`` is at or before what the GAF declares.

    Returns the pair actually compared so the caller can record it.  When the
    header could not be read, refuses unless ``allow_unverified`` is set: a
    guard that passes silently when it cannot check is not a guard.
    """
    bound = release_date(obo_version)
    if declared is None:
        if allow_unverified:
            return {"declared": None, "bound": obo_version}
        raise ValueError(
            f"{gaf_url} declares no !go-version header, so the ontology it was "
            f"built against cannot be verified against {obo_version}. Pass "
            "allow_unverified_ontology=true to load it anyway and record that "
            "the check was skipped."
        )
    if bound is None:
        raise ValueError(
            f"OntologySnapshot version {obo_version!r} carries no release date, "
            f"so it cannot be compared with the {declared.isoformat()} build "
            f"that {gaf_url} declares."
        )
    if bound > declared:
        raise ValueError(
            f"{gaf_url} was generated against GO releases/{declared.isoformat()}, "
            f"but the requested snapshot is {obo_version}, which is later. Every "
            "annotation in the file predates that graph, so binding them to it "
            "would backdate an ontology the annotators could not have used. Use "
            "the most recent published release at or before "
            f"releases/{declared.isoformat()}."
        )
    return {"declared": f"releases/{declared.isoformat()}", "bound": obo_version}

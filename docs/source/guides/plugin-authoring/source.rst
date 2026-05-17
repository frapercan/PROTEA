Source plugin guide
===================

An annotation source plugin ingests biological annotations from an
external resource (a database release, a file format, a web API) and
yields typed record objects consumed by PROTEA's persistence
operations. ``protea-core`` dispatches load requests to the source
whose ``name`` attribute matches the source key in the job payload.

Existing sources shipped in `protea-sources
<https://github.com/frapercan/protea-sources>`_: ``goa``, ``quickgo``,
``uniprot``.

.. _source-abc:

The ABC
-------

The ABC is a *marker* contract. It imposes ``name`` and ``version``
class attributes but no single shared ``stream`` / ``load`` method.
This is by design: different sources have fundamentally different
modalities.

.. code-block:: python

   from protea_contracts.annotation_source import AnnotationSource

   class AnnotationSource(ABC):
       name: str     # stable string id used by protea-core to dispatch
       version: str  # stored in AnnotationSet.source_version

Subclasses add their own modality-specific methods. For example:

- ``GoaSource.stream(payload, *, emit)`` yields ``GoaAnnotationRecord``
  objects from a GAF URL.
- ``UniProtSource.stream_fasta(payload, *, emit)`` yields
  ``UniProtProteinRecord`` objects.
- ``UniProtSource.stream_metadata(payload, *, emit)`` yields
  ``UniProtMetadataRecord`` objects.

``isinstance(plugin, AnnotationSource)`` is sufficient for
``protea-core`` to know the plugin belongs to this layer. The
operation that uses the plugin casts it to the concrete class it
expects (e.g. ``GoaSource``) and calls the modality-specific method
directly.

The payload and record types that cross the boundary live in
``protea_contracts.records``. Each source has its own ``StreamPayload``
and one or more ``Record`` dataclasses. All are frozen pydantic models
with ``extra="forbid"``: typos in the caller's payload dict raise at
construction time, not deep inside the plugin.

Key invariants:

- ``name`` must be a stable string; renaming it is a breaking change.
- ``version`` should reflect the source release so it can be stored
  in ``AnnotationSet.source_version`` for full provenance tracing.
- Parsing and HTTP belong in the plugin. Persistence (DB filtering,
  FK resolution, bulk insert, per-page commits) belongs in the
  PROTEA operation that consumes the record stream.
- Error handling: raise ``requests.HTTPError`` (or an appropriate
  exception) on network failures; do not swallow them silently.
- Events: emit ``source.<name>.download_start`` and
  ``source.<name>.download_done`` (at minimum) so PROTEA's event log
  gives operators a progress trace.

.. _source-packaging:

Packaging snippet
-----------------

.. code-block:: toml

   [tool.poetry]
   name = "protea-sources-mysource"
   version = "0.1.0"
   packages = [{ include = "protea_sources_mysource", from = "src" }]

   [tool.poetry.dependencies]
   python = ">=3.12,<4.0"
   protea-contracts = ">=0.2"
   requests = ">=2.31"

   [tool.poetry.plugins."protea.sources"]
   mysource = "protea_sources_mysource:plugin"

.. _source-tests:

Test scaffold
-------------

Copy and adapt from ``protea-sources/tests/test_goa.py``:

.. code-block:: python

   """Tests for the mysource plugin."""

   from importlib.metadata import entry_points
   from unittest.mock import MagicMock, patch

   from protea_contracts import AnnotationSource
   from protea_sources_mysource import MySource, plugin


   def test_plugin_is_mysource_instance() -> None:
       assert isinstance(plugin, MySource)


   def test_plugin_implements_annotation_source_abc() -> None:
       assert isinstance(plugin, AnnotationSource)


   def test_plugin_name_matches_entry_point_key() -> None:
       assert plugin.name == "mysource"


   def test_plugin_resolvable_via_entry_points() -> None:
       eps = entry_points(group="protea.sources")
       matches = [ep for ep in eps if ep.name == "mysource"]
       assert len(matches) == 1
       assert matches[0].load() is plugin


   def test_stream_yields_records(monkeypatch: object) -> None:
       noop = lambda *a, **k: None   # noqa: E731
       # Patch the HTTP call with a canned response body.
       with patch(
           "protea_sources_mysource.requests.get",
           return_value=_mock_response(b"record_data\n"),
       ):
           records = list(plugin.stream(..., emit=noop))
       assert len(records) >= 1

The test suite for a real source should also cover: parser correctness
against canned fixtures (no HTTP), error propagation on ``HTTPError``,
and emit event names (``download_start`` / ``download_done``). The GOA
test file is the reference implementation.

.. _source-toy-example:

Worked example: ``toy`` source
---------------------------------

The ``toy`` source reads annotation records from an in-memory list of
strings. It has no HTTP dependency and no external payload model, making
it the simplest possible source to inspect and adapt.

First, define the payload and record types in ``protea-contracts`` (or
inline for a standalone package):

.. code-block:: python

   # For a standalone toy package, define types inline.
   # In a real plugin, add them to protea_contracts/records.py and open a
   # coordinated PR against protea-contracts.

   from pydantic import BaseModel, ConfigDict

   class ToyStreamPayload(BaseModel):
       model_config = ConfigDict(frozen=True, strict=True, extra="forbid")
       records: list[str]
       """Pipe-separated lines: 'ACCESSION|GO_ID'."""

   class ToyAnnotationRecord(BaseModel):
       model_config = ConfigDict(frozen=True, strict=True, extra="forbid")
       accession: str
       go_id: str

Then the plugin itself:

.. code-block:: python

   # src/protea_sources_toy/__init__.py
   """Toy annotation source: yields records from an in-memory list.

   No HTTP, no file I/O. Useful as a template and in unit tests.

   Each string in ``ToyStreamPayload.records`` must be formatted as
   ``"ACCESSION|GO_ID"``.

   Install:
       pip install -e .
   """

   from __future__ import annotations

   from collections.abc import Iterator
   from typing import Any

   from protea_contracts.annotation_source import AnnotationSource

   # Inline payload + record types (see note above about real plugins).
   from pydantic import BaseModel, ConfigDict


   class ToyStreamPayload(BaseModel):
       model_config = ConfigDict(frozen=True, strict=True, extra="forbid")
       records: list[str]


   class ToyAnnotationRecord(BaseModel):
       model_config = ConfigDict(frozen=True, strict=True, extra="forbid")
       accession: str
       go_id: str


   class ToySource(AnnotationSource):
       """In-memory annotation source for tests and templates."""

       name = "toy"
       version = "toy_v0"

       def stream(
           self,
           payload: ToyStreamPayload,
           *,
           emit: Any,
       ) -> Iterator[ToyAnnotationRecord]:
           """Yield records parsed from the payload's ``records`` list.

           Each entry must be ``"ACCESSION|GO_ID"``. Malformed entries
           (missing separator, empty fields) are skipped.
           """
           emit(
               "source.toy.stream_start",
               None,
               {"n_records": len(payload.records)},
               "info",
           )
           for line in payload.records:
               parts = line.split("|", 1)
               if len(parts) != 2 or not parts[0] or not parts[1]:
                   continue
               yield ToyAnnotationRecord(
                   accession=parts[0].strip(),
                   go_id=parts[1].strip(),
               )
           emit("source.toy.stream_done", None, {}, "info")


   #: Module-level instance discovered via ``protea.sources`` entry_points.
   plugin = ToySource()

The corresponding ``pyproject.toml`` entry-point stanza:

.. code-block:: toml

   [tool.poetry.plugins."protea.sources"]
   toy = "protea_sources_toy:plugin"

Verify end-to-end:

.. code-block:: python

   from protea_sources_toy import ToyStreamPayload, plugin

   noop = lambda *a, **k: None

   payload = ToyStreamPayload(records=["P12345|GO:0005737", "Q67890|GO:0003674"])
   records = list(plugin.stream(payload, emit=noop))

   print(len(records))            # Expected output: 2
   print(records[0].accession)    # Expected output: P12345
   print(records[0].go_id)        # Expected output: GO:0005737
   print(plugin.name)             # Expected output: toy
   print(plugin.version)          # Expected output: toy_v0

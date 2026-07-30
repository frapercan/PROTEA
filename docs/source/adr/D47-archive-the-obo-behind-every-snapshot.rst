ADR-D47: Archive the OBO behind every snapshot
===============================================

:Status: Accepted
:Date: 2026-07-30
:Author: Francisco Miguel Pérez Canales
:Phase: T-GOBERNANZA

Context: the root of the evidence chain was a third-party link
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``LoadOntologySnapshotOperation._download`` fetches an OBO into a string,
parses it into ``GOTerm`` and ``GOTermRelationship`` rows, and discards the
bytes. The only record kept is ``ontology_snapshot.obo_url``.

``run_cafa_evaluation`` then re-fetches that URL on every run
(``_stage_evaluator_inputs`` calling ``_artifacts.download_obo``), as does
``batch_rescore_evaluation``. So the ontology behind a published score is not
something the project holds. It is a request to EBI, repeated at scoring time,
whose result is assumed to be unchanged.

Everything downstream rests on those bytes: the term universe, the True Path
Rule edges, the propagation of annotations, the Information Accretion table
(ADR-D46) and every metric computed from them. If the upstream file is revised
or withdrawn, past scores stop being reproducible and nothing in the system
reports it. The failure is silent by construction, because a successful
download looks identical to a correct one.

This is the same class of defect as the one ADR-D46 records for IA and as the
basename check in ``band_registry.ia_token``: an identifier that names an
artifact without pinning its content.

Decision
~~~~~~~~
1. **``ontology_snapshot`` gains ``obo_uri`` and ``obo_sha256``.** The raw OBO
   is archived gzipped in the artifact store under
   ``ontology_snapshot/{id}/go.obo.gz``, and the sha256 recorded is of the
   UNCOMPRESSED bytes, which is what a consumer ends up holding.

2. **A registered operation, ``archive_ontology_snapshot``**, performs the
   archival. Both columns are nullable because the ten snapshots loaded before
   this record predate it, and backfilling is what the operation is for.

3. **Backfill is gated, not blind.** The file served at ``obo_url`` today is not
   necessarily the file parsed at load time, so the operation compares the
   fetched content against the database before storing it:

   - the ``data-version`` header must equal ``obo_version``, and
   - every non-obsolete ``go_id`` already loaded for the snapshot must be
     present in the fetched file, within ``max_term_drift_pct`` (default
     ``0.0``, an exact match).

   Terms ADDED upstream are tolerated and counted: they do not stop the loaded
   snapshot from being representable. Terms MISSING do, and raise
   ``OntologyDriftError`` rather than recording a newer ontology under an older
   snapshot's identity.

   The congruence parser is deliberately independent of
   ``LoadOntologySnapshotOperation._parse_terms``. A check that inherits the
   parser it is checking cannot detect a parser-level disagreement.

4. **``run_cafa_evaluation`` prefers the archive.** ``_resolve_obo`` reads
   ``obo_uri`` through the artifact store and verifies the recorded hash. The
   upstream path survives as a fallback and now emits at ``warning`` level,
   stating that the run depends on the URL still serving the same bytes.

Consequences
~~~~~~~~~~~~
Storage is modest: a go-basic OBO is tens of megabytes and gzips to a few, so
ten snapshots cost single-digit gigabytes at most against 538 GB free.

An archived snapshot makes a run reproducible without network access to EBI,
which also removes an availability dependency from the scoring path.

The gate can fail on a legitimately revised upstream file. That is the intended
behaviour: it converts a silent substitution into a job failure that names the
missing terms, and the operator then decides whether to load a new snapshot or
to widen ``max_term_drift_pct`` deliberately and on the record.

What this record does not settle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Externally-authored reference tables still have no home. ``IA_cafa6.tsv`` is
git-tracked, arrived in a documentation commit, is what ``band_registry`` names
as authoritative for band v226, and has no row anywhere. It cannot become an
``InformationAccretionSet`` as that table is defined, because
``annotation_set_id`` is ``NOT NULL`` and no annotation set in this database
corresponds to the corpus CAFA6 counted. Whether external tables get
first-class rows (a nullable corpus plus a ``source`` field) or stay declared
inputs is an open decision.

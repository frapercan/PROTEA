ADR-D66: The instrument publishes the champion's predictions every night, as a file, beside an editorial news file
==================================================================================================================

:Status: Proposed (2026-11-23). The author asked for it on 2026-11-23: "Que el
         instrumento despliegue la prediccion del mejor modelo hasta la fecha,
         cada noche", with the champion chosen by a declared rule every night
         and the population "the corpus of the latest cut and the sequences
         submitted to the instrument". He also asked for a changelog for
         visitors, "bien visible, nuevos anuncios y estado del proyecto". The
         design below was worked out with the session that operates the
         production node and waits for the author's word.
:Date: 2026-11-23
:Author: Francisco Miguel Perez Canales
:Phase: platform closure
:Depends on: ADR-D59 (the photo of a cut), ADR-D60 (one universe, method
             contracts), ADR-D55 (evaluated explorations and their readings)

Context
~~~~~~~

**Production is a release behind.** Production serves the revision of
2026-11-10. The integration branch is 114 commits and 24 migrations ahead,
and the production database has none of the tables those migrations
create. Three of them matter here:

- the cut photos and their channel table;
- the homology table;
- the identity of a method (``a_method_has_an_identity_of_its_own``).

The production node measured the cost of the migrations: 21 tables created,
17 columns and 15 indexes added, and one data update of 87 rows. None of them
rewrites a large table, so the cost is minutes. What does not come for free
is the six sibling packages that move with the release. A pinned git install
can report success without installing, so a deployment is verified by
importing each sibling and reading its revision, never by the installer's exit
code.

**A method needs an identity before it can be published every night.** Without
it, two prediction sets on the same window, population and ontology are not
told apart: the comparison returns nothing, and reads as "the method did not
move". A champion ingested every night would hit exactly that collision.

**Rows are the wrong medium for a nightly artefact.** Measured on production,
a stored prediction row costs about 442 bytes. Three hundred terms for each of
some 617,000 annotated proteins would be about 185 million rows, some 82 GB
every night, plus as many dead rows for vacuum, on a QLC disk whose mean write
latency under ingestion is 1.7 s. Even fifty terms per protein would churn
13.6 GB a night.

**What visitors are told has an author; what runs every night does not.**
Announcements and the status of the project need a review. The facts of a
nightly run need none: what ran, on which cut, which file came out, and which
champion the rule chose. If one channel carried both, either the facts would
wait for a review they do not need, or the editorial text would be published
without one.

Decision
~~~~~~~~

**1. The champion is chosen by a declared rule, not by hand.** The candidates
are the arms with a method identity whose producer can run at the latest cut.
They are compared on the latest validation window with its headline reading:
the weighted micro F-measure, mean of the nine cells, read raw. The champion
changes only when a challenger beats it on that window by at least 0.01 in a
paired bootstrap with enough power, the comparison rule of the research line.
On 2026-11-23 the rule names the protein's own annotations, ordered by the
model with co-annotation fitted on 194->211, at 0.3543 on 211->227.

**2. The population is the corpus of the latest cut, plus the sequences
submitted to the instrument.** For cut 227 that is the 1,850,077 members of
its photo, and any sequence submitted since.

**3. The nightly run publishes; it never scores.**

- *What it does.* It runs the champion's producer, a declared job, on the
  population at the latest cut. It writes the output as one Parquet file named
  by its sha256, with a manifest and a ``SHA256SUMS`` file, to a stable path
  outside any build tree.
- *When nothing changed.* If the champion, the cut and the submissions are
  unchanged since the last night, it republishes the same file with the same
  digest and records that nothing changed.
- *Where scores come from.* Scoring stays a declared job on a declared
  window; the nightly run does not evaluate.

**4. Two files, two owners, two paths.**

- *Editorial:* ``docs/source/_data/news.json`` in the repository.

  - What it holds: announcements and the status of the project, as plain
    prose.
  - How it changes: it passes through a pull request the author reviews, and
    it ships with a declared release.
  - Who reads it: the documentation renders it, and the API serves it.
- *Generated:* the nightly record at a stable path, overwritten by the nightly
  run and by nothing else. It holds:

  - the date and the cut;
  - the champion, its measure and the window the measure was taken on;
  - the producer's job;
  - the file's digest and its counts.

Neither path can publish through the other.

**5. The API reads both at request time.** The routes follow the pattern of
``GET /stack`` and of the thesis PDF mount:

- ``/news`` returns the editorial file merged with the latest nightly record;
- ``/champion`` returns the nightly record;
- ``/champion/predictions/{accession}`` reads the protein's rows from the Parquet
  file.

All three are read-only, and all three are proposed: none of them exists yet.

No value is cached at import, so an overwrite at either path is served without
a rebuild or a restart. The web front shows the latest announcements and the
champion on its home page through these routes. It never reads a file placed
in its own public folder, because the deployed front serves a copy made at
build time.

**6. Files cross machines with the inbox discipline.**

- A delivery has its own empty directory, a ``SHA256SUMS`` file, and a
  ``READY`` file written last; nothing in it is executed.
- Predictions that enter the store go through ``ingest_method_predictions`` and
  follow its contract:

  - the aspect column is checked against the ontology snapshot, and a term the
    snapshot lacks is rejected;
  - the cap per protein is declared in the payload;
  - the method is named;
  - the distance is left empty.

**7. The order of work.** The release comes first, and nothing is scheduled
on the production node before it. Every producer today needs an evaluation set,
so the platform also needs a population mode at a cut, without a window, for
the champion's producer. Then the data the champion needs is loaded in
dependency order:

1. proteins and sequences;
2. metadata;
3. the GOA sets of the cuts;
4. the photos;
5. the windows;
6. the embedding banks, delivered as files.

Only then are the routes, the web strip and the nightly timer enabled.

Consequences
~~~~~~~~~~~~

- **The nightly publication costs one file write and no row churn**, and a
  night without changes costs a digest check.
- **Every published prediction names its champion, its cut, the window its
  measure was taken on, and its file's digest.**
- **The changelog cannot be published without the author's review**, and the
  nightly facts are not held up by one.
- **A digest of a photo or a file is compared only within one revision of the
  platform.** Across a release the digests change by design (the channel table
  is part of the photo), and the counts are the contract that crosses it.
- **The production node stays a node without a GPU.** Embedding banks arrive
  as files and are never recomputed there, and nearest-neighbour searches run
  on its CPU or arrive as predictions.

Open questions for the author
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. When to deploy the release to production, and which data of the development
   universe to bring.
2. Whether the GOA sets that the new universe does not use (69 of 75 in
   production) are replaced or kept.
3. Whether the nightly producer runs on the production node or on the desktop,
   with the file delivered to the production inbox. The current champion is
   cheap: it reads the protein's own terms. A retrieval champion would need
   the banks.
4. Which champion rule. The rule above compares the mean of the nine cells.
   The alternative is per-cell dominance: in each cell (knowledge category by
   GO branch), a challenger replaces the incumbent when its paired comparison
   on the validation window resolves a gain of at least 0.01 there. Per-cell
   dominance matches the benchmark, which reports each cell and no mean. It
   also lets the published predictions come from different arms in different
   cells. On 211->227 the ordered own annotations lead in eight of the nine
   cells by point estimate. Transfer from the nearest neighbour (ProtT5-XL)
   leads in one: biological process for proteins with no experimental
   annotation (0.3034 against 0.2696, 1,477 proteins). These are point
   estimates; the paired comparison has not been run.

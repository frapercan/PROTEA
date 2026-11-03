/**
 * What the public site may claim about the data behind it.
 *
 * Everything here is a dated, hand-maintained constant on purpose. The
 * page must not ask the API how much data exists: with the corpus being
 * rebuilt, every count the API could answer is zero or in motion, and a
 * figure that changes under the reader is worse than one that says when
 * it was taken.
 */

/**
 * Whether the live predictor can answer a visitor.
 *
 * TRUE since 2026-10-29. It was measured false on 2026-10-07, when the
 * database behind protea.ngrok.app held zero rows in ``annotation_set``,
 * ``go_term``, ``protein_go_annotation`` and ``embedding_config``, so
 * ``POST /annotate`` answered
 *
 *     409 {"detail": "No annotation sets available. Load GO annotations first."}
 *
 * before queueing anything, and the visitor read an operator instruction.
 *
 * THE EVIDENCE THIS FLIP ASKED FOR, measured 2026-10-29, rows and not job
 * status, because the old docstring was right that a green ``succeeded`` is
 * not enough:
 *
 * | table                   | rows        |
 * | ----------------------- | ----------- |
 * | annotation_set          | 75          |
 * | go_term                 | 3.118.456   |
 * | protein_go_annotation   | 663.408.837 |
 * | embedding_config        | 13          |
 * | prediction_set          | 14, ALL with rows |
 * | go_prediction           | 25.399.802  |
 *
 * AND WHAT THE ENDPOINT WOULD ACTUALLY SERVE, which is the part row counts
 * do not answer. ``_newest_annotation_set`` resolves to GOA release 235 with
 * 8.118.592 annotations, so the 409 cannot fire. ``_best_embedding_config``
 * EXISTS-probes configs and returns the first with embeddings: run as the
 * endpoint runs it, that is ``e7ba37da``, ankh-base with 684.048 vectors over
 * 684.048 distinct sequences. A complete bank, not a partial one.
 *
 * ONE CAVEAT, AND IT IS NOT A BLOCKER TODAY. That pick is arbitrary rather
 * than chosen: the preference is "smallest model that has embeddings", by
 * ``param_count`` ascending with nulls last, and every ``param_count`` in this
 * database is NULL. Four configs have vectors -- ``e7ba37da`` and the two
 * learned codes at 684.048 each, and ``f3a7fe16`` mid-computation at 256.896
 * -- so the order is whatever the query plan returns. It returns the right one
 * now. To make that a decision instead of a coincidence, pin it with
 * ``PROTEA_DEFAULT_EMBEDDING_CONFIG_ID``, which
 * :func:`_pinned_embedding_config` honours only when the config exists AND has
 * embeddings. Running ``count_backend_parameters`` would also make the stated
 * preference work as written.
 *
 * Flipping this does NOT open the door while the shared GPU is busy. That is a
 * separate, live check (``gpu.busy``) with its own banner, so a visitor
 * arriving mid-run is told the queue is busy rather than the predictor is
 * paused -- two different facts, which is why the form keeps two flags.
 */
export const LIVE_ANNOTATION_AVAILABLE = true;

/**
 * Whether the surfaces that exist to display results have results.
 *
 * Separate from the predictor: a page can be perfectly able to run and
 * still have an empty table. While this is false the navigation stops
 * offering the ten surfaces whose only content would be an empty state,
 * because an external audit on 2026-10-07 found that a visitor opening
 * them met admin buttons over blank tables and read the whole project as
 * broken. They are not deleted and the routes still work; they come back
 * with this flag.
 *
 * Flip it with the same evidence as the other one: rows, not job status.
 */
/**
 * Which result surfaces have rows, measured per route instead of all at once.
 *
 * ``DATA_SURFACES_HAVE_DATA`` was one boolean over eleven routes, and it was
 * right when it was written: on 2026-10-07 every one of them was empty. It is
 * wrong now, and the cost is not symmetric -- a single flag hides the six
 * surfaces that hold the campaign's work in order to protect a visitor from
 * the three that are still empty.
 *
 * MEASURED on the server's own database, 2026-10-27:
 *
 * | route                             | rows |
 * | --------------------------------- | ---- |
 * | /instrument/embeddings            | 8 configs, 684.048 vectors |
 * | /instrument/functional-annotation | 1.084.104 predictions, 6 sets |
 * | /instrument/annotations           | 75 annotation sets, 66 snapshots |
 * | /instrument/query-sets            | 5.810 entries |
 * | /instrument/evaluation            | 130 evaluation sets (2026-10-29) |
 * | /feature-registry                 | 78 features, schema 1.7.0 |
 * | /instrument/graph                 | GET /v1/graph answers with 10 nodes |
 * | /instrument/benchmark             | 130 sets, 14 results (2026-10-29) |
 * | /instrument/datasets              | 0 |
 * | /instrument/reranker              | 0 reranker models |
 * | /instrument/scoring               | 0 (/v1/scoring/rerankers is empty) |
 *
 * ``benchmark`` stays hidden deliberately: three results is a table with one
 * row of nine cells, which is the blank-table-with-admin-buttons the original
 * audit objected to. It comes back with the arms, not with this edit.
 *
 * Update an entry the way the old flag asked: by counting ROWS, not by reading
 * a green job. And update the table above in the same commit, because an entry
 * whose evidence is not written down is a guess that looks like a measurement.
 */
export const SURFACE_HAS_ROWS: Record<string, boolean> = {
  "/instrument/embeddings": true,
  "/instrument/functional-annotation": true,
  "/instrument/annotations": true,
  "/instrument/query-sets": true,
  "/instrument/evaluation": true,
  "/feature-registry": true,
  "/instrument/graph": true,
  // Measured on this server 2026-10-28, and two of these had gone stale in the
  // direction the docstring below says is the expensive one: invisible and
  // live. evaluation_result holds 14 rows on 2026-10-29 (8 when this comment
  // was written), which is what the matrix reads, and scoring_config holds 1.
  // Both were false and both were hidden from an anonymous visitor while they
  // had something to show.
  "/instrument/benchmark": true,
  "/instrument/scoring": true,
  // Still true, and checked rather than assumed: dataset holds 0 rows and
  // reranker_model holds 0, which the graph also reports as the reason
  // `features` and `reranking` are blocked.
  "/instrument/datasets": false,
  "/instrument/reranker": false,
  // Stratified evaluation. Listed explicitly although an absent route defaults
  // to visible, because this one is the surface the campaign exists to show and
  // a reader of this table should not have to infer it: 36 cells over the
  // 230->235 window, 32.181 rows placed.
  "/instrument/stratum": true,
};

/**
 * Whether this route may appear in navigation.
 *
 * A route not in the table is not gated at all, so adding a surface does not
 * silently hide it: the failure direction is "visible and empty", which an
 * operator notices, rather than "invisible and live", which took three weeks
 * to notice last time.
 */
export function surfaceHasRows(href: string): boolean {
  return SURFACE_HAS_ROWS[href] ?? true;
}

/**
 * The ingest that is running instead, as a dated snapshot.
 *
 * These are true and checkable, and they describe engineering rather than
 * a withdrawn result, which is the honest thing to put where a retracted
 * figure used to be. Update the whole object with its ``asOf`` when it
 * moves; never update a number without the date.
 */
export const CAMPAIGN_STATUS = {
  asOf: "2026-10-29",
  /**
   * Releases READ so far, of the whole series.
   *
   * These were one number until an external reviewer caught it: the copy
   * said the campaign "has read 75 releases, 802.1 GB of files" when 75
   * and 802.1 GB are the size of the SERIES and only 60 had been read.
   * An overstatement on the one page whose argument is that its claims
   * can be checked. Two fields now, and the copy says "X of Y".
   *
   * Now 75 of 75, measured 2026-10-29: annotation_set holds one row per GOA
   * release from 156 to 235. The two fields stay even though they are equal,
   * because the next series will not be, and collapsing them is how the
   * overstatement happened.
   */
  releasesRead: 75,
  /** GOA releases in the series (156 to 235; 206 to 210 were never published). */
  goaReleases: 75,
  /** Total size of the whole series, measured by reading all 75 headers. */
  gafGigabytes: 802.1,
  /**
   * Proteins admitted to the corpus so far, each carrying the release that
   * admitted it.
   *
   * 721.486 on 2026-10-29, and the universe pass is no longer running. It is
   * LOWER than the 745.421 this said on 2026-10-07 and that is not a
   * regression: the base was restarted on 2026-10-05 and the figure here was
   * taken from the previous build-up. Counted as the field defines itself --
   * ``first_admitted_release IS NOT NULL`` -- and not as every row of
   * ``protein``, which is 792.535 and includes the 71.049 that came from
   * UniProt rather than from a GAF.
   */
  proteinsAdmitted: 721_486,
} as const;

/**
 * The one result figure the public page states, and why only this one.
 *
 * External, dated, third-party, and permanent: a Kaggle leaderboard that
 * anyone can open and check.
 *
 * THE REASON IS NO LONGER THAT THERE ARE NONE. This said the database held
 * zero evaluations, which was true when it was written and stopped being true
 * on 2026-10-28: it holds eight results, all eight sealed, across SEVEN
 * distinct frames. The policy does not change and the premise does. No
 * internal figure is stated because the researcher has not declared one
 * publishable, and because seven frames over eight numbers means almost no
 * two of them may be read side by side -- which the seal now says out loud
 * instead of leaving unknown. The LAFA board's "first in seven of nine cells" is the
 * one to be careful with: beyond the wipe, 594 of that campaign's 1,296
 * results had been evaluated past the v227 holdout mark. It belongs in a
 * dated record, not in a headline.
 */
export const EXTERNAL_RESULT = {
  competition: "CAFA 6",
  rank: 19,
  teams: 2_186,
} as const;

/**
 * What the thesis on this site was written against.
 *
 * The argument states the LAFA board in the present tense throughout,
 * which was true when written. Rather than edit the prose, the site
 * scopes it with this date, so a reader knows what period the figures
 * belong to.
 */
export const ARGUMENT_RECORD = {
  board: "LAFA",
  frame: "Sep 2025 to Mar 2026",
} as const;

/**
 * External links the public page states, each checked to return 200 on
 * 2026-10-07. A dead link on a page someone is evaluating is worse than
 * no link, so nothing goes here that has not been fetched.
 *
 */
export const LINKS = {
  /** The LAFA benchmark the method is submitted to. Title checked: "CAFA Forever". */
  lafa: "https://functionbench.net/",
  /** The author's profile and the repository. Both checked, both 200. */
  github: "https://github.com/frapercan",
  repo: "https://github.com/frapercan/PROTEA",
  /** The competition itself. Title checked: "CAFA 6 Protein Function Prediction | Kaggle". */
  cafaCompetition: "https://www.kaggle.com/competitions/cafa-6-protein-function-prediction",
  /** The CAFA project, for a reader who wants to know what CAFA is. */
  cafaProject: "https://biofunctionprediction.org/cafa/",
  /** Our deployment of the official evaluator, which is what makes the
   *  numbers on this site reproducible by a third party. */
  evaluator: "https://github.com/frapercan/cafaeval-protea",
  evaluatorDocs: "https://cafaeval-protea.readthedocs.io/",
  /** Upstream, so the provenance of the evaluator is visible too. */
  evaluatorUpstream: "https://github.com/claradepaolis/CAFA-evaluator-PK",
} as const;

/**
 * What the platform is, in figures a reader can check.
 *
 * Counted on 2026-10-07, not rounded up from memory. The API numbers come
 * from the live OpenAPI document (117 paths, 136 GET/POST/PUT/PATCH/DELETE
 * operations). The test count is an AST walk over tests/, counting
 * functions named test_*, which is why it is 4,495 and not "more than
 * 4,000". A figure like this is the kind an interviewer checks, so it
 * is better slightly awkward and true.
 */
export const PLATFORM = {
  apiOperations: 136,
  apiRoutes: 117,
  backendTests: 4_495,
} as const;

/**
 * Who built it.
 *
 * An external audit on 2026-10-07 found the name nowhere on the site,
 * while the legal notice said the service was "operated personally"
 * without saying by whom. For a page someone is being evaluated from,
 * that is the first thing a reader looks for. Taken from the thesis
 * frontmatter and the git author, not guessed. No email here: publishing
 * a personal address is the author's call, not this file's.
 */
export const AUTHOR = {
  name: "Francisco Miguel Pérez Canales",
  /**
   * ORCID, once it is known.
   *
   * The only ORCID-shaped string anywhere in these repositories is the
   * placeholder 0000-0000-0000-0000, so it was not written here as a
   * guess: a wrong ORCID on a public page points a reader at another
   * researcher. Supplied by the author and checked against the public
   * record, which reads "Francisco M. Perez-Canales".
   */
  orcid: "0009-0008-6422-2303" as string | null,
} as const;

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
 * Measured false on 2026-10-07. The database behind protea.ngrok.app holds
 * zero rows in ``annotation_set``, ``go_term``, ``protein_go_annotation``
 * and ``embedding_config``, so ``POST /annotate`` returns
 *
 *     409 {"detail": "No annotation sets available. Load GO annotations first."}
 *
 * before it queues anything, and ``lib/api.ts`` surfaces that body
 * verbatim to the visitor. A dead button that prints an operator
 * instruction is worse than a button that explains itself.
 *
 * FLIP THIS in the same change that can show a ``PredictionSet`` WITH
 * ROWS. A green ``succeeded`` job is not enough evidence: with an empty
 * reference pool the predict operation short-circuits, logs
 * ``predict_go_terms.no_queries``, creates no prediction set at all, and
 * the form's ``resolvePredictionSet`` then retries eight times and lands
 * the visitor on an empty page. Verify rows, not job status.
 */
export const LIVE_ANNOTATION_AVAILABLE = false;

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
 * | /instrument/evaluation            | 125 evaluation sets |
 * | /feature-registry                 | 78 features, schema 1.7.0 |
 * | /instrument/graph                 | GET /v1/graph answers with 10 nodes |
 * | /instrument/benchmark             | 125 sets but **3** results |
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
  // live. evaluation_result holds 8 rows, which is what the matrix reads, and
  // scoring_config holds 1. Both were false and both were hidden from an
  // anonymous visitor while they had something to show.
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
  asOf: "2026-10-07",
  /**
   * Releases READ so far, of the whole series.
   *
   * These were one number until an external reviewer caught it: the copy
   * said the campaign "has read 75 releases, 802.1 GB of files" when 75
   * and 802.1 GB are the size of the SERIES and only 60 had been read.
   * An overstatement on the one page whose argument is that its claims
   * can be checked. Two fields now, and the copy says "X of Y".
   */
  releasesRead: 60,
  /** GOA releases in the series (156 to 235; 206 to 210 were never published). */
  goaReleases: 75,
  /** Total size of the whole series, measured by reading all 75 headers. */
  gafGigabytes: 802.1,
  /**
   * Proteins admitted to the corpus so far, each carrying the release that
   * admitted it. Still climbing while the universe pass runs.
   */
  proteinsAdmitted: 745_421,
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

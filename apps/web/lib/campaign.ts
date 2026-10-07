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
 * The ingest that is running instead, as a dated snapshot.
 *
 * These are true and checkable, and they describe engineering rather than
 * a withdrawn result, which is the honest thing to put where a retracted
 * figure used to be. Update the whole object with its ``asOf`` when it
 * moves; never update a number without the date.
 */
export const CAMPAIGN_STATUS = {
  asOf: "2026-10-07",
  /** GOA releases in the series (156 to 235; 206 to 210 were never published). */
  goaReleases: 75,
  /** Total size of those GAF files, measured by reading all 75 headers. */
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
 * anyone can open and check. Everything else PROTEA has measured came
 * from a campaign that was wiped on 2026-09-14, and the current database
 * holds zero evaluations, so no internal figure can be stated in the
 * present tense. The LAFA board's "first in seven of nine cells" is the
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
 * There is deliberately no LAFA link. LAFA is named in the prose because
 * it is what the method is submitted to, but no URL for it was found in
 * this repository and inventing one is not an option.
 */
export const LINKS = {
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

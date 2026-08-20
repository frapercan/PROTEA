// The campaign as a line, and what the surface should open on.
//
// The instrument is organised by artifact: embeddings here, prediction sets
// there, evaluations somewhere else. Nobody works that way. The work is a
// sequence of rungs, each asking one question, and a reader arriving at the
// board cannot see which question they are looking at the answer to.
//
// This is also where the default comes from. The board used to open on "all
// evaluation sets", which puts two different temporal windows in one table:
// a reader comparing rows down a column was comparing 226-to-227 numbers
// against 220-to-230 ones without being told.

import { baseUrl } from "@/lib/api";

export type Rung = {
  rung: string;
  window: string;
  /** Read off the axes that vary, so it cannot drift from the grid. */
  question: string;
  models: string[];
  ks: number[];
  arms: number;
  succeeded: number;
  running: number;
  failed: number;
  evaluated: number;
  evaluation_set_ids: string[];
  /** Null when either endpoint's publication date is unrecorded. */
  window_dates: { from: string; to: string } | null;
  scorers?: string[];
  best: {
    model: string;
    k: number;
    value: number;
    metric: string;
    cells: number;
    evaluation_result_id: string;
  } | null;
  started_at: string;
};

export type RungsResponse = { rungs: Rung[]; metric: string };

/**
 * The rung a reader arriving with no opinion should be looking at.
 *
 * The newest one, by rung number rather than by date: a rung reopened to
 * add an arm is still the rung it was, and sorting by timestamp would let
 * a late arm on rung 1 displace rung 2.
 */
export function currentRung(rungs: Rung[]): Rung | null {
  if (rungs.length === 0) return null;
  return rungs.reduce((a, b) => (Number(b.rung) > Number(a.rung) ? b : a));
}

/**
 * The evaluation set the board should open on.
 *
 * Null when the current rung has produced none yet, or has produced more
 * than one. Picking one of several would be a claim about which of them
 * is the rung, and the honest answer there is to leave the selector alone.
 */
export function defaultEvalSet(rungs: Rung[]): string | null {
  const rung = currentRung(rungs);
  if (!rung || rung.evaluation_set_ids.length !== 1) return null;
  return rung.evaluation_set_ids[0];
}

export type RungProgress = {
  /** Arms computed, out of the grid the rung declares. */
  computed: number;
  /** Arms with a score. Never above `computed`. */
  scored: number;
  total: number;
  /** True while any arm is still being computed. */
  live: boolean;
};

/**
 * How far along a rung is, in the two steps that can lag independently.
 *
 * Kept separate because they do: a rung can have every arm computed and
 * none of them scored, which is exactly the state that looks finished
 * from the queue and is not.
 */
export function rungProgress(rung: Rung): RungProgress {
  return {
    computed: rung.succeeded,
    scored: Math.min(rung.evaluated, rung.succeeded),
    total: rung.arms,
    live: rung.running > 0,
  };
}

/** One line saying where a rung stands, without a chart. */
export function progressLabel(p: RungProgress): string {
  if (p.scored === p.total) return `${p.total} arms, all scored`;
  if (p.computed === p.total)
    return `${p.total} arms computed, ${p.scored} scored`;
  return `${p.computed} of ${p.total} arms computed, ${p.scored} scored`;
}

export function getRungs(): Promise<RungsResponse> {
  // Not cacheable: this is the surface a reader watches while a rung fills
  // in, and a stale line saying "0 scored" beside a board already showing
  // the scores is worse than a slower one.
  // Through baseUrl() rather than a hardcoded public path. A server
  // component's fetch runs in Node, where a relative URL cannot be resolved
  // at all: it throws "Failed to parse URL from /api-proxy/rungs" before any
  // request is made. baseUrl() already handles exactly this, substituting an
  // internal absolute URL when there is no window, and its own comment
  // describes this failure. This call was bypassing it.
  //
  // The consequence was invisible because the one server-side caller wraps
  // this in a catch that drops the frame on failure. That catch was written
  // for an occasional network error. The failure is total and structural, so
  // a permanent absence read as an intermittent one, and the front page never
  // once showed the campaign's window.
  return fetch(`${baseUrl()}/rungs`, { cache: "no-store" }).then((r) => {
    if (!r.ok) throw new Error(`rungs: ${r.status}`);
    return r.json() as Promise<RungsResponse>;
  });
}


/**
 * The frame a reader is being shown, as dates rather than releases.
 *
 * The campaign's naming discipline is explicit: "a reader must be able to
 * follow the entire argument without meeting a single identifier". A window
 * is stored as release numbers and must never be printed as them.
 *
 * The front matter used to hardcode this and had drifted: it declared a
 * six-month frame ("Sep 2025 to Mar 2026") for a window that actually runs
 * from April 2024 to March 2026. Nothing compared the constant to the
 * database, so nothing noticed.
 */
export function frameLabel(rung: Rung | null): string | null {
  if (!rung?.window_dates) return null;
  const month = (iso: string) =>
    new Date(`${iso}T00:00:00Z`).toLocaleDateString("en", {
      month: "short",
      year: "numeric",
      timeZone: "UTC",
    });
  return `${month(rung.window_dates.from)} to ${month(rung.window_dates.to)}`;
}

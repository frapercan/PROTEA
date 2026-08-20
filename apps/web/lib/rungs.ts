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
  return fetch("/api-proxy/rungs", { cache: "no-store" }).then((r) => {
    if (!r.ok) throw new Error(`rungs: ${r.status}`);
    return r.json() as Promise<RungsResponse>;
  });
}

// The experiment graph, as the surface reads it.
//
// The ladder model this replaces described the campaign as an ordered
// sequence of steps, each a grid of jobs. That shape could not say what a
// step had established, only how many cells had finished, and it kept
// reporting a full grid after the evidence behind it was deleted.
//
// The graph model asks a different question of the record. A NODE is one
// decision over a field or a group of inseparable fields. The EDGE into a
// node carries a STRENGTH saying what the record can support about that
// decision, and the five values are exhaustive and mutually exclusive:
// a comparison that separated, a comparison that ran and did not, a value
// nobody ever chose, a comparison that could not have resolved anything,
// and a level that cannot be produced at all.
//
// Nothing here derives, filters or repairs the payload. Every number on
// the page is the number the endpoint returned, so a reader chasing a
// surprising cell lands in one query rather than in a client-side pipeline.

import { ApiError, baseUrl } from "@/lib/api";

/**
 * What the record supports about one decision.
 *
 * - `measured`   a declared comparison separated against its floor.
 * - `chosen`     the comparison ran with power, did not separate, and the
 *                value was picked and recorded.
 * - `inherited`  nobody ever decided; the value is the one it always was.
 * - `unpowered`  the comparison could not have resolved anything. Known
 *                from the design, before the comparison ran.
 * - `blocked`    a level cannot be produced because its artifact has no
 *                producer.
 */
export type EdgeStrength =
  | "measured"
  | "chosen"
  | "inherited"
  | "unpowered"
  | "blocked";

/** Declaration order, which is also pipeline order on the page. */
export const EDGE_STRENGTHS: EdgeStrength[] = [
  "measured",
  "chosen",
  "inherited",
  "unpowered",
  "blocked",
];

export function isEdgeStrength(v: unknown): v is EdgeStrength {
  return typeof v === "string" && (EDGE_STRENGTHS as string[]).includes(v);
}

/**
 * The frame: which window, scored against what, attributable or not.
 *
 * `sealed_rows` / `unsealed_rows` are the load-bearing pair. A result row
 * that does not name the frame it was produced under cannot be attributed
 * to one, and a number that cannot be attributed cannot be compared. The
 * page shows the unsealed count before it shows any score.
 */
/** When a window opened, when it closed, and how long it ran. */
export type WindowSpan = {
  from: string;
  to: string;
  days: number;
  months: number;
};

/** One dated release on the frame's axis, and its part in the frame. */
export type TimelineMark = {
  kind: "annotation_set" | "ontology_snapshot";
  label: string;
  date: string;
  role: "window_start" | "window_end" | "pivot" | "inside" | "before" | "beyond";
  in_window: boolean;
  is_pivot: boolean;
};

/** Every dated release the record holds, laid out against the window. */
export type GraphTimeline = {
  window: { from: string | null; to: string | null };
  marks: TimelineMark[];
};

export type GraphFrame = {
  /**
   * The dates behind the release numbers.
   *
   * 220 and 227 name two files and date nothing. How long a window ran bounds
   * how much annotation could accumulate in it, which is the first thing asked
   * of a temporal benchmark and the last thing a bare release number answers.
   */
  window_span: WindowSpan | null;
  declared: boolean;
  evaluation_set_id: string | null;
  /** Rendered verbatim; the endpoint formats it, e.g. `220->227`. */
  window: string | null;
  window_role: string | null;
  mode: string | null;
  pivot_snapshot: { id: string; version: string } | null;
  information_accretion_set: { id: string; regime: string; sha256: string } | null;
  query_set: { id: string; name: string; entries: number } | null;
  sealed_rows: number;
  unsealed_rows: number;
};

/** One decision over a field or a group of inseparable fields. */
/** One field of a node and the value it stands at. */
export type HeldValue = {
  field: string;
  value: string;
};

export type GraphNode = {
  key: string;
  title: string;
  /** Position in the pipeline, 1-based, ascending. */
  stage: number;
  question: string;
  strength: EdgeStrength;
  levels_instantiated: number;
  levels_available: number;
  varying_fields: string[];
  constant_fields: string[];
  /**
   * What the node currently stands at, field by field.
   *
   * A strength says how firmly a decision is held and nothing about what was
   * decided. Without the value, an inherited default nobody chose reads
   * exactly like a deliberate choice nobody measured.
   */
  held: HeldValue[];
  /** Why this node cannot answer. Null when it can. */
  blocked_reason: string | null;
  results: number;
};

/** One level's number on one panel. */
export type PanelResult = {
  level: string;
  f_micro_w: number | null;
  tau: number | null;
};

/**
 * One of the nine regions: a knowledge category by an aspect.
 *
 * Panels are never summed, averaged or otherwise collapsed. Cardinality is
 * a vector over the nine, not a scalar, and a single headline over all of
 * them is a claim the model does not license.
 */
export type GraphPanel = {
  category: string;
  aspect: string;
  units: number | null;
  /**
   * The smallest difference this panel could resolve, from its population
   * alone, at the cheapest contrast class measured. A gap smaller than this
   * is not a small result; it is no result, and the panel says so before
   * anything is run.
   */
  detectable_effect: number | null;
  results: PanelResult[];
};

/** A level that cannot be produced, and what would unblock it. */
export type GraphBlocked = {
  node: string;
  what: string;
  why: string;
  precondition: string;
};

export type GraphResponse = {
  frame: GraphFrame;
  nodes: GraphNode[];
  timeline: GraphTimeline | null;
  panels: GraphPanel[];
  blocked: GraphBlocked[];
};

/** Canonical row order for the panel grid. */
export const PANEL_CATEGORIES = ["NK", "LK", "PK"];
/** Canonical column order for the panel grid. */
export const PANEL_ASPECTS = ["BPO", "MFO", "CCO"];

/**
 * True when the endpoint answered and there is nothing behind the answer.
 *
 * Kept apart from the loading and error states on purpose. An instrument
 * that shows a skeleton forever cannot be told from one waiting on a slow
 * query, and both are wrong when the honest report is that no frame has
 * been declared and no level instantiated.
 */
export function isEmptyGraph(g: GraphResponse): boolean {
  if (g.frame.declared) return false;
  if (g.nodes.some((n) => n.levels_instantiated > 0 || n.results > 0)) return false;
  if (g.panels.some((p) => (p.units ?? 0) > 0 || p.results.length > 0)) return false;
  // A graph with nothing instantiated still has something to say when it names
  // what is blocked and what each blocked node is waiting for. Treating that as
  // empty hides the only actionable content an empty record carries.
  if (g.blocked.length > 0) return false;
  return true;
}

/** Every level name that appears on any panel, in a neutral order.
 *
 * Alphabetical rather than ranked. Ranking rows would need one number per
 * level across the nine panels, and there is no such number: collapsing
 * the panels to produce one is exactly what the model forbids. The reader
 * orders by a panel they pick, which is a claim about that panel only.
 */
export function panelLevels(panels: GraphPanel[]): string[] {
  const seen = new Set<string>();
  for (const p of panels) for (const r of p.results) seen.add(r.level);
  return [...seen].sort((a, b) => a.localeCompare(b));
}

/** Index the panels by `CATEGORY/ASPECT` so lookups do not rescan. */
export function panelKey(category: string, aspect: string): string {
  return `${category}/${aspect}`;
}

export function indexPanels(panels: GraphPanel[]): Map<string, GraphPanel> {
  return new Map(panels.map((p) => [panelKey(p.category, p.aspect), p]));
}

/** `level -> result` for one panel. */
export function indexPanelResults(panel: GraphPanel | undefined): Map<string, PanelResult> {
  if (!panel) return new Map();
  return new Map(panel.results.map((r) => [r.level, r]));
}

/**
 * The strongest level on one panel, and how far apart the levels are.
 *
 * Both are per-panel and stay per-panel. `spread` is max minus min inside
 * this panel, which is what a reader needs to judge whether the ordering
 * here is worth anything; it is never compared across panels on the page.
 */
export function panelSummary(
  panel: GraphPanel,
  metric: "f_micro_w" | "tau",
): { best: PanelResult; spread: number } | null {
  // Only results that actually carry the metric can be summarised. A stored
  // result missing it is not a zero, so it takes no part in the best or the
  // spread, and a panel where none carries it has no summary at all.
  const scored = panel.results.filter((r) => r[metric] != null);
  if (scored.length === 0) return null;
  let best = scored[0];
  let lo = scored[0][metric] as number;
  let hi = scored[0][metric] as number;
  for (const r of scored) {
    const v = r[metric] as number;
    if (v > (best[metric] as number)) best = r;
    if (v < lo) lo = v;
    if (v > hi) hi = v;
  }
  return { best, spread: hi - lo };
}

/**
 * Fetch the graph.
 *
 * `async` is load-bearing: `baseUrl()` throws synchronously when
 * NEXT_PUBLIC_API_URL is unset, and in a non-async function that throw
 * escapes before a promise exists, so a caller's `.catch()` never
 * attaches. Making it async puts a missing base URL, a bad status and a
 * wrong body shape in the one channel the caller already handles.
 */
export async function getGraph(): Promise<GraphResponse> {
  const path = "/graph";
  let res: Response;
  try {
    // Never cached. This is the surface an operator refreshes while a run
    // seals its rows, and a stale "8 unsealed" beside a sealed board is
    // worse than a slower answer.
    res = await fetch(`${baseUrl()}${path}`, { cache: "no-store" });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new ApiError("network", 0, path, msg);
  }
  if (!res.ok) {
    const body = await res.text();
    const msg = body.trimStart().startsWith("<") || body.trim() === ""
      ? `HTTP ${res.status} ${res.statusText}`
      : body;
    throw new ApiError(
      res.status === 401 ? "unauthorized" : res.status === 403 ? "forbidden" : "http",
      res.status,
      path,
      msg,
    );
  }
  const body: unknown = await res.json();
  // A 200 is not an answer. The e2e mock replies 200 with `[]` to every
  // route it does not know; casting that to the response type would put a
  // TypeError outside the promise chain, where the caller's catch cannot
  // see it. Reject here so a wrong shape arrives through the same channel
  // as a wrong status.
  if (!isGraphResponse(body)) {
    throw new ApiError("http", res.status, path, "graph: response does not have the graph shape");
  }
  return body;
}

function isGraphResponse(body: unknown): body is GraphResponse {
  if (typeof body !== "object" || body === null) return false;
  const b = body as Partial<GraphResponse>;
  if (typeof b.frame !== "object" || b.frame === null) return false;
  if (typeof b.frame.declared !== "boolean") return false;
  return Array.isArray(b.nodes) && Array.isArray(b.panels) && Array.isArray(b.blocked);
}

// The proteins one panel is made of, as the surface reads them.
//
// A panel prints a population and a pooled score, and neither can be opened.
// This is the descent: from a cell of the nine, to the arm that scored it, to
// the rows underneath with what each protein scored and what put it in its
// band.
//
// Two endpoints, and the split between them is the point. `/strata/compare`
// says WHO scored this cell and how well, one row per arm, and that is the arm
// picker. `/strata/{arm}/proteins` says WHICH proteins, and it is only ever
// fetched for the arm the reader chose, because the per-protein artefact is
// per arm and there are sixteen of them.
//
// Nothing here derives a measure. Every score rendered on the page is the score
// the endpoint returned; the only computation in this module is ordering,
// labelling and grouping, none of which produces a number a reader could quote.

import { ApiError, baseUrl } from "@/lib/api";
import type { StratumRow } from "@/lib/stratumCompare";

/** One protein in a panel: what it scored, and what put it where it is. */
export type StratumProtein = {
  accession: string;
  /** Null when the record holds no length for it, which is why it is unplaced. */
  residues: number | null;
  /** Null when the protein could not be placed on the retrieval axes. */
  length_band: string | null;
  homology_band: string | null;
  donor_evidence: string | null;
  /** Identity to the nearest non-self donor, per cent. Null when none came back. */
  best_identity: number | null;
  donor_is_experimental: boolean | null;
  taxonomic_relation: string | null;
  precision_w: number;
  recall_w: number;
  f_w: number;
  /** Weight of the ground truth this protein carried, which is what the
   *  weighted metrics are weighted BY. A protein with almost none of it moves
   *  the panel almost not at all, and a table without this column invites
   *  reading every row as equally load-bearing. */
  n_gt_w: number;
  pred_w: number;
  tp_w: number;
};

/** Which arm produced these scores, named by everything that can vary. */
export type StratumArm = {
  prediction_set_id: string;
  embedding_name: string | null;
  scoring_name: string | null;
  donor_policy: string | null;
  /** The depth this arm was read at, and WHICH of the three depths that is:
   *  'retrieval depth 30' fetched thirty neighbours, 'cut at sequence rank 30'
   *  truncated an already-retrieved list at thirty. The API has always sent a
   *  string here and this type said number, which is the same mistake the
   *  string used to make: a name that does not say what the value is. */
  depth: string | null;
  metric: string | null;
};

export type StratumProteinsResponse = {
  evaluation_result_id: string;
  arm: StratumArm;
  setting: string;
  where: {
    category: string;
    aspect: string;
    length: string | null;
    homology: string | null;
  };
  tau: number | null;
  /** Every protein the artefact scored in this panel. The panel's own count. */
  panel_population: number;
  /** How many of them could be put on the length and homology axes. */
  placed: number;
  unplaced: { no_donor: number; no_length: number; off_scale: number };
  /** How many survive the bands the caller pinned. */
  matched: number;
  /** How many of the matched the run scored nothing at all on. A count, never
   *  a rate: it says what the pooled cell cannot, which is whether the cell's
   *  number is a spread mass or a small mass beside a wall of zeros. */
  scored_zero: number;
  returned: number;
  offset: number;
  limit: number;
  sort: string;
  proteins: StratumProtein[];
};

/** How rows may be ordered, in the order the control offers them. */
export const SORTS = [
  "f_asc",
  "f_desc",
  "identity_asc",
  "identity_desc",
  "accession",
] as const;

export type Sort = (typeof SORTS)[number];

export function isSort(v: unknown): v is Sort {
  return typeof v === "string" && (SORTS as readonly string[]).includes(v);
}

/**
 * The fields an arm can be told apart by, in the order they read.
 *
 * The list has to be complete or the label lies. Eight arms of this campaign
 * share a prediction set and differ only in the scoring configuration and the
 * donor policy, so a picker naming an arm by its model alone offers eight rows
 * reading `esm2_650m` and a reader choosing between them is choosing blind.
 */
export const ARM_FIELDS = [
  "display_name",
  "k",
  "scoring_name",
  "donor_policy",
  "metric",
] as const;

export type ArmField = (typeof ARM_FIELDS)[number];

/** One arm, with the cells of this panel it holds. */
export type PanelArm = {
  evaluation_result_id: string;
  row: StratumRow;
  /** Every stratum cell this arm has at the pinned coordinates. */
  cells: StratumRow[];
};

/**
 * The arms that have this panel, each with the cells it holds there.
 *
 * Grouped rather than listed, because the compare endpoint returns one row per
 * (arm, cell) and a panel is crossed by length and homology: an arm appears
 * once for every band it was stratified into. Order is the endpoint's, which is
 * by model then depth then scoring name, so the picker does not reorder itself
 * when a score changes.
 */
export function groupArms(rows: StratumRow[]): PanelArm[] {
  const out: PanelArm[] = [];
  const index = new Map<string, PanelArm>();
  for (const row of rows) {
    const seen = index.get(row.evaluation_result_id);
    if (seen) {
      seen.cells.push(row);
      continue;
    }
    const arm: PanelArm = {
      evaluation_result_id: row.evaluation_result_id,
      row,
      cells: [row],
    };
    index.set(row.evaluation_result_id, arm);
    out.push(arm);
  }
  return out;
}

/** Which fields actually differ across these arms, read off the rows. */
export function varyingArmFields(arms: PanelArm[]): ArmField[] {
  const varying = ARM_FIELDS.filter(
    (f) => new Set(arms.map((a) => String(a.row[f] ?? ""))).size > 1,
  );
  // With one arm nothing varies, and a label of the empty string names
  // nothing. Fall back to the whole vocabulary rather than to silence.
  return varying.length > 0 ? [...varying] : [...ARM_FIELDS];
}

/** An arm as one line, named by whichever fields moved. */
export function armLabel(arm: PanelArm, fields: readonly ArmField[]): string {
  const parts = fields
    .map((f) => (f === "k" ? `k=${arm.row.k}` : arm.row[f]))
    .filter((v) => v != null && v !== "")
    .map(String);
  return parts.length > 0 ? parts.join(" / ") : arm.evaluation_result_id.slice(0, 8);
}

/**
 * The single cell an arm holds here, or null when the coordinates name more.
 *
 * Load-bearing. With length and homology unpinned, an arm holds every band of
 * the panel, and those cells must never be summarised into one number: the
 * populations differ by an order of magnitude, so any average over them is a
 * reweighting nobody chose. So the picker shows a score only when the
 * coordinates identify exactly one cell, and says how many it covers otherwise.
 */
export function soleCell(arm: PanelArm): StratumRow | null {
  return arm.cells.length === 1 ? arm.cells[0] : null;
}

/** Aspect as the strata artefact spells it, from the form the panels print. */
const ASPECT_WIRE: Record<string, string> = {
  BPO: "P",
  MFO: "F",
  CCO: "C",
};

/**
 * The single-char code the strata columns carry.
 *
 * The nine panels are labelled BPO / MFO / CCO and the strata artefact's aspect
 * column holds P / F / C. Passing the panel's spelling to the compare endpoint
 * pins an axis to a value no cell has, and the reply is an empty table rather
 * than an error, which reads as "this cell was never scored".
 */
export function aspectWire(aspect: string): string {
  return ASPECT_WIRE[aspect.toUpperCase()] ?? aspect;
}

const ASPECT_CAFA: Record<string, string> = Object.fromEntries(
  Object.entries(ASPECT_WIRE).map(([cafa, wire]) => [wire, cafa]),
);

/**
 * The three-letter form every user-facing surface prints.
 *
 * Built by inverting the map above rather than written out again. Two
 * hand-kept tables of the same closed set drift, and the drift here is silent:
 * an aspect that renders under one name and filters under another shows an
 * empty cell where the data is fine.
 */
export function aspectCafa(aspect: string): string {
  return ASPECT_CAFA[aspect.toUpperCase()] ?? aspect;
}

/** Where the descent lives. One place, so every surface links to one URL. */
export const STRATUM_PATH = "/instrument/stratum";

/**
 * A link into one cell, built from the coordinates a caller already holds.
 *
 * Exported rather than inlined at each call site because the cell view is
 * reachable from more than one surface: the nine panels of the experiment
 * graph, and any per-stratum table that knows its evaluation set. A second
 * hand-built query string is a second place for the aspect spelling to be
 * wrong, and that failure is silent: the wrong spelling pins an axis to a
 * value no cell has, so the page opens empty and reads as "never scored".
 */
export function stratumHref(at: {
  evaluationSetId?: string | null;
  category: string;
  aspect: string;
  length?: string | null;
  homology?: string | null;
  /** An evaluation result to open on, when the caller is already in one. */
  arm?: string | null;
}): string {
  const p = new URLSearchParams({
    category: at.category,
    aspect: aspectCafa(at.aspect),
  });
  if (at.evaluationSetId) p.set("set", at.evaluationSetId);
  if (at.length) p.set("length", at.length);
  if (at.homology) p.set("homology", at.homology);
  if (at.arm) p.set("arm", at.arm);
  return `${STRATUM_PATH}?${p.toString()}`;
}

export type ProteinQuery = {
  evaluationResultId: string;
  setting: string;
  aspect: string;
  length?: string | null;
  homology?: string | null;
  sort?: Sort;
  limit?: number;
  offset?: number;
};

function query(q: ProteinQuery): string {
  const p = new URLSearchParams({ setting: q.setting, aspect: q.aspect });
  if (q.length) p.set("length", q.length);
  if (q.homology) p.set("homology", q.homology);
  if (q.sort) p.set("sort", q.sort);
  if (q.limit != null) p.set("limit", String(q.limit));
  if (q.offset) p.set("offset", String(q.offset));
  return p.toString();
}

async function read(path: string): Promise<unknown> {
  let res: Response;
  try {
    // Never cached by the browser. The endpoint holds its own five-minute
    // cache, so a page turn costs a round trip and not a recomputation, and a
    // second layer here would only make a rescored arm invisible for longer.
    res = await fetch(`${baseUrl()}${path}`, { cache: "no-store" });
  } catch (e) {
    throw new ApiError("network", 0, path, e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) {
    const body = await res.text();
    const message =
      body.trimStart().startsWith("<") || body.trim() === ""
        ? `HTTP ${res.status} ${res.statusText}`
        : body;
    throw new ApiError(
      res.status === 401 ? "unauthorized" : res.status === 403 ? "forbidden" : "http",
      res.status,
      path,
      message,
    );
  }
  return res.json();
}

/** One page of a panel's proteins, for one arm. */
export async function getStratumProteins(
  q: ProteinQuery,
): Promise<StratumProteinsResponse> {
  const path = `/strata/${q.evaluationResultId}/proteins?${query(q)}`;
  const body = (await read(path)) as Partial<StratumProteinsResponse>;
  // A 200 is not an answer. The e2e mock replies 200 with `[]` to every route
  // it does not know, and casting that would throw a TypeError outside the
  // promise chain where the caller's catch cannot see it.
  if (!body || !Array.isArray(body.proteins) || typeof body.panel_population !== "number") {
    throw new ApiError("http", 200, path, "stratum proteins: response has the wrong shape");
  }
  return body as StratumProteinsResponse;
}

export type SettingStrata = {
  /** Which knowledge setting these rows are, echoed so a payload arriving
   *  after the reader moved on can be told from one that is current. */
  setting: string;
  rows: StratumRow[];
  arms_total: number;
  arms_with_strata: number;
};

/**
 * Every cell of every arm in one knowledge setting.
 *
 * Fetched whole rather than pinned per cell. The reader moves between panels
 * and between bands constantly, and each move would otherwise cost a request
 * for a table the endpoint already assembled; the axes are pinned here
 * instead. The setting is NOT one of them: the three are different
 * populations and a table mixing them is not comparable down a column, so
 * changing it refetches.
 */
export async function getSettingStrata(
  evaluationSetId: string,
  setting: string,
): Promise<SettingStrata> {
  const p = new URLSearchParams({ setting });
  const path = `/strata/compare/${evaluationSetId}?${p.toString()}`;
  const body = (await read(path)) as {
    setting?: string;
    rows?: StratumRow[];
    arms_total?: number;
    arms_with_strata?: number;
  };
  if (!body || !Array.isArray(body.rows)) {
    throw new ApiError("http", 200, path, "strata compare: response has the wrong shape");
  }
  return {
    setting: body.setting ?? setting,
    rows: body.rows,
    arms_total: body.arms_total ?? 0,
    arms_with_strata: body.arms_with_strata ?? 0,
  };
}

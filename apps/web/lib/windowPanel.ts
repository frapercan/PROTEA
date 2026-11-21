// What a window's panel reads and how it reads it, apart from how it is drawn.
//
// The data is the window summary (GET /annotations/evaluation-sets/{id}/summary):
// per result its arm, its nine cells and the strata profiles its stratification
// left on it. Everything here is a pure function of that summary and of the
// panel's state, so the views stay drawings and the rules are tested once.

import type { PairedComparison, PanelComparison } from "./pairedComparison";

export const SETTINGS = ["NK", "LK", "PK"] as const;
export const NAMESPACES = ["BPO", "MFO", "CCO"] as const;
export const CELLS = SETTINGS.flatMap((s) => NAMESPACES.map((n) => `${s}:${n}`));
/** The profiles are keyed by aspect letter, the cells by namespace. */
export const ASPECT_LETTER: Record<string, string> = { BPO: "P", MFO: "F", CCO: "C" };
export const HOMOLOGY_BANDS = [">90", "60-90", "30-60", "<=30", "none"] as const;
export const LENGTH_BANDS = ["<=512", "512-1024", "1024-2048", ">2048"] as const;
export const DEPTHS = [1, 2, 3, 5, 10, 20, 30] as const;
export const SCORINGS = [
  "embedding_only",
  "vote_fraction",
  "embedding_plus_vote",
  "evidence_veto",
] as const;
/** A band shows its number only from this many proteins (the strata floor). */
export const MIN_PROTEINS = 30;

export type PanelCell = {
  f: number | null;
  tau: number | null;
  edge: string | null;
  targets: number | null;
};
/** ``[proteins, tp, pred, n_gt, f_micro]`` under the stratification's variant. */
export type Pool = [number, number, number, number, number];
export type AspectProfiles = {
  placed?: Pool;
  homology?: Record<string, Pool>;
  length?: Record<string, Pool>;
};
export type PanelArm = {
  model: string | null;
  embedding_config_id: string | null;
  scoring: string | null;
  method: string | null;
  depth: number | null;
  exclude_self: boolean | null;
  reading: string;
};
export type SummaryResult = {
  id: string;
  created_at: string | null;
  prediction_set_id: string;
  job_id: string | null;
  arm: PanelArm;
  cells: Record<string, PanelCell>;
  strata: {
    job_id: string | null;
    variant: string | null;
    profiles: Record<string, Record<string, AspectProfiles>>;
  } | null;
};
export type WindowSummary = { evaluation_set_id: string; results: SummaryResult[] };

/** Message keys for the bands, whose own names carry characters a key should not. */
export const BAND_KEY: Record<string, string> = {
  ">90": "above90",
  "60-90": "from60to90",
  "30-60": "from30to60",
  "<=30": "upTo30",
  none: "none",
  "<=512": "upTo512",
  "512-1024": "from512to1024",
  "1024-2048": "from1024to2048",
  ">2048": "above2048",
};

export type Axis = "homology" | "length";
export const BANDS: Record<Axis, readonly string[]> = {
  homology: HOMOLOGY_BANDS,
  length: LENGTH_BANDS,
};

/** A model as people name it: "facebook/esm2_t33_650M_UR50D" is ESM-2 650M. */
export function modelName(model: string | null): string {
  if (!model) return "";
  const name = model.split("/").pop() ?? model;
  const esm2 = /^esm2_t\d+_(\d+[MB])_/i.exec(name);
  if (esm2) return `ESM-2 ${esm2[1].toUpperCase()}`;
  const esmc = /^esmc_(\d+[mb])$/i.exec(name);
  if (esmc) return `ESM C ${esmc[1].toUpperCase()}`;
  const ankh = /^ankh[-_](base|large)$/i.exec(name);
  if (ankh) return `Ankh ${ankh[1].toLowerCase()}`;
  if (/^prot_t5_xl/i.test(name)) return "ProtT5-XL";
  return name;
}

/**
 * A method as people name it. The short name is an identifier (code, variant,
 * cut: ``own-annotations-nonexp-lafa-211``); the panel shows what it is: "own
 * annotations", "count map: themes". The cut is the window's start and the
 * frame is the window's, so neither says anything a row of one window needs.
 */
export function methodName(method: string | null): string {
  if (!method) return "";
  const bare = method.replace(/-\d+$/, "").replace(/-lafa$/, "");
  if (bare.startsWith("own-annotations")) return "own annotations";
  const map = /^count-map-(themes|families)(?:-(permissive|strict))?-/.exec(bare);
  if (map) return `count map: ${map[1]}${map[2] ? ` (${map[2]})` : ""}`;
  return bare;
}

export const SCORING_SHORT: Record<string, string> = {
  embedding_only: "emb",
  vote_fraction: "vote",
  embedding_plus_vote: "emb+vote",
  evidence_veto: "veto",
};

export function isNeighbour(r: SummaryResult): boolean {
  return r.arm.method == null && r.arm.model != null;
}

/** The arm in full: "ESM-2 650M · embedding_only · k=1", or the method's name. */
export function armLabel(r: SummaryResult): string {
  if (!isNeighbour(r)) return methodName(r.arm.method) || r.prediction_set_id.slice(0, 8);
  const parts = [modelName(r.arm.model), r.arm.scoring ?? "candidates"];
  if (r.arm.depth != null) parts.push(`k=${r.arm.depth}`);
  return parts.join(" · ");
}

/** The arm in a few characters, for rows and cells: "650M emb k=1". */
export function armShort(r: SummaryResult): string {
  if (!isNeighbour(r)) return methodName(r.arm.method) || r.prediction_set_id.slice(0, 8);
  const model = modelName(r.arm.model).replace(/^ESM-2 /, "");
  const scoring = r.arm.scoring ? (SCORING_SHORT[r.arm.scoring] ?? r.arm.scoring) : "cand";
  return `${model} ${scoring}${r.arm.depth != null ? ` k=${r.arm.depth}` : ""}`;
}

/** Superseded by the protocol: an arm whose targets' own sequences could vote. */
export function isSuperseded(r: SummaryResult): boolean {
  return r.arm.exclude_self === false;
}

/** The own-annotations arm is the default reference (the panel's decision a). */
export function isOwnAnnotations(r: SummaryResult): boolean {
  return (r.arm.method ?? "").startsWith("own-annotations");
}

export function cellF(r: SummaryResult | undefined, cell: string): number | null {
  return r?.cells[cell]?.f ?? null;
}

export function cellEdge(r: SummaryResult | undefined, cell: string): string | null {
  return r?.cells[cell]?.edge ?? null;
}

/** One band of one cell: its proteins and its F, or null where nothing was pooled. */
export function bandOf(
  r: SummaryResult | undefined,
  cell: string,
  axis: Axis,
  band: string,
): { n: number; f: number } | null {
  const [setting, namespace] = cell.split(":");
  const pool = r?.strata?.profiles?.[setting]?.[ASPECT_LETTER[namespace]]?.[axis]?.[band];
  return pool ? { n: pool[0], f: pool[4] } : null;
}

export type PanelFilters = {
  reading: string;
  superseded: boolean;
  models: string[];
  scorings: string[];
  depths: number[];
};

/** The results the panel shows: one reading, filtered by factor; methods always. */
export function visibleResults(results: SummaryResult[], f: PanelFilters): SummaryResult[] {
  return results.filter((r) => {
    if (r.arm.reading !== f.reading) return false;
    if (!f.superseded && isSuperseded(r)) return false;
    if (!isNeighbour(r)) return true;
    if (f.models.length && !f.models.includes(modelName(r.arm.model))) return false;
    if (f.scorings.length && !f.scorings.includes(r.arm.scoring ?? "")) return false;
    if (f.depths.length && (r.arm.depth == null || !f.depths.includes(r.arm.depth))) return false;
    return true;
  });
}

/** The reference: the one asked for if it is shown, else own annotations, else the first. */
export function referenceOf(shown: SummaryResult[], asked: string | null): SummaryResult | undefined {
  return (
    shown.find((r) => r.id === asked) ?? shown.find(isOwnAnnotations) ?? shown[0]
  );
}

/** The models present, in the order the window first met them. */
export function modelsOf(results: SummaryResult[]): string[] {
  const seen: string[] = [];
  for (const r of results) {
    const name = isNeighbour(r) ? modelName(r.arm.model) : null;
    if (name && !seen.includes(name)) seen.push(name);
  }
  return seen;
}

/** Each time the best so far improved: who, from what, to what. */
export function championSteps<T>(
  points: { item: T; value: number }[],
): { item: T; before: number | null; now: number; at: number }[] {
  const steps: { item: T; before: number | null; now: number; at: number }[] = [];
  let best: number | null = null;
  points.forEach((p, at) => {
    if (best == null || p.value > best) {
      steps.push({ item: p.item, before: best, now: p.value, at });
      best = p.value;
    }
  });
  return steps;
}

/** A band's ranking, and the winner's margin over the best arm of the other family. */
export function bandRanking(
  shown: SummaryResult[],
  cell: string,
  axis: Axis,
  band: string,
): {
  ranked: { result: SummaryResult; n: number; f: number }[];
  rival: { result: SummaryResult; n: number; f: number } | null;
} {
  const ranked = shown
    .map((result) => ({ result, band: bandOf(result, cell, axis, band) }))
    .filter((x): x is { result: SummaryResult; band: { n: number; f: number } } =>
      x.band != null && x.band.n >= MIN_PROTEINS,
    )
    .map((x) => ({ result: x.result, n: x.band.n, f: x.band.f }))
    .sort((a, b) => b.f - a.f);
  const winner = ranked[0];
  const rival = winner
    ? (ranked.find((x) => isNeighbour(x.result) !== isNeighbour(winner.result)) ?? null)
    : null;
  return { ranked, rival };
}

/**
 * A trajectory step's paired comparison on one cell, read as the new best minus
 * the one before it. A comparison run the other way round is read turned over:
 * its difference and interval negated and its verdict swapped. Null when the
 * pair was never compared on that cell.
 */
export function stepComparison(
  comparisons: PairedComparison[],
  newer: string,
  before: string,
  cell: string,
): { panel: PanelComparison; comparison: PairedComparison } | null {
  const direct = comparisons.find(
    (c) => c.evaluation_result_id === newer && c.baseline_evaluation_result_id === before && c.panels[cell],
  );
  if (direct) return { panel: direct.panels[cell], comparison: direct };
  const reverse = comparisons.find(
    (c) => c.evaluation_result_id === before && c.baseline_evaluation_result_id === newer && c.panels[cell],
  );
  if (!reverse) return null;
  const p = reverse.panels[cell];
  const negated = (v: number | null) => (v == null ? null : -v);
  const verdict = p.verdict === "a_greater" ? "b_greater" : p.verdict === "b_greater" ? "a_greater" : p.verdict;
  return {
    panel: { ...p, delta: negated(p.delta), ci_low: negated(p.ci_high), ci_high: negated(p.ci_low), verdict },
    comparison: reverse,
  };
}

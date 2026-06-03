// Pure resolvers for the home-showcase best-result stage badge.
//
// The showcase renders `best.stage` as a coloured badge with a
// translated label. `best.stage` originates from the API and is not
// guaranteed to be one of the three known pipeline stages
// (baseline / alignment_weighted / reranker). When it is an unknown
// value, `STAGE_LABELS[best.stage]` is undefined and feeding that to
// next-intl's `t()` throws MISSING_MESSAGE, crashing the home page.
// These helpers guarantee a defined badge class and a renderable label
// for any input so the component never passes undefined to `t()`.

export const STAGE_LABELS: Record<string, string> = {
  baseline: "pipelineStageBaseline",
  alignment_weighted: "pipelineStageAlignmentWeighted",
  reranker: "pipelineStageReranker",
};

export const STAGE_BADGE: Record<string, string> = {
  baseline: "bg-slate-100 text-slate-700 ring-1 ring-inset ring-slate-200",
  alignment_weighted: "bg-amber-50 text-amber-800 ring-1 ring-inset ring-amber-200",
  reranker: "bg-blue-50 text-blue-800 ring-1 ring-inset ring-blue-200",
};

const FALLBACK_BADGE = "bg-slate-100 text-slate-700 ring-1 ring-inset ring-slate-200";

/** Badge colour classes for a stage, falling back to neutral slate. */
export function stageBadgeClass(stage: string): string {
  return STAGE_BADGE[stage] ?? FALLBACK_BADGE;
}

/**
 * The i18n message key for a known stage, or null for an unknown stage.
 * Callers pass the raw `stage` through verbatim when this returns null.
 */
export function stageLabelKey(stage: string): string | null {
  return STAGE_LABELS[stage] ?? null;
}

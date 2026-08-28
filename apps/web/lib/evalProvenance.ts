// Pure resolvers for the method-surface provenance badges
// (slice F-METHOD-EVAL-SURFACE).
//
// Every EvaluationResult can carry four optional provenance markers that
// make a benchmark number self-describing on /benchmark and /evaluation:
//
//   - frame:           "lafa" | "internal" | null  (scoring frame)
//   - temporal_window: free text, e.g. "SELECT_220_227" | "FINAL_227_230"
//   - arms_enabled:    flag dict { knn, reranker, mlp_tower, interpro }
//   - leakage_role:    "select" | "test" | "probe" | null  (ADR D40)
//
// All four are nullable: rows written before the columns existed read
// back as null/undefined and these resolvers return an explicit
// "unknown" empty state so the UI never blanks out or throws.

export type EvalFrame = "lafa" | "internal";
export type EvalLeakageRole = "select" | "test" | "probe";
export type EvalArms = Record<string, boolean>;

/** Minimal provenance shape shared by the matrix rows and the
 *  evaluation-result list. Every field is optional so legacy payloads
 *  (which omit them entirely) type-check unchanged. */
export type EvalProvenance = {
  frame?: EvalFrame | string | null;
  temporal_window?: string | null;
  arms_enabled?: EvalArms | null;
  leakage_role?: EvalLeakageRole | string | null;
  /** Which generation of the prediction set produced the row, from the
   *  self-hit audit. A cell can hold several generations of one measurement,
   *  and the API now hands us the winner rather than the highest scorer, so
   *  the reader should be able to see which one they are looking at. */
  prediction_set_status?: EvalGeneration | string | null;
  /** Share of queries that retrieved themselves. Near 0.99 on a healthy run;
   *  a low value is the signature of the search misattributing neighbours. */
  self_hit_rate?: number | null;
};

export type EvalGeneration =
  | "current"
  | "superseded"
  | "damaged"
  | "incomplete";

export type Badge = {
  label: string;
  className: string;
  title: string;
};

const UNKNOWN_BADGE_CLASS =
  "bg-slate-50 text-slate-400 ring-1 ring-inset ring-slate-200";

// ── Frame ────────────────────────────────────────────────────────────────

const FRAME_BADGE: Record<EvalFrame, Badge> = {
  lafa: {
    label: "LAFA-frame",
    className: "bg-violet-50 text-violet-800 ring-1 ring-inset ring-violet-200",
    title:
      "Scored in the parity-locked LAFA frame: the number is comparable to the LAFA leaderboard. For CAFA, quote fmax_w from the same row instead.",
  },
  internal: {
    label: "internal",
    className: "bg-slate-100 text-slate-700 ring-1 ring-inset ring-slate-300",
    title:
      "Scored in the internal lab / full-GT frame: not directly comparable to the LAFA leaderboard.",
  },
};

const FRAME_UNKNOWN: Badge = {
  label: "frame ?",
  className: UNKNOWN_BADGE_CLASS,
  title: "Scoring frame not recorded for this result (predates the provenance fields).",
};

/** Badge for the scoring frame, with an explicit unknown state. */
export function frameBadge(frame: string | null | undefined): Badge {
  if (frame === "lafa" || frame === "internal") return FRAME_BADGE[frame];
  return FRAME_UNKNOWN;
}

// ── Temporal window ────────────────────────────────────────────────────────

export type WindowKind = "select" | "final" | "other" | "unknown";

export type WindowLabel = {
  kind: WindowKind;
  label: string;
  className: string;
  title: string;
};

/** Resolve a free-text temporal-window token into a kind + display label.
 *  Tokens conventionally read ``SELECT_<t-1>_<t0>`` (selection window) or
 *  ``FINAL_<t0>_<t1>`` (report-once test window); anything else is "other",
 *  and a missing token is "unknown". The raw token is preserved as the
 *  label so the exact band stays visible. */
export function windowLabel(token: string | null | undefined): WindowLabel {
  if (token == null || token.trim() === "") {
    return {
      kind: "unknown",
      label: "window ?",
      className: UNKNOWN_BADGE_CLASS,
      title: "Temporal window not recorded for this result.",
    };
  }
  const upper = token.toUpperCase();
  if (upper.startsWith("SELECT")) {
    return {
      kind: "select",
      label: token,
      className: "bg-amber-50 text-amber-800 ring-1 ring-inset ring-amber-200",
      title: "Selection / threshold-tuning window. Champions may be picked here.",
    };
  }
  if (upper.startsWith("FINAL") || upper.startsWith("TEST")) {
    return {
      kind: "final",
      label: token,
      className: "bg-emerald-50 text-emerald-800 ring-1 ring-inset ring-emerald-200",
      title: "Report-once test window. Held out; scored only after selection is frozen.",
    };
  }
  return {
    kind: "other",
    label: token,
    className: "bg-slate-100 text-slate-700 ring-1 ring-inset ring-slate-300",
    title: "Ad-hoc evaluation window (not bound to the rolling-origin protocol).",
  };
}

// ── Method arms ────────────────────────────────────────────────────────────

/** Canonical arm order + display label. Driven by a constant (not the
 *  payload keys) so the badge row is stable and new arms slot in here. */
export const ARM_ORDER: { key: string; label: string }[] = [
  { key: "knn", label: "KNN" },
  { key: "reranker", label: "Reranker" },
  { key: "mlp_tower", label: "MLP" },
  { key: "interpro", label: "InterPro" },
];

/** `enabled` is null when the record does not carry this arm at all, which is
 *  not the same as false. The producer used to write mlp_tower and interpro as
 *  literal false because nothing inspected them, so "not looked at" and "did not
 *  contribute" arrived at the reader as the same badge. `Boolean(undefined)`
 *  carried that collapse all the way to the screen. */
export type ArmBadge = { key: string; label: string; enabled: boolean | null };

/** One badge per canonical arm, or ``null`` when the composition was never
 *  recorded at all (so the caller can show an empty state rather than a row of
 *  all-off badges). An arm the record does not mention gets `enabled: null`. */
export function armsList(arms: EvalArms | null | undefined): ArmBadge[] | null {
  if (arms == null || typeof arms !== "object") return null;
  return ARM_ORDER.map((a) => ({
    key: a.key,
    label: a.label,
    enabled: a.key in arms ? Boolean(arms[a.key]) : null,
  }));
}

// ── Leakage role ───────────────────────────────────────────────────────────

const LEAKAGE_BADGE: Record<EvalLeakageRole, Badge> = {
  select: {
    label: "select",
    className: "bg-amber-50 text-amber-800 ring-1 ring-inset ring-amber-200",
    title: "Used for model / threshold selection. Must sit on the SELECT window.",
  },
  test: {
    label: "test",
    className: "bg-emerald-50 text-emerald-800 ring-1 ring-inset ring-emerald-200",
    title: "Report-once measurement on the held-out window. The publishable number.",
  },
  probe: {
    label: "probe",
    className: "bg-sky-50 text-sky-800 ring-1 ring-inset ring-sky-200",
    title: "Exploratory read. Must not feed selection (winner's-curse guard, ADR D40).",
  },
};

const LEAKAGE_UNKNOWN: Badge = {
  label: "role ?",
  className: UNKNOWN_BADGE_CLASS,
  title: "Leakage-hygiene role not recorded for this result.",
};

/** Badge for the ADR D40 leakage-hygiene role, with an unknown state. */
export function leakageBadge(role: string | null | undefined): Badge {
  if (role === "select" || role === "test" || role === "probe") {
    return LEAKAGE_BADGE[role];
  }
  return LEAKAGE_UNKNOWN;
}

// ── Generation ───────────────────────────────────────────────────────────

const GENERATION_BADGE: Record<EvalGeneration, Badge> = {
  current: {
    label: "current",
    className: "bg-emerald-50 text-emerald-800 ring-1 ring-inset ring-emerald-200",
    title:
      "From the live generation of this cell. The self-hit audit found no sign of the search misattributing neighbours.",
  },
  superseded: {
    label: "superseded",
    className: "bg-slate-100 text-slate-700 ring-1 ring-inset ring-slate-300",
    title:
      "A clean earlier generation, kept as evidence and displaced by a later recompute of the same cell.",
  },
  damaged: {
    label: "damaged",
    className: "bg-rose-50 text-rose-800 ring-1 ring-inset ring-rose-200",
    title:
      "Fewer than 95 per cent of queries retrieved themselves, the signature of the out-of-memory defect that assigned each query a later query's neighbours. Do not read this number.",
  },
  incomplete: {
    label: "incomplete",
    className: "bg-amber-50 text-amber-800 ring-1 ring-inset ring-amber-200",
    title:
      "The run never covered the whole evaluation population, so the score is over a subset nobody chose.",
  },
};

const GENERATION_UNKNOWN: Badge = {
  label: "ungraded",
  className: UNKNOWN_BADGE_CLASS,
  title:
    "This prediction set predates the self-hit audit, so nothing is known for or against it.",
};

/** Badge for the generation a row came from, plus its self-hit rate when known.
 *
 *  Returns null only when the field is absent entirely, so a graded row always
 *  shows its verdict and an ungraded one says so rather than looking clean. */
export function generationBadge(
  status: EvalGeneration | string | null | undefined,
  selfHitRate?: number | null,
): Badge | null {
  if (status === undefined) return null;
  const base =
    status && status in GENERATION_BADGE
      ? GENERATION_BADGE[status as EvalGeneration]
      : GENERATION_UNKNOWN;
  if (selfHitRate == null) return base;
  const pct = (selfHitRate * 100).toFixed(1);
  return { ...base, title: `${base.title} Self-hit rate ${pct} per cent.` };
}

/** True when at least one provenance field is populated. Lets callers skip
 *  the badge strip entirely (vs. rendering four "unknown" chips) on rows
 *  that predate the fields. */
export function hasAnyProvenance(p: EvalProvenance | null | undefined): boolean {
  if (!p) return false;
  return (
    (p.frame ?? null) !== null ||
    (p.temporal_window ?? null) !== null ||
    (p.leakage_role ?? null) !== null ||
    (p.arms_enabled != null && typeof p.arms_enabled === "object") ||
    (p.prediction_set_status ?? null) !== null
  );
}
